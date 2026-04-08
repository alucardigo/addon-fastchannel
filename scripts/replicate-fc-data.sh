#!/bin/bash
# =============================================================================
# Replicacao de dados FastChannel: PROD -> HOMOLOG/DEV
# Executa via SSH no servidor de homologacao (que tem acesso a ambos BDs)
# =============================================================================
# Uso: ssh -l sankhya 100.72.97.11 "bash /tmp/replicate-fc-data.sh [homolog|dev|both]"
# Agendamento: cron diario as 03:00 AM
# =============================================================================

set -e

SQLCMD="/opt/mssql-tools/bin/sqlcmd"
PROD_SERVER="bellube-sql02.cldns.top"
PROD_USER="sankhya"
PROD_PASS="azsxdc"
PROD_DB=""  # Default database (sankhya)

HOMOLOG_SERVER="172.16.127.12"
HOMOLOG_USER="SANKHYA"
HOMOLOG_PASS="tecsis"
HOMOLOG_DB="SANKHYA_TESTE"

# DEV = local Docker MSSQL (ajustar conforme necessario)
DEV_SERVER="172.16.127.12"
DEV_USER="SANKHYA"
DEV_PASS="tecsis"
DEV_DB="SANKHYA_DEV"

TARGET="${1:-homolog}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOGFILE="/tmp/fc-replicate-${TIMESTAMP}.log"
TMPDIR="/tmp/fc-replicate-${TIMESTAMP}"
mkdir -p "$TMPDIR"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOGFILE"
}

run_prod() {
    $SQLCMD -S "$PROD_SERVER" -U "$PROD_USER" -P "$PROD_PASS" -Q "$1" -s '|' -W -h -1 -t 30 2>>"$LOGFILE"
}

run_target() {
    local server="$1" user="$2" pass="$3" db="$4" sql="$5"
    $SQLCMD -S "$server" -U "$user" -P "$pass" -d "$db" -Q "$sql" -t 60 2>>"$LOGFILE"
}

run_target_file() {
    local server="$1" user="$2" pass="$3" db="$4" file="$5"
    $SQLCMD -S "$server" -U "$user" -P "$pass" -d "$db" -i "$file" -t 120 2>>"$LOGFILE"
}

replicate_to() {
    local ENV_NAME="$1" SERVER="$2" USER="$3" PASS="$4" DB="$5"
    log "=== Replicando para $ENV_NAME ($SERVER/$DB) ==="

    # 1. TGFMAR - Atualizar campos AD_FAST e AD_FASTREF
    log "1/6 Replicando TGFMAR (AD_FAST, AD_FASTREF)..."
    run_prod "SELECT CODIGO, AD_FAST, AD_FASTREF FROM TGFMAR WHERE AD_FAST IS NOT NULL" > "$TMPDIR/tgfmar.csv"
    local count=0
    while IFS='|' read CODIGO AD_FAST AD_FASTREF; do
        CODIGO=$(echo $CODIGO | tr -d ' ')
        AD_FAST=$(echo $AD_FAST | tr -d ' ')
        AD_FASTREF=$(echo $AD_FASTREF | tr -d ' ')
        if [[ $CODIGO =~ ^[0-9]+$ ]]; then
            echo "UPDATE TGFMAR SET AD_FAST='$AD_FAST', AD_FASTREF='$AD_FASTREF' WHERE CODIGO=$CODIGO;" >> "$TMPDIR/tgfmar.sql"
            count=$((count+1))
        fi
    done < "$TMPDIR/tgfmar.csv"
    run_target_file "$SERVER" "$USER" "$PASS" "$DB" "$TMPDIR/tgfmar.sql"
    log "  TGFMAR: $count registros atualizados"

    # 2. TGFTAB - Atualizar AD_TIPO_FAST
    log "2/6 Replicando TGFTAB (AD_TIPO_FAST)..."
    run_prod "SELECT NUTAB, AD_TIPO_FAST FROM TGFTAB WHERE AD_TIPO_FAST IN ('D','C')" > "$TMPDIR/tgftab.csv"
    count=0
    while IFS='|' read NUTAB AD_TIPO_FAST; do
        NUTAB=$(echo $NUTAB | tr -d ' ')
        AD_TIPO_FAST=$(echo $AD_TIPO_FAST | tr -d ' ')
        if [[ $NUTAB =~ ^[0-9]+$ ]]; then
            echo "UPDATE TGFTAB SET AD_TIPO_FAST='$AD_TIPO_FAST' WHERE NUTAB=$NUTAB;" >> "$TMPDIR/tgftab.sql"
            count=$((count+1))
        fi
    done < "$TMPDIR/tgftab.csv"
    if [ -f "$TMPDIR/tgftab.sql" ]; then
        run_target_file "$SERVER" "$USER" "$PASS" "$DB" "$TMPDIR/tgftab.sql"
    fi
    log "  TGFTAB: $count registros atualizados"

    # 3. TGFEXC - Precos das tabelas FC (DELETE + INSERT)
    log "3/6 Replicando TGFEXC (precos FC)..."
    # Primeiro deletar existentes
    run_target "$SERVER" "$USER" "$PASS" "$DB" \
        "DELETE FROM TGFEXC WHERE NUTAB IN (SELECT NUTAB FROM TGFTAB WHERE AD_TIPO_FAST IN ('D','C'))"
    # Exportar de PROD
    run_prod "SELECT NUTAB, CODPROD, CODLOCAL, CONTROLE, VLRVENDA, TIPO, MODBASEICMS FROM TGFEXC WHERE NUTAB IN (SELECT NUTAB FROM TGFTAB WHERE AD_TIPO_FAST IN ('D','C'))" > "$TMPDIR/tgfexc.csv"
    count=0
    > "$TMPDIR/tgfexc.sql"
    while IFS='|' read NUTAB CODPROD CODLOCAL CONTROLE VLRVENDA TIPO MODBASEICMS; do
        NUTAB=$(echo $NUTAB | tr -d ' ')
        CODPROD=$(echo $CODPROD | tr -d ' ')
        CODLOCAL=$(echo $CODLOCAL | tr -d ' ')
        VLRVENDA=$(echo $VLRVENDA | tr -d ' ')
        TIPO=$(echo $TIPO | tr -d ' ')
        MODBASEICMS=$(echo $MODBASEICMS | tr -d ' ')
        if [[ $NUTAB =~ ^[0-9]+$ ]] && [[ $CODPROD =~ ^[0-9]+$ ]]; then
            echo "INSERT INTO TGFEXC (NUTAB,CODPROD,CODLOCAL,CONTROLE,VLRVENDA,TIPO,MODBASEICMS) VALUES ($NUTAB,$CODPROD,$CODLOCAL,' ',$VLRVENDA,'$TIPO','$MODBASEICMS');" >> "$TMPDIR/tgfexc.sql"
            count=$((count+1))
        fi
    done < "$TMPDIR/tgfexc.csv"
    if [ -f "$TMPDIR/tgfexc.sql" ] && [ $count -gt 0 ]; then
        run_target_file "$SERVER" "$USER" "$PASS" "$DB" "$TMPDIR/tgfexc.sql"
    fi
    log "  TGFEXC: $count registros inseridos"

    # 4. TGFLOC - Garantir local 99000000 existe
    log "4/6 Verificando TGFLOC 99000000..."
    local exists=$(run_target "$SERVER" "$USER" "$PASS" "$DB" "SELECT COUNT(*) FROM TGFLOC WHERE CODLOCAL=99000000" | tr -d ' ' | grep -E '^[0-9]+$' | head -1)
    if [ "$exists" = "0" ] || [ -z "$exists" ]; then
        run_target "$SERVER" "$USER" "$PASS" "$DB" \
            "INSERT INTO TGFLOC (CODLOCAL,DESCRLOCAL,TIPO,CODPARC) VALUES (99000000,'LOCAL PADRAO','G',0)"
        log "  TGFLOC: local 99000000 criado"
    else
        log "  TGFLOC: local 99000000 ja existe"
    fi

    # 5. TGFEST - Estoque no local FC (DELETE + INSERT do CODLOCAL=99000000)
    log "5/6 Replicando TGFEST (estoque FC CODLOCAL=99000000)..."
    run_target "$SERVER" "$USER" "$PASS" "$DB" "DELETE FROM TGFEST WHERE CODLOCAL=99000000"
    run_prod "SELECT CODEMP, CODPROD, CODLOCAL, CODPARC, ESTOQUE, RESERVA, TIPO FROM TGFEST WHERE CODLOCAL=99000000" > "$TMPDIR/tgfest.csv"
    count=0
    > "$TMPDIR/tgfest.sql"
    while IFS='|' read CODEMP CODPROD CODLOCAL CODPARC ESTOQUE RESERVA TIPO; do
        CODEMP=$(echo $CODEMP | tr -d ' ')
        CODPROD=$(echo $CODPROD | tr -d ' ')
        CODLOCAL=$(echo $CODLOCAL | tr -d ' ')
        CODPARC=$(echo $CODPARC | tr -d ' ')
        ESTOQUE=$(echo $ESTOQUE | tr -d ' ')
        RESERVA=$(echo $RESERVA | tr -d ' ')
        TIPO=$(echo $TIPO | tr -d ' ')
        if [[ $CODEMP =~ ^[0-9]+$ ]] && [[ $CODPROD =~ ^[0-9]+$ ]]; then
            echo "INSERT INTO TGFEST (CODEMP,CODPROD,CODLOCAL,CODPARC,ESTOQUE,RESERVA,TIPO) VALUES ($CODEMP,$CODPROD,$CODLOCAL,$CODPARC,$ESTOQUE,$RESERVA,'$TIPO');" >> "$TMPDIR/tgfest.sql"
            count=$((count+1))
        fi
    done < "$TMPDIR/tgfest.csv"
    if [ -f "$TMPDIR/tgfest.sql" ] && [ $count -gt 0 ]; then
        # Split em chunks de 1000 para nao sobrecarregar
        split -l 1000 "$TMPDIR/tgfest.sql" "$TMPDIR/tgfest_chunk_"
        for chunk in "$TMPDIR"/tgfest_chunk_*; do
            run_target_file "$SERVER" "$USER" "$PASS" "$DB" "$chunk"
        done
    fi
    log "  TGFEST: $count registros inseridos"

    # 6. AD_FCDEPARA/AD_FCCONFIG - Nao replicar (config especifica de cada ambiente)
    log "6/6 Pulando AD_FCDEPARA/AD_FCCONFIG (config por ambiente)"

    log "=== Replicacao para $ENV_NAME concluida! ==="
}

# =============================================================================
# MAIN
# =============================================================================
log "Inicio da replicacao FastChannel PROD -> $TARGET"

case "$TARGET" in
    homolog)
        replicate_to "HOMOLOG" "$HOMOLOG_SERVER" "$HOMOLOG_USER" "$HOMOLOG_PASS" "$HOMOLOG_DB"
        ;;
    dev)
        replicate_to "DEV" "$DEV_SERVER" "$DEV_USER" "$DEV_PASS" "$DEV_DB"
        ;;
    both)
        replicate_to "HOMOLOG" "$HOMOLOG_SERVER" "$HOMOLOG_USER" "$HOMOLOG_PASS" "$HOMOLOG_DB"
        replicate_to "DEV" "$DEV_SERVER" "$DEV_USER" "$DEV_PASS" "$DEV_DB"
        ;;
    *)
        log "Uso: $0 [homolog|dev|both]"
        exit 1
        ;;
esac

# Cleanup
rm -rf "$TMPDIR"
log "Fim da replicacao. Log em: $LOGFILE"
