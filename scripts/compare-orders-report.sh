#!/bin/bash
# =============================================================================
# Comparativo de Pedidos: PROD (legado) vs HOMOLOG (addon)
# Gera relatorio HTML e envia por email diariamente
# =============================================================================
# Uso: ssh -l sankhya 100.72.97.11 "bash /tmp/compare-orders-report.sh"
# =============================================================================

set -e

SQLCMD="/opt/mssql-tools/bin/sqlcmd"
PROD_SERVER="bellube-sql02.cldns.top"
PROD_USER="sankhya"
PROD_PASS="azsxdc"

HOMOLOG_SERVER="172.16.127.12"
HOMOLOG_USER="SANKHYA"
HOMOLOG_PASS="tecsis"
HOMOLOG_DB="SANKHYA_TESTE"

ADDON_URL="http://172.16.127.11:8080/addon-fastchannel/fc-direct"
EMAILS="suporteti@bellube.com.br,danielmendes@bellube.com.br"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
REPORT="/tmp/fc-compare-${TIMESTAMP}.html"
TODAY=$(date +%Y-%m-%d)
YESTERDAY=$(date -d "yesterday" +%Y-%m-%d 2>/dev/null || date -v-1d +%Y-%m-%d 2>/dev/null)

query_prod() {
    $SQLCMD -S "$PROD_SERVER" -U "$PROD_USER" -P "$PROD_PASS" -Q "$1" -s '|' -W -h -1 -t 30 2>/dev/null
}

query_homolog() {
    $SQLCMD -S "$HOMOLOG_SERVER" -U "$HOMOLOG_USER" -P "$HOMOLOG_PASS" -d "$HOMOLOG_DB" -Q "$1" -s '|' -W -h -1 -t 30 2>/dev/null
}

# =============================================================================
# Coletar dados
# =============================================================================

# 1. Pedidos FC importados em PROD (via legado) - ultimas 24h
PROD_ORDERS=$(query_prod "
SELECT C.NUNOTA, C.NUMNOTA, C.DTNEG, C.CODPARC, P.RAZAOSOCIAL, C.VLRNOTA, C.STATUSNOTA, C.AD_NUMFAST
FROM TGFCAB C
INNER JOIN TGFPAR P ON C.CODPARC = P.CODPARC
WHERE C.AD_NUMFAST IS NOT NULL AND C.AD_NUMFAST != ''
AND C.DTNEG >= '$YESTERDAY'
ORDER BY C.DTNEG DESC
")

PROD_COUNT=$(echo "$PROD_ORDERS" | grep -c '|' 2>/dev/null || echo "0")

# 2. Pedidos FC importados em HOMOLOG (via addon)
HOMOLOG_ORDERS=$(query_homolog "
SELECT P.FC_ORDER_ID, P.STATUS_IMPORT, P.NUNOTA, P.DH_IMPORT, P.NOME_CLIENTE, P.VALOR_TOTAL, P.LAST_ERROR
FROM AD_FCPEDIDO P
WHERE P.DH_IMPORT >= '$YESTERDAY'
ORDER BY P.DH_IMPORT DESC
")

HOMOLOG_COUNT=$(echo "$HOMOLOG_ORDERS" | grep -c '|' 2>/dev/null || echo "0")

# 3. Pedidos FC em ambos (cruzamento por AD_NUMFAST = FC_ORDER_ID)
CROSS_MATCH=$(query_prod "
SELECT C.AD_NUMFAST
FROM TGFCAB C
WHERE C.AD_NUMFAST IS NOT NULL AND C.AD_NUMFAST != ''
AND C.DTNEG >= '$YESTERDAY'
")

# 4. Totais gerais
PROD_TOTAL=$(query_prod "SELECT COUNT(*) FROM TGFCAB WHERE AD_NUMFAST IS NOT NULL AND AD_NUMFAST != ''" | tr -d ' ')
HOMOLOG_TOTAL=$(query_homolog "SELECT COUNT(*) FROM AD_FCPEDIDO" | tr -d ' ')

PROD_ERRORS=$(query_prod "SELECT COUNT(*) FROM TGFCAB WHERE AD_NUMFAST IS NOT NULL AND STATUSNOTA = 'L'" | tr -d ' ')
HOMOLOG_ERRORS=$(query_homolog "SELECT COUNT(*) FROM AD_FCPEDIDO WHERE STATUS_IMPORT = 'ERRO'" | tr -d ' ')
HOMOLOG_SUCCESS=$(query_homolog "SELECT COUNT(*) FROM AD_FCPEDIDO WHERE STATUS_IMPORT = 'SUCESSO'" | tr -d ' ')

# 5. Estoque e Precos status (addon)
ADDON_STOCK=$(curl -s --max-time 10 "${ADDON_URL}?serviceName=FCEstoqueSP.list&pageSize=1" 2>/dev/null | grep -o '"total":[0-9]*' | head -1 | cut -d: -f2)
ADDON_PRICE=$(curl -s --max-time 10 "${ADDON_URL}?serviceName=FCPrecosSP.list&pageSize=1" 2>/dev/null | grep -o '"total":[0-9]*' | head -1 | cut -d: -f2)

# =============================================================================
# Gerar HTML
# =============================================================================
cat > "$REPORT" << 'HTMLEOF'
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
.container { max-width: 900px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
h1 { color: #333; border-bottom: 3px solid #2196F3; padding-bottom: 10px; }
h2 { color: #555; margin-top: 25px; }
table { width: 100%; border-collapse: collapse; margin: 15px 0; }
th { background: #2196F3; color: white; padding: 10px; text-align: left; }
td { padding: 8px 10px; border-bottom: 1px solid #ddd; }
tr:hover { background: #f0f0f0; }
.metric { display: inline-block; min-width: 180px; margin: 10px; padding: 15px; border-radius: 5px; text-align: center; }
.metric-value { font-size: 28px; font-weight: bold; }
.metric-label { font-size: 12px; color: #666; margin-top: 5px; }
.green { background: #e8f5e9; color: #2e7d32; }
.red { background: #ffebee; color: #c62828; }
.blue { background: #e3f2fd; color: #1565c0; }
.yellow { background: #fff8e1; color: #f57f17; }
.ok { color: #2e7d32; font-weight: bold; }
.warn { color: #f57f17; font-weight: bold; }
.fail { color: #c62828; font-weight: bold; }
.footer { margin-top: 30px; padding-top: 15px; border-top: 1px solid #ddd; color: #999; font-size: 11px; }
</style>
</head>
<body>
<div class="container">
HTMLEOF

cat >> "$REPORT" << EOF
<h1>FastChannel - Comparativo Legado vs Addon</h1>
<p>Relatorio gerado em: <b>$(date '+%d/%m/%Y %H:%M')</b> | Periodo: <b>${YESTERDAY} a ${TODAY}</b></p>

<h2>Metricas Gerais</h2>
<div>
  <div class="metric blue">
    <div class="metric-value">${PROD_TOTAL:-0}</div>
    <div class="metric-label">Pedidos PROD (total)</div>
  </div>
  <div class="metric blue">
    <div class="metric-value">${HOMOLOG_TOTAL:-0}</div>
    <div class="metric-label">Pedidos HOMOLOG (total)</div>
  </div>
  <div class="metric green">
    <div class="metric-value">${HOMOLOG_SUCCESS:-0}</div>
    <div class="metric-label">HOMOLOG Sucesso</div>
  </div>
  <div class="metric red">
    <div class="metric-value">${HOMOLOG_ERRORS:-0}</div>
    <div class="metric-label">HOMOLOG Erros</div>
  </div>
</div>

<h2>Ultimas 24h</h2>
<div>
  <div class="metric green">
    <div class="metric-value">${PROD_COUNT}</div>
    <div class="metric-label">Pedidos PROD (24h)</div>
  </div>
  <div class="metric green">
    <div class="metric-value">${HOMOLOG_COUNT}</div>
    <div class="metric-label">Pedidos HOMOLOG (24h)</div>
  </div>
</div>

<h2>Status Estoque/Precos (Addon)</h2>
<table>
<tr><th>Indicador</th><th>Valor</th><th>Status</th></tr>
<tr><td>Produtos com estoque</td><td>${ADDON_STOCK:-N/A}</td><td>$([ "${ADDON_STOCK:-0}" -gt 1000 ] 2>/dev/null && echo '<span class="ok">OK</span>' || echo '<span class="warn">VERIFICAR</span>')</td></tr>
<tr><td>Produtos com preco</td><td>${ADDON_PRICE:-N/A}</td><td>$([ "${ADDON_PRICE:-0}" -gt 100 ] 2>/dev/null && echo '<span class="ok">OK</span>' || echo '<span class="warn">VERIFICAR</span>')</td></tr>
</table>

<h2>Pedidos PROD (Legado) - Ultimas 24h</h2>
<table>
<tr><th>NUNOTA</th><th>NUMNOTA</th><th>Data</th><th>Cliente</th><th>Valor</th><th>Status</th><th>FC Order</th></tr>
EOF

if [ -n "$PROD_ORDERS" ]; then
    echo "$PROD_ORDERS" | grep '|' | while IFS='|' read NUNOTA NUMNOTA DTNEG CODPARC RAZAO VALOR STATUS FCORDER; do
        echo "<tr><td>$NUNOTA</td><td>$NUMNOTA</td><td>$DTNEG</td><td>$RAZAO</td><td>R\$ $VALOR</td><td>$STATUS</td><td>$FCORDER</td></tr>" >> "$REPORT"
    done
else
    echo "<tr><td colspan='7'>Nenhum pedido FC nas ultimas 24h em PROD</td></tr>" >> "$REPORT"
fi

cat >> "$REPORT" << EOF
</table>

<h2>Pedidos HOMOLOG (Addon) - Ultimas 24h</h2>
<table>
<tr><th>FC Order</th><th>Status</th><th>NUNOTA</th><th>Data Import</th><th>Cliente</th><th>Valor</th><th>Erro</th></tr>
EOF

if [ -n "$HOMOLOG_ORDERS" ]; then
    echo "$HOMOLOG_ORDERS" | grep '|' | while IFS='|' read FCORDER STATUS NUNOTA DHIMPORT NOME VALOR ERRO; do
        STATUS_CLASS="ok"
        [ "$STATUS" = "ERRO" ] && STATUS_CLASS="fail"
        echo "<tr><td>$FCORDER</td><td><span class='$STATUS_CLASS'>$STATUS</span></td><td>$NUNOTA</td><td>$DHIMPORT</td><td>$NOME</td><td>R\$ $VALOR</td><td>$ERRO</td></tr>" >> "$REPORT"
    done
else
    echo "<tr><td colspan='7'>Nenhum pedido FC nas ultimas 24h em HOMOLOG</td></tr>" >> "$REPORT"
fi

cat >> "$REPORT" << EOF
</table>

<div class="footer">
Relatorio automatico - FastChannel Integration Addon<br>
Gerado por: replicate-fc-data / compare-orders-report
</div>
</div>
</body>
</html>
EOF

echo "Relatorio gerado em: $REPORT"

# =============================================================================
# Enviar por email
# =============================================================================
if command -v sendmail &> /dev/null || command -v mail &> /dev/null; then
    (
    echo "From: addon-fastchannel@bellube.com.br"
    echo "To: $EMAILS"
    echo "Subject: [FC Report] Comparativo Legado vs Addon - $(date '+%d/%m/%Y')"
    echo "MIME-Version: 1.0"
    echo "Content-Type: text/html; charset=utf-8"
    echo ""
    cat "$REPORT"
    ) | sendmail -t 2>/dev/null || echo "WARN: sendmail falhou, tentando curl..."

    # Fallback: usar o addon FC que ja tem SMTP configurado
    curl -s --max-time 15 -X POST "${ADDON_URL}?serviceName=FCAdminSP.enviarEmail" \
        -H "Content-Type: application/json" \
        -d "{\"to\":\"$EMAILS\",\"subject\":\"[FC Report] Comparativo Legado vs Addon - $(date '+%d/%m/%Y')\",\"htmlBody\":$(cat "$REPORT" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))' 2>/dev/null || echo '\"Relatorio disponivel em '$REPORT'\"')}" 2>/dev/null || \
    echo "WARN: Fallback email tambem falhou. Relatorio salvo em $REPORT"
else
    echo "WARN: sendmail nao encontrado. Relatorio salvo em $REPORT"
fi

echo "DONE"
