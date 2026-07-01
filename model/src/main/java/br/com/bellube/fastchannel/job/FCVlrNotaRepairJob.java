package br.com.bellube.fastchannel.job;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.service.LogService;
import br.com.bellube.fastchannel.util.DBUtil;
import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * [REDE DE SEGURANCA - 2026-05-14]
 * Job de Repair Periodico de VLRNOTA em pedidos Fastchannel.
 *
 * <p><b>Por que existe</b>: as camadas existentes (forceVlrNotaCorrect, forceItemValuesFromFc,
 * NotaFiscalListener.repairVlrNotaAfterConfirmation) cobrem TODOS os caminhos JAPE conhecidos,
 * mas dependem de eventos especificos serem disparados:
 * <ul>
 *   <li>{@code forceVlrNotaCorrect} - so' atua DURANTE a importacao do pedido (caminho controlado)</li>
 *   <li>{@code repairVlrNotaAfterConfirmation} - so' atua quando JAPE dispara afterUpdate (nao
 *       cobre stored procedures que mexem TGFCAB direto via SQL, ex: STP_CONFIRMANOTA2 chamada
 *       via outros caminhos)</li>
 * </ul>
 *
 * <p><b>Estrategia</b>: varredura periodica do banco que DETECTA qualquer divergencia em
 * pedidos FC e CORRIGE via UPDATE direto, INDEPENDENTE de eventos JAPE. Pega:
 * <ul>
 *   <li>Pedidos importados ANTES da v1.2.74 estar em PROD (caso historico 4857418)</li>
 *   <li>Pedidos onde STP_CONFIRMANOTA2 quebrou VLRNOTA sem disparar afterUpdate JAPE</li>
 *   <li>Pedidos editados manualmente onde alguem mexeu itens mas nao recalculou header</li>
 *   <li>Qualquer outro caminho ainda nao mapeado</li>
 * </ul>
 *
 * <p><b>Idempotencia</b>: o UPDATE so' atua quando {@code ABS(VLRNOTA - formula) > 0.01}.
 * Iteracoes posteriores sao no-op para pedidos ja corretos.
 *
 * <p><b>Escopo da varredura</b>: apenas pedidos FC ({@code AD_NUMFAST IS NOT NULL}) dos
 * ultimos 30 dias, para limitar custo da query. Pedidos antigos sao corrigidos manualmente
 * via SQL on-demand.
 *
 * <p><b>Configuracao no Sankhya</b>:
 * <ol>
 *   <li>Sankhya > Configuracoes > Eventos Programaveis > Agendamento</li>
 *   <li>Classe: {@code br.com.bellube.fastchannel.job.FCVlrNotaRepairJob}</li>
 *   <li>Intervalo recomendado: 60-120 segundos</li>
 * </ol>
 *
 * <p><b>Defesa em camadas</b>:
 * <ol>
 *   <li>{@code forceItemValuesFromFc} (v1.2.71) - durante save do item</li>
 *   <li>{@code forceVlrNotaCorrect} (pre-existente) - apos save do header</li>
 *   <li>{@code NotaFiscalListener.repairVlrNotaAfterConfirmation} (v1.2.74) - reativo via JAPE</li>
 *   <li>{@code FCVlrNotaRepairJob} (v1.2.76, este job) - varredura proativa periodica</li>
 * </ol>
 */
public class FCVlrNotaRepairJob implements EventoProgramavelJava {

    private static final Logger log = Logger.getLogger(FCVlrNotaRepairJob.class.getName());

    /** Janela de dias para varrer no banco (limita o custo da query). */
    private static final int LOOKBACK_DAYS = 30;

    public void executeScheduler() throws Exception {
        FastchannelConfig config = FastchannelConfig.getInstance();
        if (!config.isAtivo()) {
            return;
        }

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        int repaired = 0;
        int errors = 0;
        List<RepairLog> repairs = new ArrayList<RepairLog>();

        try {
            conn = DBUtil.getConnection();

            // Detecta pedidos FC com VLRNOTA divergente da formula correta.
            // Formula: VLRNOTA = SUM(VLRTOT - VLRDESC) + VLRFRETE + VLRJURO
            String detectSql =
                "SELECT c.NUNOTA, c.AD_NUMFAST, c.VLRNOTA, " +
                "  ISNULL((SELECT SUM(VLRTOT - ISNULL(VLRDESC,0)) FROM TGFITE WHERE NUNOTA=c.NUNOTA),0) " +
                "    + ISNULL(c.VLRFRETE,0) + ISNULL(c.VLRJURO,0) AS VLRNOTA_CORRETO " +
                "FROM TGFCAB c " +
                "WHERE c.AD_NUMFAST IS NOT NULL " +
                "  AND c.DTNEG >= DATEADD(day, -" + LOOKBACK_DAYS + ", GETDATE()) " +
                "  AND ABS(c.VLRNOTA - ( " +
                "    ISNULL((SELECT SUM(VLRTOT - ISNULL(VLRDESC,0)) FROM TGFITE WHERE NUNOTA=c.NUNOTA),0) " +
                "    + ISNULL(c.VLRFRETE,0) + ISNULL(c.VLRJURO,0) " +
                "  )) > 0.01";

            stmt = conn.prepareStatement(detectSql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                RepairLog item = new RepairLog();
                item.nuNota = rs.getBigDecimal("NUNOTA");
                item.adNumFast = rs.getString("AD_NUMFAST");
                item.vlrNotaAntes = rs.getBigDecimal("VLRNOTA");
                item.vlrNotaCorreto = rs.getBigDecimal("VLRNOTA_CORRETO");
                repairs.add(item);
            }
            DBUtil.closeAll(rs, stmt, null);
            rs = null; stmt = null;

            if (repairs.isEmpty()) {
                log.fine("[FCVlrNotaRepairJob] Nenhuma divergencia detectada.");
                return;
            }

            // Aplica correcao em cada pedido divergente.
            for (RepairLog item : repairs) {
                try {
                    PreparedStatement upd = null;
                    try {
                        upd = conn.prepareStatement(
                            "UPDATE TGFCAB SET VLRNOTA = ( " +
                            "  ISNULL((SELECT SUM(VLRTOT - ISNULL(VLRDESC,0)) FROM TGFITE WHERE NUNOTA=TGFCAB.NUNOTA),0) " +
                            "  + ISNULL(VLRFRETE,0) + ISNULL(VLRJURO,0) " +
                            ") " +
                            "WHERE NUNOTA = ? " +
                            "  AND ABS(VLRNOTA - ( " +
                            "    ISNULL((SELECT SUM(VLRTOT - ISNULL(VLRDESC,0)) FROM TGFITE WHERE NUNOTA=TGFCAB.NUNOTA),0) " +
                            "    + ISNULL(VLRFRETE,0) + ISNULL(VLRJURO,0) " +
                            "  )) > 0.01");
                        upd.setBigDecimal(1, item.nuNota);
                        int affected = upd.executeUpdate();
                        if (affected > 0) {
                            repaired++;
                            String msg = "[FCVlrNotaRepairJob] REPAIR aplicado NUNOTA="
                                    + item.nuNota + " AD_NUMFAST=" + item.adNumFast
                                    + " VLRNOTA " + item.vlrNotaAntes + " -> " + item.vlrNotaCorreto;
                            log.info(msg);
                            LogService.getInstance().info(LogService.OP_ORDER_IMPORT, msg);
                        }
                    } finally {
                        DBUtil.closeStatement(upd);
                    }
                } catch (Exception e) {
                    errors++;
                    log.log(Level.WARNING,
                            "[FCVlrNotaRepairJob] Falha ao reparar NUNOTA " + item.nuNota, e);
                }
            }

            String summary = String.format(
                    "[FCVlrNotaRepairJob] Concluido. Detectados=%d Reparados=%d Erros=%d",
                    repairs.size(), repaired, errors);
            log.info(summary);
            if (repaired > 0 || errors > 0) {
                LogService.getInstance().info(LogService.OP_ORDER_IMPORT, summary);
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "[FCVlrNotaRepairJob] Falha geral no job", e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
    }

    private static class RepairLog {
        BigDecimal nuNota;
        String adNumFast;
        BigDecimal vlrNotaAntes;
        BigDecimal vlrNotaCorreto;
    }

    @Override public void beforeInsert(PersistenceEvent event) {}
    @Override public void beforeUpdate(PersistenceEvent event) {}
    @Override public void beforeDelete(PersistenceEvent event) {}
    @Override public void afterInsert(PersistenceEvent event) {}
    @Override public void afterUpdate(PersistenceEvent event) {}
    @Override public void afterDelete(PersistenceEvent event) {}
    @Override public void beforeCommit(TransactionContext transactionContext) {}
}
