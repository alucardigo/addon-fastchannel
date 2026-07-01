package br.com.bellube.fastchannel.listener;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.OrderInvoiceDTO;
import br.com.bellube.fastchannel.http.FastchannelOrdersClient;
import br.com.bellube.fastchannel.service.LogService;
import br.com.bellube.fastchannel.service.QueueService;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.PersistenceEventAdapter;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.studio.annotations.Listener;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Listener de Nota Fiscal (TGFCAB/TGFNOT).
 *
 * Captura eventos de faturamento para notificar o Fastchannel.
 */
@Listener(instanceNames = {"CabecalhoNota"})
public class NotaFiscalListener extends PersistenceEventAdapter {

    private static final Logger log = Logger.getLogger(NotaFiscalListener.class.getName());

    /**
     * [FIX 2026-05-15 v1.2.81] Cache de NFs ja enviadas com sucesso ou que falharam 404
     * pra evitar spam de tentativas a cada afterUpdate da TGFCAB. Cada UPDATE no
     * cabecalho (mesmo trivial) dispara afterUpdate, e o NotaFiscalListener tentava
     * enviar NF/status repetidamente. Key = NUNOTA. Eviction simples por tamanho.
     */
    private static final java.util.Set<String> INVOICE_TERMINAL_STATES =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<String, Boolean>());
    private static final int INVOICE_CACHE_MAX = 5000;

    @Override
    public void afterUpdate(PersistenceEvent event) throws Exception {
        processNotaUpdate(event);
    }

    private void processNotaUpdate(PersistenceEvent event) {
        try {
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                return;
            }

            if (!config.isSyncStatusEnabled()) {
                log.fine("Sincronizacao de status desabilitada. Pulando atualizacao.");
                return;
            }

            DynamicVO vo = (DynamicVO) event.getVo();
            BigDecimal nuNota = vo.asBigDecimal("NUNOTA");
            String statusNota = vo.asString("STATUSNOTA");

            String orderId = getOrderIdByNuNota(nuNota);
            if (orderId == null) {
                return;
            }

            log.info("Nota " + nuNota + " atualizada (Pedido FC: " + orderId + ") - Status: " + statusNota);

            // [FIX 2026-05-11] Re-aplicar formula correta de VLRNOTA apos confirmacao.
            // A procedure nativa Sankhya STP_CONFIRMANOTA2 (chamada via ServicosNfeSPBean.confirmarNotas)
            // recalcula VLRNOTA do cabecalho como SUM(VLRTOT) - sem considerar VLRDESC dos itens.
            // Isso quebra o cupom de desconto FC (NUNOTA 4851833 reportado: cupom 337.34
            // foi ignorado, VLRNOTA virou bruto 5890.44 em vez de liquido 5553.10).
            // Idempotente via guarda ABS(diff) > 0.01.
            repairVlrNotaAfterConfirmation(nuNota);

            String chaveNfe = vo.asString("CHAVENFE");
            if (chaveNfe != null && !chaveNfe.isEmpty()) {
                processInvoiceCreated(nuNota, orderId, vo);
            }

            if (statusNota != null) {
                processStatusChange(nuNota, orderId, statusNota);
            }

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao processar atualizacao de nota", e);
        }
    }

    /**
     * [FIX 2026-05-11] Garante que VLRNOTA do cabecalho segue a formula correta:
     *   VLRNOTA = SUM(VLRTOT - VLRDESC) + VLRFRETE + VLRJURO
     *
     * Necessario porque a procedure nativa Sankhya STP_CONFIRMANOTA2 (confirmacao automatica
     * de nota) recalcula VLRNOTA do cabecalho como SUM(VLRTOT) - ignorando VLRDESC dos itens.
     * Como o addon Fastchannel grava o cupom em VLRDESC dos itens, a confirmacao "perde" o
     * cupom no VLRNOTA do cabecalho, embora os itens continuem com os valores corretos.
     *
     * Idempotente: o UPDATE usa NativeSql (nao retrigera este listener) e so' atua quando
     * |VLRNOTA_atual - formula| > 0.01.
     *
     * Falha em try/catch para nao abortar a atualizacao da nota se o reforco nao funcionar.
     */
    private void repairVlrNotaAfterConfirmation(BigDecimal nuNota) {
        if (nuNota == null || nuNota.compareTo(BigDecimal.ZERO) <= 0) {
            return;
        }
        JdbcWrapper jdbc = null;
        try {
            jdbc = EntityFacadeFactory.getDWFFacade().getJdbcWrapper();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql(
                "UPDATE TGFCAB SET VLRNOTA = ( " +
                "  SELECT ISNULL(SUM(VLRTOT - ISNULL(VLRDESC, 0)), 0) FROM TGFITE WHERE NUNOTA = TGFCAB.NUNOTA " +
                ") + ISNULL(VLRFRETE, 0) + ISNULL(VLRJURO, 0) " +
                "WHERE NUNOTA = :nuNota " +
                "  AND ABS(ISNULL(VLRNOTA, 0) - ( " +
                "    (SELECT ISNULL(SUM(VLRTOT - ISNULL(VLRDESC, 0)), 0) FROM TGFITE WHERE NUNOTA = TGFCAB.NUNOTA) " +
                "    + ISNULL(VLRFRETE, 0) + ISNULL(VLRJURO, 0) " +
                "  )) > 0.01"
            );
            sql.setNamedParameter("nuNota", nuNota);
            sql.executeUpdate();
            log.info("[VLRNOTA-REPAIR] NUNOTA " + nuNota + " - formula reaplicada (no-op se ja estava correto).");
        } catch (Exception e) {
            log.log(Level.WARNING, "[VLRNOTA-REPAIR] Falha ao reaplicar formula VLRNOTA para NUNOTA " + nuNota
                    + ". Pode estar divergente apos confirmacao - verificar manualmente.", e);
        } finally {
            if (jdbc != null) {
                try { jdbc.closeSession(); } catch (Exception ignore) { /* ignore */ }
            }
        }
    }

    private void processInvoiceCreated(BigDecimal nuNota, String orderId, DynamicVO vo) {
        // [FIX 2026-05-15 v1.2.81] Idempotencia: pular se ja' processada nesta sessao.
        // Cada UPDATE no TGFCAB (status, valor, etc) dispara afterUpdate; sem este guard
        // o addon enviava NF repetidamente para o FC, causando spam de 404 quando o FC
        // ja' tinha a NF anexada.
        String cacheKey = nuNota != null ? nuNota.toPlainString() : null;
        if (cacheKey != null && INVOICE_TERMINAL_STATES.contains(cacheKey)) {
            log.fine("NF NUNOTA " + nuNota + " ja processada nesta sessao - skip.");
            return;
        }

        try {
            OrderInvoiceDTO invoice = new OrderInvoiceDTO();
            invoice.setNuNota(nuNota);
            invoice.setInvoiceKey(safeAsString(vo, "CHAVENFE"));
            // [FIX 2026-05-14 v1.2.79] NUMNOTA em TGFCAB e' numerico. vo.asString("NUMNOTA")
            // lanca ClassCastException ("BigDecimal cannot be cast to String") porque JAPE
            // faz cast direto baseado no tipo declarado do VO, nao converte. Ler como
            // BigDecimal e converter manualmente.
            BigDecimal numNota = vo.asBigDecimal("NUMNOTA");
            invoice.setInvoiceNumber(numNota != null && numNota.compareTo(BigDecimal.ZERO) > 0
                    ? numNota.toPlainString() : null);
            invoice.setInvoiceSeries(safeAsString(vo, "SERIENOTA"));
            invoice.setInvoiceDate(vo.asTimestamp("DTFATUR"));
            invoice.setTotalValue(vo.asBigDecimal("VLRNOTA"));

            FastchannelOrdersClient ordersClient = new FastchannelOrdersClient();
            ordersClient.sendInvoice(orderId, invoice);

            ordersClient.updateOrderStatus(orderId,
                    FastchannelConstants.STATUS_INVOICE_CREATED,
                    "Nota fiscal emitida: " + invoice.getInvoiceNumber());

            LogService.getInstance().info(LogService.OP_ORDER_IMPORT,
                    "NF enviada para pedido " + orderId, invoice.getInvoiceNumber());

            // Sucesso: marcar como processada
            cacheInvoiceTerminal(cacheKey);

        } catch (Exception e) {
            // [FIX 2026-05-15 v1.2.81] HTTP 404 ao enviar NF e' tratado como TERMINAL:
            // o FC ja' tem NF anexada (ou pedido foi cancelado). Marcar como terminal pra
            // nao retentar a cada afterUpdate. Loga INFO em vez de SEVERE.
            String errMsg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
            boolean is404 = errMsg.contains("resource not found")
                    || errMsg.contains("\"statuscode\": 404");
            if (is404) {
                log.info("NF para pedido " + orderId + " ja' anexada no FC (HTTP 404). "
                        + "Marcando como terminal para parar retry.");
                cacheInvoiceTerminal(cacheKey);
            } else {
                log.log(Level.WARNING, "Erro ao enviar NF para Fastchannel", e);
                LogService.getInstance().error(LogService.OP_ORDER_IMPORT,
                        "Falha ao enviar NF para pedido " + orderId, e);
            }
        }
    }

    /**
     * [FIX 2026-05-15 v1.2.81] Marca NUNOTA como terminal para invoice (ja' enviada ou
     * rejeitada definitivamente). Evita spam de tentativas repetidas a cada afterUpdate.
     * Eviction simples por tamanho - apaga 25% mais antigos quando atinge limite.
     */
    private static void cacheInvoiceTerminal(String cacheKey) {
        if (cacheKey == null) return;
        if (INVOICE_TERMINAL_STATES.size() >= INVOICE_CACHE_MAX) {
            // Eviction simples: limpa tudo. Nao precisa LRU sofisticado - apenas evita OOM.
            INVOICE_TERMINAL_STATES.clear();
        }
        INVOICE_TERMINAL_STATES.add(cacheKey);
    }

    /**
     * [HELPER 2026-05-14] Le campo do VO como String tolerante a tipos.
     * vo.asString() lanca ClassCastException quando o campo e' numerico no schema JAPE.
     * Este helper tenta asString primeiro e cai em asBigDecimal/Object.toString() em fallback.
     */
    private static String safeAsString(DynamicVO vo, String field) {
        if (vo == null || field == null) return null;
        try {
            return vo.asString(field);
        } catch (ClassCastException cce) {
            try {
                BigDecimal asNum = vo.asBigDecimal(field);
                return asNum != null ? asNum.toPlainString() : null;
            } catch (Exception inner) {
                try {
                    Object raw = vo.getProperty(field);
                    return raw != null ? raw.toString() : null;
                } catch (Exception ignored) {
                    return null;
                }
            }
        } catch (Exception e) {
            return null;
        }
    }

    private void processStatusChange(BigDecimal nuNota, String orderId, String statusNota) {
        try {
            int fcStatus;
            String message;

            switch (statusNota) {
                case "L":
                    fcStatus = FastchannelConstants.STATUS_APPROVED;
                    message = "Pedido aprovado";
                    break;
                case "P":
                    fcStatus = FastchannelConstants.STATUS_CREATED;
                    message = "Pedido pendente";
                    break;
                case "F":
                    fcStatus = FastchannelConstants.STATUS_INVOICE_CREATED;
                    message = "Pedido faturado";
                    break;
                case "C":
                    fcStatus = FastchannelConstants.STATUS_DENIED;
                    message = "Pedido cancelado";
                    break;
                default:
                    return;
            }

            QueueService queueService = QueueService.getInstance();
            queueService.enqueueOrderStatus(nuNota, orderId, fcStatus);

            log.info("Status do pedido " + orderId + " enfileirado: " + statusNota + " -> " + fcStatus);

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao processar mudanca de status", e);
        }
    }

    private String getOrderIdByNuNota(BigDecimal nuNota) {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT ORDER_ID FROM AD_FCPEDIDO WHERE NUNOTA = :nuNota");
            sql.setNamedParameter("nuNota", nuNota);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getString("ORDER_ID");
            }
            closeQuietly(rs);

            sql = new NativeSql(jdbc);
            sql.appendSql("SELECT AD_FASTCHANNEL_ID FROM TGFCAB WHERE NUNOTA = :nuNota");
            sql.setNamedParameter("nuNota", nuNota);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getString("AD_FASTCHANNEL_ID");
            }

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar OrderId", e);
        } finally {
            closeQuietly(rs);
            if (jdbc != null) { try { jdbc.closeSession(); } catch (Exception ignored) {} }
        }
        return null;
    }

    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (Exception ignored) {}
        }
    }
}
