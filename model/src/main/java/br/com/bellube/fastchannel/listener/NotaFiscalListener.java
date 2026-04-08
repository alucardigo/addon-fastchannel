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

    private void processInvoiceCreated(BigDecimal nuNota, String orderId, DynamicVO vo) {
        try {
            OrderInvoiceDTO invoice = new OrderInvoiceDTO();
            invoice.setNuNota(nuNota);
            invoice.setInvoiceKey(vo.asString("CHAVENFE"));
            invoice.setInvoiceNumber(vo.asString("NUMNOTA") != null ?
                    vo.asBigDecimal("NUMNOTA").toString() : null);
            invoice.setInvoiceSeries(vo.asString("SERIENOTA"));
            invoice.setInvoiceDate(vo.asTimestamp("DTFATUR"));
            invoice.setTotalValue(vo.asBigDecimal("VLRNOTA"));

            FastchannelOrdersClient ordersClient = new FastchannelOrdersClient();
            ordersClient.sendInvoice(orderId, invoice);

            ordersClient.updateOrderStatus(orderId,
                    FastchannelConstants.STATUS_INVOICE_CREATED,
                    "Nota fiscal emitida: " + invoice.getInvoiceNumber());

            LogService.getInstance().info(LogService.OP_ORDER_IMPORT,
                    "NF enviada para pedido " + orderId, invoice.getInvoiceNumber());

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao enviar NF para Fastchannel", e);
            LogService.getInstance().error(LogService.OP_ORDER_IMPORT,
                    "Falha ao enviar NF para pedido " + orderId, e);
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
        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

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
        }
        return null;
    }

    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (Exception ignored) {}
        }
    }
}
