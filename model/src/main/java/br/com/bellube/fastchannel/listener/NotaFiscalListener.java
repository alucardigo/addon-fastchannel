package br.com.bellube.fastchannel.listener;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.OrderInvoiceDTO;
import br.com.bellube.fastchannel.http.FastchannelOrdersClient;
import br.com.bellube.fastchannel.service.LogService;
import br.com.bellube.fastchannel.service.QueueService;
import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Listener de Nota Fiscal (TGFCAB/TGFNOT).
 *
 * Captura eventos de faturamento para notificar o Fastchannel:
 * - Nota fiscal emitida (enviar chave e n?mero da NF)
 * - Altera??o de status do pedido
 *
 * Configura??o no Sankhya:
 * - Eventos Program?veis > Listeners
 * - Entidade: CabecalhoNota (TGFCAB)
 * - Eventos: afterUpdate
 */
public class NotaFiscalListener implements EventoProgramavelJava {

    private static final Logger log = Logger.getLogger(NotaFiscalListener.class.getName());

    @Override
    public void beforeInsert(PersistenceEvent event) throws Exception {
        // Not used
    }

    @Override
    public void beforeUpdate(PersistenceEvent event) throws Exception {
        // Not used
    }

    @Override
    public void beforeDelete(PersistenceEvent event) throws Exception {
        // Not used
    }

    @Override
    public void afterInsert(PersistenceEvent event) throws Exception {
        // Not used for TGFCAB
    }

    @Override
    public void afterUpdate(PersistenceEvent event) throws Exception {
        processNotaUpdate(event);
    }

    @Override
    public void afterDelete(PersistenceEvent event) throws Exception {
        // Not used
    }

    @Override
    public void beforeCommit(TransactionContext transactionContext) throws Exception {
        // Not used
    }

    public void executeScheduler() throws Exception {
        // Not used - this is a listener, not a scheduler
    }

    private void processNotaUpdate(PersistenceEvent event) {
        try {
            // Verificar se integra??o est? ativa
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                return;
            }

            DynamicVO vo = (DynamicVO) event.getVo();
            BigDecimal nuNota = vo.asBigDecimal("NUNOTA");
            String statusNota = vo.asString("STATUSNOTA");

            // Verificar se ? um pedido do Fastchannel
            String orderId = getOrderIdByNuNota(nuNota);
            if (orderId == null) {
                return; // N?o ? um pedido Fastchannel
            }

            log.info("Nota " + nuNota + " atualizada (Pedido FC: " + orderId + ") - Status: " + statusNota);

            // Verificar se foi faturada (STATUSNOTA = 'F' ou tem CHAVENFE)
            String chaveNfe = vo.asString("CHAVENFE");
            if (chaveNfe != null && !chaveNfe.isEmpty()) {
                // Nota fiscal emitida - notificar Fastchannel
                processInvoiceCreated(nuNota, orderId, vo);
            }

            // Verificar mudan?a de status
            if (statusNota != null) {
                processStatusChange(nuNota, orderId, statusNota);
            }

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao processar atualiza??o de nota", e);
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

            // Enviar para Fastchannel
            FastchannelOrdersClient ordersClient = new FastchannelOrdersClient();
            ordersClient.sendInvoice(orderId, invoice);

            // Atualizar status para "Faturado"
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
                case "L": // Liberado
                    fcStatus = FastchannelConstants.STATUS_APPROVED;
                    message = "Pedido aprovado";
                    break;

                case "P": // Pendente
                    fcStatus = FastchannelConstants.STATUS_CREATED;
                    message = "Pedido pendente";
                    break;

                case "F": // Faturado
                    fcStatus = FastchannelConstants.STATUS_INVOICE_CREATED;
                    message = "Pedido faturado";
                    break;

                case "C": // Cancelado
                    fcStatus = FastchannelConstants.STATUS_DENIED;
                    message = "Pedido cancelado";
                    break;

                default:
                    return; // Status n?o mapeado
            }

            // Enfileirar atualiza??o de status (usa fila para garantir entrega)
            QueueService queueService = QueueService.getInstance();
            queueService.enqueueOrderStatus(nuNota, orderId, fcStatus);

            log.info("Status do pedido " + orderId + " enfileirado: " + statusNota + " -> " + fcStatus);

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao processar mudan?a de status", e);
        }
    }

    private String getOrderIdByNuNota(BigDecimal nuNota) {
        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            // Primeiro tentar na AD_FCPEDIDO
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT ORDER_ID FROM AD_FCPEDIDO WHERE NUNOTA = :nuNota");
            sql.setNamedParameter("nuNota", nuNota);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getString("ORDER_ID");
            }
            closeQuietly(rs);

            // Fallback: verificar campo customizado em TGFCAB
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

