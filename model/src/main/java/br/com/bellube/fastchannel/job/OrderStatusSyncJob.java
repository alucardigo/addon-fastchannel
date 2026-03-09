package br.com.bellube.fastchannel.job;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.dto.OrderInvoiceDTO;
import br.com.bellube.fastchannel.http.FastchannelOrdersClient;
import br.com.bellube.fastchannel.service.LogService;
import br.com.bellube.fastchannel.util.DBUtil;
import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Catch-up de status de pedido entre Sankhya e Fastchannel.
 */
public class OrderStatusSyncJob implements EventoProgramavelJava {

    private static final Logger log = Logger.getLogger(OrderStatusSyncJob.class.getName());

    public void executeScheduler() throws Exception {
        FastchannelConfig cfg = FastchannelConfig.getInstance();
        if (!cfg.isAtivo() || !cfg.isSyncStatusEnabled()) {
            return;
        }

        FastchannelOrdersClient ordersClient = new FastchannelOrdersClient();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                    "SELECT ORDER_ID, STATUS_FC, STATUS_SKW, NUNOTA, NUNOTA_FATURA, NUCHAVE_NFE " +
                            "FROM AD_FCPEDIDO " +
                            "WHERE COALESCE(CAST(STATUS_FC AS VARCHAR(30)), '') <> COALESCE(CAST(STATUS_SKW AS VARCHAR(30)), '')");
            rs = stmt.executeQuery();

            while (rs.next()) {
                String orderId = rs.getString("ORDER_ID");
                int statusSkw = rs.getInt("STATUS_SKW");
                try {
                    ordersClient.updateOrderStatus(orderId, statusSkw, "Catch-up automatico de status");
                    if (rs.getString("NUCHAVE_NFE") != null && rs.getString("NUNOTA_FATURA") != null) {
                        OrderInvoiceDTO invoice = new OrderInvoiceDTO();
                        invoice.setInvoiceNumber(rs.getString("NUNOTA_FATURA"));
                        invoice.setInvoiceKey(rs.getString("NUCHAVE_NFE"));
                        ordersClient.sendInvoice(orderId, invoice);
                    }
                } catch (Exception e) {
                    log.log(Level.WARNING, "Falha ao sincronizar status do pedido " + orderId, e);
                }
            }
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
    }

    @Override public void beforeInsert(PersistenceEvent event) {}
    @Override public void beforeUpdate(PersistenceEvent event) {}
    @Override public void beforeDelete(PersistenceEvent event) {}
    @Override public void afterInsert(PersistenceEvent event) {}
    @Override public void afterUpdate(PersistenceEvent event) {}
    @Override public void afterDelete(PersistenceEvent event) {}
    @Override public void beforeCommit(TransactionContext transactionContext) {}
}

