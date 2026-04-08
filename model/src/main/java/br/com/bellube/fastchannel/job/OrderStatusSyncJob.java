package br.com.bellube.fastchannel.job;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.OrderInvoiceDTO;
import br.com.bellube.fastchannel.http.FastchannelOrdersClient;
import br.com.bellube.fastchannel.service.LogService;
import br.com.bellube.fastchannel.util.DBUtil;
import br.com.bellube.fastchannel.util.DbColumnSupport;
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
 *
 * STATUS_SKW armazena o status Sankhya (ex: 'L'=Liberado, 'F'=Faturado, 'E'=Entregue).
 * STATUS_FC armazena o ultimo codigo FC enviado (ex: 201, 300, 301).
 * O job mapeia STATUS_SKW para o codigo FC equivalente e sincroniza quando divergem.
 */
public class OrderStatusSyncJob implements EventoProgramavelJava {

    private static final Logger log = Logger.getLogger(OrderStatusSyncJob.class.getName());

    /**
     * Mapeia status Sankhya (TGFCAB.STATUSNOTA) para codigo de status FastChannel.
     * Retorna -1 se o status nao tem mapeamento (nenhuma acao necessaria).
     */
    static int mapSankhyaStatusToFc(String statusSkw) {
        if (statusSkw == null) return -1;
        switch (statusSkw.trim().toUpperCase()) {
            case "L": return FastchannelConstants.STATUS_APPROVED;       // Liberado → 201
            case "F": return FastchannelConstants.STATUS_INVOICE_CREATED; // Faturado → 300
            case "E": return FastchannelConstants.STATUS_DELIVERED;       // Entregue → 301
            case "C":
            case "X": return FastchannelConstants.STATUS_DENIED;          // Cancelado → 400
            default:
                // Tenta parsear numericamente (caso STATUS_SKW ja seja o codigo FC)
                try { return Integer.parseInt(statusSkw.trim()); } catch (NumberFormatException e) { return -1; }
        }
    }

    public void executeScheduler() throws Exception {
        FastchannelConfig cfg = FastchannelConfig.getInstance();
        if (!cfg.isAtivo() || !cfg.isSyncStatusEnabled()) {
            return;
        }

        FastchannelOrdersClient ordersClient = new FastchannelOrdersClient();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        // --- Parte 1: Sincronizar status divergente Sankhya → FC ---
        try {
            conn = DBUtil.getConnection();

            // Detectar colunas opcionais de NF (schema pode variar entre ambientes)
            boolean hasNuNotaFatura = DbColumnSupport.hasColumn(conn, "AD_FCPEDIDO", "NUNOTA_FATURA");
            boolean hasNuChaveNfe   = DbColumnSupport.hasColumn(conn, "AD_FCPEDIDO", "NUCHAVE_NFE");
            boolean hasNfNumero     = DbColumnSupport.hasColumn(conn, "AD_FCPEDIDO", "NF_NUMERO");
            boolean hasNfChave      = DbColumnSupport.hasColumn(conn, "AD_FCPEDIDO", "NF_CHAVE");

            // Coluna de numero da NF: prioridade NF_NUMERO > NUNOTA_FATURA
            String colNfNumero = hasNfNumero     ? "NF_NUMERO"
                               : hasNuNotaFatura ? "NUNOTA_FATURA"
                               : "CAST(NULL AS VARCHAR(50)) AS NF_NUMERO";

            // Coluna de chave NFe: prioridade NF_CHAVE > NUCHAVE_NFE
            String colNfChave = hasNfChave    ? "NF_CHAVE"
                              : hasNuChaveNfe ? "NUCHAVE_NFE"
                              : "CAST(NULL AS VARCHAR(50)) AS NF_CHAVE";

            // Selecionar pedidos onde STATUS_SKW esta preenchido (ou seja, temos dados suficientes)
            // Filtramos no Java para evitar logica SQL complexa de mapeamento
            String selectSql =
                    "SELECT ORDER_ID, STATUS_FC, STATUS_SKW, NUNOTA, " +
                    colNfNumero + ", " + colNfChave + " " +
                    "FROM AD_FCPEDIDO " +
                    "WHERE STATUS_SKW IS NOT NULL AND STATUS_FC IS NOT NULL AND NUNOTA IS NOT NULL";

            stmt = conn.prepareStatement(selectSql);
            rs = stmt.executeQuery();

            LogService logService = LogService.getInstance();
            while (rs.next()) {
                String orderId   = rs.getString("ORDER_ID");
                String statusSkw = rs.getString("STATUS_SKW");
                int    statusFc  = rs.getInt("STATUS_FC");

                // Mapear status Sankhya para codigo FC equivalente
                int targetFcStatus = mapSankhyaStatusToFc(statusSkw);
                if (targetFcStatus < 0) {
                    log.log(Level.FINE, "Pedido " + orderId + ": STATUS_SKW=" + statusSkw + " sem mapeamento FC, ignorando.");
                    continue;
                }

                // Somente sincronizar se houver divergencia real
                if (targetFcStatus == statusFc) {
                    continue;
                }

                try {
                    ordersClient.updateOrderStatus(orderId, targetFcStatus,
                            "Catch-up automatico de status (" + statusSkw + " -> " + targetFcStatus + ")");
                    logService.info(LogService.OP_ORDER_IMPORT,
                            "Status sincronizado: pedido " + orderId
                                    + " SKW=" + statusSkw + " FC=" + statusFc + " -> " + targetFcStatus, orderId);

                    // Enviar NF se disponivel e status indica faturamento
                    if (targetFcStatus == FastchannelConstants.STATUS_INVOICE_CREATED) {
                        String nfNumero = rs.getString("NF_NUMERO");
                        String nfChave  = rs.getString("NF_CHAVE");
                        if (nfChave != null && nfNumero != null) {
                            OrderInvoiceDTO invoice = new OrderInvoiceDTO();
                            invoice.setInvoiceNumber(nfNumero);
                            invoice.setInvoiceKey(nfChave);
                            ordersClient.sendInvoice(orderId, invoice);
                            logService.info(LogService.OP_ORDER_IMPORT,
                                    "NF enviada para pedido " + orderId + ": " + nfNumero, orderId);
                        }
                    }
                } catch (Exception e) {
                    log.log(Level.WARNING, "Falha ao sincronizar status do pedido " + orderId, e);
                    logService.error(LogService.OP_ORDER_IMPORT,
                            "Falha no catch-up de status do pedido " + orderId, orderId, e);
                }
            }
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }

        // --- Parte 2: Marcar IsSynched=true na FC para pedidos importados com sucesso ---
        conn  = null;
        stmt  = null;
        rs    = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                    "SELECT ORDER_ID, NUNOTA FROM AD_FCPEDIDO " +
                    "WHERE STATUS_IMPORT = 'SUCESSO' AND NUNOTA IS NOT NULL");
            rs = stmt.executeQuery();
            LogService logService = LogService.getInstance();
            while (rs.next()) {
                String orderId = rs.getString("ORDER_ID");
                String nuNota  = rs.getString("NUNOTA");
                try {
                    ordersClient.markAsSynced(orderId, nuNota);
                } catch (Exception e) {
                    log.log(Level.FINE, "Pedido " + orderId + " ja synced ou falhou: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Falha no catch-up de IsSynched", e);
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
