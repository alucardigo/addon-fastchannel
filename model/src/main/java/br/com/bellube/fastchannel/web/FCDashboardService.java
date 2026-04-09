package br.com.bellube.fastchannel.web;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.http.FastchannelOrdersClient;
import br.com.bellube.fastchannel.util.DBUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service para o Dashboard principal.
 */
public class FCDashboardService {

    private static final Logger log = Logger.getLogger(FCDashboardService.class.getName());

    public Map<String, Object> snapshot(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;

        try {
            conn = DBUtil.getConnection();

            // Connection status (baseado na configuracao ativa)
            FastchannelConfig cfg = FastchannelConfig.getInstance();
            boolean configAtiva = cfg.isAtivo();
            boolean hasClientId = cfg.getClientId() != null && !cfg.getClientId().trim().isEmpty();
            boolean hasClientSecret = cfg.getClientSecret() != null && !cfg.getClientSecret().trim().isEmpty();
            boolean hasBaseUrl = cfg.getBaseUrl() != null && !cfg.getBaseUrl().trim().isEmpty();
            boolean apiOnline = hasBaseUrl;
            boolean tokenValid = hasClientId && hasClientSecret;

            Map<String, Object> connection = new HashMap<>();
            connection.put("apiOnline", apiOnline);
            connection.put("tokenValid", tokenValid);
            connection.put("integrationActive", configAtiva);
            connection.put("lastCheck", new Date());
            result.put("connection", connection);

            // Queue stats
            Map<String, Object> queue = new HashMap<>();
            queue.put("pending", countByStatus(conn, "AD_FCQUEUE", "STATUS", "PENDENTE"));
            queue.put("processing", countByStatus(conn, "AD_FCQUEUE", "STATUS", "PROCESSANDO"));
            queue.put("completed", countByStatus24h(conn, "AD_FCQUEUE", "STATUS", "ENVIADO", "DH_PROCESSAMENTO"));
            queue.put("errors", countByStatus(conn, "AD_FCQUEUE", "STATUS", "ERRO")
                    + countByStatus(conn, "AD_FCQUEUE", "STATUS", "ERRO_FATAL"));
            result.put("queue", queue);

            // Orders stats
            Map<String, Object> orders = new HashMap<>();
            orders.put("today", countOrdersToday(conn));
            orders.put("pending", countByStatus(conn, "AD_FCPEDIDO", "STATUS_IMPORT", "PENDENTE"));
            orders.put("error", countByStatus(conn, "AD_FCPEDIDO", "STATUS_IMPORT", "ERRO"));
            result.put("orders", orders);

            // Fastchannel orders (API) - usa lastOrderSync para mostrar apenas pedidos
            // desde a ultima sincronizacao (ou 72h se nunca sincronizou).
            // Isso evita exibir milhares de pedidos historicos ja processados.
            Map<String, Object> ordersFc = new HashMap<>();
            try {
                FastchannelOrdersClient fcClient = new FastchannelOrdersClient();
                Timestamp lastOrderSync = cfg.getLastOrderSync();
                FastchannelOrdersClient.OrderListResult fcResult = fcClient.listOrdersWithMeta(lastOrderSync, 1, 50, false);
                List<OrderDTO> apiOrders = fcResult.getOrders();

                // Conta APENAS pedidos que existem na FC API mas NAO foram integrados no Sankhya
                int realPending = 0;
                Map<String, Integer> statusCounts = new HashMap<>();
                if (apiOrders != null) {
                    for (OrderDTO order : apiOrders) {
                        String orderId = order.getOrderId();
                        boolean alreadyImported = isOrderImported(conn, orderId);
                        if (!alreadyImported) {
                            realPending++;
                        }
                        // Status sample apenas dos NAO importados
                        if (!alreadyImported) {
                            String status = order.getStatusDescription() != null ? order.getStatusDescription() : "SEM_STATUS";
                            statusCounts.put(status, statusCounts.getOrDefault(status, 0) + 1);
                        }
                    }
                }
                ordersFc.put("pendingTotal", realPending);
                ordersFc.put("statusSample", statusCounts);
                ordersFc.put("sampleSize", apiOrders != null ? apiOrders.size() : 0);
                ordersFc.put("totalApi", apiOrders != null ? apiOrders.size() : 0);
            } catch (Exception e) {
                ordersFc.put("error", e.getMessage());
            }
            result.put("ordersFastchannel", ordersFc);

            // Sync stats
            Map<String, Object> sync = new HashMap<>();
            sync.put("stock", countSync24h(conn, "STOCK_SYNC"));
            sync.put("price", countSync24h(conn, "PRICE_SYNC"));
            sync.put("product", countSync24h(conn, "PRODUCT_SYNC"));
            result.put("sync", sync);

            // Recent logs
            result.put("logs", getRecentLogs(conn, 10));

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao carregar dashboard", e);
            result.put("error", e.getMessage());
        } finally {
            DBUtil.closeConnection(conn);
        }

        return result;
    }

    /**
     * Verifica se um pedido FC ja foi importado no Sankhya (existe em AD_FCPEDIDO com qualquer status).
     */
    private boolean isOrderImported(Connection conn, String orderId) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            // Verifica AD_FCPEDIDO e TGFCAB (AD_NUMFAST) para alinhar com isOrderAlreadyImported
            stmt = conn.prepareStatement(
                "SELECT CASE WHEN EXISTS (SELECT 1 FROM AD_FCPEDIDO WHERE ORDER_ID = ?) " +
                "OR EXISTS (SELECT 1 FROM TGFCAB WHERE AD_NUMFAST = ?) THEN 1 ELSE 0 END AS IMPORTED");
            stmt.setString(1, orderId);
            stmt.setString(2, orderId);
            rs = stmt.executeQuery();
            rs.next();
            return rs.getInt("IMPORTED") > 0;
        } catch (Exception e) {
            return false;
        } finally {
            DBUtil.closeResultSet(rs);
            DBUtil.closeStatement(stmt);
        }
    }

    private int countByStatus(Connection conn, String table, String column, String status) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM " + table + " WHERE " + column + " = ?");
            stmt.setString(1, status);
            rs = stmt.executeQuery();
            rs.next();
            return rs.getInt("CNT");
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao contar " + table + ": " + status, e);
            return 0;
        } finally {
            DBUtil.closeResultSet(rs);
            DBUtil.closeStatement(stmt);
        }
    }

    private int countByStatus24h(Connection conn, String table, String column, String status, String dateColumn) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM " + table + " WHERE " + column + " = ? AND " + dateColumn + " >= ?");
            stmt.setString(1, status);
            stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis() - 24 * 60 * 60 * 1000));
            rs = stmt.executeQuery();
            rs.next();
            return rs.getInt("CNT");
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao contar " + table + " 24h: " + status, e);
            return 0;
        } finally {
            DBUtil.closeResultSet(rs);
            DBUtil.closeStatement(stmt);
        }
    }

    private int countOrdersToday(Connection conn) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM AD_FCPEDIDO WHERE DH_IMPORTACAO >= ?");
            Calendar cal = Calendar.getInstance();
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            stmt.setTimestamp(1, new Timestamp(cal.getTimeInMillis()));
            rs = stmt.executeQuery();
            rs.next();
            return rs.getInt("CNT");
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao contar pedidos hoje", e);
            return 0;
        } finally {
            DBUtil.closeResultSet(rs);
            DBUtil.closeStatement(stmt);
        }
    }

    private int countSync24h(Connection conn, String operacao) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM AD_FCLOG WHERE OPERACAO = ? AND NIVEL = 'INFO' AND DH_REGISTRO >= ?");
            stmt.setString(1, operacao);
            stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis() - 24 * 60 * 60 * 1000));
            rs = stmt.executeQuery();
            rs.next();
            return rs.getInt("CNT");
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao contar sync: " + operacao, e);
            return 0;
        } finally {
            DBUtil.closeResultSet(rs);
            DBUtil.closeStatement(stmt);
        }
    }

    private List<Map<String, Object>> getRecentLogs(Connection conn, int limit) {
        List<Map<String, Object>> logs = new ArrayList<>();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT TOP (?) DH_REGISTRO, OPERACAO, NIVEL, MENSAGEM FROM AD_FCLOG ORDER BY DH_REGISTRO DESC");
            stmt.setInt(1, limit);
            rs = stmt.executeQuery();

            while (rs.next()) {
                Map<String, Object> logItem = new HashMap<>();
                logItem.put("timestamp", rs.getTimestamp("DH_REGISTRO"));
                logItem.put("operation", rs.getString("OPERACAO"));
                logItem.put("level", rs.getString("NIVEL"));
                logItem.put("message", rs.getString("MENSAGEM"));
                logs.add(logItem);
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao carregar logs recentes", e);
        } finally {
            DBUtil.closeResultSet(rs);
            DBUtil.closeStatement(stmt);
        }
        return logs;
    }
}
