package br.com.bellube.fastchannel.web;

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

            // Connection status
            Map<String, Object> connection = new HashMap<>();
            connection.put("apiOnline", true);
            connection.put("tokenValid", true);
            connection.put("lastCheck", new Date());
            result.put("connection", connection);

            // Queue stats
            Map<String, Object> queue = new HashMap<>();
            queue.put("pending", countByStatus(conn, "AD_FCQUEUE", "STATUS", "PENDENTE"));
            queue.put("processing", countByStatus(conn, "AD_FCQUEUE", "STATUS", "PROCESSANDO"));
            queue.put("completed", countByStatus24h(conn, "AD_FCQUEUE", "STATUS", "CONCLUIDO", "DH_PROCESSAMENTO"));
            queue.put("errors", countByStatus(conn, "AD_FCQUEUE", "STATUS", "ERRO"));
            result.put("queue", queue);

            // Orders stats
            Map<String, Object> orders = new HashMap<>();
            orders.put("today", countOrdersToday(conn));
            orders.put("pending", countByStatus(conn, "AD_FCPEDIDO", "STATUS_IMPORT", "PENDENTE"));
            orders.put("error", countByStatus(conn, "AD_FCPEDIDO", "STATUS_IMPORT", "ERRO"));
            result.put("orders", orders);

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
            stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM AD_FCLOG WHERE OPERACAO = ? AND NIVEL = 'INFO' AND DH_LOG >= ?");
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
            stmt = conn.prepareStatement("SELECT TOP (?) DH_LOG, OPERACAO, NIVEL, MENSAGEM FROM AD_FCLOG ORDER BY DH_LOG DESC");
            stmt.setInt(1, limit);
            rs = stmt.executeQuery();

            while (rs.next()) {
                Map<String, Object> logItem = new HashMap<>();
                logItem.put("timestamp", rs.getTimestamp("DH_LOG"));
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
