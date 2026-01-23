package br.com.bellube.fastchannel.web;

import br.com.bellube.fastchannel.util.DBUtil;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service para gerenciamento de configuracoes.
 */
public class FCConfigService {

    private static final Logger log = Logger.getLogger(FCConfigService.class.getName());

    public Map<String, Object> get(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();

            stmt = conn.prepareStatement("SELECT * FROM AD_FCCONFIG ORDER BY CODCONFIG");
            rs = stmt.executeQuery();

            if (rs.next()) {
                result.put("id", rs.getBigDecimal("CODCONFIG"));
                result.put("ativo", "S".equals(rs.getString("ATIVO")));
                result.put("codEmp", rs.getBigDecimal("CODEMP"));
                result.put("tipNeg", rs.getBigDecimal("TIPNEG"));
                BigDecimal topPedido = rs.getBigDecimal("CODTIPOPER");
                if (topPedido == null) {
                    topPedido = rs.getBigDecimal("TOP_PEDIDO");
                }
                result.put("topPedido", topPedido);
                result.put("codParc", rs.getBigDecimal("CODPARC_PADRAO"));
                result.put("clientId", rs.getString("CLIENT_ID"));
                result.put("clientSecret", rs.getString("CLIENT_SECRET"));
                result.put("authUrl", rs.getString("AUTH_URL"));
                result.put("scope", rs.getString("SCOPE"));
                result.put("baseUrl", rs.getString("BASE_URL"));
                String subscriptionKey = rs.getString("SUBSCRIPTION_KEY");
                String subscriptionKeyDistribution = rs.getString("SUBSCRIPTION_KEY_DISTRIBUTION");
                String subscriptionKeyConsumption = rs.getString("SUBSCRIPTION_KEY_CONSUMPTION");
                if (isBlank(subscriptionKeyDistribution)) {
                    subscriptionKeyDistribution = subscriptionKey;
                }
                if (isBlank(subscriptionKeyConsumption)) {
                    subscriptionKeyConsumption = subscriptionKey;
                }
                result.put("subscriptionKeyDistribution", subscriptionKeyDistribution);
                result.put("subscriptionKeyConsumption", subscriptionKeyConsumption);
                result.put("subscriptionKey", subscriptionKeyDistribution);
                result.put("timeout", rs.getBigDecimal("TIMEOUT_MS"));
                result.put("intervalOrders", rs.getBigDecimal("INTERVAL_ORDERS"));
                result.put("intervalQueue", rs.getBigDecimal("INTERVAL_QUEUE"));
                result.put("logRetention", rs.getBigDecimal("LOG_RETENTION_DAYS"));
                result.put("maxRetries", rs.getBigDecimal("MAX_RETRIES"));
            } else {
                // Valores padrao
                result.put("ativo", false);
                result.put("timeout", 30000);
                result.put("intervalOrders", 5);
                result.put("intervalQueue", 2);
                result.put("logRetention", 30);
                result.put("maxRetries", 3);
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao carregar config", e);
            result.put("error", e.getMessage());
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }

        return result;
    }

    public Map<String, Object> save(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;
        PreparedStatement checkStmt = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();

            // Check if exists
            checkStmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM AD_FCCONFIG");
            rs = checkStmt.executeQuery();
            rs.next();
            int count = rs.getInt("CNT");
            rs.close();
            checkStmt.close();

            BigDecimal topPedido = getBigDecimal(params, "topPedido");
            String subscriptionKey = getString(params, "subscriptionKey");
            String subscriptionKeyDistribution = getString(params, "subscriptionKeyDistribution");
            String subscriptionKeyConsumption = getString(params, "subscriptionKeyConsumption");
            if (isBlank(subscriptionKeyDistribution)) {
                subscriptionKeyDistribution = subscriptionKey;
            }
            if (isBlank(subscriptionKeyConsumption)) {
                subscriptionKeyConsumption = subscriptionKey;
            }
            if (isBlank(subscriptionKey)) {
                subscriptionKey = subscriptionKeyDistribution;
            }

            String sql;
            if (count == 0) {
                // INSERT
                sql = "INSERT INTO AD_FCCONFIG (" +
                        "ATIVO, CODEMP, TIPNEG, CODTIPOPER, TOP_PEDIDO, CODPARC_PADRAO, " +
                        "CLIENT_ID, CLIENT_SECRET, AUTH_URL, SCOPE, BASE_URL, " +
                        "SUBSCRIPTION_KEY, SUBSCRIPTION_KEY_DISTRIBUTION, SUBSCRIPTION_KEY_CONSUMPTION, " +
                        "TIMEOUT_MS, INTERVAL_ORDERS, INTERVAL_QUEUE, " +
                        "LOG_RETENTION_DAYS, MAX_RETRIES" +
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            } else {
                // UPDATE
                sql = "UPDATE AD_FCCONFIG SET " +
                        "ATIVO = ?, CODEMP = ?, TIPNEG = ?, " +
                        "CODTIPOPER = ?, TOP_PEDIDO = ?, CODPARC_PADRAO = ?, " +
                        "CLIENT_ID = ?, CLIENT_SECRET = ?, " +
                        "AUTH_URL = ?, SCOPE = ?, BASE_URL = ?, " +
                        "SUBSCRIPTION_KEY = ?, " +
                        "SUBSCRIPTION_KEY_DISTRIBUTION = ?, " +
                        "SUBSCRIPTION_KEY_CONSUMPTION = ?, " +
                        "TIMEOUT_MS = ?, " +
                        "INTERVAL_ORDERS = ?, INTERVAL_QUEUE = ?, " +
                        "LOG_RETENTION_DAYS = ?, MAX_RETRIES = ?";
            }

            stmt = conn.prepareStatement(sql);
            int idx = 1;
            stmt.setString(idx++, getBoolean(params, "ativo") ? "S" : "N");
            stmt.setBigDecimal(idx++, getBigDecimal(params, "codEmp"));
            stmt.setBigDecimal(idx++, getBigDecimal(params, "tipNeg"));
            stmt.setBigDecimal(idx++, topPedido);
            stmt.setBigDecimal(idx++, topPedido);
            stmt.setBigDecimal(idx++, getBigDecimal(params, "codParc"));
            stmt.setString(idx++, getString(params, "clientId"));
            stmt.setString(idx++, getString(params, "clientSecret"));
            stmt.setString(idx++, getString(params, "authUrl"));
            stmt.setString(idx++, getString(params, "scope"));
            stmt.setString(idx++, getString(params, "baseUrl"));
            stmt.setString(idx++, subscriptionKey);
            stmt.setString(idx++, subscriptionKeyDistribution);
            stmt.setString(idx++, subscriptionKeyConsumption);
            stmt.setBigDecimal(idx++, getBigDecimal(params, "timeout"));
            stmt.setBigDecimal(idx++, getBigDecimal(params, "intervalOrders"));
            stmt.setBigDecimal(idx++, getBigDecimal(params, "intervalQueue"));
            stmt.setBigDecimal(idx++, getBigDecimal(params, "logRetention"));
            stmt.setBigDecimal(idx++, getBigDecimal(params, "maxRetries"));

            stmt.executeUpdate();

            result.put("success", true);
            result.put("message", "Configuracoes salvas com sucesso!");

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao salvar config", e);
            result.put("success", false);
            result.put("message", "Erro ao salvar: " + e.getMessage());
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
            DBUtil.closeStatement(checkStmt);
        }

        return result;
    }

    private String getString(Map<String, Object> params, String key) {
        Object value = params.get(key);
        return value != null ? value.toString() : null;
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private BigDecimal getBigDecimal(Map<String, Object> params, String key) {
        Object value = params.get(key);
        if (value == null) return null;
        if (value instanceof Number) {
            return BigDecimal.valueOf(((Number) value).doubleValue());
        }
        try {
            return new BigDecimal(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private boolean getBoolean(Map<String, Object> params, String key) {
        Object value = params.get(key);
        if (value == null) return false;
        if (value instanceof Boolean) return (Boolean) value;
        return "true".equalsIgnoreCase(value.toString()) || "S".equalsIgnoreCase(value.toString());
    }
}
