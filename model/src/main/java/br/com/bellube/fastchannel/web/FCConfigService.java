package br.com.bellube.fastchannel.web;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.util.DBUtil;
import br.com.bellube.fastchannel.util.DbColumnSupport;

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

            stmt = conn.prepareStatement("SELECT TOP 1 * FROM AD_FCCONFIG ORDER BY CODCONFIG DESC");
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
                result.put("subscriptionKey", subscriptionKey);
                result.put("subscriptionKeyDistribution", subscriptionKeyDistribution);
                result.put("subscriptionKeyConsumption", subscriptionKeyConsumption);
                result.put("timeout", rs.getBigDecimal("TIMEOUT_MS"));
                result.put("intervalOrders", rs.getBigDecimal("INTERVAL_ORDERS"));
                result.put("intervalQueue", rs.getBigDecimal("INTERVAL_QUEUE"));
                result.put("logRetention", rs.getBigDecimal("LOG_RETENTION_DAYS"));
                result.put("maxRetries", rs.getBigDecimal("MAX_RETRIES"));
                if (DbColumnSupport.hasColumn(rs, "PRICE_TABLE_TIPOS")) {
                    result.put("priceTableTipos", rs.getString("PRICE_TABLE_TIPOS"));
                }
                if (DbColumnSupport.hasColumn(rs, "PRICE_TABLE_IDS")) {
                    result.put("priceTableIds", rs.getString("PRICE_TABLE_IDS"));
                }
                if (DbColumnSupport.hasColumn(rs, "UI_SOURCE_DEFAULT")) {
                    result.put("uiSourceDefault", rs.getObject("UI_SOURCE_DEFAULT"));
                    result.put("uiEnableSource2", "S".equals(rs.getString("UI_ENABLE_SOURCE_2")));
                    result.put("uiEnableSource3", "S".equals(rs.getString("UI_ENABLE_SOURCE_3")));
                }
                if (DbColumnSupport.hasColumn(rs, "SYNC_STATUS_ENABLED")) {
                    result.put("syncStatusEnabled", "S".equals(rs.getString("SYNC_STATUS_ENABLED")));
                }
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

            Map<String, Object> existing = new HashMap<>();
            if (count > 0) {
                try (PreparedStatement readStmt = conn.prepareStatement("SELECT TOP 1 * FROM AD_FCCONFIG ORDER BY CODCONFIG DESC")) {
                    try (ResultSet readRs = readStmt.executeQuery()) {
                        if (readRs.next()) {
                            existing.put("codEmp", readRs.getBigDecimal("CODEMP"));
                            existing.put("tipNeg", readRs.getBigDecimal("TIPNEG"));
                            BigDecimal topPedidoExisting = readRs.getBigDecimal("CODTIPOPER");
                            if (topPedidoExisting == null) {
                                topPedidoExisting = readRs.getBigDecimal("TOP_PEDIDO");
                            }
                            existing.put("topPedido", topPedidoExisting);
                            existing.put("codParc", readRs.getBigDecimal("CODPARC_PADRAO"));
                            existing.put("subscriptionKey", readRs.getString("SUBSCRIPTION_KEY"));
                            existing.put("subscriptionKeyDistribution", readRs.getString("SUBSCRIPTION_KEY_DISTRIBUTION"));
                            existing.put("subscriptionKeyConsumption", readRs.getString("SUBSCRIPTION_KEY_CONSUMPTION"));
                            if (DbColumnSupport.hasColumn(readRs, "PRICE_TABLE_TIPOS")) {
                                existing.put("priceTableTipos", readRs.getString("PRICE_TABLE_TIPOS"));
                            }
                            if (DbColumnSupport.hasColumn(readRs, "PRICE_TABLE_IDS")) {
                                existing.put("priceTableIds", readRs.getString("PRICE_TABLE_IDS"));
                            }
                            if (DbColumnSupport.hasColumn(readRs, "UI_SOURCE_DEFAULT")) {
                                existing.put("uiSourceDefault", readRs.getObject("UI_SOURCE_DEFAULT"));
                                existing.put("uiEnableSource2", readRs.getString("UI_ENABLE_SOURCE_2"));
                                existing.put("uiEnableSource3", readRs.getString("UI_ENABLE_SOURCE_3"));
                            }
                        }
                    }
                }
            }

            BigDecimal codEmp = getBigDecimalOrExisting(params, "codEmp", existing.get("codEmp"));
            BigDecimal tipNeg = getBigDecimalOrExisting(params, "tipNeg", existing.get("tipNeg"));
            BigDecimal topPedidoFinal = getBigDecimalOrExisting(params, "topPedido", existing.get("topPedido"));
            BigDecimal codParc = getBigDecimalOrExisting(params, "codParc", existing.get("codParc"));
            String priceTableTipos = getStringOrExisting(params, "priceTableTipos", existing.get("priceTableTipos"));
            String priceTableIds = getStringOrExisting(params, "priceTableIds", existing.get("priceTableIds"));
            String subscriptionKeyFinal = getStringOrExisting(params, "subscriptionKey", existing.get("subscriptionKey"));
            String subscriptionKeyDistributionFinal = getStringOrExisting(params, "subscriptionKeyDistribution", existing.get("subscriptionKeyDistribution"));
            String subscriptionKeyConsumptionFinal = getStringOrExisting(params, "subscriptionKeyConsumption", existing.get("subscriptionKeyConsumption"));
            Object uiSourceDefault = params.get("uiSourceDefault") != null ? params.get("uiSourceDefault") : existing.get("uiSourceDefault");
            String uiEnableSource2 = params.containsKey("uiEnableSource2") ? (getBoolean(params, "uiEnableSource2") ? "S" : "N") : (existing.get("uiEnableSource2") != null ? existing.get("uiEnableSource2").toString() : null);
            String uiEnableSource3 = params.containsKey("uiEnableSource3") ? (getBoolean(params, "uiEnableSource3") ? "S" : "N") : (existing.get("uiEnableSource3") != null ? existing.get("uiEnableSource3").toString() : null);

            boolean hasPriceTableTipos = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "PRICE_TABLE_TIPOS");
            boolean hasPriceTableIds = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "PRICE_TABLE_IDS");
            boolean hasUiSourceDefault = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "UI_SOURCE_DEFAULT");
            boolean hasUiSource2 = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "UI_ENABLE_SOURCE_2");
            boolean hasUiSource3 = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "UI_ENABLE_SOURCE_3");
            boolean hasSyncStatusEnabled = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "SYNC_STATUS_ENABLED");

            String sql;
            if (count == 0) {
                // INSERT
                sql = "INSERT INTO AD_FCCONFIG (" +
                        "ATIVO, CODEMP, TIPNEG, CODTIPOPER, TOP_PEDIDO, CODPARC_PADRAO, " +
                        "CLIENT_ID, CLIENT_SECRET, AUTH_URL, SCOPE, BASE_URL, " +
                        "SUBSCRIPTION_KEY, SUBSCRIPTION_KEY_DISTRIBUTION, SUBSCRIPTION_KEY_CONSUMPTION, " +
                        "TIMEOUT_MS, INTERVAL_ORDERS, INTERVAL_QUEUE, " +
                        "LOG_RETENTION_DAYS, MAX_RETRIES";
                if (hasPriceTableTipos) sql += ", PRICE_TABLE_TIPOS";
                if (hasPriceTableIds) sql += ", PRICE_TABLE_IDS";
                if (hasUiSourceDefault) sql += ", UI_SOURCE_DEFAULT";
                if (hasUiSource2) sql += ", UI_ENABLE_SOURCE_2";
                if (hasUiSource3) sql += ", UI_ENABLE_SOURCE_3";
                if (hasSyncStatusEnabled) sql += ", SYNC_STATUS_ENABLED";
                sql += ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?";
                if (hasPriceTableTipos) sql += ", ?";
                if (hasPriceTableIds) sql += ", ?";
                if (hasUiSourceDefault) sql += ", ?";
                if (hasUiSource2) sql += ", ?";
                if (hasUiSource3) sql += ", ?";
                if (hasSyncStatusEnabled) sql += ", ?";
                sql += ")";
            } else {
                // UPDATE (ultimo registro)
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
                if (hasPriceTableTipos) sql += ", PRICE_TABLE_TIPOS = ?";
                if (hasPriceTableIds) sql += ", PRICE_TABLE_IDS = ?";
                if (hasUiSourceDefault) sql += ", UI_SOURCE_DEFAULT = ?";
                if (hasUiSource2) sql += ", UI_ENABLE_SOURCE_2 = ?";
                if (hasUiSource3) sql += ", UI_ENABLE_SOURCE_3 = ?";
                if (hasSyncStatusEnabled) sql += ", SYNC_STATUS_ENABLED = ?";
                sql += " WHERE CODCONFIG = (SELECT MAX(CODCONFIG) FROM AD_FCCONFIG)";
            }

            stmt = conn.prepareStatement(sql);
            int idx = 1;
            stmt.setString(idx++, getBoolean(params, "ativo") ? "S" : "N");
            stmt.setBigDecimal(idx++, codEmp);
            stmt.setBigDecimal(idx++, tipNeg);
            stmt.setBigDecimal(idx++, topPedidoFinal);
            stmt.setBigDecimal(idx++, topPedidoFinal);
            stmt.setBigDecimal(idx++, codParc);
            stmt.setString(idx++, getString(params, "clientId"));
            stmt.setString(idx++, getString(params, "clientSecret"));
            stmt.setString(idx++, getString(params, "authUrl"));
            stmt.setString(idx++, getString(params, "scope"));
            stmt.setString(idx++, getString(params, "baseUrl"));
            stmt.setString(idx++, subscriptionKeyFinal);
            stmt.setString(idx++, subscriptionKeyDistributionFinal);
            stmt.setString(idx++, subscriptionKeyConsumptionFinal);
            stmt.setBigDecimal(idx++, getBigDecimal(params, "timeout"));
            stmt.setBigDecimal(idx++, getBigDecimal(params, "intervalOrders"));
            stmt.setBigDecimal(idx++, getBigDecimal(params, "intervalQueue"));
            stmt.setBigDecimal(idx++, getBigDecimal(params, "logRetention"));
            stmt.setBigDecimal(idx++, getBigDecimal(params, "maxRetries"));
            if (hasPriceTableTipos) stmt.setString(idx++, priceTableTipos);
            if (hasPriceTableIds) stmt.setString(idx++, priceTableIds);
            if (hasUiSourceDefault) stmt.setObject(idx++, uiSourceDefault);
            if (hasUiSource2) stmt.setString(idx++, uiEnableSource2);
            if (hasUiSource3) stmt.setString(idx++, uiEnableSource3);
            if (hasSyncStatusEnabled) stmt.setString(idx++, getBoolean(params, "syncStatusEnabled") ? "S" : "N");

            stmt.executeUpdate();
            FastchannelConfig.getInstance().reload();

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

    private BigDecimal getBigDecimalOrExisting(Map<String, Object> params, String key, Object existingValue) {
        if (params.containsKey(key)) {
            return getBigDecimal(params, key);
        }
        if (existingValue instanceof BigDecimal) {
            return (BigDecimal) existingValue;
        }
        if (existingValue instanceof Number) {
            return BigDecimal.valueOf(((Number) existingValue).doubleValue());
        }
        if (existingValue != null) {
            try {
                return new BigDecimal(existingValue.toString());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private String getStringOrExisting(Map<String, Object> params, String key, Object existingValue) {
        if (params.containsKey(key)) {
            return getString(params, key);
        }
        return existingValue != null ? existingValue.toString() : null;
    }

    private boolean getBoolean(Map<String, Object> params, String key) {
        Object value = params.get(key);
        if (value == null) return false;
        if (value instanceof Boolean) return (Boolean) value;
        return "true".equalsIgnoreCase(value.toString()) || "S".equalsIgnoreCase(value.toString());
    }
}
