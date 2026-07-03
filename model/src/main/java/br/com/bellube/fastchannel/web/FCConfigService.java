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
                if (DbColumnSupport.hasColumn(rs, "SANKHYA_SERVER_URL")) {
                    result.put("sankhyaServerUrl", rs.getString("SANKHYA_SERVER_URL"));
                }
                if (DbColumnSupport.hasColumn(rs, "SANKHYA_USER")) {
                    result.put("sankhyaUser", rs.getString("SANKHYA_USER"));
                }
                if (DbColumnSupport.hasColumn(rs, "SANKHYA_PASSWORD")) {
                    result.put("sankhyaPassword", rs.getString("SANKHYA_PASSWORD"));
                }
                if (DbColumnSupport.hasColumn(rs, "SANKHYA_OAUTH_CLIENT_ID")) {
                    result.put("sankhyaOAuthClientId", rs.getString("SANKHYA_OAUTH_CLIENT_ID"));
                }
                if (DbColumnSupport.hasColumn(rs, "SANKHYA_OAUTH_CLIENT_SECRET")) {
                    result.put("sankhyaOAuthClientSecret", rs.getString("SANKHYA_OAUTH_CLIENT_SECRET"));
                }
                if (DbColumnSupport.hasColumn(rs, "SANKHYA_GATEWAY_X_TOKEN")) {
                    result.put("sankhyaGatewayXToken", rs.getString("SANKHYA_GATEWAY_X_TOKEN"));
                }
                if (DbColumnSupport.hasColumn(rs, "DISABLE_DUPLICATE_CHECK")) {
                    result.put("disableDuplicateCheck", "S".equals(rs.getString("DISABLE_DUPLICATE_CHECK")));
                }
                if (DbColumnSupport.hasColumn(rs, "EMAIL_NOTIFICACAO")) {
                    result.put("emailNotificacao", rs.getString("EMAIL_NOTIFICACAO"));
                    result.put("emailHabilitado", "S".equals(rs.getString("EMAIL_HABILITADO")));
                    result.put("smtpHost", rs.getString("SMTP_HOST"));
                }
                if (DbColumnSupport.hasColumn(rs, "STORAGE_ID")) {
                    result.put("storageId", rs.getString("STORAGE_ID"));
                }
                if (DbColumnSupport.hasColumn(rs, "RESELLER_ID")) {
                    result.put("resellerId", rs.getString("RESELLER_ID"));
                }
            } else {
                // Valores padrao
                result.put("ativo", false);
                result.put("timeout", 30000);
                result.put("intervalOrders", 5);
                result.put("intervalQueue", 2);
                result.put("logRetention", 30);
                result.put("maxRetries", 3);
                result.put("disableDuplicateCheck", false);
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
                            existing.put("ativo", readRs.getString("ATIVO"));
                            existing.put("clientId", readRs.getString("CLIENT_ID"));
                            existing.put("clientSecret", readRs.getString("CLIENT_SECRET"));
                            existing.put("authUrl", readRs.getString("AUTH_URL"));
                            existing.put("scope", readRs.getString("SCOPE"));
                            existing.put("baseUrl", readRs.getString("BASE_URL"));
                            existing.put("subscriptionKey", readRs.getString("SUBSCRIPTION_KEY"));
                            existing.put("subscriptionKeyDistribution", readRs.getString("SUBSCRIPTION_KEY_DISTRIBUTION"));
                            existing.put("subscriptionKeyConsumption", readRs.getString("SUBSCRIPTION_KEY_CONSUMPTION"));
                            existing.put("timeout", readRs.getObject("TIMEOUT_MS"));
                            existing.put("intervalOrders", readRs.getObject("INTERVAL_ORDERS"));
                            existing.put("intervalQueue", readRs.getObject("INTERVAL_QUEUE"));
                            existing.put("logRetention", readRs.getObject("LOG_RETENTION_DAYS"));
                            existing.put("maxRetries", readRs.getObject("MAX_RETRIES"));
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
                            if (DbColumnSupport.hasColumn(readRs, "SANKHYA_SERVER_URL")) {
                                existing.put("sankhyaServerUrl", readRs.getString("SANKHYA_SERVER_URL"));
                            }
                            if (DbColumnSupport.hasColumn(readRs, "SANKHYA_USER")) {
                                existing.put("sankhyaUser", readRs.getString("SANKHYA_USER"));
                            }
                            if (DbColumnSupport.hasColumn(readRs, "SANKHYA_PASSWORD")) {
                                existing.put("sankhyaPassword", readRs.getString("SANKHYA_PASSWORD"));
                            }
                            if (DbColumnSupport.hasColumn(readRs, "SANKHYA_OAUTH_CLIENT_ID")) {
                                existing.put("sankhyaOAuthClientId", readRs.getString("SANKHYA_OAUTH_CLIENT_ID"));
                            }
                            if (DbColumnSupport.hasColumn(readRs, "SANKHYA_OAUTH_CLIENT_SECRET")) {
                                existing.put("sankhyaOAuthClientSecret", readRs.getString("SANKHYA_OAUTH_CLIENT_SECRET"));
                            }
                            if (DbColumnSupport.hasColumn(readRs, "SANKHYA_GATEWAY_X_TOKEN")) {
                                existing.put("sankhyaGatewayXToken", readRs.getString("SANKHYA_GATEWAY_X_TOKEN"));
                            }
                            if (DbColumnSupport.hasColumn(readRs, "DISABLE_DUPLICATE_CHECK")) {
                                existing.put("disableDuplicateCheck", readRs.getString("DISABLE_DUPLICATE_CHECK"));
                            }
                            if (DbColumnSupport.hasColumn(readRs, "EMAIL_NOTIFICACAO")) {
                                existing.put("emailNotificacao", readRs.getString("EMAIL_NOTIFICACAO"));
                                existing.put("emailHabilitado", readRs.getString("EMAIL_HABILITADO"));
                                existing.put("smtpHost", readRs.getString("SMTP_HOST"));
                            }
                            if (DbColumnSupport.hasColumn(readRs, "STORAGE_ID")) {
                                existing.put("storageId", readRs.getString("STORAGE_ID"));
                            }
                            if (DbColumnSupport.hasColumn(readRs, "RESELLER_ID")) {
                                existing.put("resellerId", readRs.getString("RESELLER_ID"));
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
            String sankhyaServerUrl = getStringOrExisting(params, "sankhyaServerUrl", existing.get("sankhyaServerUrl"));
            String sankhyaUser = getStringOrExisting(params, "sankhyaUser", existing.get("sankhyaUser"));
            String sankhyaPassword = getStringOrExisting(params, "sankhyaPassword", existing.get("sankhyaPassword"));
            String sankhyaOAuthClientId = getStringOrExisting(params, "sankhyaOAuthClientId", existing.get("sankhyaOAuthClientId"));
            String sankhyaOAuthClientSecret = getStringOrExisting(params, "sankhyaOAuthClientSecret", existing.get("sankhyaOAuthClientSecret"));
            String sankhyaGatewayXToken = getStringOrExisting(params, "sankhyaGatewayXToken", existing.get("sankhyaGatewayXToken"));
            String disableDuplicateCheck = params.containsKey("disableDuplicateCheck")
                    ? (getBoolean(params, "disableDuplicateCheck") ? "S" : "N")
                    : (existing.get("disableDuplicateCheck") != null ? existing.get("disableDuplicateCheck").toString() : null);

            boolean hasPriceTableTipos = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "PRICE_TABLE_TIPOS");
            boolean hasPriceTableIds = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "PRICE_TABLE_IDS");
            boolean hasUiSourceDefault = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "UI_SOURCE_DEFAULT");
            boolean hasUiSource2 = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "UI_ENABLE_SOURCE_2");
            boolean hasUiSource3 = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "UI_ENABLE_SOURCE_3");
            boolean hasSyncStatusEnabled = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "SYNC_STATUS_ENABLED");
            boolean hasSankhyaServerUrl = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "SANKHYA_SERVER_URL");
            boolean hasSankhyaUser = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "SANKHYA_USER");
            boolean hasSankhyaPassword = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "SANKHYA_PASSWORD");
            boolean hasSankhyaOAuthClientId = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "SANKHYA_OAUTH_CLIENT_ID");
            boolean hasSankhyaOAuthClientSecret = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "SANKHYA_OAUTH_CLIENT_SECRET");
            boolean hasSankhyaGatewayXToken = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "SANKHYA_GATEWAY_X_TOKEN");
            boolean hasDisableDuplicateCheck = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "DISABLE_DUPLICATE_CHECK");
            boolean hasEmailNotificacao = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "EMAIL_NOTIFICACAO");
            boolean hasEmailHabilitado = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "EMAIL_HABILITADO");
            boolean hasSmtpHost = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "SMTP_HOST");
            boolean hasStorageId = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "STORAGE_ID");
            boolean hasResellerId = DbColumnSupport.hasColumn(conn, "AD_FCCONFIG", "RESELLER_ID");
            String emailNotificacao = getStringOrExisting(params, "emailNotificacao", existing.get("emailNotificacao"));
            String emailHabilitado = params.containsKey("emailHabilitado") ? (getBoolean(params, "emailHabilitado") ? "S" : "N") : (existing.get("emailHabilitado") != null ? existing.get("emailHabilitado").toString() : "N");
            String smtpHost = getStringOrExisting(params, "smtpHost", existing.get("smtpHost"));
            String storageIdVal = getStringOrExisting(params, "storageId", existing.get("storageId"));
            String resellerIdVal = getStringOrExisting(params, "resellerId", existing.get("resellerId"));

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
                if (hasSankhyaServerUrl) sql += ", SANKHYA_SERVER_URL";
                if (hasSankhyaUser) sql += ", SANKHYA_USER";
                if (hasSankhyaPassword) sql += ", SANKHYA_PASSWORD";
                if (hasSankhyaOAuthClientId) sql += ", SANKHYA_OAUTH_CLIENT_ID";
                if (hasSankhyaOAuthClientSecret) sql += ", SANKHYA_OAUTH_CLIENT_SECRET";
                if (hasSankhyaGatewayXToken) sql += ", SANKHYA_GATEWAY_X_TOKEN";
                if (hasDisableDuplicateCheck) sql += ", DISABLE_DUPLICATE_CHECK";
                if (hasEmailNotificacao) sql += ", EMAIL_NOTIFICACAO";
                if (hasEmailHabilitado) sql += ", EMAIL_HABILITADO";
                if (hasSmtpHost) sql += ", SMTP_HOST";
                if (hasStorageId) sql += ", STORAGE_ID";
                if (hasResellerId) sql += ", RESELLER_ID";
                sql += ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?";
                if (hasPriceTableTipos) sql += ", ?";
                if (hasPriceTableIds) sql += ", ?";
                if (hasUiSourceDefault) sql += ", ?";
                if (hasUiSource2) sql += ", ?";
                if (hasUiSource3) sql += ", ?";
                if (hasSyncStatusEnabled) sql += ", ?";
                if (hasSankhyaServerUrl) sql += ", ?";
                if (hasSankhyaUser) sql += ", ?";
                if (hasSankhyaPassword) sql += ", ?";
                if (hasSankhyaOAuthClientId) sql += ", ?";
                if (hasSankhyaOAuthClientSecret) sql += ", ?";
                if (hasSankhyaGatewayXToken) sql += ", ?";
                if (hasDisableDuplicateCheck) sql += ", ?";
                if (hasEmailNotificacao) sql += ", ?";
                if (hasEmailHabilitado) sql += ", ?";
                if (hasSmtpHost) sql += ", ?";
                if (hasStorageId) sql += ", ?";
                if (hasResellerId) sql += ", ?";
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
                if (hasSankhyaServerUrl) sql += ", SANKHYA_SERVER_URL = ?";
                if (hasSankhyaUser) sql += ", SANKHYA_USER = ?";
                if (hasSankhyaPassword) sql += ", SANKHYA_PASSWORD = ?";
                if (hasSankhyaOAuthClientId) sql += ", SANKHYA_OAUTH_CLIENT_ID = ?";
                if (hasSankhyaOAuthClientSecret) sql += ", SANKHYA_OAUTH_CLIENT_SECRET = ?";
                if (hasSankhyaGatewayXToken) sql += ", SANKHYA_GATEWAY_X_TOKEN = ?";
                if (hasDisableDuplicateCheck) sql += ", DISABLE_DUPLICATE_CHECK = ?";
                if (hasEmailNotificacao) sql += ", EMAIL_NOTIFICACAO = ?";
                if (hasEmailHabilitado) sql += ", EMAIL_HABILITADO = ?";
                if (hasSmtpHost) sql += ", SMTP_HOST = ?";
                if (hasStorageId) sql += ", STORAGE_ID = ?";
                if (hasResellerId) sql += ", RESELLER_ID = ?";
                sql += " WHERE CODCONFIG = (SELECT MAX(CODCONFIG) FROM AD_FCCONFIG)";
            }

            // Ler valores existentes para OAuth (evitar sobrescrever com null em save parcial)
            String existingClientId = existing.containsKey("clientId") ? (String) existing.get("clientId") : null;
            String existingClientSecret = existing.containsKey("clientSecret") ? (String) existing.get("clientSecret") : null;
            String existingAuthUrl = existing.containsKey("authUrl") ? (String) existing.get("authUrl") : null;
            String existingScope = existing.containsKey("scope") ? (String) existing.get("scope") : null;
            String existingBaseUrl = existing.containsKey("baseUrl") ? (String) existing.get("baseUrl") : null;
            String existingAtivo = existing.containsKey("ativo") ? (String) existing.get("ativo") : null;

            stmt = conn.prepareStatement(sql);
            int idx = 1;
            // Ativo: preservar valor existente se nao enviado
            String ativoFinal = params.containsKey("ativo")
                    ? (getBoolean(params, "ativo") ? "S" : "N")
                    : (existingAtivo != null ? existingAtivo : "N");
            stmt.setString(idx++, ativoFinal);
            stmt.setBigDecimal(idx++, codEmp);
            stmt.setBigDecimal(idx++, tipNeg);
            stmt.setBigDecimal(idx++, topPedidoFinal);
            stmt.setBigDecimal(idx++, topPedidoFinal);
            stmt.setBigDecimal(idx++, codParc);
            stmt.setString(idx++, getStringOrExisting(params, "clientId", existingClientId));
            stmt.setString(idx++, getStringOrExisting(params, "clientSecret", existingClientSecret));
            stmt.setString(idx++, getStringOrExisting(params, "authUrl", existingAuthUrl));
            stmt.setString(idx++, getStringOrExisting(params, "scope", existingScope));
            stmt.setString(idx++, getStringOrExisting(params, "baseUrl", existingBaseUrl));
            stmt.setString(idx++, subscriptionKeyFinal);
            stmt.setString(idx++, subscriptionKeyDistributionFinal);
            stmt.setString(idx++, subscriptionKeyConsumptionFinal);
            stmt.setBigDecimal(idx++, getBigDecimalOrExisting(params, "timeout", existing.get("timeout")));
            stmt.setBigDecimal(idx++, getBigDecimalOrExisting(params, "intervalOrders", existing.get("intervalOrders")));
            stmt.setBigDecimal(idx++, getBigDecimalOrExisting(params, "intervalQueue", existing.get("intervalQueue")));
            stmt.setBigDecimal(idx++, getBigDecimalOrExisting(params, "logRetention", existing.get("logRetention")));
            stmt.setBigDecimal(idx++, getBigDecimalOrExisting(params, "maxRetries", existing.get("maxRetries")));
            if (hasPriceTableTipos) stmt.setString(idx++, priceTableTipos);
            if (hasPriceTableIds) stmt.setString(idx++, priceTableIds);
            if (hasUiSourceDefault) stmt.setObject(idx++, uiSourceDefault);
            if (hasUiSource2) stmt.setString(idx++, uiEnableSource2);
            if (hasUiSource3) stmt.setString(idx++, uiEnableSource3);
            if (hasSyncStatusEnabled) {
                String syncVal = params.containsKey("syncStatusEnabled")
                        ? (getBoolean(params, "syncStatusEnabled") ? "S" : "N")
                        : (existing.containsKey("syncStatusEnabled") ? (String) existing.get("syncStatusEnabled") : "N");
                stmt.setString(idx++, syncVal);
            }
            if (hasSankhyaServerUrl) stmt.setString(idx++, sankhyaServerUrl);
            if (hasSankhyaUser) stmt.setString(idx++, sankhyaUser);
            if (hasSankhyaPassword) stmt.setString(idx++, sankhyaPassword);
            if (hasSankhyaOAuthClientId) stmt.setString(idx++, sankhyaOAuthClientId);
            if (hasSankhyaOAuthClientSecret) stmt.setString(idx++, sankhyaOAuthClientSecret);
            if (hasSankhyaGatewayXToken) stmt.setString(idx++, sankhyaGatewayXToken);
            if (hasDisableDuplicateCheck) stmt.setString(idx++, disableDuplicateCheck);
            if (hasEmailNotificacao) stmt.setString(idx++, emailNotificacao);
            if (hasEmailHabilitado) stmt.setString(idx++, emailHabilitado);
            if (hasSmtpHost) stmt.setString(idx++, smtpHost);
            if (hasStorageId) stmt.setString(idx++, storageIdVal);
            if (hasResellerId) stmt.setString(idx++, resellerIdVal);

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
