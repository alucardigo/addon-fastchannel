package br.com.bellube.fastchannel.web;

import br.com.bellube.fastchannel.auth.FastchannelTokenManager;
import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.http.FastchannelHttpClient;
import br.com.bellube.fastchannel.http.FastchannelOrdersClient;
import br.com.bellube.fastchannel.job.OutboxProcessorJob;
import br.com.bellube.fastchannel.service.OrderService;
import br.com.bellube.fastchannel.service.QueueService;
import br.com.bellube.fastchannel.util.DBUtil;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service para operacoes administrativas.
 */
public class FCAdminService {

    private static final Logger log = Logger.getLogger(FCAdminService.class.getName());

    public Map<String, Object> testarConexao(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        StringBuilder mensagem = new StringBuilder();
        boolean sucesso = true;

        try {
            mensagem.append("=== Teste de Conexao Fastchannel ===\n\n");

            // 1. Verificar configuracao
            mensagem.append("1. Verificando configuracao...\n");
            FastchannelConfig config = FastchannelConfig.getInstance();

            if (!config.isAtivo()) {
                mensagem.append("   [AVISO] Integracao esta desativada (nao bloqueia conexao)\n");
            } else {
                mensagem.append("   [OK] Integracao ativa\n");
            }

            if (config.getClientId() == null || config.getClientId().isEmpty()) {
                mensagem.append("   [ERRO] Client ID nao configurado!\n");
                sucesso = false;
            } else {
                mensagem.append("   [OK] Client ID configurado\n");
            }

            if (config.getClientSecret() == null || config.getClientSecret().isEmpty()) {
                mensagem.append("   [ERRO] Client Secret nao configurado!\n");
                sucesso = false;
            } else {
                mensagem.append("   [OK] Client Secret configurado\n");
            }

            if (config.getBaseUrl() == null || config.getBaseUrl().isEmpty()) {
                mensagem.append("   [ERRO] URL Base nao configurada!\n");
                sucesso = false;
            } else {
                mensagem.append("   [OK] URL Base: ").append(config.getBaseUrl()).append("\n");
            }

            // 2. Testar autenticacao OAuth2
            if (sucesso) {
                mensagem.append("\n2. Testando autenticacao OAuth2...\n");
                try {
                    FastchannelTokenManager tokenManager = FastchannelTokenManager.getInstance();
                    String token = tokenManager.getValidToken();

                    if (token != null && !token.isEmpty()) {
                        mensagem.append("   [OK] Token obtido com sucesso!\n");
                        mensagem.append("   Token (primeiros 20 chars): ")
                                .append(token.substring(0, Math.min(20, token.length()))).append("...\n");
                    } else {
                        mensagem.append("   [ERRO] Token vazio retornado!\n");
                        sucesso = false;
                    }
                } catch (Exception e) {
                    mensagem.append("   [ERRO] Falha na autenticacao: ").append(e.getMessage()).append("\n");
                    sucesso = false;
                }
            }

            // 3. Testar conectividade com API
            if (sucesso) {
                mensagem.append("\n3. Testando conectividade com API...\n");
                try {
                    FastchannelHttpClient httpClient = new FastchannelHttpClient();
                    FastchannelHttpClient.HttpResult httpResult = httpClient.getOrders("/orders?PageNumber=1&PageSize=1");

                    if (httpResult.isSuccess()) {
                        mensagem.append("   [OK] API respondeu com sucesso!\n");
                    } else {
                        mensagem.append("   [ERRO] API retornou erro HTTP ").append(httpResult.getStatusCode()).append("\n");
                        sucesso = false;
                    }
                } catch (Exception e) {
                    mensagem.append("   [ERRO] Falha na conectividade: ").append(e.getMessage()).append("\n");
                    sucesso = false;
                }
            }

            // Resultado final
            mensagem.append("\n=== Resultado Final ===\n");
            if (sucesso) {
                mensagem.append("[SUCESSO] Conexao com Fastchannel OK!");
                result.put("success", true);
            } else {
                mensagem.append("[FALHA] Problemas encontrados na conexao.");
                result.put("success", false);
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro no teste de conexao", e);
            mensagem.append("\n[ERRO FATAL] ").append(e.getMessage());
            result.put("success", false);
        }

        result.put("message", mensagem.toString());
        return result;
    }

    public Map<String, Object> importarPedidos(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();

        try {
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                result.put("success", false);
                result.put("message", "Integracao nao esta ativa!");
                return result;
            }

            boolean retryOnly = getBoolean(params, "retryOnly");
            int retryLimit = getInt(params, "retryLimit", 50);
            if (retryLimit <= 0) retryLimit = 50;

            OrderService orderService = new OrderService();
            int retriedImported = retryErroredOrders(orderService, retryLimit);
            int imported = retryOnly ? 0 : orderService.importPendingOrders();
            int totalImported = retriedImported + imported;
            String diagnostic = null;
            if (!retryOnly && imported == 0) {
                diagnostic = buildOrdersDiagnostic(config);
            }

            result.put("success", true);
            String message = "Importacao concluida! " + totalImported +
                    " pedido(s) importado(s). Reprocessados: " + retriedImported +
                    ", novos da API: " + imported + ".";
            if (diagnostic != null && !diagnostic.isEmpty()) {
                message += " " + diagnostic;
                result.put("diagnostic", diagnostic);
            }
            result.put("message", message);
            result.put("count", totalImported);
            result.put("reprocessedCount", retriedImported);
            result.put("pendingApiCount", imported);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao importar pedidos", e);
            result.put("success", false);
            result.put("message", "Erro: " + e.getMessage());
        }

        return result;
    }

    public Map<String, Object> processarFila(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();

        try {
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                result.put("success", false);
                result.put("message", "Integracao nao esta ativa!");
                return result;
            }

            QueueService queueService = QueueService.getInstance();
            int pendingBefore = queueService.countPending();

            // Processamento manual imediato da fila.
            OutboxProcessorJob job = new OutboxProcessorJob();
            job.executeScheduler();

            int pendingAfter = queueService.countPending();
            int processed = Math.max(0, pendingBefore - pendingAfter);

            result.put("success", true);
            result.put("message", "Fila processada. Pendentes antes: " + pendingBefore + ", depois: " + pendingAfter + ".");
            result.put("count", processed);
            result.put("pendingBefore", pendingBefore);
            result.put("pendingAfter", pendingAfter);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao processar fila", e);
            result.put("success", false);
            result.put("message", "Erro: " + e.getMessage());
        }

        return result;
    }

    private int retryErroredOrders(OrderService orderService, int limit) throws Exception {
        if (orderService == null || limit <= 0) return 0;

        List<String> orderIds = loadErroredOrderIds(limit);
        if (orderIds.isEmpty()) return 0;

        FastchannelOrdersClient ordersClient = new FastchannelOrdersClient();
        int imported = 0;

        for (String orderId : orderIds) {
            try {
                OrderDTO order = ordersClient.getOrder(orderId);
                if (order == null) {
                    log.warning("Pedido " + orderId + " nao encontrado no Fastchannel para reprocessamento.");
                    continue;
                }
                BigDecimal nuNota = orderService.importOrder(order);
                if (nuNota != null) {
                    imported++;
                }
            } catch (Exception e) {
                log.log(Level.WARNING, "Falha ao reprocessar pedido " + orderId, e);
            }
        }

        return imported;
    }

    private String buildOrdersDiagnostic(FastchannelConfig config) {
        try {
            FastchannelOrdersClient client = new FastchannelOrdersClient();
            Timestamp lastSync = config != null ? config.getLastOrderSync() : null;

            FastchannelOrdersClient.OrderListResult withCursor = client.listOrdersWithMeta(lastSync, 1, 1, Boolean.FALSE);
            FastchannelOrdersClient.OrderListResult withoutCursor = client.listOrdersWithMeta(null, 1, 1, Boolean.FALSE);

            int withCursorCount = estimateTotal(withCursor);
            int withoutCursorCount = estimateTotal(withoutCursor);

            return "Diagnostico API: pendentes (IsSynched=false) com cursor="
                    + (lastSync != null ? lastSync : "null")
                    + " => " + withCursorCount
                    + ", sem cursor => " + withoutCursorCount + ".";
        } catch (Exception e) {
            log.log(Level.WARNING, "Falha ao montar diagnostico de pedidos", e);
            return "Diagnostico API indisponivel: " + e.getMessage();
        }
    }

    private int estimateTotal(FastchannelOrdersClient.OrderListResult result) {
        if (result == null) return 0;
        if (result.getTotalRecords() != null) return result.getTotalRecords();
        if (result.getOrders() != null) return result.getOrders().size();
        return 0;
    }

    private List<String> loadErroredOrderIds(int limit) {
        List<String> ids = new ArrayList<>();
        if (limit <= 0) return ids;

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                    "SELECT ORDER_ID FROM AD_FCPEDIDO " +
                    "WHERE NUNOTA IS NULL " +
                    "AND UPPER(COALESCE(STATUS_IMPORT, '')) IN ('ERRO', 'PENDENTE') " +
                    "ORDER BY DH_IMPORTACAO DESC " +
                    "OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY");
            stmt.setInt(1, limit);
            rs = stmt.executeQuery();
            while (rs.next()) {
                String orderId = rs.getString("ORDER_ID");
                if (orderId != null && !orderId.trim().isEmpty()) {
                    ids.add(orderId.trim());
                }
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao carregar pedidos ERRO/PENDENTE para reprocessamento", e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return ids;
    }

    public Map<String, Object> diagnosticoSchema(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TABLE_NAME, COLUMN_NAME ");
            sql.appendSql("FROM INFORMATION_SCHEMA.COLUMNS ");
            sql.appendSql("WHERE TABLE_NAME IN ('TGFTOP','TGFCAB','TSIUSU','AD_FCCONFIG') ");
            sql.appendSql("AND (COLUMN_NAME LIKE 'CODCEN%' OR COLUMN_NAME LIKE 'CODTIP%' ");
            sql.appendSql("OR COLUMN_NAME LIKE 'CODNAT%' OR COLUMN_NAME LIKE 'CODVEND%' ");
            sql.appendSql("OR COLUMN_NAME IN ('NOMUSU','NOMEUSU','CODUSU')) ");
            sql.appendSql("ORDER BY TABLE_NAME, COLUMN_NAME");
            rs = sql.executeQuery();

            List<Map<String, Object>> cols = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("table", rs.getString("TABLE_NAME"));
                row.put("column", rs.getString("COLUMN_NAME"));
                cols.add(row);
            }

            result.put("success", true);
            result.put("columns", cols);
            result.put("count", cols.size());
        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro no diagnostico de schema", e);
            result.put("success", false);
            result.put("message", e.getMessage());
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception ignored) {}
            }
            closeJdbc(jdbc);
        }
        return result;
    }

    public Map<String, Object> autocorrigirCentroResultado(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();

            BigDecimal codCenCusAtual = null;
            BigDecimal codNatAtual = null;
            NativeSql sqlAtual = new NativeSql(jdbc);
            sqlAtual.appendSql("SELECT TOP 1 CODCONFIG, CODCENCUS, CODNAT FROM AD_FCCONFIG ORDER BY CODCONFIG DESC");
            rs = sqlAtual.executeQuery();
            BigDecimal codConfig = null;
            if (rs.next()) {
                codConfig = rs.getBigDecimal("CODCONFIG");
                codCenCusAtual = rs.getBigDecimal("CODCENCUS");
                codNatAtual = rs.getBigDecimal("CODNAT");
            }
            closeQuietly(rs);

            if (codConfig == null) {
                result.put("success", false);
                result.put("message", "AD_FCCONFIG sem registros");
                return result;
            }

            BigDecimal codCenCus = codCenCusAtual;
            BigDecimal codNat = codNatAtual;
            String origemCenCus = null;
            String origemNat = null;

            FastchannelConfig config = FastchannelConfig.getInstance();
            String usuario = config.getSankhyaUser();
            if (codCenCus == null && usuario != null && !usuario.trim().isEmpty()) {
                NativeSql sqlUsu = new NativeSql(jdbc);
                sqlUsu.appendSql("SELECT TOP 1 CODCENCUSPAD FROM TSIUSU ");
                sqlUsu.appendSql("WHERE UPPER(NOMEUSU)=UPPER(:nomeUsu) AND CODCENCUSPAD IS NOT NULL");
                sqlUsu.setNamedParameter("nomeUsu", usuario);
                rs = sqlUsu.executeQuery();
                if (rs.next()) {
                    codCenCus = rs.getBigDecimal("CODCENCUSPAD");
                    origemCenCus = "TSIUSU.CODCENCUSPAD";
                }
                closeQuietly(rs);
            }

            if (codCenCus == null || codNat == null) {
                NativeSql sqlCab = new NativeSql(jdbc);
                sqlCab.appendSql("SELECT TOP 1 CODCENCUS, CODNAT FROM TGFCAB ");
                sqlCab.appendSql("WHERE (CODCENCUS IS NOT NULL OR CODNAT IS NOT NULL) ORDER BY NUNOTA DESC");
                rs = sqlCab.executeQuery();
                if (rs.next()) {
                    if (codCenCus == null) {
                        codCenCus = rs.getBigDecimal("CODCENCUS");
                        if (codCenCus != null) {
                            origemCenCus = "TGFCAB.CODCENCUS";
                        }
                    }
                    if (codNat == null) {
                        codNat = rs.getBigDecimal("CODNAT");
                        if (codNat != null) {
                            origemNat = "TGFCAB.CODNAT";
                        }
                    }
                }
                closeQuietly(rs);
            }

            if (codCenCus == null && codNat == null) {
                result.put("success", false);
                result.put("message", "Nao foi possivel encontrar CODCENCUS/CODNAT para autocorrecao");
                return result;
            }

            NativeSql sqlUpd = new NativeSql(jdbc);
            sqlUpd.appendSql("UPDATE AD_FCCONFIG SET CODCENCUS = :codCenCus, CODNAT = :codNat WHERE CODCONFIG = :codConfig");
            sqlUpd.setNamedParameter("codCenCus", codCenCus);
            sqlUpd.setNamedParameter("codNat", codNat);
            sqlUpd.setNamedParameter("codConfig", codConfig);
            sqlUpd.executeUpdate();

            config.reload();

            result.put("success", true);
            result.put("message", "CODCENCUS/CODNAT atualizados com sucesso");
            result.put("codCenCus", codCenCus);
            result.put("codNat", codNat);
            result.put("origemCenCus", origemCenCus);
            result.put("origemNat", origemNat);
            return result;
        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro na autocorrecao de CODCENCUS", e);
            result.put("success", false);
            result.put("message", e.getMessage());
            return result;
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
    }

    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception ignored) {}
        }
    }

    private JdbcWrapper openJdbc() throws Exception {
        JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
        jdbc.openSession();
        return jdbc;
    }

    private void closeJdbc(JdbcWrapper jdbc) {
        if (jdbc != null) {
            try {
                jdbc.closeSession();
            } catch (Exception e) {
                log.log(Level.FINE, "Erro ao fechar JdbcWrapper", e);
            }
        }
    }

    private int getInt(Map<String, Object> params, String key, int defaultValue) {
        Object value = params.get(key);
        if (value == null) return defaultValue;
        if (value instanceof Number) return ((Number) value).intValue();
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private boolean getBoolean(Map<String, Object> params, String key) {
        Object value = params.get(key);
        if (value == null) return false;
        if (value instanceof Boolean) return (Boolean) value;
        String text = value.toString();
        return "true".equalsIgnoreCase(text) || "1".equals(text) || "S".equalsIgnoreCase(text);
    }
}
