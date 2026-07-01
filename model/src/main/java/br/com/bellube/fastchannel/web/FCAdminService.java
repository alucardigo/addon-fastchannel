package br.com.bellube.fastchannel.web;

import br.com.bellube.fastchannel.auth.FastchannelTokenManager;
import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.http.FastchannelHttpClient;
import br.com.bellube.fastchannel.http.FastchannelOrdersClient;
import br.com.bellube.fastchannel.job.OutboxProcessorJob;
import br.com.bellube.fastchannel.job.PriceFullSyncJob;
import br.com.bellube.fastchannel.job.StockFullSyncJob;
import br.com.bellube.fastchannel.service.LogService;
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
    private static final int ORDER_IMPORT_CLAIM_TIMEOUT_MINUTES =
            FastchannelConstants.DEFAULT_ORDER_IMPORT_CLAIM_TIMEOUT_MINUTES;

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
            RetrySummary retrySummary = retryErroredOrders(orderService, retryLimit);
            int retriedImported = retrySummary.imported;
            int imported = retryOnly ? 0 : orderService.importPendingOrders();
            int totalImported = retriedImported + imported;
            String diagnostic = null;
            if (!retryOnly && imported == 0) {
                diagnostic = buildOrdersDiagnostic(config);
            }

            boolean success = retrySummary.failed == 0;
            String message = "Importacao concluida! " + totalImported +
                    " pedido(s) importado(s). Reprocessados: " + retriedImported +
                    ", novos da API: " + imported +
                    ", pulados: " + retrySummary.skipped +
                    ", falharam no retry: " + retrySummary.failed + ".";
            if (diagnostic != null && !diagnostic.isEmpty()) {
                message += " " + diagnostic;
                result.put("diagnostic", diagnostic);
            }
            if (retryOnly && retrySummary.eligible == 0) {
                message += " Nenhum pedido elegivel para retry.";
            }
            result.put("success", success);
            result.put("message", message);
            result.put("count", totalImported);
            result.put("reprocessedCount", retriedImported);
            result.put("pendingApiCount", imported);
            result.put("retrySkippedCount", retrySummary.skipped);
            result.put("retryFailedCount", retrySummary.failed);
            result.put("retryEligibleCount", retrySummary.eligible);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao importar pedidos", e);
            result.put("success", false);
            result.put("message", "Erro: " + e.getMessage());
        }

        return result;
    }

    /**
     * [FCAdminSP.markAsUnsynced] Devolve pedidos da Fastchannel ao pool de pendentes,
     * zerando IsSynched e ExternalId via PUT /orders/{id}/sync.
     *
     * Uso: cenario de coexistencia paralela addon+legado. O addon importa para TOP de
     * teste isolada; ao terminar o teste, esta operacao "devolve" os pedidos para o
     * legado consumir de verdade no ambiente.
     *
     * Params esperados:
     *   - orderIds: List<String> ou String com IDs separados por virgula (ex.: "4656,4657,4658")
     *
     * Retorno:
     *   - success: boolean
     *   - total: int
     *   - marked: int
     *   - failed: int
     *   - errors: List<Map> {orderId, error}
     *   - message: String human-readable
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> markAsUnsynced(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();

        try {
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                result.put("success", false);
                result.put("message", "Integracao Fastchannel nao esta ativa.");
                return result;
            }

            // Aceita "orderIds" como List ou String separada por virgula
            List<String> orderIds = new ArrayList<>();
            Object rawOrderIds = params != null ? params.get("orderIds") : null;
            if (rawOrderIds instanceof List) {
                for (Object o : (List<Object>) rawOrderIds) {
                    if (o != null) {
                        String s = o.toString().trim();
                        if (!s.isEmpty()) orderIds.add(s);
                    }
                }
            } else if (rawOrderIds instanceof String) {
                for (String s : ((String) rawOrderIds).split("[,;\\s]+")) {
                    String trimmed = s.trim();
                    if (!trimmed.isEmpty()) orderIds.add(trimmed);
                }
            }

            if (orderIds.isEmpty()) {
                result.put("success", false);
                result.put("message", "Parametro 'orderIds' obrigatorio (List ou string separada por virgula).");
                return result;
            }

            FastchannelOrdersClient ordersClient = new FastchannelOrdersClient();
            int marked = 0;
            int failed = 0;
            List<Map<String, Object>> errors = new ArrayList<>();

            for (String orderId : orderIds) {
                try {
                    ordersClient.markAsUnsynced(orderId);
                    marked++;
                    log.info("[FCAdminSP.markAsUnsynced] " + orderId + " desmarcado com sucesso.");
                } catch (Exception e) {
                    failed++;
                    Map<String, Object> err = new HashMap<>();
                    err.put("orderId", orderId);
                    err.put("error", e.getMessage());
                    errors.add(err);
                    log.log(Level.WARNING, "[FCAdminSP.markAsUnsynced] Falha em " + orderId, e);
                }
            }

            result.put("success", failed == 0);
            result.put("total", orderIds.size());
            result.put("marked", marked);
            result.put("failed", failed);
            result.put("errors", errors);
            result.put("message", marked + " de " + orderIds.size()
                + " pedido(s) devolvido(s) ao pool FC"
                + (failed > 0 ? " (" + failed + " falha(s))" : "") + ".");

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro geral em markAsUnsynced", e);
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

    private RetrySummary retryErroredOrders(OrderService orderService, int limit) throws Exception {
        if (orderService == null || limit <= 0) return RetrySummary.empty();

        List<String> orderIds = loadErroredOrderIds(limit);
        if (orderIds.isEmpty()) return RetrySummary.empty();

        FastchannelOrdersClient ordersClient = new FastchannelOrdersClient();
        LogService logService = LogService.getInstance();
        int imported = 0;
        int skipped = 0;
        int failed = 0;

        for (String orderId : orderIds) {
            try {
                OrderDTO order = ordersClient.getOrder(orderId);
                if (order == null) {
                    log.warning("Retry: pedido " + orderId + " nao encontrado no Fastchannel.");
                    failed++;
                    continue;
                }
                BigDecimal nuNota = orderService.importOrder(order);
                if (nuNota != null) {
                    log.info("Retry: pedido " + orderId + " reprocessado com sucesso. NUNOTA=" + nuNota);
                    imported++;
                } else {
                    log.info("Retry: pedido " + orderId + " pulado por outra execucao em andamento (PROCESSANDO recente).");
                    logService.logOrderRetrySkipped(orderId, "PROCESSANDO recente — outra execucao em andamento");
                    skipped++;
                }
            } catch (Exception e) {
                log.log(Level.WARNING, "Retry: pedido " + orderId + " falhou de novo.", e);
                failed++;
            }
        }

        log.info("Retry concluido: " + imported + " reprocessados, " + skipped + " pulados (em andamento), " + failed + " falharam.");
        return new RetrySummary(orderIds.size(), imported, skipped, failed);
    }

    private static final class RetrySummary {
        private final int eligible;
        private final int imported;
        private final int skipped;
        private final int failed;

        private RetrySummary(int eligible, int imported, int skipped, int failed) {
            this.eligible = eligible;
            this.imported = imported;
            this.skipped = skipped;
            this.failed = failed;
        }

        private static RetrySummary empty() {
            return new RetrySummary(0, 0, 0, 0);
        }
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
            Timestamp staleCutoff = new Timestamp(System.currentTimeMillis() - (ORDER_IMPORT_CLAIM_TIMEOUT_MINUTES * 60L * 1000L));
            stmt = conn.prepareStatement(
                    "SELECT ORDER_ID FROM AD_FCPEDIDO " +
                    "WHERE NUNOTA IS NULL " +
                    "AND (UPPER(COALESCE(STATUS_IMPORT, '')) IN ('ERRO', 'PENDENTE') " +
                    "OR (UPPER(COALESCE(STATUS_IMPORT, '')) = 'PROCESSANDO' " +
                    "AND (DH_IMPORTACAO IS NULL OR DH_IMPORTACAO < ?))) " +
                    "ORDER BY DH_IMPORTACAO DESC " +
                    "OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY");
            stmt.setTimestamp(1, staleCutoff);
            stmt.setInt(2, limit);
            rs = stmt.executeQuery();
            while (rs.next()) {
                String orderId = rs.getString("ORDER_ID");
                if (orderId != null && !orderId.trim().isEmpty()) {
                    ids.add(orderId.trim());
                }
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao carregar pedidos ERRO/PENDENTE/PROCESSANDO vencido para reprocessamento", e);
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

    public Map<String, Object> syncPrecosCompleto(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        try {
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                result.put("success", false);
                result.put("message", "Integracao nao esta ativa!");
                return result;
            }
            new PriceFullSyncJob().executeScheduler();
            result.put("success", true);
            result.put("message", "Sincronizacao completa de precos executada.");
        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro no sync completo de precos", e);
            result.put("success", false);
            result.put("message", "Erro: " + e.getMessage());
        }
        return result;
    }

    public Map<String, Object> listarTopsPedido(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                "SELECT CODTIPOPER, DESCROPER, TIPMOV FROM TGFTOP WHERE TIPMOV = 'P' ORDER BY CODTIPOPER");
            rs = stmt.executeQuery();
            List<Map<String, Object>> items = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> item = new HashMap<>();
                item.put("codTipOper", rs.getBigDecimal("CODTIPOPER"));
                try { item.put("descrOper", rs.getString("DESCROPER")); } catch (Exception e) { item.put("descrOper", null); }
                item.put("tipMov", rs.getString("TIPMOV"));
                items.add(item);
            }
            result.put("items", items);
            result.put("total", items.size());
        } catch (Exception e) {
            result.put("error", e.getMessage());
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return result;
    }

    public Map<String, Object> syncEstoqueCompleto(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        try {
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                result.put("success", false);
                result.put("message", "Integracao nao esta ativa!");
                return result;
            }
            new StockFullSyncJob().executeScheduler();
            result.put("success", true);
            result.put("message", "Sincronizacao completa de estoque executada.");
        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro no sync completo de estoque", e);
            result.put("success", false);
            result.put("message", "Erro: " + e.getMessage());
        }
        return result;
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

    /**
     * Limpa batches "lixo" (Min=0/Max=0/Disabled=true) na FC para todos os SKUs configurados
     * ou apenas um especifico se "sku" for passado nos params.
     */
    public Map<String, Object> cleanupGarbageBatches(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        String singleSku = params != null ? (String) params.get("sku") : null;
        try {
            br.com.bellube.fastchannel.http.FastchannelPriceClient distClient =
                    new br.com.bellube.fastchannel.http.FastchannelPriceClient(br.com.bellube.fastchannel.http.FastchannelPriceClient.Channel.DISTRIBUTION);
            br.com.bellube.fastchannel.http.FastchannelPriceClient consClient =
                    new br.com.bellube.fastchannel.http.FastchannelPriceClient(br.com.bellube.fastchannel.http.FastchannelPriceClient.Channel.CONSUMPTION);

            int totalRemovido = 0;
            int totalSkus = 0;
            java.util.List<String> skus = new java.util.ArrayList<>();

            if (singleSku != null && !singleSku.trim().isEmpty()) {
                skus.add(singleSku.trim());
            } else {
                // Pega SKUs das tabelas configuradas via /prices?PriceTableIds=X&PageSize=5000
                String priceTableIds = FastchannelConfig.getInstance().getPriceTableIds();
                if (priceTableIds == null || priceTableIds.trim().isEmpty()) {
                    result.put("error", "AD_FCCONFIG.PRICE_TABLE_IDS vazio - informe sku=X explicito ou configure PRICE_TABLE_IDS");
                    return result;
                }
                java.util.Set<String> uniqueSkus = new java.util.HashSet<>();
                for (String idStr : priceTableIds.split(",")) {
                    idStr = idStr.trim();
                    if (idStr.isEmpty()) continue;
                    try {
                        java.math.BigDecimal tableId = new java.math.BigDecimal(idStr);
                        java.util.List<br.com.bellube.fastchannel.dto.PriceDTO> prices = distClient.listPricesForTable(tableId);
                        if (prices != null) {
                            for (br.com.bellube.fastchannel.dto.PriceDTO p : prices) {
                                if (p.getSku() != null) uniqueSkus.add(p.getSku().trim());
                            }
                        }
                    } catch (Exception e) {
                        log.log(Level.FINE, "Erro listando precos pra tabela " + idStr, e);
                    }
                }
                skus.addAll(uniqueSkus);
            }

            for (String sku : skus) {
                totalSkus++;
                try {
                    int removed = distClient.cleanupGarbageBatches(sku);
                    totalRemovido += removed;
                    if (removed > 0) {
                        log.info("[cleanupGarbageBatches] SKU=" + sku + " removidos=" + removed);
                    }
                } catch (Exception e) {
                    log.log(Level.WARNING, "Falha cleanup SKU " + sku, e);
                }
            }
            result.put("success", true);
            result.put("skusProcessados", totalSkus);
            result.put("batchesRemovidos", totalRemovido);
            result.put("message", "Limpeza concluida. Removidos " + totalRemovido + " batches lixo de " + totalSkus + " SKU(s).");
        } catch (Throwable t) {
            result.put("success", false);
            result.put("error", t.getClass().getSimpleName() + ": " + t.getMessage());
            log.log(Level.WARNING, "[cleanupGarbageBatches] Falha geral", t);
        }
        return result;
    }

    private boolean getBoolean(Map<String, Object> params, String key) {
        Object value = params.get(key);
        if (value == null) return false;
        if (value instanceof Boolean) return (Boolean) value;
        String text = value.toString();
        return "true".equalsIgnoreCase(text) || "1".equals(text) || "S".equalsIgnoreCase(text);
    }

    /**
     * Registra o addon no {@code AddonInstallStore} local do WildFly
     * ({@code standalone/configuration/addons.properties}, criptografado com AES).
     *
     * <p>Quando o addon aparece no UI Sankhya (Administracao do Sistema > Add-ons Instalados)
     * com Origem={@code Gerenciador de Pacotes} e sem Data de Instalacao, significa que
     * o .ear foi carregado pelo scanner do WildFly (presente em {@code deployments/})
     * mas NAO esta registrado em {@code AddonInstallStore}. Esse endpoint forca o
     * registro via reflection: chama {@code saveAddon(ctx, clientId)} que escreve
     * a entrada criptografada no arquivo.
     *
     * <p>Apos o registro, a tela deve passar a mostrar Origem={@code Place}.
     */
    public Map<String, Object> registerAsPlace(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        String ctx = (String) params.getOrDefault("ctx", "addon-fastchannel");
        String clientId = (String) params.getOrDefault("clientId", "169614"); // solutionId

        try {
            Class<?> storeClass = Class.forName("br.com.sankhya.module.stores.AddonInstallStore");
            Object store = storeClass.getDeclaredConstructor().newInstance();

            java.lang.reflect.Method saveAddon = storeClass.getMethod("saveAddon", String.class, String.class);
            saveAddon.invoke(store, ctx, clientId);

            result.put("success", true);
            result.put("ctx", ctx);
            result.put("clientId", clientId);
            result.put("message", "addon-fastchannel registrado em AddonInstallStore (addons.properties). " +
                "Refresh no UI (Administracao do Sistema > Add-ons Instalados) deve mostrar Origem=Place.");
            log.info("[registerAsPlace] ctx=" + ctx + " clientId=" + clientId);

            // Broadcast cluster (best-effort; funciona em single-node)
            try {
                Class<?> swClass = Class.forName("br.com.sankhya.skw.cluster.SWInstance");
                java.lang.reflect.Method broadcast = swClass.getMethod("broadcast", String.class, Object[].class);
                broadcast.invoke(null, "placemm.cluster.add-on.save.properties", new Object[]{ctx, clientId});
                result.put("clusterBroadcast", true);
            } catch (Throwable ignore) {
                result.put("clusterBroadcast", false);
            }

        } catch (ClassNotFoundException e) {
            result.put("success", false);
            result.put("error", "Classe AddonInstallStore nao encontrada: " + e.getMessage());
            log.log(Level.WARNING, "[registerAsPlace] ClassNotFound", e);
        } catch (Throwable t) {
            result.put("success", false);
            result.put("error", t.getClass().getSimpleName() + ": " + t.getMessage());
            log.log(Level.WARNING, "[registerAsPlace] Erro reflection", t);
        }
        return result;
    }

    /**
     * Limpa o cache em memoria do placemm ({@code InstalledAddonsCache}) via reflection.
     *
     * <p>Usado quando o Place UI fica com estado inconsistente apos falhas sucessivas
     * de instalacao — ex: erro {@code PreparedStatement com parametro nulo na entidade
     * 'SolutionBinary': param[0] = null} ao clicar Reiniciar/Reiniciar DD.
     *
     * <p>Esse cache e um {@code Map<String, AddonDescriptor>} estatico dentro da JVM
     * do WildFly e normalmente so e limpo com restart do servidor. Esse endpoint
     * permite limpar sem restart, desde que o classloader do addon tenha acesso
     * a classe {@code br.com.sankhya.mm.place.caches.InstalledAddonsCache}
     * (disponivel atraves do modulo placemm).
     */
    public Map<String, Object> clearPlaceCache(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        try {
            Class<?> cacheClass = Class.forName("br.com.sankhya.mm.place.caches.InstalledAddonsCache");

            // Antes de limpar: coletar descritores pra log
            int sizeBefore = 0;
            try {
                java.lang.reflect.Method getAll = cacheClass.getMethod("getAll");
                Object coll = getAll.invoke(null);
                if (coll instanceof java.util.Collection) {
                    sizeBefore = ((java.util.Collection<?>) coll).size();
                }
            } catch (Exception ignore) {
                // se getAll nao acessivel, segue sem contagem
            }

            java.lang.reflect.Method clear = cacheClass.getMethod("clearCache");
            clear.invoke(null);

            result.put("success", true);
            result.put("cacheClass", cacheClass.getName());
            result.put("entriesRemoved", sizeBefore);
            result.put("message", "InstalledAddonsCache.clearCache() invocado com sucesso. " +
                "Proxima operacao no Place UI vai repopular o cache a partir do TSSBNR.");
            log.info("[clearPlaceCache] " + sizeBefore + " entrada(s) removida(s) do InstalledAddonsCache");
        } catch (ClassNotFoundException e) {
            result.put("success", false);
            result.put("error", "Classe InstalledAddonsCache nao encontrada no classloader. " +
                "O modulo placemm pode nao estar acessivel a partir deste addon. Detalhe: " + e.getMessage());
            log.log(Level.WARNING, "[clearPlaceCache] ClassNotFound", e);
        } catch (NoSuchMethodException e) {
            result.put("success", false);
            result.put("error", "Metodo clearCache() nao encontrado na classe InstalledAddonsCache. " +
                "Versao do placemm pode estar desalinhada. Detalhe: " + e.getMessage());
            log.log(Level.WARNING, "[clearPlaceCache] NoSuchMethod", e);
        } catch (Throwable t) {
            result.put("success", false);
            result.put("error", "Falha ao invocar clearCache: " + t.getClass().getSimpleName() + ": " + t.getMessage());
            log.log(Level.WARNING, "[clearPlaceCache] Erro reflection", t);
        }
        return result;
    }
}
