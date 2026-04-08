package br.com.bellube.fastchannel.web;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.StockDTO;
import br.com.bellube.fastchannel.http.FastchannelStockClient;
import br.com.bellube.fastchannel.job.OutboxProcessorJob;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.LogService;
import br.com.bellube.fastchannel.service.QueueService;
import br.com.bellube.fastchannel.service.StockResolver;
import br.com.bellube.fastchannel.util.DBUtil;
import br.com.bellube.fastchannel.util.FastchannelProductFilter;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service para gerenciamento de estoque.
 */
public class FCEstoqueService {

    private static final Logger log = Logger.getLogger(FCEstoqueService.class.getName());

    public Map<String, Object> list(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;
        PreparedStatement countStmt = null;
        PreparedStatement stmt = null;
        ResultSet rsCount = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();

            String source = getString(params, "source");
            if (source == null || source.isEmpty()) {
                source = "sankhya";
            }

            String skuExpr = FastchannelProductFilter.resolveSkuExpression(conn);
            String marcaJoin = FastchannelProductFilter.resolveMarcaJoin(conn);
            StringBuilder where = new StringBuilder("1=1");
            List<Object> queryParams = new ArrayList<>();
            FastchannelProductFilter.appendConfiguredEmpresasFilter(where, queryParams, conn, "E.CODEMP");
            FastchannelProductFilter.appendConfiguredLocaisFilter(where, queryParams, conn, "E.CODLOCAL");
            // Tela operacional: deve mostrar todo o recorte configurado, inclusive produtos
            // ainda nao mapeados, para facilitar diagnostico de pendencias.

            String sku = getString(params, "sku");
            if (sku != null && !sku.isEmpty()) {
                where.append(" AND ").append(skuExpr).append(" LIKE ?");
                queryParams.add("%" + sku + "%");
            }

            BigDecimal codProd = getBigDecimal(params, "codProd");
            if (codProd != null) {
                where.append(" AND P.CODPROD = ?");
                queryParams.add(codProd);
            }

            BigDecimal codLocal = getBigDecimal(params, "codLocal");
            if (codLocal != null) {
                where.append(" AND E.CODLOCAL = ?");
                queryParams.add(codLocal);
            }

            BigDecimal codEmp = getBigDecimal(params, "codEmp");
            if (codEmp != null) {
                where.append(" AND E.CODEMP = ?");
                queryParams.add(codEmp);
            }

            if ("queue".equalsIgnoreCase(source)) {
                where.append(" AND QL.IDQUEUE IS NOT NULL");

                String status = getString(params, "status");
                if (status != null && !status.isEmpty()) {
                    where.append(" AND QL.STATUS = ?");
                    queryParams.add(status);
                }

                String dataInicio = getString(params, "dataInicio");
                if (dataInicio != null && !dataInicio.isEmpty()) {
                    where.append(" AND QL.DH_CRIACAO >= ?");
                    queryParams.add(dataInicio);
                }

                String dataFim = getString(params, "dataFim");
                if (dataFim != null && !dataFim.isEmpty()) {
                    where.append(" AND QL.DH_CRIACAO <= ?");
                    queryParams.add(dataFim + " 23:59:59");
                }
            }

            int page = getInt(params, "page", 1);
            int pageSize = getInt(params, "pageSize", 20);
            int offset = (page - 1) * pageSize;

            // FROM base sem OUTER APPLY (rapido para COUNT)
            String fromBase = " FROM TGFEST E " +
                    " INNER JOIN TGFPRO P ON P.CODPROD = E.CODPROD " +
                    marcaJoin +
                    " AND E.CODPARC = 0 ";

            // COUNT total de linhas (produto+local+empresa)
            String countSql = "SELECT COUNT(*) AS CNT " + fromBase + " WHERE " + where;
            countStmt = conn.prepareStatement(countSql);
            setParameters(countStmt, queryParams);
            rsCount = countStmt.executeQuery();
            rsCount.next();
            int total = rsCount.getInt("CNT");

            // Query com OUTER APPLY apenas para a pagina atual (paginada)
            String fromFull = fromBase +
                    " OUTER APPLY ( " +
                    "   SELECT TOP 1 Q.IDQUEUE, Q.STATUS, Q.DH_CRIACAO, Q.DH_PROCESSAMENTO, Q.LAST_ERROR " +
                    "   FROM AD_FCQUEUE Q " +
                    "   WHERE Q.ENTITY_TYPE = '" + FastchannelConstants.ENTITY_ESTOQUE + "' " +
                    "     AND Q.ENTITY_ID = P.CODPROD " +
                    "   ORDER BY Q.DH_CRIACAO DESC " +
                    " ) QL " +
                    " OUTER APPLY ( " +
                    "   SELECT TOP 1 L.NIVEL, L.MENSAGEM, L.DH_REGISTRO " +
                    "   FROM AD_FCLOG L " +
                    "   WHERE L.OPERACAO = 'STOCK_SYNC' " +
                    "     AND L.REFERENCIA = " + skuExpr + " " +
                    "   ORDER BY L.DH_REGISTRO DESC " +
                    " ) LL ";

            // Estoque por produto com LOCAL, EMPRESA, e De-Para para storageId/resellerId
            String sql = "SELECT " +
                    " P.CODPROD, P.DESCRPROD, " + skuExpr + " AS SKU, " +
                    " E.CODLOCAL, E.CODEMP, " +
                    " E.ESTOQUE - ISNULL(E.RESERVADO, 0) AS ESTOQUE, " +
                    " DS.COD_EXTERNO AS STORAGE_ID, " +
                    " DR.COD_EXTERNO AS RESELLER_ID, " +
                    " QL.IDQUEUE, QL.STATUS AS QUEUE_STATUS, QL.DH_CRIACAO AS QUEUE_DATE, QL.LAST_ERROR, " +
                    " LL.NIVEL AS LOG_LEVEL, LL.MENSAGEM AS LOG_MESSAGE, LL.DH_REGISTRO AS LOG_DATE " +
                    fromFull +
                    " LEFT JOIN AD_FCDEPARA DS ON DS.TIPO_ENTIDADE = 'STOCK_STORAGE' " +
                    "   AND DS.COD_SANKHYA = CAST(E.CODLOCAL AS VARCHAR) " +
                    "   AND (DS.INTEGRA_AUTO IS NULL OR DS.INTEGRA_AUTO = 'S') " +
                    " LEFT JOIN AD_FCDEPARA DR ON DR.TIPO_ENTIDADE = 'STOCK_RESELLER' " +
                    "   AND DR.COD_SANKHYA = CAST(E.CODEMP AS VARCHAR) " +
                    "   AND (DR.INTEGRA_AUTO IS NULL OR DR.INTEGRA_AUTO = 'S') " +
                    " WHERE " + where +
                    " ORDER BY " + resolveStockOrderBy(params) + " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";

            stmt = conn.prepareStatement(sql);
            int paramIndex = setParameters(stmt, queryParams);
            stmt.setInt(paramIndex++, offset);
            stmt.setInt(paramIndex, pageSize);
            rs = stmt.executeQuery();

            List<Map<String, Object>> items = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> item = new HashMap<>();
                item.put("codProd", rs.getBigDecimal("CODPROD"));
                item.put("descricao", rs.getString("DESCRPROD"));
                String skuLocal = normalizeSku(rs.getString("SKU"));
                BigDecimal codProdRow = rs.getBigDecimal("CODPROD");
                String skuOutbound = resolveOutboundSku(codProdRow, skuLocal);
                item.put("skuLocal", skuLocal);
                item.put("sku", skuOutbound != null && !skuOutbound.isEmpty() ? skuOutbound : skuLocal);
                item.put("codLocal", rs.getBigDecimal("CODLOCAL"));
                item.put("codEmp", rs.getInt("CODEMP"));
                item.put("storageId", rs.getString("STORAGE_ID"));
                item.put("resellerId", rs.getString("RESELLER_ID"));
                item.put("estoque", rs.getBigDecimal("ESTOQUE"));
                item.put("queueId", rs.getBigDecimal("IDQUEUE"));
                item.put("queueStatus", rs.getString("QUEUE_STATUS"));
                item.put("queueDate", rs.getTimestamp("QUEUE_DATE"));
                item.put("queueError", rs.getString("LAST_ERROR"));
                item.put("logLevel", rs.getString("LOG_LEVEL"));
                item.put("logMessage", rs.getString("LOG_MESSAGE"));
                item.put("logDate", rs.getTimestamp("LOG_DATE"));
                items.add(item);
            }

            boolean includeFc = getBoolean(params, "includeFc", false);
            if (includeFc) {
                enrichItemsWithFastchannelStock(items);
            }

            result.put("items", items);
            result.put("total", total);
            result.put("page", page);
            result.put("pageSize", pageSize);
            result.put("source", source);
            result.put("includesFc", includeFc);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao listar estoque", e);
            result.put("error", e.getMessage());
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
            DBUtil.closeResultSet(rsCount);
            DBUtil.closeStatement(countStmt);
        }

        return result;
    }

    public Map<String, Object> compararFC(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;

        try {
            conn = DBUtil.getConnection();
            String sku = getString(params, "sku");
            boolean skuProvided = sku != null && !sku.isEmpty();
            BigDecimal codProd = getBigDecimal(params, "codProd");
            BigDecimal codLocal = getBigDecimal(params, "codLocal");
            BigDecimal codEmp = getBigDecimal(params, "codEmp");

            if ((sku == null || sku.isEmpty()) && codProd == null) {
                result.put("error", "sku ou codProd obrigatorio");
                return result;
            }

            if (codProd == null) {
                codProd = findCodProdBySku(conn, sku);
            }
            // Usar a mesma regra canonica de SKU usada por EstoqueListener/StockFullSyncJob
            DeparaService deparaCompare = DeparaService.getInstance();
            String skuOutbound = deparaCompare.getSkuForStock(codProd);
            if (skuOutbound == null || skuOutbound.isEmpty()) {
                result.put("error", "Nao foi possivel resolver SKU pela regra da marca (getSkuForStock) para CODPROD=" + codProd);
                return result;
            }

            FastchannelConfig config = FastchannelConfig.getInstance();
            if (codLocal == null) codLocal = config.getCodLocal();
            if (codEmp == null) codEmp = config.getCodemp();

            BigDecimal estoque = new StockResolver().resolve(codProd, codEmp, codLocal);
            if (estoque == null) estoque = BigDecimal.ZERO;

            FastchannelStockClient client = new FastchannelStockClient();
            String storageIdCompare = deparaCompare.getCodigoExterno(DeparaService.TIPO_STOCK_STORAGE, codLocal);
            if (storageIdCompare == null || storageIdCompare.isEmpty()) {
                storageIdCompare = config.getStorageId();
            }
            StockDTO fcStock = null;
            List<String> tentativas = new ArrayList<>();
            try {
                fcStock = client.getStock(skuOutbound, storageIdCompare);
                if (fcStock == null) {
                    tentativas.add(skuOutbound + ":404/vazio (StorageId=" + storageIdCompare + ")");
                }
            } catch (Exception ex) {
                tentativas.add(skuOutbound + ":" + ex.getMessage());
            }

            result.put("sku", skuOutbound);
            result.put("skuLocal", sku);
            result.put("codProd", codProd);
            result.put("codLocal", codLocal);
            result.put("codEmp", codEmp);
            result.put("estoqueSankhya", estoque);
            result.put("storageId", DeparaService.getInstance()
                    .getCodigoExterno(DeparaService.TIPO_STOCK_STORAGE, codLocal));
            result.put("resellerId", DeparaService.getInstance()
                    .getCodigoExterno(DeparaService.TIPO_STOCK_RESELLER, codEmp));

            if (fcStock != null) {
                result.put("estoqueFastchannel", fcStock.getQuantity());
                result.put("fcAvailable", fcStock.getAvailableQuantity());
                result.put("fcReserved", fcStock.getReservedQuantity());
                result.put("fcStorageId", fcStock.getStorageId());
                result.put("fcLastUpdate", fcStock.getLastUpdate());
                result.put("skuUsadoFastchannel", skuOutbound);
            } else {
                result.put("estoqueFastchannel", null);
                if (!tentativas.isEmpty()) {
                    result.put("fcDiagnostico", "Nenhum SKU encontrado na FC. Tentativas: " + tentativas);
                }
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao comparar estoque", e);
            result.put("error", e.getMessage());
        } finally {
            DBUtil.closeConnection(conn);
        }

        return result;
    }

    public Map<String, Object> forcarSync(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;

        int ok = 0;
        int errors = 0;
        List<String> errorDetails = new ArrayList<>();

        try {
            conn = DBUtil.getConnection();
            List<Map<String, Object>> items = extractItems(params);
            if (items.isEmpty()) {
                Map<String, Object> single = new HashMap<>();
                single.put("sku", getString(params, "sku"));
                single.put("codProd", getBigDecimal(params, "codProd"));
                single.put("codLocal", getBigDecimal(params, "codLocal"));
                single.put("codEmp", getBigDecimal(params, "codEmp"));
                items.add(single);
            }

            FastchannelConfig config = FastchannelConfig.getInstance();
            FastchannelStockClient stockClient = new FastchannelStockClient();

            for (Map<String, Object> item : items) {
                try {
                    String sku = asString(item.get("sku"));
                    boolean skuProvided = sku != null && !sku.isEmpty();
                    BigDecimal codProd = asBigDecimal(item.get("codProd"));
                    BigDecimal codLocal = asBigDecimal(item.get("codLocal"));
                    BigDecimal codEmp = asBigDecimal(item.get("codEmp"));

                    if (codProd == null && (sku == null || sku.isEmpty())) {
                        throw new Exception("sku/codProd ausente");
                    }
                    if (codProd == null) codProd = findCodProdBySku(conn, sku);
                    if (sku == null || sku.isEmpty()) sku = findSkuByCodProd(conn, codProd);
                    String skuOutbound = chooseOutboundSku(codProd, sku, skuProvided);
                    if (skuOutbound == null || skuOutbound.isEmpty()) {
                        throw new Exception("SKU nao resolvido para CODPROD " + codProd);
                    }

                    if (codLocal == null) codLocal = config.getCodLocal();
                    if (codEmp == null) codEmp = config.getCodemp();
                    DeparaService depara = DeparaService.getInstance();
                    String storageId = resolveStorageId(depara, config, codLocal);
                    String resellerId = resolveResellerId(depara, config, codEmp);
                    if (storageId == null || storageId.isEmpty()) {
                        throw new Exception("StorageId nao mapeado para CODLOCAL " + codLocal);
                    }
                    if (resellerId == null || resellerId.isEmpty()) {
                        resellerId = "";
                    }

                    BigDecimal estoque = new StockResolver().resolve(codProd, codEmp, codLocal);
                    if (estoque == null) estoque = BigDecimal.ZERO;
                    stockClient.updateStock(skuOutbound, estoque, storageId, resellerId);

                    // GET verificativo - confirmar que o valor chegou na FC
                    LogService logService = LogService.getInstance();
                    StockDTO verificacao = null;
                    try {
                        verificacao = stockClient.getStock(skuOutbound, storageId);
                    } catch (Exception getEx) {
                        log.warning("GET verificativo falhou para SKU " + skuOutbound + ": " + getEx.getMessage());
                    }

                    try {
                        if (verificacao != null && verificacao.getQuantity() != null) {
                            BigDecimal fcQty = verificacao.getQuantity();
                            if (fcQty.compareTo(estoque) == 0) {
                                logService.logStockSync(skuOutbound, estoque, true,
                                        "forcarSync OK. Sankhya=" + estoque + " FC=" + fcQty
                                                + " StorageId=" + storageId + " ResellerId=" + resellerId);
                            } else {
                                logService.logStockSync(skuOutbound, estoque, true,
                                        "forcarSync DIVERGENTE. Enviado=" + estoque + " FC retornou=" + fcQty
                                                + " StorageId=" + storageId + " ResellerId=" + resellerId);
                            }
                        } else {
                            logService.logStockSync(skuOutbound, estoque, true,
                                    "forcarSync enviado (PUT OK), GET verificativo nao retornou dados."
                                            + " StorageId=" + storageId + " ResellerId=" + resellerId);
                        }
                    } catch (Exception logEx) {
                        log.log(Level.FINE, "Falha ao gravar log de verificacao", logEx);
                    }

                    ok++;
                } catch (Exception e) {
                    errors++;
                    String itemRef = item.get("sku") != null ? String.valueOf(item.get("sku")) : String.valueOf(item.get("codProd"));
                    errorDetails.add(itemRef + ": " + e.getMessage());
                    log.log(Level.WARNING, "Falha ao sincronizar estoque", e);
                    try {
                        LogService.getInstance().logStockSync(
                                itemRef, BigDecimal.ZERO, false, "forcarSync ERRO: " + e.getMessage());
                    } catch (Exception logEx) {
                        log.log(Level.FINE, "Falha ao gravar log de erro", logEx);
                    }
                }
            }
            result.put("success", errors == 0);
            result.put("processed", ok);
            result.put("errors", errors);
            result.put("message", "Sincronizados imediatamente: " + ok + ", erros: " + errors);
            if (!errorDetails.isEmpty()) {
                result.put("errorDetails", errorDetails);
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao forcar sync de estoque", e);
            result.put("success", false);
            result.put("message", e.getMessage());
        } finally {
            DBUtil.closeConnection(conn);
        }

        return result;
    }

    public Map<String, Object> reprocessar(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        int ok = 0;
        int errors = 0;

        try {
            QueueService queueService = QueueService.getInstance();
            Object rawIds = params.get("queueIds");
            List<Object> ids = rawIds instanceof List ? (List<Object>) rawIds : new ArrayList<>();

            if (!ids.isEmpty()) {
                for (Object idObj : ids) {
                    try {
                        BigDecimal id = asBigDecimal(idObj);
                        if (id != null) {
                            queueService.resetForRetry(id);
                            ok++;
                        }
                    } catch (Exception e) {
                        errors++;
                        log.log(Level.WARNING, "Erro ao reprocessar item", e);
                    }
                }
                if (ok > 0) {
                    processQueueNowBestEffort();
                }
            } else {
                Map<String, Object> syncResult = forcarSync(params);
                result.putAll(syncResult);
                return result;
            }

            result.put("success", errors == 0);
            result.put("processed", ok);
            result.put("errors", errors);
            result.put("message", "Reprocessados: " + ok + ", erros: " + errors);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao reprocessar estoque", e);
            result.put("success", false);
            result.put("message", e.getMessage());
        }

        return result;
    }

    private BigDecimal findCodProdBySku(Connection conn, String sku) throws Exception {
        if (sku == null || sku.isEmpty()) return null;
        String normalizedSku = normalizeSku(sku);
        BigDecimal mapped = DeparaService.getInstance().getCodProdBySkuOrEan(normalizedSku);
        if (mapped != null) {
            return mapped;
        }
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT CODPROD FROM TGFPRO WHERE REFERENCIA = ?");
            stmt.setString(1, normalizedSku);
            rs = stmt.executeQuery();
            if (rs.next()) return rs.getBigDecimal("CODPROD");
            return null;
        } finally {
            DBUtil.closeAll(rs, stmt, null);
        }
    }

    private String findSkuByCodProd(Connection conn, BigDecimal codProd) throws Exception {
        if (codProd == null) return null;
        String mapped = DeparaService.getInstance().getSkuWithFallback(codProd);
        if (mapped != null && !mapped.isEmpty()) {
            return mapped;
        }
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT REFERENCIA FROM TGFPRO WHERE CODPROD = ?");
            stmt.setBigDecimal(1, codProd);
            rs = stmt.executeQuery();
            if (rs.next()) return rs.getString("REFERENCIA");
            return null;
        } finally {
            DBUtil.closeAll(rs, stmt, null);
        }
    }

    private BigDecimal getStock(Connection conn, BigDecimal codProd, BigDecimal codLocal, BigDecimal codEmp) throws Exception {
        if (codProd == null) return null;

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            StringBuilder sql = new StringBuilder("SELECT ESTOQUE FROM TGFEST WHERE CODPROD = ?");
            List<Object> params = new ArrayList<>();
            params.add(codProd);
            if (codLocal != null) {
                sql.append(" AND CODLOCAL = ?");
                params.add(codLocal);
            }
            if (codEmp != null) {
                sql.append(" AND CODEMP = ?");
                params.add(codEmp);
            }
            stmt = conn.prepareStatement(sql.toString());
            int idx = 1;
            for (Object param : params) {
                if (param instanceof BigDecimal) {
                    stmt.setBigDecimal(idx++, (BigDecimal) param);
                } else {
                    stmt.setObject(idx++, param);
                }
            }
            rs = stmt.executeQuery();
            if (rs.next()) {
                BigDecimal estoque = rs.getBigDecimal("ESTOQUE");
                return estoque != null ? estoque : BigDecimal.ZERO;
            }
            return BigDecimal.ZERO;
        } finally {
            DBUtil.closeAll(rs, stmt, null);
        }
    }

    private List<Map<String, Object>> extractItems(Map<String, Object> params) {
        List<Map<String, Object>> items = new ArrayList<>();
        Object raw = params.get("items");
        if (raw instanceof List) {
            for (Object itemObj : (List<?>) raw) {
                if (itemObj instanceof Map) {
                    items.add((Map<String, Object>) itemObj);
                }
            }
        }
        return items;
    }

    private String resolveStockOrderBy(Map<String, Object> params) {
        String sortBy = getString(params, "sortBy");
        String sortDir = getString(params, "sortDir");
        boolean desc = sortDir == null || !"asc".equalsIgnoreCase(sortDir);
        String dir = desc ? "DESC" : "ASC";

        if (sortBy != null) {
            switch (sortBy.toLowerCase()) {
                case "sku": return "SKU " + dir;
                case "codprod": return "P.CODPROD " + dir;
                case "descricao": return "P.DESCRPROD " + dir;
                case "local":
                case "codlocal": return "E.CODLOCAL " + dir;
                case "estoque": return "ESTOQUE " + dir;
            }
        }
        return "P.CODPROD, E.CODEMP, E.CODLOCAL";
    }

    private String getString(Map<String, Object> params, String key) {
        Object value = params.get(key);
        return value != null ? value.toString() : null;
    }

    private int getInt(Map<String, Object> params, String key, int defaultVal) {
        Object value = params.get(key);
        if (value == null) return defaultVal;
        if (value instanceof Number) return ((Number) value).intValue();
        try { return Integer.parseInt(value.toString()); } catch (Exception e) { return defaultVal; }
    }

    private BigDecimal getBigDecimal(Map<String, Object> params, String key) {
        Object value = params.get(key);
        return asBigDecimal(value);
    }

    private boolean getBoolean(Map<String, Object> params, String key, boolean defaultValue) {
        Object value = params.get(key);
        if (value == null) return defaultValue;
        if (value instanceof Boolean) return (Boolean) value;
        String text = value.toString().trim();
        if (text.isEmpty()) return defaultValue;
        return "true".equalsIgnoreCase(text) || "1".equals(text) || "s".equalsIgnoreCase(text) || "sim".equalsIgnoreCase(text);
    }

    private BigDecimal asBigDecimal(Object value) {
        if (value == null) return null;
        if (value instanceof BigDecimal) return (BigDecimal) value;
        if (value instanceof Number) return BigDecimal.valueOf(((Number) value).doubleValue());
        try { return new BigDecimal(value.toString()); } catch (Exception e) { return null; }
    }

    private String asString(Object value) {
        return value != null ? value.toString() : null;
    }

    private int setParameters(PreparedStatement stmt, List<Object> params) throws Exception {
        int idx = 1;
        for (Object param : params) {
            if (param instanceof BigDecimal) {
                stmt.setBigDecimal(idx++, (BigDecimal) param);
            } else {
                stmt.setObject(idx++, param);
            }
        }
        return idx;
    }

    private void processQueueNowBestEffort() {
        try {
            new OutboxProcessorJob().executeScheduler();
        } catch (Exception e) {
            log.log(Level.WARNING, "Falha ao processar fila imediatamente (estoque)", e);
        }
    }

    private void enrichItemsWithFastchannelStock(List<Map<String, Object>> items) {
        if (items == null || items.isEmpty()) {
            return;
        }
        FastchannelStockClient client = new FastchannelStockClient();
        DeparaService depara = DeparaService.getInstance();
        Map<String, StockDTO> cache = new HashMap<>();

        for (Map<String, Object> item : items) {
            try {
                BigDecimal codProd = asBigDecimal(item.get("codProd"));
                // Usar a mesma regra canonica de SKU usada por EstoqueListener/StockFullSyncJob
                String sku = depara.getSkuForStock(codProd);
                if (sku == null || sku.isEmpty()) {
                    item.put("estoqueFastchannel", null);
                    item.put("delta", null);
                    item.put("apiStatus", "SEM_SKU");
                    continue;
                }

                String storageId = asString(item.get("storageId"));
                String cacheKey = sku + "|" + (storageId != null ? storageId : "");

                StockDTO fetched;
                if (cache.containsKey(cacheKey)) {
                    fetched = cache.get(cacheKey);
                } else {
                    fetched = client.getStock(sku, storageId);
                    cache.put(cacheKey, fetched);
                }

                applyStockToItem(item, fetched);
                item.put("apiStatus", fetched != null ? "OK" : "NAO_ENCONTRADO");
            } catch (Exception e) {
                item.put("estoqueFastchannel", null);
                item.put("delta", null);
                item.put("fcError", e.getMessage());
                item.put("apiStatus", "ERRO");
            }
        }
    }

    private void applyStockToItem(Map<String, Object> item, StockDTO stock) {
        if (stock != null) {
            item.put("estoqueFastchannel", stock.getQuantity());
            item.put("fcAvailable", stock.getAvailableQuantity());
            item.put("fcReserved", stock.getReservedQuantity());
            item.put("fcStorageId", stock.getStorageId());
            item.put("fcLastUpdate", stock.getLastUpdate());
            BigDecimal localQty = asBigDecimal(item.get("estoque"));
            BigDecimal fcQty = stock.getQuantity();
            item.put("delta", localQty != null && fcQty != null ? localQty.subtract(fcQty) : null);
        } else {
            item.put("estoqueFastchannel", null);
            item.put("delta", null);
        }
    }

    private String resolveOutboundSku(BigDecimal codProd, String fallbackSku) {
        if (codProd != null) {
            DeparaService depara = DeparaService.getInstance();
            // Priorizar regra da marca (mesma logica que EstoqueListener/StockFullSyncJob)
            String skuByRule = normalizeSku(depara.getSkuForStock(codProd));
            if (skuByRule != null && !skuByRule.isEmpty()) {
                return skuByRule;
            }
            // Fallback: de-para explicito
            String mapped = normalizeSku(depara.getCodigoExternoAtivo(DeparaService.TIPO_PRODUTO, codProd));
            if (mapped != null && !mapped.isEmpty()) {
                return mapped;
            }
        }
        String normalizedFallback = normalizeSku(fallbackSku);
        if (normalizedFallback != null && !normalizedFallback.isEmpty()) {
            return normalizedFallback;
        }
        return null;
    }

    private String chooseOutboundSku(BigDecimal codProd, String skuCandidate, boolean preferProvidedSku) {
        String normalizedCandidate = normalizeSku(skuCandidate);
        if (preferProvidedSku && normalizedCandidate != null && !normalizedCandidate.isEmpty()) {
            return normalizedCandidate;
        }
        return resolveOutboundSku(codProd, normalizedCandidate);
    }

    private List<String> resolveSkuCandidates(BigDecimal codProd, String primarySku, String fallbackSku) {
        LinkedHashSet<String> candidates = new LinkedHashSet<>();
        addCandidate(candidates, primarySku);
        if (codProd != null) {
            DeparaService depara = DeparaService.getInstance();
            addCandidate(candidates, depara.getCodigoExternoAtivo(DeparaService.TIPO_PRODUTO, codProd));
            addCandidate(candidates, depara.getSku(codProd));
            addCandidate(candidates, depara.getSkuForStock(codProd));
            addCandidate(candidates, codProd.toPlainString());
        }
        addCandidate(candidates, fallbackSku);
        return new ArrayList<>(candidates);
    }

    private void addCandidate(Set<String> candidates, String value) {
        String normalized = normalizeSku(value);
        if (normalized != null && !normalized.isEmpty()) {
            candidates.add(normalized);
        }
    }

    private String normalizeSku(String sku) {
        if (sku == null) return null;
        String normalized = sku.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    private String resolveStorageId(DeparaService deparaService, FastchannelConfig config, BigDecimal codLocal) {
        String storageId = deparaService.getCodigoExternoAtivo(DeparaService.TIPO_STOCK_STORAGE, codLocal);
        if (storageId == null || storageId.isEmpty()) {
            storageId = config.getStorageId();
        }
        return storageId;
    }

    private String resolveResellerId(DeparaService deparaService, FastchannelConfig config, BigDecimal codEmp) {
        String resellerId = deparaService.getCodigoExternoAtivo(DeparaService.TIPO_STOCK_RESELLER, codEmp);
        if (resellerId == null || resellerId.isEmpty()) {
            resellerId = config.getResellerId();
        }
        return resellerId;
    }

    private void appendMappedProductFilter(StringBuilder where, List<Object> queryParams, Connection conn, String columnExpr) {
        if (!hasConfiguredProductMapping(conn)) {
            return;
        }
        boolean hasIntegraAuto = FastchannelProductFilter.supportsColumn(conn, "AD_FCDEPARA", "INTEGRA_AUTO");
        where.append(" AND EXISTS (SELECT 1 FROM AD_FCDEPARA DPROD ");
        where.append("WHERE DPROD.TIPO_ENTIDADE = ? ");
        where.append("AND DPROD.COD_SANKHYA = ").append(columnExpr).append(" ");
        if (hasIntegraAuto) {
            where.append("AND COALESCE(DPROD.INTEGRA_AUTO, 'S') = 'S' ");
        }
        where.append(")");
        queryParams.add(DeparaService.TIPO_PRODUTO);
    }

    private boolean hasConfiguredProductMapping(Connection conn) {
        if (conn == null) return false;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            boolean hasIntegraAuto = FastchannelProductFilter.supportsColumn(conn, "AD_FCDEPARA", "INTEGRA_AUTO");
            String sql = "SELECT TOP 1 1 AS FOUND FROM AD_FCDEPARA " +
                    "WHERE TIPO_ENTIDADE = ? " +
                    (hasIntegraAuto ? "AND COALESCE(INTEGRA_AUTO, 'S') = 'S' " : "") +
                    "AND COD_SANKHYA IS NOT NULL";
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, DeparaService.TIPO_PRODUTO);
            rs = stmt.executeQuery();
            return rs.next();
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel validar de-para de produto para estoque", e);
            return false;
        } finally {
            DBUtil.closeAll(rs, stmt, null);
        }
    }

}
