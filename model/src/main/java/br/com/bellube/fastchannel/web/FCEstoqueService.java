package br.com.bellube.fastchannel.web;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.StockDTO;
import br.com.bellube.fastchannel.http.FastchannelStockClient;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.QueueService;
import br.com.bellube.fastchannel.service.StockResolver;
import br.com.bellube.fastchannel.util.DBUtil;

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

            StringBuilder where = new StringBuilder("1=1");
            List<Object> queryParams = new ArrayList<>();

            String sku = getString(params, "sku");
            if (sku != null && !sku.isEmpty()) {
                where.append(" AND P.REFERENCIA LIKE ?");
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

            if ("queue".equalsIgnoreCase(source)) {
                where.append(" AND QL.IDQUEUE IS NOT NULL");
            }

            int page = getInt(params, "page", 1);
            int pageSize = getInt(params, "pageSize", 20);
            int offset = (page - 1) * pageSize;

            String fromSql = " FROM TGFEST E " +
                    " INNER JOIN TGFPRO P ON P.CODPROD = E.CODPROD " +
                    " OUTER APPLY ( " +
                    "   SELECT TOP 1 Q.IDQUEUE, Q.STATUS, Q.DH_CRIACAO, Q.DH_PROCESSAMENTO, Q.LAST_ERROR " +
                    "   FROM AD_FCQUEUE Q " +
                    "   WHERE Q.ENTITY_TYPE = '" + FastchannelConstants.ENTITY_ESTOQUE + "' " +
                    "     AND (Q.ENTITY_ID = P.CODPROD OR Q.ENTITY_KEY = P.REFERENCIA) " +
                    "   ORDER BY Q.DH_CRIACAO DESC " +
                    " ) QL " +
                    " OUTER APPLY ( " +
                    "   SELECT TOP 1 L.NIVEL, L.MENSAGEM, L.DH_REGISTRO " +
                    "   FROM AD_FCLOG L " +
                    "   WHERE L.OPERACAO = '" + "STOCK_SYNC" + "' " +
                    "     AND L.REFERENCIA = P.REFERENCIA " +
                    "   ORDER BY L.DH_REGISTRO DESC " +
                    " ) LL ";

            String countSql = "SELECT COUNT(*) AS CNT " + fromSql + " WHERE " + where;
            countStmt = conn.prepareStatement(countSql);
            setParameters(countStmt, queryParams);
            rsCount = countStmt.executeQuery();
            rsCount.next();
            int total = rsCount.getInt("CNT");

            String sql = "SELECT " +
                    " P.CODPROD, P.DESCRPROD, P.REFERENCIA AS SKU, " +
                    " E.CODLOCAL, E.CODEMP, E.ESTOQUE, " +
                    " QL.IDQUEUE, QL.STATUS AS QUEUE_STATUS, QL.DH_CRIACAO AS QUEUE_DATE, QL.LAST_ERROR, " +
                    " LL.NIVEL AS LOG_LEVEL, LL.MENSAGEM AS LOG_MESSAGE, LL.DH_REGISTRO AS LOG_DATE " +
                    fromSql +
                    " WHERE " + where +
                    " ORDER BY P.CODPROD OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";

            stmt = conn.prepareStatement(sql);
            int paramIndex = setParameters(stmt, queryParams);
            stmt.setInt(paramIndex++, offset);
            stmt.setInt(paramIndex, pageSize);
            rs = stmt.executeQuery();

            List<Map<String, Object>> items = new ArrayList<>();
            DeparaService deparaService = DeparaService.getInstance();
            while (rs.next()) {
                Map<String, Object> item = new HashMap<>();
                item.put("codProd", rs.getBigDecimal("CODPROD"));
                item.put("descricao", rs.getString("DESCRPROD"));
                item.put("sku", rs.getString("SKU"));
                BigDecimal codLocalRow = rs.getBigDecimal("CODLOCAL");
                BigDecimal codEmpRow = rs.getBigDecimal("CODEMP");
                item.put("codLocal", codLocalRow);
                item.put("codEmp", codEmpRow);
                item.put("estoque", rs.getBigDecimal("ESTOQUE"));
                item.put("storageId", deparaService.getCodigoExterno(DeparaService.TIPO_STOCK_STORAGE, codLocalRow));
                item.put("resellerId", deparaService.getCodigoExterno(DeparaService.TIPO_STOCK_RESELLER, codEmpRow));
                item.put("queueId", rs.getBigDecimal("IDQUEUE"));
                item.put("queueStatus", rs.getString("QUEUE_STATUS"));
                item.put("queueDate", rs.getTimestamp("QUEUE_DATE"));
                item.put("queueError", rs.getString("LAST_ERROR"));
                item.put("logLevel", rs.getString("LOG_LEVEL"));
                item.put("logMessage", rs.getString("LOG_MESSAGE"));
                item.put("logDate", rs.getTimestamp("LOG_DATE"));
                items.add(item);
            }

            result.put("items", items);
            result.put("total", total);
            result.put("page", page);
            result.put("pageSize", pageSize);
            result.put("source", source);

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
            if (sku == null || sku.isEmpty()) {
                sku = findSkuByCodProd(conn, codProd);
            }

            FastchannelConfig config = FastchannelConfig.getInstance();
            if (codLocal == null) codLocal = config.getCodLocal();
            if (codEmp == null) codEmp = config.getCodemp();

            BigDecimal estoque = new StockResolver().resolve(codProd, codEmp, codLocal);
            if (estoque == null) estoque = BigDecimal.ZERO;

            FastchannelStockClient client = new FastchannelStockClient();
            StockDTO fcStock = client.getStock(sku);

            result.put("sku", sku);
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
            } else {
                result.put("estoqueFastchannel", null);
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
            QueueService queueService = QueueService.getInstance();

            for (Map<String, Object> item : items) {
                try {
                    String sku = asString(item.get("sku"));
                    BigDecimal codProd = asBigDecimal(item.get("codProd"));
                    BigDecimal codLocal = asBigDecimal(item.get("codLocal"));
                    BigDecimal codEmp = asBigDecimal(item.get("codEmp"));

                    if (codProd == null && (sku == null || sku.isEmpty())) {
                        throw new Exception("sku/codProd ausente");
                    }
                    if (codProd == null) codProd = findCodProdBySku(conn, sku);
                    if (sku == null || sku.isEmpty()) sku = findSkuByCodProd(conn, codProd);

                    if (codLocal == null) codLocal = config.getCodLocal();
                    if (codEmp == null) codEmp = config.getCodemp();

                    BigDecimal estoque = new StockResolver().resolve(codProd, codEmp, codLocal);
                    if (estoque == null) estoque = BigDecimal.ZERO;
                    queueService.enqueueStock(codProd, sku, estoque);
                    ok++;
                } catch (Exception e) {
                    errors++;
                    log.log(Level.WARNING, "Falha ao enfileirar estoque", e);
                }
            }

            result.put("success", errors == 0);
            result.put("processed", ok);
            result.put("errors", errors);
            result.put("message", "Enfileirado: " + ok + ", erros: " + errors);

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
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT CODPROD FROM TGFPRO WHERE REFERENCIA = ?");
            stmt.setString(1, sku);
            rs = stmt.executeQuery();
            if (rs.next()) return rs.getBigDecimal("CODPROD");
            return null;
        } finally {
            DBUtil.closeAll(rs, stmt, null);
        }
    }

    private String findSkuByCodProd(Connection conn, BigDecimal codProd) throws Exception {
        if (codProd == null) return null;
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
}
