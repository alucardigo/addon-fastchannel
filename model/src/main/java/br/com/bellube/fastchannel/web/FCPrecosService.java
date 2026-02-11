package br.com.bellube.fastchannel.web;

import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.PriceDTO;
import br.com.bellube.fastchannel.http.FastchannelPriceClient;
import br.com.bellube.fastchannel.service.LogService;
import br.com.bellube.fastchannel.service.QueueService;
import br.com.bellube.fastchannel.util.DBUtil;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service para gerenciamento de precos com suporte a 3 fontes de dados.
 * Fonte 1: Sankhya (banco local TGFEXC)
 * Fonte 2: API Fastchannel
 * Fonte 3: Fila + Logs de sincronizacao
 */
public class FCPrecosService {

    private static final Logger log = Logger.getLogger(FCPrecosService.class.getName());
    private static volatile Boolean hasCodEmpColumn;
    private static volatile Boolean hasAtivoColumn;
    private static volatile Boolean hasDtInicColumn;
    private static volatile Boolean hasDtFimColumn;

    public Map<String, Object> list(Map<String, Object> params) {
        int source = getInt(params, "source", 1);

        switch (source) {
            case 2:
                return listFromAPI(params);
            case 3:
                return listFromQueueLogs(params);
            default:
                return listFromSankhya(params);
        }
    }

    private Map<String, Object> listFromSankhya(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;
        PreparedStatement countStmt = null;
        PreparedStatement stmt = null;
        ResultSet rsCount = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();

            StringBuilder where = new StringBuilder("1=1");
            List<Object> queryParams = new ArrayList<>();

            String sku = getString(params, "sku");
            if (sku != null && !sku.isEmpty()) {
                where.append(" AND P.REFERENCIA LIKE ?");
                queryParams.add("%" + sku + "%");
            }

            Object codProd = params.get("codProd");
            if (codProd != null) {
                where.append(" AND E.CODPROD = ?");
                queryParams.add(codProd);
            }

            Object nuTab = params.get("nuTab");
            if (nuTab != null) {
                where.append(" AND E.NUTAB = ?");
                queryParams.add(nuTab);
            }

            Object codEmp = params.get("codEmp");
            if (codEmp != null && supportsCodEmp(conn)) {
                where.append(" AND E.CODEMP = ?");
                queryParams.add(codEmp);
            }

            boolean useDtInic = supportsDtInic(conn);
            boolean useDtFim = supportsDtFim(conn);

            String dataInicio = getString(params, "dataInicio");
            if (useDtInic && dataInicio != null && !dataInicio.isEmpty()) {
                where.append(" AND E.DTINIC >= ?");
                queryParams.add(dataInicio);
            }

            String dataFim = getString(params, "dataFim");
            if (useDtInic && dataFim != null && !dataFim.isEmpty()) {
                where.append(" AND E.DTINIC <= ?");
                queryParams.add(dataFim + " 23:59:59");
            }

            String status = getString(params, "status");
            if (status != null && !status.isEmpty()) {
                where.append(" AND Q.STATUS = ?");
                queryParams.add(status);
            }

            int page = getInt(params, "page", 1);
            int pageSize = getInt(params, "pageSize", 20);
            int offset = (page - 1) * pageSize;

            String queueJoin = "LEFT JOIN ( " +
                    "  SELECT IDQUEUE, ENTITY_KEY, STATUS, LAST_ERROR, DH_CRIACAO FROM ( " +
                    "    SELECT Q.IDQUEUE, Q.ENTITY_KEY, Q.STATUS, Q.LAST_ERROR, Q.DH_CRIACAO, " +
                    "           ROW_NUMBER() OVER (PARTITION BY Q.ENTITY_KEY ORDER BY Q.DH_CRIACAO DESC) AS RN " +
                    "    FROM AD_FCQUEUE Q " +
                    "    WHERE Q.ENTITY_TYPE = '" + FastchannelConstants.ENTITY_PRECO + "' " +
                    "      AND Q.STATUS IN ('" + FastchannelConstants.QUEUE_STATUS_PENDENTE + "', '" +
                    FastchannelConstants.QUEUE_STATUS_PROCESSANDO + "', '" + FastchannelConstants.QUEUE_STATUS_ERRO + "') " +
                    "  ) Q1 WHERE Q1.RN = 1 " +
                    ") Q ON Q.ENTITY_KEY = P.REFERENCIA ";

            // Count total
            String countSql = "SELECT COUNT(*) AS CNT FROM TGFEXC E " +
                    "INNER JOIN TGFPRO P ON E.CODPROD = P.CODPROD " +
                    queueJoin +
                    "WHERE " + where;
            countStmt = conn.prepareStatement(countSql);
            setParameters(countStmt, queryParams);
            rsCount = countStmt.executeQuery();
            rsCount.next();
            int total = rsCount.getInt("CNT");

            // Get page com ultimo registro de fila e log (compatível Oracle/SQL Server)
            String sql = "SELECT E.NUTAB, E.CODPROD, P.REFERENCIA, P.DESCRPROD, " +
                    "E.VLRVENDA, " +
                    (useDtInic ? "E.DTINIC" : "NULL") + " AS DTINIC, " +
                    (useDtFim ? "E.DTFIM" : "NULL") + " AS DTFIM, " +
                    (supportsAtivo(conn) ? "E.ATIVO" : "NULL") + " AS ATIVO, " +
                    "Q.IDQUEUE AS QUEUE_ID, Q.STATUS AS QUEUE_STATUS, Q.LAST_ERROR AS QUEUE_ERROR, Q.DH_CRIACAO AS QUEUE_DH, " +
                    "L.IDLOG AS LOG_ID, L.NIVEL AS LOG_NIVEL, L.MENSAGEM AS LOG_MSG, L.DH_REGISTRO AS LOG_DH " +
                    "FROM TGFEXC E " +
                    "INNER JOIN TGFPRO P ON E.CODPROD = P.CODPROD " +
                    queueJoin +
                    "LEFT JOIN ( " +
                    "  SELECT IDLOG, NIVEL, MENSAGEM, REFERENCIA, DH_REGISTRO FROM ( " +
                    "    SELECT L.IDLOG, L.NIVEL, L.MENSAGEM, L.REFERENCIA, L.DH_REGISTRO, " +
                    "           ROW_NUMBER() OVER (PARTITION BY L.REFERENCIA ORDER BY L.DH_REGISTRO DESC) AS RN " +
                    "    FROM AD_FCLOG L WHERE L.OPERACAO = '" + LogService.OP_PRICE_SYNC + "' " +
                    "  ) L1 WHERE L1.RN = 1 " +
                    ") L ON L.REFERENCIA = P.REFERENCIA " +
                    "WHERE " + where +
                    " ORDER BY " + (useDtInic ? "E.DTINIC" : "E.NUTAB") + " DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";

            stmt = conn.prepareStatement(sql);
            int paramIndex = setParameters(stmt, queryParams);
            stmt.setInt(paramIndex++, offset);
            stmt.setInt(paramIndex, pageSize);
            rs = stmt.executeQuery();

            List<Map<String, Object>> items = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> item = new HashMap<>();
                item.put("nuTab", rs.getBigDecimal("NUTAB"));
                item.put("codProd", rs.getBigDecimal("CODPROD"));
                item.put("sku", rs.getString("REFERENCIA"));
                item.put("descricao", rs.getString("DESCRPROD"));
                item.put("vlrVenda", rs.getBigDecimal("VLRVENDA"));
                item.put("dtInic", rs.getTimestamp("DTINIC"));
                item.put("dtFim", rs.getTimestamp("DTFIM"));
                item.put("ativo", rs.getString("ATIVO"));
                item.put("queueId", rs.getObject("QUEUE_ID"));
                item.put("queueStatus", rs.getString("QUEUE_STATUS"));
                item.put("queueError", rs.getString("QUEUE_ERROR"));
                item.put("logId", rs.getObject("LOG_ID"));
                item.put("logNivel", rs.getString("LOG_NIVEL"));
                item.put("logMsg", rs.getString("LOG_MSG"));
                item.put("logDh", rs.getTimestamp("LOG_DH"));
                items.add(item);
            }

            result.put("items", items);
            result.put("total", total);
            result.put("page", page);
            result.put("pageSize", pageSize);
            result.put("source", 1);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao listar precos Sankhya", e);
            result.put("error", e.getMessage());
        } finally {
            DBUtil.closeAll(rsCount, countStmt, null);
            DBUtil.closeAll(rs, stmt, conn);
        }

        return result;
    }

    private Map<String, Object> listFromAPI(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;
        PreparedStatement countStmt = null;
        PreparedStatement stmt = null;
        ResultSet rsCount = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();

            // Primeiro busca SKUs do Sankhya
            boolean useAtivo = supportsAtivo(conn);
            StringBuilder where = new StringBuilder("1=1");
            if (useAtivo) {
                where.append(" AND E.ATIVO = 'S'");
            }
            List<Object> queryParams = new ArrayList<>();

            String sku = getString(params, "sku");
            if (sku != null && !sku.isEmpty()) {
                where.append(" AND P.REFERENCIA LIKE ?");
                queryParams.add("%" + sku + "%");
            }

            Object codProd = params.get("codProd");
            if (codProd != null) {
                where.append(" AND P.CODPROD = ?");
                queryParams.add(codProd);
            }

            Object nuTab = params.get("nuTab");
            if (nuTab != null) {
                where.append(" AND E.NUTAB = ?");
                queryParams.add(nuTab);
            }

            Object codEmp = params.get("codEmp");
            if (codEmp != null && supportsCodEmp(conn)) {
                where.append(" AND E.CODEMP = ?");
                queryParams.add(codEmp);
            }

            int page = getInt(params, "page", 1);
            int pageSize = getInt(params, "pageSize", 20);
            int offset = (page - 1) * pageSize;

            // Count total
            String countSql = "SELECT COUNT(DISTINCT P.REFERENCIA) AS CNT " +
                    "FROM TGFEXC E " +
                    "INNER JOIN TGFPRO P ON E.CODPROD = P.CODPROD " +
                    "WHERE " + where;
            countStmt = conn.prepareStatement(countSql);
            setParameters(countStmt, queryParams);
            rsCount = countStmt.executeQuery();
            rsCount.next();
            int total = rsCount.getInt("CNT");

            // Get SKUs distinct com paginação
            String sql = "SELECT DISTINCT P.CODPROD, P.REFERENCIA, P.DESCRPROD, E.NUTAB " +
                    "FROM TGFEXC E " +
                    "INNER JOIN TGFPRO P ON E.CODPROD = P.CODPROD " +
                    "WHERE " + where +
                    " ORDER BY P.REFERENCIA OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";

            stmt = conn.prepareStatement(sql);
            int paramIndex = setParameters(stmt, queryParams);
            stmt.setInt(paramIndex++, offset);
            stmt.setInt(paramIndex, pageSize);
            rs = stmt.executeQuery();

            // Para cada SKU, consulta na API Fastchannel
            List<Map<String, Object>> items = new ArrayList<>();
            FastchannelPriceClient priceClient = new FastchannelPriceClient();

            while (rs.next()) {
                String skuRef = rs.getString("REFERENCIA");
                Map<String, Object> item = new HashMap<>();
                item.put("codProd", rs.getBigDecimal("CODPROD"));
                item.put("sku", skuRef);
                item.put("descricao", rs.getString("DESCRPROD"));
                item.put("nuTab", rs.getBigDecimal("NUTAB"));

                try {
                    PriceDTO priceFC = priceClient.getPrice(skuRef);
                    if (priceFC != null) {
                        item.put("vlrVenda", priceFC.getPrice());
                        item.put("listPrice", priceFC.getListPrice());
                        item.put("promotionalPrice", priceFC.getPromotionalPrice());
                        item.put("resellerId", priceFC.getResellerId());
                        item.put("lastUpdate", priceFC.getLastUpdate());
                        item.put("apiStatus", "OK");
                        item.put("apiMessage", "OK");
                    } else {
                        item.put("vlrVenda", BigDecimal.ZERO);
                        item.put("apiStatus", "NAO_ENCONTRADO");
                        item.put("apiMessage", "SKU nao encontrado no Fastchannel");
                    }
                } catch (Exception e) {
                    log.log(Level.WARNING, "Erro ao consultar API para SKU " + skuRef, e);
                    item.put("vlrVenda", null);
                    item.put("apiStatus", "ERRO");
                    item.put("apiError", e.getMessage());
                    item.put("apiMessage", e.getMessage());
                }

                items.add(item);
            }

            result.put("items", items);
            result.put("total", total);
            result.put("page", page);
            result.put("pageSize", pageSize);
            result.put("source", 2);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao listar precos API", e);
            result.put("error", e.getMessage());
        } finally {
            DBUtil.closeAll(rsCount, countStmt, null);
            DBUtil.closeAll(rs, stmt, conn);
        }

        return result;
    }

    private Map<String, Object> listFromQueueLogs(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;
        PreparedStatement countStmt = null;
        PreparedStatement stmt = null;
        ResultSet rsCount = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();

            StringBuilder where = new StringBuilder("1=1 AND Q.ENTITY_TYPE = '" + FastchannelConstants.ENTITY_PRECO + "'");
            List<Object> queryParams = new ArrayList<>();

            String sku = getString(params, "sku");
            if (sku != null && !sku.isEmpty()) {
                where.append(" AND Q.ENTITY_KEY LIKE ?");
                queryParams.add("%" + sku + "%");
            }

            Object codProd = params.get("codProd");
            if (codProd != null) {
                where.append(" AND Q.ENTITY_ID = ?");
                queryParams.add(codProd);
            }

            String status = getString(params, "status");
            if (status != null && !status.isEmpty()) {
                where.append(" AND Q.STATUS = ?");
                queryParams.add(status);
            }

            Object codEmp = params.get("codEmp");
            if (codEmp != null && supportsCodEmp(conn)) {
                where.append(" AND E.CODEMP = ?");
                queryParams.add(codEmp);
            }

            int page = getInt(params, "page", 1);
            int pageSize = getInt(params, "pageSize", 20);
            int offset = (page - 1) * pageSize;

            // Count total
            String countSql = "SELECT COUNT(*) AS CNT FROM AD_FCQUEUE Q " +
                    "LEFT JOIN TGFPRO P ON P.CODPROD = Q.ENTITY_ID " +
                    "LEFT JOIN TGFEXC E ON E.CODPROD = Q.ENTITY_ID " +
                    "WHERE " + where;
            countStmt = conn.prepareStatement(countSql);
            setParameters(countStmt, queryParams);
            rsCount = countStmt.executeQuery();
            rsCount.next();
            int total = rsCount.getInt("CNT");

            // Get page com ultimo log (compatível Oracle/SQL Server) e descricao do produto
            String sql = "SELECT Q.IDQUEUE, Q.ENTITY_KEY AS SKU, Q.ENTITY_ID AS CODPROD, " +
                    "P.DESCRPROD AS DESCRPROD, " +
                    "Q.STATUS AS QUEUE_STATUS, Q.RETRY_COUNT, Q.LAST_ERROR, Q.DH_CRIACAO, Q.PRIORITY, " +
                    "L.IDLOG AS LOG_ID, L.NIVEL AS LOG_NIVEL, L.MENSAGEM AS LOG_MSG, L.DH_REGISTRO AS LOG_DH " +
                    "FROM AD_FCQUEUE Q " +
                    "LEFT JOIN TGFPRO P ON P.CODPROD = Q.ENTITY_ID " +
                    "LEFT JOIN TGFEXC E ON E.CODPROD = Q.ENTITY_ID " +
                    "LEFT JOIN ( " +
                    "  SELECT IDLOG, NIVEL, MENSAGEM, REFERENCIA, DH_REGISTRO FROM ( " +
                    "    SELECT L.IDLOG, L.NIVEL, L.MENSAGEM, L.REFERENCIA, L.DH_REGISTRO, " +
                    "           ROW_NUMBER() OVER (PARTITION BY L.REFERENCIA ORDER BY L.DH_REGISTRO DESC) AS RN " +
                    "    FROM AD_FCLOG L WHERE L.OPERACAO = '" + LogService.OP_PRICE_SYNC + "' " +
                    "  ) L1 WHERE L1.RN = 1 " +
                    ") L ON L.REFERENCIA = Q.ENTITY_KEY " +
                    "WHERE " + where +
                    " ORDER BY Q.DH_CRIACAO DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";

            stmt = conn.prepareStatement(sql);
            int paramIndex = setParameters(stmt, queryParams);
            stmt.setInt(paramIndex++, offset);
            stmt.setInt(paramIndex, pageSize);
            rs = stmt.executeQuery();

            List<Map<String, Object>> items = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> item = new HashMap<>();
                item.put("queueId", rs.getBigDecimal("IDQUEUE"));
                item.put("sku", rs.getString("SKU"));
                item.put("codProd", rs.getBigDecimal("CODPROD"));
                item.put("queueStatus", rs.getString("QUEUE_STATUS"));
                item.put("retryCount", rs.getInt("RETRY_COUNT"));
                item.put("queueError", rs.getString("LAST_ERROR"));
                item.put("dhCriacao", rs.getTimestamp("DH_CRIACAO"));
                item.put("priority", rs.getBigDecimal("PRIORITY"));
                item.put("descricao", rs.getString("DESCRPROD"));
                item.put("logId", rs.getObject("LOG_ID"));
                item.put("logNivel", rs.getString("LOG_NIVEL"));
                item.put("logMsg", rs.getString("LOG_MSG"));
                item.put("logDh", rs.getTimestamp("LOG_DH"));
                items.add(item);
            }

            result.put("items", items);
            result.put("total", total);
            result.put("page", page);
            result.put("pageSize", pageSize);
            result.put("source", 3);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao listar fila/logs precos", e);
            result.put("error", e.getMessage());
        } finally {
            DBUtil.closeAll(rsCount, countStmt, null);
            DBUtil.closeAll(rs, stmt, conn);
        }

        return result;
    }

    public Map<String, Object> compararFC(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        String sku = getString(params, "sku");

        if (sku == null || sku.isEmpty()) {
            result.put("error", "SKU obrigatorio");
            return result;
        }

        try {
            BigDecimal precoSankhya = getPrecoSankhya(sku);
            FastchannelPriceClient priceClient = new FastchannelPriceClient();
            PriceDTO priceFC = priceClient.getPrice(sku);

            result.put("sku", sku);
            result.put("precoSankhya", precoSankhya);

            if (priceFC != null) {
                result.put("precoFastchannel", priceFC.getPrice());
                result.put("listPrice", priceFC.getListPrice());
                result.put("resellerId", priceFC.getResellerId());
                BigDecimal delta = precoSankhya.subtract(priceFC.getPrice());
                result.put("delta", delta);
                result.put("divergencia", delta.compareTo(BigDecimal.ZERO) != 0);
            } else {
                result.put("precoFastchannel", null);
                result.put("message", "SKU nao encontrado no Fastchannel");
                result.put("divergencia", true);
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao comparar preco com FC", e);
            result.put("error", e.getMessage());
        }

        return result;
    }

    public Map<String, Object> forcarSync(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Object codProdObj = params.get("codProd");
        String sku = getString(params, "sku");

        if (codProdObj == null || sku == null || sku.isEmpty()) {
            result.put("success", false);
            result.put("message", "codProd e sku obrigatorios");
            return result;
        }

        try {
            BigDecimal codProd = toBigDecimal(codProdObj);
            QueueService queueService = QueueService.getInstance();
            queueService.enqueuePrice(codProd, sku);

            result.put("success", true);
            result.put("message", "Item enfileirado para sincronizacao");
            log.info("Preco enfileirado para sync: " + sku);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao forcar sync preco", e);
            result.put("success", false);
            result.put("message", e.getMessage());
        }

        return result;
    }

    public Map<String, Object> reprocessar(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Object queueIdObj = params.get("queueId");

        if (queueIdObj == null) {
            return forcarSync(params);
        }

        try {
            BigDecimal queueId = toBigDecimal(queueIdObj);
            QueueService queueService = QueueService.getInstance();
            queueService.resetForRetry(queueId);

            result.put("success", true);
            result.put("message", "Item resetado para reprocessamento");
            log.info("Item de preco resetado para retry: " + queueId);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao reprocessar item preco", e);
            result.put("success", false);
            result.put("message", e.getMessage());
        }

        return result;
    }

    /**
     * Sincroniza múltiplos itens em lote
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> syncEmLote(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Object itemsObj = params.get("items");
        Object skusObj = params.get("skus");
        List<Map<String, Object>> items = new ArrayList<>();

        try {
            if (itemsObj instanceof List) {
                for (Object obj : (List<?>) itemsObj) {
                    if (obj instanceof Map) {
                        items.add((Map<String, Object>) obj);
                    } else if (obj instanceof String) {
                        Map<String, Object> item = new HashMap<>();
                        item.put("sku", obj.toString());
                        items.add(item);
                    }
                }
            } else if (skusObj instanceof List) {
                for (Object obj : (List<?>) skusObj) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("sku", obj);
                    items.add(item);
                }
            } else if (skusObj instanceof String) {
                for (String sku : ((String) skusObj).split(",")) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("sku", sku);
                    items.add(item);
                }
            }

            if (items.isEmpty()) {
                result.put("success", false);
                result.put("message", "Lista de itens ou SKUs obrigatoria");
                return result;
            }

            int successCount = 0;
            int errorCount = 0;
            List<String> errors = new ArrayList<>();
            QueueService queueService = QueueService.getInstance();

            for (Map<String, Object> item : items) {
                String sku = item.get("sku") != null ? item.get("sku").toString().trim() : null;
                Object codProdObj = item.get("codProd");
                if (sku == null || sku.isEmpty()) continue;

                try {
                    BigDecimal codProd = codProdObj != null ? toBigDecimal(codProdObj) : getCodProdFromSku(sku);
                    if (codProd == null) {
                        errors.add(sku + ": Produto nao encontrado");
                        errorCount++;
                        continue;
                    }
                    queueService.enqueuePrice(codProd, sku.trim());
                    successCount++;
                } catch (Exception e) {
                    errors.add(sku + ": " + e.getMessage());
                    errorCount++;
                }
            }

            result.put("success", true);
            result.put("total", items.size());
            result.put("successCount", successCount);
            result.put("errorCount", errorCount);
            if (!errors.isEmpty()) {
                result.put("errors", errors);
            }
            result.put("message", String.format("Sincronizacao em lote: %d sucesso, %d erro(s)", successCount, errorCount));

            log.info("Sync em lote de precos concluido: " + successCount + " sucesso, " + errorCount + " erro(s)");

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao sincronizar precos em lote", e);
            result.put("success", false);
            result.put("message", e.getMessage());
        }

        return result;
    }

    private BigDecimal getCodProdFromSku(String sku) throws Exception {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                "SELECT P.CODPROD FROM TGFPRO P WHERE P.REFERENCIA = ?"
            );
            stmt.setString(1, sku);
            rs = stmt.executeQuery();

            if (rs.next()) {
                return rs.getBigDecimal("CODPROD");
            }

            return null;

        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
    }

    private BigDecimal getPrecoSankhya(String sku) throws Exception {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();
            String orderCol = supportsDtInic(conn) ? "E.DTINIC" : "E.NUTAB";
            String ativoFilter = supportsAtivo(conn) ? " AND E.ATIVO = 'S' " : " ";
            stmt = conn.prepareStatement(
                "SELECT VLRVENDA FROM ( " +
                "  SELECT E.VLRVENDA, ROW_NUMBER() OVER (ORDER BY " + orderCol + " DESC) AS RN " +
                "  FROM TGFEXC E " +
                "  INNER JOIN TGFPRO P ON E.CODPROD = P.CODPROD " +
                "  WHERE P.REFERENCIA = ? " + ativoFilter +
                ") X WHERE X.RN = 1"
            );
            stmt.setString(1, sku);
            rs = stmt.executeQuery();

            if (rs.next()) {
                BigDecimal vlr = rs.getBigDecimal("VLRVENDA");
                return vlr != null ? vlr : BigDecimal.ZERO;
            }

            return BigDecimal.ZERO;

        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
    }

    private String getString(Map<String, Object> params, String key) {
        Object value = params.get(key);
        return value != null ? value.toString() : null;
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

    private BigDecimal toBigDecimal(Object value) {
        if (value == null) return null;
        if (value instanceof BigDecimal) return (BigDecimal) value;
        if (value instanceof Number) return BigDecimal.valueOf(((Number) value).doubleValue());
        try {
            return new BigDecimal(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private int setParameters(PreparedStatement stmt, List<Object> params) throws Exception {
        int idx = 1;
        for (Object param : params) {
            stmt.setObject(idx++, param);
        }
        return idx;
    }

    private boolean supportsCodEmp(Connection conn) {
        if (hasCodEmpColumn != null) return hasCodEmpColumn;
        hasCodEmpColumn = supportsColumn(conn, "TGFEXC", "CODEMP");
        return hasCodEmpColumn;
    }

    private boolean supportsAtivo(Connection conn) {
        if (hasAtivoColumn != null) return hasAtivoColumn;
        hasAtivoColumn = supportsColumn(conn, "TGFEXC", "ATIVO");
        return hasAtivoColumn;
    }

    private boolean supportsDtInic(Connection conn) {
        if (hasDtInicColumn != null) return hasDtInicColumn;
        hasDtInicColumn = supportsColumn(conn, "TGFEXC", "DTINIC");
        return hasDtInicColumn;
    }

    private boolean supportsDtFim(Connection conn) {
        if (hasDtFimColumn != null) return hasDtFimColumn;
        hasDtFimColumn = supportsColumn(conn, "TGFEXC", "DTFIM");
        return hasDtFimColumn;
    }

    private boolean supportsColumn(Connection conn, String table, String column) {
        boolean found = false;
        try {
            try (ResultSet rs = conn.getMetaData().getColumns(null, null, table, column)) {
                if (rs.next()) return true;
            }
            try (ResultSet rs = conn.getMetaData().getColumns(null, null, table.toUpperCase(), column.toUpperCase())) {
                if (rs.next()) return true;
            }
            try (ResultSet rs = conn.getMetaData().getColumns(null, null, table.toLowerCase(), column.toLowerCase())) {
                found = rs.next();
            }
        } catch (Exception e) {
            return false;
        }
        return found;
    }
}
