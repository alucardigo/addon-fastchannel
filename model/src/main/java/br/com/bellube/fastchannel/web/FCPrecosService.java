package br.com.bellube.fastchannel.web;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.PriceBatchItemDTO;
import br.com.bellube.fastchannel.dto.PriceDTO;
import br.com.bellube.fastchannel.service.PriceBatchResolver;
import br.com.bellube.fastchannel.http.FastchannelPriceClient;
import br.com.bellube.fastchannel.job.OutboxProcessorJob;
import br.com.bellube.fastchannel.service.LogService;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.PriceResolver;
import br.com.bellube.fastchannel.service.PriceService;
import br.com.bellube.fastchannel.service.PriceTableResolver;
import br.com.bellube.fastchannel.service.QueueService;
import br.com.bellube.fastchannel.util.DBUtil;
import br.com.bellube.fastchannel.util.FastchannelProductFilter;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
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

    /**
     * [POOL-EXHAUSTION GUARD] Mutex de execucao do syncEmLote.
     *
     * INCIDENTE 2026-04-13 10:33:04–10:34:58: usuario clicou "Sincronizar Selecionados"
     * duas vezes (threads default task-321 + default task-332), cada uma processando 20 itens.
     * Cada item abre ~15 conexoes DB (resolveTableToNuTabMap x4, resolvers, verify, batch).
     * Total: 40 items × 15 conn = ~600 conn → esgotou pool MGEDS (800 max) → IJ000655
     * "No managed connections available" → SISTEMA INTEIRO CAIU.
     *
     * Este AtomicBoolean impede execucoes paralelas. A segunda chamada retorna imediatamente
     * com mensagem amigavel.
     */
    private static final AtomicBoolean SYNC_EM_LOTE_RUNNING = new AtomicBoolean(false);

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
            String skuExpr = FastchannelProductFilter.resolveSkuExpression(conn);
            String marcaJoin = FastchannelProductFilter.resolveMarcaJoin(conn);

            StringBuilder where = new StringBuilder("1=1");
            List<Object> queryParams = new ArrayList<>();
            appendConfiguredEmpresasFilter(where, queryParams, conn, "E.CODEMP");
            // Filtro por locais de estoque removido: precos sao filtrados apenas por empresa e tabela configurada.

            String sku = getString(params, "sku");
            if (sku != null && !sku.isEmpty()) {
                where.append(" AND ").append(skuExpr).append(" LIKE ?");
                queryParams.add("%" + sku + "%");
            }

            BigDecimal codProd = getBigDecimal(params, "codProd");
            if (codProd != null) {
                where.append(" AND E.CODPROD = ?");
                queryParams.add(codProd);
            }

            BigDecimal nuTab = getBigDecimal(params, "nuTab");
            String priceTableId = getString(params, "priceTableId");
            if (nuTab != null) {
                where.append(" AND E.NUTAB = ?");
                queryParams.add(nuTab);
            } else if (priceTableId != null && !priceTableId.isEmpty()) {
                appendPriceTableFilter(where, queryParams, priceTableId, conn);
            } else {
                List<BigDecimal> tables = loadConfiguredPriceTables(conn);
                if (tables.isEmpty()) {
                    tables = new PriceTableResolver().resolveEligibleTables();
                }
                if (!tables.isEmpty()) {
                    where.append(" AND E.NUTAB IN (");
                    for (int i = 0; i < tables.size(); i++) {
                        if (i > 0) where.append(", ");
                        where.append("?");
                        queryParams.add(tables.get(i));
                    }
                    where.append(")");
                }
            }

            BigDecimal codEmp = getBigDecimal(params, "codEmp");
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

            int page = getInt(params, "page", 1);
            int pageSize = getInt(params, "pageSize", 20);
            int offset = (page - 1) * pageSize;

            String queueJoin = "LEFT JOIN ( " +
                    "  SELECT IDQUEUE, ENTITY_ID, ENTITY_KEY, STATUS, LAST_ERROR, DH_CRIACAO FROM ( " +
                    "    SELECT Q.IDQUEUE, Q.ENTITY_ID, Q.ENTITY_KEY, Q.STATUS, Q.LAST_ERROR, Q.DH_CRIACAO, " +
                    "           ROW_NUMBER() OVER (PARTITION BY Q.ENTITY_ID ORDER BY Q.DH_CRIACAO DESC) AS RN " +
                    "    FROM AD_FCQUEUE Q " +
                    "    WHERE Q.ENTITY_TYPE = '" + FastchannelConstants.ENTITY_PRECO + "' " +
                    "      AND Q.STATUS IN ('" + FastchannelConstants.QUEUE_STATUS_PENDENTE + "', '" +
                    FastchannelConstants.QUEUE_STATUS_PROCESSANDO + "', '" + FastchannelConstants.QUEUE_STATUS_ERRO + "') " +
                    "  ) Q1 WHERE Q1.RN = 1 " +
                    ") Q ON Q.ENTITY_ID = P.CODPROD ";

            // Filtro para pegar apenas a NUTAB mais recente (global) por CODTAB
            // Produtos que so existem em versoes antigas da tabela NAO aparecem
            String latestNutabJoin = "INNER JOIN TGFTAB T ON T.NUTAB = E.NUTAB " +
                    "INNER JOIN ( " +
                    "  SELECT T2.CODTAB, MAX(T2.NUTAB) AS MAX_NUTAB " +
                    "  FROM TGFTAB T2 " +
                    "  GROUP BY T2.CODTAB " +
                    ") LN ON LN.CODTAB = T.CODTAB AND LN.MAX_NUTAB = E.NUTAB ";

            // Count total
            String countSql = "SELECT COUNT(*) AS CNT FROM TGFEXC E " +
                    "INNER JOIN TGFPRO P ON E.CODPROD = P.CODPROD " +
                    latestNutabJoin +
                    marcaJoin +
                    queueJoin +
                    "WHERE " + where;
            countStmt = conn.prepareStatement(countSql);
            setParameters(countStmt, queryParams);
            rsCount = countStmt.executeQuery();
            rsCount.next();
            int total = rsCount.getInt("CNT");

            // Get page com ultimo registro de fila e log (compativel Oracle/SQL Server)
            String sql = "SELECT E.NUTAB, E.CODPROD, " + skuExpr + " AS REFERENCIA, P.DESCRPROD, " +
                    "E.VLRVENDA, " +
                    (useDtInic ? "E.DTINIC" : "NULL") + " AS DTINIC, " +
                    (useDtFim ? "E.DTFIM" : "NULL") + " AS DTFIM, " +
                    (supportsAtivo(conn) ? "E.ATIVO" : "NULL") + " AS ATIVO, " +
                    "Q.IDQUEUE AS QUEUE_ID, Q.STATUS AS QUEUE_STATUS, Q.LAST_ERROR AS QUEUE_ERROR, Q.DH_CRIACAO AS QUEUE_DH, " +
                    "L.IDLOG AS LOG_ID, L.NIVEL AS LOG_NIVEL, L.MENSAGEM AS LOG_MSG, L.DH_REGISTRO AS LOG_DH " +
                    "FROM TGFEXC E " +
                    "INNER JOIN TGFPRO P ON E.CODPROD = P.CODPROD " +
                    latestNutabJoin +
                    marcaJoin +
                    queueJoin +
                    "LEFT JOIN ( " +
                    "  SELECT IDLOG, NIVEL, MENSAGEM, REFERENCIA, DH_REGISTRO FROM ( " +
                    "    SELECT L.IDLOG, L.NIVEL, L.MENSAGEM, L.REFERENCIA, L.DH_REGISTRO, " +
                    "           ROW_NUMBER() OVER (PARTITION BY L.REFERENCIA ORDER BY L.DH_REGISTRO DESC) AS RN " +
                    "    FROM AD_FCLOG L WHERE L.OPERACAO = '" + LogService.OP_PRICE_SYNC + "' " +
                    "  ) L1 WHERE L1.RN = 1 " +
                    ") L ON L.REFERENCIA = P.REFERENCIA " +
                    "WHERE " + where +
                    " ORDER BY " + resolveOrderBy(params, useDtInic) + " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";

            stmt = conn.prepareStatement(sql);
            int paramIndex = setParameters(stmt, queryParams);
            stmt.setInt(paramIndex++, offset);
            stmt.setInt(paramIndex, pageSize);
            rs = stmt.executeQuery();

            List<Map<String, Object>> items = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> item = new HashMap<>();
                item.put("nuTab", rs.getBigDecimal("NUTAB"));
                BigDecimal codProdRow = rs.getBigDecimal("CODPROD");
                item.put("codProd", codProdRow);
                String skuLocal = normalizeSku(rs.getString("REFERENCIA"));
                String skuOutbound = resolveOutboundSku(codProdRow, skuLocal);
                item.put("skuLocal", skuLocal);
                item.put("sku", skuOutbound != null && !skuOutbound.isEmpty() ? skuOutbound : skuLocal);
                item.put("descricao", rs.getString("DESCRPROD"));
                BigDecimal nuTabRow = rs.getBigDecimal("NUTAB");
                PriceResolver.PriceResult priceResult = null;
                if (nuTabRow != null && codProdRow != null) {
                    priceResult = new PriceResolver().resolve(codProdRow, nuTabRow);
                }
                // SalePrice (preco calculado) e ListPrice (preco tabela origem)
                item.put("vlrVenda", priceResult != null ? priceResult.getPriceDecimal() : null);
                item.put("vlrTabela", priceResult != null ? priceResult.getListPriceDecimal() : null);
                item.put("priceTableId", resolvePriceTableId(nuTabRow));
                item.put("ativo", rs.getString("ATIVO"));
                item.put("queueId", rs.getObject("QUEUE_ID"));
                item.put("queueStatus", rs.getString("QUEUE_STATUS"));
                item.put("queueError", rs.getString("QUEUE_ERROR"));
                item.put("logId", rs.getObject("LOG_ID"));
                item.put("logNivel", rs.getString("LOG_NIVEL"));
                item.put("logMsg", rs.getString("LOG_MSG"));
                item.put("logDh", readTimestamp(rs, "LOG_DH"));
                items.add(item);
            }

            // Marcar quais produtos tem preco escalonado (Descontos Promocionais)
            enrichItemsWithBatchFlag(items, conn);

            boolean includeFc = getBoolean(params, "includeFc", false);
            if (includeFc) {
                enrichItemsWithFastchannelPrice(items);
            }

            result.put("items", items);
            result.put("total", total);
            result.put("page", page);
            result.put("pageSize", pageSize);
            result.put("source", 1);
            result.put("includesFc", includeFc);

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
            String skuExpr = FastchannelProductFilter.resolveSkuExpression(conn);
            String marcaJoin = FastchannelProductFilter.resolveMarcaJoin(conn);

            // Primeiro busca SKUs do Sankhya
            boolean useAtivo = supportsAtivo(conn);
            StringBuilder where = new StringBuilder("1=1");
            List<Object> queryParams = new ArrayList<>();
            appendConfiguredEmpresasFilter(where, queryParams, conn, "E.CODEMP");
            if (useAtivo) {
                where.append(" AND E.ATIVO = 'S'");
            }

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

            BigDecimal nuTab = getBigDecimal(params, "nuTab");
            String priceTableId = getString(params, "priceTableId");
            if (nuTab != null) {
                where.append(" AND E.NUTAB = ?");
                queryParams.add(nuTab);
            } else if (priceTableId != null && !priceTableId.isEmpty()) {
                appendPriceTableFilter(where, queryParams, priceTableId, conn);
            } else {
                List<BigDecimal> tables = loadConfiguredPriceTables(conn);
                if (tables.isEmpty()) {
                    tables = new PriceTableResolver().resolveEligibleTables();
                }
                if (!tables.isEmpty()) {
                    where.append(" AND E.NUTAB IN (");
                    for (int i = 0; i < tables.size(); i++) {
                        if (i > 0) where.append(", ");
                        where.append("?");
                        queryParams.add(tables.get(i));
                    }
                    where.append(")");
                }
            }

            BigDecimal codEmp = getBigDecimal(params, "codEmp");
            if (codEmp != null && supportsCodEmp(conn)) {
                where.append(" AND E.CODEMP = ?");
                queryParams.add(codEmp);
            }

            int page = getInt(params, "page", 1);
            int pageSize = getInt(params, "pageSize", 20);
            int offset = (page - 1) * pageSize;

            // Count total
            String countSql = "SELECT COUNT(DISTINCT P.CODPROD) AS CNT " +
                    "FROM TGFEXC E " +
                    "INNER JOIN TGFPRO P ON E.CODPROD = P.CODPROD " +
                    marcaJoin +
                    "WHERE " + where;
            countStmt = conn.prepareStatement(countSql);
            setParameters(countStmt, queryParams);
            rsCount = countStmt.executeQuery();
            rsCount.next();
            int total = rsCount.getInt("CNT");

            // Get SKUs distinct com paginacao
            String sql = "SELECT DISTINCT P.CODPROD, " + skuExpr + " AS REFERENCIA, P.DESCRPROD, E.NUTAB " +
                    "FROM TGFEXC E " +
                    "INNER JOIN TGFPRO P ON E.CODPROD = P.CODPROD " +
                    marcaJoin +
                    "WHERE " + where +
                    " ORDER BY REFERENCIA OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";

            stmt = conn.prepareStatement(sql);
            int paramIndex = setParameters(stmt, queryParams);
            stmt.setInt(paramIndex++, offset);
            stmt.setInt(paramIndex, pageSize);
            rs = stmt.executeQuery();

            // Para cada SKU, consulta na API Fastchannel
            List<Map<String, Object>> items = new ArrayList<>();
            FastchannelPriceClient priceClient = new FastchannelPriceClient();

            while (rs.next()) {
                BigDecimal codProdRow = rs.getBigDecimal("CODPROD");
                String skuRef = normalizeSku(rs.getString("REFERENCIA"));
                String outboundSku = resolveOutboundSku(codProdRow, skuRef);
                Map<String, Object> item = new HashMap<>();
                item.put("codProd", codProdRow);
                item.put("skuLocal", skuRef);
                item.put("sku", outboundSku != null && !outboundSku.isEmpty() ? outboundSku : skuRef);
                item.put("descricao", rs.getString("DESCRPROD"));
                BigDecimal nuTabRow = rs.getBigDecimal("NUTAB");
                item.put("nuTab", nuTabRow);
                item.put("priceTableId", resolvePriceTableId(nuTabRow));

                try {
                    if (outboundSku == null || outboundSku.isEmpty()) {
                        item.put("vlrVenda", null);
                        item.put("apiStatus", "SEM_SKU");
                        item.put("apiMessage", "Produto sem ProductId/SKU outbound mapeado");
                        items.add(item);
                        continue;
                    }

                    PriceDTO priceFC = priceClient.getPrice(outboundSku);
                    if (priceFC != null) {
                        item.put("vlrVenda", priceFC.getPrice());
                        item.put("listPrice", priceFC.getListPrice());
                        item.put("promotionalPrice", priceFC.getPromotionalPrice());
                        item.put("resellerId", priceFC.getResellerId());
                        item.put("lastUpdate", priceFC.getLastUpdate());
                        if (priceFC.getPriceTableId() != null) {
                            item.put("priceTableId", priceFC.getPriceTableId());
                        }
                        item.put("apiStatus", "OK");
                        item.put("apiMessage", "OK");
                    } else {
                        item.put("vlrVenda", BigDecimal.ZERO);
                        item.put("apiStatus", "NAO_ENCONTRADO");
                        item.put("apiMessage", "SKU nao encontrado no Fastchannel");
                    }
                } catch (Exception e) {
                    log.log(Level.WARNING, "Erro ao consultar API para ProductId " + outboundSku, e);
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

            BigDecimal codProd = getBigDecimal(params, "codProd");
            if (codProd != null) {
                where.append(" AND Q.ENTITY_ID = ?");
                queryParams.add(codProd);
            }

            String status = getString(params, "status");
            if (status != null && !status.isEmpty()) {
                where.append(" AND Q.STATUS = ?");
                queryParams.add(status);
            }

            BigDecimal codEmp = getBigDecimal(params, "codEmp");
            if (codEmp != null && supportsCodEmp(conn)) {
                where.append(" AND E.CODEMP = ?");
                queryParams.add(codEmp);
            }
            appendConfiguredEmpresasFilter(where, queryParams, conn, "E.CODEMP");

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

            // Get page com ultimo log (compativel Oracle/SQL Server) e descricao do produto
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
                item.put("dhCriacao", readTimestamp(rs, "DH_CRIACAO"));
                item.put("priority", rs.getBigDecimal("PRIORITY"));
                item.put("descricao", rs.getString("DESCRPROD"));
                item.put("logId", rs.getObject("LOG_ID"));
                item.put("logNivel", rs.getString("LOG_NIVEL"));
                item.put("logMsg", rs.getString("LOG_MSG"));
                item.put("logDh", readTimestamp(rs, "LOG_DH"));
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

    /**
     * Lista tabelas de preco disponiveis no portal Fastchannel.
     * Endpoint de discovery para validar PriceTableIds corretos.
     */
    public Map<String, Object> listFcTables(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        try {
            FastchannelPriceClient distClient = new FastchannelPriceClient(FastchannelPriceClient.Channel.DISTRIBUTION);
            FastchannelPriceClient consClient = new FastchannelPriceClient(FastchannelPriceClient.Channel.CONSUMPTION);

            List<Map<String, Object>> distTables = new ArrayList<>();
            List<Map<String, Object>> consTables = new ArrayList<>();

            try {
                distTables = distClient.listPriceTables();
            } catch (Exception e) {
                log.log(Level.WARNING, "Erro ao buscar tabelas Distribution", e);
                result.put("distError", e.getMessage());
            }

            try {
                consTables = consClient.listPriceTables();
            } catch (Exception e) {
                log.log(Level.WARNING, "Erro ao buscar tabelas Consumption", e);
                result.put("consError", e.getMessage());
            }

            result.put("distribution", distTables);
            result.put("consumption", consTables);
            result.put("total", distTables.size() + consTables.size());
        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao listar tabelas FC", e);
            result.put("error", e.getMessage());
        }
        return result;
    }

    /**
     * Lista precos escalonados (batch pricing) para um CODPROD/NUTAB.
     */
    public Map<String, Object> listBatches(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        BigDecimal codProd = getBigDecimal(params, "codProd");
        BigDecimal nuTab = getBigDecimal(params, "nuTab");
        String priceTableIdStr = getString(params, "priceTableId");

        if (codProd == null) {
            result.put("error", "codProd obrigatorio");
            return result;
        }

        try {
            Connection conn = null;
            try {
                conn = DBUtil.getConnection();
                // Se nuTab nao informado, resolver a mais recente
                if (nuTab == null) {
                    List<BigDecimal> tables = loadConfiguredPriceTables(conn);
                    if (!tables.isEmpty()) {
                        nuTab = tables.get(0);
                    }
                }
            } finally {
                DBUtil.closeConnection(conn);
            }

            BigDecimal priceTableId = null;
            if (priceTableIdStr != null && !priceTableIdStr.isEmpty()) {
                priceTableId = new BigDecimal(priceTableIdStr);
            } else if (nuTab != null) {
                priceTableId = resolvePriceTableId(nuTab);
            }

            PriceBatchResolver batchResolver = new PriceBatchResolver();
            List<PriceBatchItemDTO> batches = batchResolver.resolve(codProd, nuTab, priceTableId);

            List<Map<String, Object>> items = new ArrayList<>();
            for (PriceBatchItemDTO b : batches) {
                Map<String, Object> item = new HashMap<>();
                item.put("minimumBatchSize", b.getMinimumBatchSize());
                item.put("maximumBatchSize", b.getMaximumBatchSize());
                item.put("unitaryPriceForBatch", b.getUnitaryPriceForBatch());
                item.put("batchDisabled", b.getBatchDisabled());
                item.put("priceTableId", b.getPriceTableId());
                items.add(item);
            }
            result.put("items", items);
            result.put("total", items.size());
            result.put("codProd", codProd);
            result.put("nuTab", nuTab);
        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao listar batches", e);
            result.put("error", e.getMessage());
        }
        return result;
    }

    public Map<String, Object> compararFC(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        String sku = getString(params, "sku");
        BigDecimal codProd = getBigDecimal(params, "codProd");
        BigDecimal requestedNuTab = getBigDecimal(params, "nuTab");
        String requestedPriceTableId = getString(params, "priceTableId");

        if ((sku == null || sku.isEmpty()) && codProd == null) {
            result.put("error", "sku ou codProd obrigatorio");
            return result;
        }

        try {
            if (codProd == null) {
                codProd = getCodProdFromSku(sku);
            }
            if ((sku == null || sku.isEmpty()) && codProd != null) {
                sku = resolveOutboundSku(codProd, null);
            }
            String outboundSku = resolveOutboundSku(codProd, sku);
            if (outboundSku == null || outboundSku.isEmpty()) {
                result.put("error", "Nao foi possivel resolver SKU/ProductId para comparacao");
                return result;
            }
            result.put("sku", outboundSku);
            result.put("skuLocal", sku);
            result.put("codProd", codProd);

            // Se nenhum NUTAB/priceTableId especifico, usar a mesma logica do forcarSync
            // para garantir consistencia entre sync e compare
            BigDecimal effectiveNuTab = requestedNuTab;
            if (effectiveNuTab == null && (requestedPriceTableId == null || requestedPriceTableId.isEmpty())) {
                effectiveNuTab = findEligibleNuTabForProduct(codProd);
            }
            PriceInfo priceInfo = getPrecoSankhyaInfo(sku, codProd, effectiveNuTab, requestedPriceTableId);
            BigDecimal precoSankhya = priceInfo.getPriceCentavos();
            result.put("precoSankhya", precoSankhya);
            result.put("nuTab", priceInfo.getNuTab());
            result.put("priceTableId", priceInfo.getPriceTableId());

            // Usar apenas o SKU outbound — evitar multiplas requests por candidato
            List<String> skuCandidates = Collections.singletonList(outboundSku);
            PriceLookupResult lookupResult = findPriceInFastchannel(skuCandidates, priceInfo.getNuTab(), priceInfo.getPriceTableId());
            PriceDTO priceFC = lookupResult.price;

            if (priceFC != null) {
                result.put("precoFastchannel", priceFC.getPrice());
                result.put("listPrice", priceFC.getListPrice());
                result.put("resellerId", priceFC.getResellerId());
                result.put("priceChannel", lookupResult.channel != null ? lookupResult.channel.name() : null);
                if (lookupResult.usedSku != null && !lookupResult.usedSku.isEmpty()) {
                    result.put("skuUsadoFastchannel", lookupResult.usedSku);
                }
                if (priceFC.getPriceTableId() != null) {
                    result.put("priceTableIdFastchannel", priceFC.getPriceTableId());
                }
                BigDecimal delta = precoSankhya.subtract(priceFC.getPrice());
                result.put("delta", delta);
                result.put("divergencia", delta.compareTo(BigDecimal.ZERO) != 0);
            } else {
                result.put("precoFastchannel", null);
                result.put("divergencia", true);
                result.put("fcDiagnostico", "Nenhum preco encontrado na FC. SKU candidatos tentados: " + skuCandidates
                        + " | Canais tentados: CONSUMPTION, DISTRIBUTION"
                        + (priceInfo.getNuTab() != null ? " | NUTAB=" + priceInfo.getNuTab() : ""));
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

        if (codProdObj == null && (sku == null || sku.isEmpty())) {
            result.put("success", false);
            result.put("message", "codProd ou sku obrigatorio");
            return result;
        }

        try {
            BigDecimal codProd = codProdObj != null ? toBigDecimal(codProdObj) : getCodProdFromSku(sku);
            if (codProd == null) {
                result.put("success", false);
                result.put("message", "Produto nao encontrado para os dados informados");
                return result;
            }
            String outboundSku = resolveOutboundSku(codProd, sku);
            if (outboundSku == null || outboundSku.isEmpty()) {
                result.put("success", false);
                result.put("message", "Nao foi possivel resolver SKU para sincronizacao");
                return result;
            }
            BigDecimal eligibleNuTab = findEligibleNuTabForProduct(codProd);
            if (eligibleNuTab == null) {
                result.put("success", false);
                result.put("message", "Produto fora das tabelas de preco elegiveis/configuradas com integracao automatica");
                return result;
            }
            new PriceService().syncPrice(codProd, outboundSku);

            // GET verificativo - confirmar que o preco chegou na FC
            LogService logService = LogService.getInstance();
            PriceDTO verificacao = null;
            try {
                FastchannelPriceClient verifyClient = new FastchannelPriceClient();
                verificacao = verifyClient.getPrice(outboundSku);
            } catch (Exception getEx) {
                log.warning("GET verificativo preco falhou para SKU " + outboundSku + ": " + getEx.getMessage());
            }

            try {
                if (verificacao != null && verificacao.getPrice() != null) {
                    logService.logPriceSync(outboundSku, true,
                            "forcarSync OK. SKU=" + outboundSku + " FC ListPrice=" + verificacao.getListPrice()
                                    + " FC SalePrice=" + verificacao.getPrice()
                                    + " PriceTableId=" + verificacao.getPriceTableId()
                                    + " NUTAB elegivel=" + eligibleNuTab);
                } else {
                    logService.logPriceSync(outboundSku, true,
                            "forcarSync enviado (PUT OK), GET verificativo nao retornou dados."
                                    + " SKU=" + outboundSku + " NUTAB elegivel=" + eligibleNuTab);
                }
            } catch (Exception logEx) {
                log.log(Level.FINE, "Falha ao gravar log de verificacao preco", logEx);
            }

            result.put("success", true);
            result.put("message", "Preco sincronizado imediatamente (NUTAB elegivel: " + eligibleNuTab + ")");
            if (verificacao != null) {
                result.put("fcListPrice", verificacao.getListPrice());
                result.put("fcSalePrice", verificacao.getPrice());
                result.put("fcPriceTableId", verificacao.getPriceTableId());
            }
            log.info("Preco sincronizado imediatamente: " + outboundSku);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao forcar sync preco", e);
            try {
                LogService.getInstance().logPriceSync(
                        sku != null ? sku : String.valueOf(codProdObj), false,
                        "forcarSync ERRO: " + e.getMessage());
            } catch (Exception logEx) {
                log.log(Level.FINE, "Falha ao gravar log de erro preco", logEx);
            }
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
            processQueueNowBestEffort();

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
     * Sincroniza um lote de precos Sankhya → Fastchannel.
     *
     * <h3>INCIDENTE 2026-04-13 10:33 — pool exhaustion IJ000655</h3>
     * <p>Causado por dois cliques em "Sincronizar Selecionados" gerando 2 threads
     * paralelos × 20 items × ~15 conexoes/item = 600 conexoes → pool MGEDS esgotado
     * → sistema inteiro indisponivel por varios minutos.</p>
     *
     * <h3>Defesas implementadas</h3>
     * <ol>
     *   <li><b>MUTEX</b>: {@link #SYNC_EM_LOTE_RUNNING} AtomicBoolean impede execucoes paralelas.
     *       Segunda chamada retorna HTTP 200 com success=false e mensagem amigavel.</li>
     *   <li><b>CONNECTION REUSE</b>: uma unica conexao DB para todo o lote (resolve SKU,
     *       tabela elegivel, etc). Reduz de ~15 conn/item para 1 conn total do batch
     *       (mais as 2-3 da PriceService.syncPrice por item que sao internas).</li>
     *   <li><b>CACHE TABELA</b>: resolveTableToNuTabMap cacheado por batch — nao recalcula
     *       4x por item como antes.</li>
     * </ol>
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> syncEmLote(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();

        // [DEFESA 1] MUTEX — impede execucoes paralelas que esgotam o pool
        if (!SYNC_EM_LOTE_RUNNING.compareAndSet(false, true)) {
            log.warning("[POOL-GUARD] syncEmLote ja esta em execucao em outra thread. Recusando chamada paralela.");
            result.put("success", false);
            result.put("message", "Sincronizacao ja esta em andamento. Aguarde a conclusao antes de iniciar outra.");
            return result;
        }

        Connection batchConn = null;
        try {
            Object itemsObj = params.get("items");
            Object skusObj = params.get("skus");
            List<Map<String, Object>> items = new ArrayList<>();

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

            // [DEFESA 2] CONNECTION REUSE — uma unica conexao para lookups do batch
            batchConn = DBUtil.getConnection();

            // [DEFESA 3] CACHE TABELA — resolve uma vez, reutiliza para todos os itens
            PriceService priceService = new PriceService();

            int successCount = 0;
            int errorCount = 0;
            List<String> errors = new ArrayList<>();

            for (Map<String, Object> item : items) {
                String sku = item.get("sku") != null ? item.get("sku").toString().trim() : null;
                Object codProdObj = item.get("codProd");
                if ((sku == null || sku.isEmpty()) && codProdObj == null) {
                    continue;
                }

                try {
                    BigDecimal codProd = codProdObj != null ? toBigDecimal(codProdObj) : getCodProdFromSkuWithConn(sku, batchConn);
                    if (codProd == null) {
                        errors.add((sku != null ? sku : String.valueOf(codProdObj)) + ": Produto nao encontrado");
                        errorCount++;
                        continue;
                    }
                    String outboundSku = resolveOutboundSku(codProd, sku);
                    if (outboundSku == null || outboundSku.isEmpty()) {
                        errors.add((sku != null ? sku : codProd.toPlainString()) + ": SKU/ProductId nao resolvido");
                        errorCount++;
                        continue;
                    }
                    BigDecimal eligibleNuTab = findEligibleNuTabForProductWithConn(codProd, batchConn);
                    if (eligibleNuTab == null) {
                        errors.add((sku != null ? sku : codProd.toPlainString())
                                + ": Sem tabela de preco elegivel/configurada com integracao automatica");
                        errorCount++;
                        continue;
                    }
                    priceService.syncPrice(codProd, outboundSku);
                    successCount++;
                } catch (Exception e) {
                    errors.add((sku != null ? sku : String.valueOf(codProdObj)) + ": " + e.getMessage());
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
        } finally {
            DBUtil.closeConnection(batchConn);
            SYNC_EM_LOTE_RUNNING.set(false);
        }

        return result;
    }

    /**
     * Sincroniza TODOS os precos que correspondem ao filtro atual (tabela + priceTableId).
     * Alternativa ao syncEmLote para quando o usuario quer sincronizar tudo de uma vez
     * sem precisar selecionar items na UI, com mesmas defesas de pool.
     */
    public Map<String, Object> syncAll(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();

        if (!SYNC_EM_LOTE_RUNNING.compareAndSet(false, true)) {
            result.put("success", false);
            result.put("message", "Sincronizacao ja em andamento. Aguarde.");
            return result;
        }

        Connection batchConn = null;
        try {
            batchConn = DBUtil.getConnection();
            String priceTableId = getString(params, "priceTableId");

            // Montar query para obter TODOS os SKU/CODPROD do filtro
            StringBuilder where = new StringBuilder("1=1");
            List<Object> qp = new ArrayList<>();
            appendConfiguredEmpresasFilter(where, qp, batchConn, "E.CODEMP");

            if (priceTableId != null && !priceTableId.isEmpty()) {
                appendPriceTableFilter(where, qp, priceTableId, batchConn);
            } else {
                List<BigDecimal> tables = loadConfiguredPriceTables(batchConn);
                if (tables.isEmpty()) tables = new PriceTableResolver().resolveEligibleTables();
                if (!tables.isEmpty()) {
                    where.append(" AND E.NUTAB IN (");
                    for (int i = 0; i < tables.size(); i++) {
                        if (i > 0) where.append(", ");
                        where.append("?");
                        qp.add(tables.get(i));
                    }
                    where.append(")");
                }
            }

            // Filtro por latest NUTAB per CODTAB
            String latestNutabJoin = "INNER JOIN TGFTAB T ON T.NUTAB = E.NUTAB " +
                "INNER JOIN (SELECT T2.CODTAB, MAX(T2.NUTAB) AS MAX_NUTAB FROM TGFTAB T2 GROUP BY T2.CODTAB) LN " +
                "ON LN.CODTAB = T.CODTAB AND LN.MAX_NUTAB = E.NUTAB ";
            String skuExpr = FastchannelProductFilter.resolveSkuExpression(batchConn);
            String marcaJoin = FastchannelProductFilter.resolveMarcaJoin(batchConn);

            String sql = "SELECT E.CODPROD, " + skuExpr + " AS SKU FROM TGFEXC E " +
                "INNER JOIN TGFPRO P ON E.CODPROD = P.CODPROD " + latestNutabJoin + marcaJoin +
                "WHERE " + where + " ORDER BY E.CODPROD";

            PreparedStatement stmt = batchConn.prepareStatement(sql);
            setParameters(stmt, qp);
            ResultSet rs = stmt.executeQuery();

            List<Map<String, Object>> items = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> item = new HashMap<>();
                item.put("codProd", rs.getBigDecimal("CODPROD"));
                item.put("sku", rs.getString("SKU"));
                items.add(item);
            }
            DBUtil.closeAll(rs, stmt, null);

            if (items.isEmpty()) {
                result.put("success", true);
                result.put("message", "Nenhum item encontrado no filtro.");
                result.put("total", 0);
                return result;
            }

            // Reusar syncEmLote com a lista completa
            Map<String, Object> syncParams = new HashMap<>();
            syncParams.put("items", items);

            // Liberar o mutex temporariamente para syncEmLote poder adquirir
            SYNC_EM_LOTE_RUNNING.set(false);
            result = syncEmLote(syncParams);
            result.put("total", items.size());
            batchConn = null; // Fechada por syncEmLote internamente? Nao — vamos deixar o finally
            return result;

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro em syncAll", e);
            result.put("success", false);
            result.put("message", "Erro: " + e.getMessage());
            return result;
        } finally {
            DBUtil.closeConnection(batchConn);
            SYNC_EM_LOTE_RUNNING.set(false);
        }
    }

    /**
     * Espelhamento: zera precos de produtos que existem na Fastchannel mas NAO existem
     * mais na tabela Sankhya (TGFEXC). Envia PUT com SalePrice=0, ListPrice=0 para
     * cada SKU orfao.
     *
     * <p>Cenario: o key user removeu um produto da tabela Sankhya mas ele continua
     * ativo na FC com preco antigo. Este metodo faz a "limpeza" (mirror).</p>
     */
    public Map<String, Object> mirrorCleanup(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();

        if (!SYNC_EM_LOTE_RUNNING.compareAndSet(false, true)) {
            result.put("success", false);
            result.put("message", "Operacao de sincronizacao ja em andamento.");
            return result;
        }

        Connection batchConn = null;
        try {
            String priceTableId = getString(params, "priceTableId");

            batchConn = DBUtil.getConnection();

            // Se nenhum priceTableId informado, resolver todas as tabelas elegiveis
            // e processar cada uma sequencialmente
            if (priceTableId == null || priceTableId.isEmpty()) {
                Map<String, BigDecimal> fcTableMap = new PriceTableResolver().resolveTableToNuTabMap();
                if (fcTableMap.isEmpty()) {
                    result.put("success", false);
                    result.put("message", "Nenhuma tabela de preco elegivel encontrada na configuracao.");
                    return result;
                }
                int totalOrphans = 0;
                int totalZeroed = 0;
                int totalFailed = 0;
                List<String> allErrors = new ArrayList<>();
                for (Map.Entry<String, BigDecimal> entry : fcTableMap.entrySet()) {
                    Map<String, Object> subParams = new HashMap<>(params);
                    subParams.put("priceTableId", entry.getKey());
                    // Liberar mutex para chamada recursiva
                    SYNC_EM_LOTE_RUNNING.set(false);
                    Map<String, Object> subResult = mirrorCleanup(subParams);
                    totalOrphans += getInt(subResult, "orphanCount", 0);
                    totalZeroed += getInt(subResult, "zeroed", 0);
                    totalFailed += getInt(subResult, "failed", 0);
                    Object subErrors = subResult.get("errors");
                    if (subErrors instanceof List) {
                        allErrors.addAll((List<String>) subErrors);
                    }
                }
                result.put("success", totalFailed == 0);
                result.put("orphanCount", totalOrphans);
                result.put("zeroed", totalZeroed);
                result.put("failed", totalFailed);
                if (!allErrors.isEmpty()) result.put("errors", allErrors);
                result.put("message", String.format("Mirror TODAS tabelas: %d orfao(s) encontrado(s), %d zerado(s)%s.",
                    totalOrphans, totalZeroed, totalFailed > 0 ? ", " + totalFailed + " falha(s)" : ""));
                return result;
            }

            // 1) Buscar todos os SKUs ativos nesta tabela Sankhya
            Set<String> sankhyaSkus = new HashSet<>();
            {
                String skuExpr = FastchannelProductFilter.resolveSkuExpression(batchConn);
                StringBuilder where = new StringBuilder("1=1");
                List<Object> qp = new ArrayList<>();
                appendConfiguredEmpresasFilter(where, qp, batchConn, "E.CODEMP");
                appendPriceTableFilter(where, qp, priceTableId, batchConn);

                String latestNutabJoin = "INNER JOIN TGFTAB T ON T.NUTAB = E.NUTAB " +
                    "INNER JOIN (SELECT T2.CODTAB, MAX(T2.NUTAB) AS MAX_NUTAB FROM TGFTAB T2 GROUP BY T2.CODTAB) LN " +
                    "ON LN.CODTAB = T.CODTAB AND LN.MAX_NUTAB = E.NUTAB ";
                String marcaJoin = FastchannelProductFilter.resolveMarcaJoin(batchConn);

                String sql = "SELECT " + skuExpr + " AS SKU FROM TGFEXC E " +
                    "INNER JOIN TGFPRO P ON E.CODPROD = P.CODPROD " + latestNutabJoin + marcaJoin +
                    "WHERE " + where;
                PreparedStatement stmt = batchConn.prepareStatement(sql);
                setParameters(stmt, qp);
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    String s = rs.getString("SKU");
                    if (s != null && !s.trim().isEmpty()) sankhyaSkus.add(s.trim());
                }
                DBUtil.closeAll(rs, stmt, null);
            }

            // 2) Listar TODOS os precos desta tabela na Fastchannel via API em lote
            // GET /prices?PriceTableId=X&PageNumber=1&PageSize=500 (paginado)
            FastchannelPriceClient fcClient = new FastchannelPriceClient();
            List<PriceDTO> fcPrices = fcClient.listPricesForTable(new BigDecimal(priceTableId));

            // 3) Diff: SKUs que existem na FC com preco > 0 mas NAO existem na tabela Sankhya
            List<String> orphans = new ArrayList<>();
            for (PriceDTO fcPrice : fcPrices) {
                String fcSku = fcPrice.getSku() != null ? fcPrice.getSku().trim() : "";
                if (!fcSku.isEmpty() && !sankhyaSkus.contains(fcSku)) {
                    // Verifica se tem preco real (nao ja zerado)
                    boolean hasPrice = (fcPrice.getPrice() != null && fcPrice.getPrice().compareTo(BigDecimal.ZERO) > 0)
                        || (fcPrice.getListPrice() != null && fcPrice.getListPrice().compareTo(BigDecimal.ZERO) > 0);
                    if (hasPrice) {
                        orphans.add(fcSku);
                    }
                }
            }

            log.info("[MIRROR] Sankhya=" + sankhyaSkus.size()
                + " SKUs, Fastchannel=" + fcPrices.size()
                + " SKUs, Orfaos encontrados=" + orphans.size());

            if (orphans.isEmpty()) {
                result.put("success", true);
                result.put("message", "Nenhum item orfao encontrado. Tabelas ja estao espelhadas.");
                result.put("orphanCount", 0);
                return result;
            }

            // 4) Zerar cada orfao na FC
            int zeroed = 0;
            int failed = 0;
            List<String> errors = new ArrayList<>();

            for (String orphanSku : orphans) {
                try {
                    PriceDTO zeroDto = new PriceDTO();
                    zeroDto.setSku(orphanSku);
                    zeroDto.setPriceTableId(new BigDecimal(priceTableId));
                    zeroDto.setPrice(BigDecimal.ZERO);
                    zeroDto.setListPrice(BigDecimal.ZERO);
                    fcClient.updatePrice(zeroDto);
                    zeroed++;
                    log.info("[MIRROR] Zerado SKU orfao " + orphanSku + " na FC tabela " + priceTableId);
                } catch (Exception e) {
                    failed++;
                    errors.add(orphanSku + ": " + e.getMessage());
                    log.warning("[MIRROR] Falha ao zerar SKU " + orphanSku + ": " + e.getMessage());
                }
            }

            result.put("success", failed == 0);
            result.put("orphanCount", orphans.size());
            result.put("zeroed", zeroed);
            result.put("failed", failed);
            if (!errors.isEmpty()) result.put("errors", errors);
            result.put("message", String.format("Mirror: %d orfao(s) encontrado(s), %d zerado(s)%s.",
                orphans.size(), zeroed, failed > 0 ? ", " + failed + " falha(s)" : ""));

            log.info("[MIRROR] Concluido para tabela " + priceTableId + ": "
                + orphans.size() + " orfaos, " + zeroed + " zerados, " + failed + " falhas");

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro em mirrorCleanup", e);
            result.put("success", false);
            result.put("message", "Erro: " + e.getMessage());
        } finally {
            DBUtil.closeConnection(batchConn);
            SYNC_EM_LOTE_RUNNING.set(false);
        }

        return result;
    }

    /**
     * Resolve CODPROD a partir do SKU reutilizando uma conexao existente.
     * Evita abrir/fechar conexao para cada item do batch.
     */
    private BigDecimal getCodProdFromSkuWithConn(String sku, Connection conn) {
        if (sku == null || sku.trim().isEmpty() || conn == null) return null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(
                "SELECT TOP 1 P.CODPROD FROM TGFPRO P " +
                "LEFT JOIN TGFMAR M ON P.CODPROD = M.CODPROD " +
                "WHERE CAST(P.CODPROD AS VARCHAR(50)) = ? " +
                "OR P.REFFORN = ? " +
                "OR (M.AD_FAST = 'S' AND M.MARCA = ?) " +
                "ORDER BY P.CODPROD");
            stmt.setString(1, sku.trim());
            stmt.setString(2, sku.trim());
            stmt.setString(3, sku.trim());
            rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODPROD");
            }
        } catch (Exception e) {
            log.fine("getCodProdFromSkuWithConn falhou para " + sku + ": " + e.getMessage());
        } finally {
            DBUtil.closeAll(rs, stmt, null); // NÃO fechar conn — pertence ao batch
        }
        try {
            return getCodProdFromSku(sku); // fallback com conexao propria
        } catch (Exception fallbackEx) {
            log.fine("getCodProdFromSku fallback tambem falhou: " + fallbackEx.getMessage());
            return null;
        }
    }

    /**
     * findEligibleNuTabForProduct usando conexao compartilhada do batch.
     */
    private BigDecimal findEligibleNuTabForProductWithConn(BigDecimal codProd, Connection conn) {
        if (codProd == null) return null;

        Set<BigDecimal> candidateTables = new LinkedHashSet<>(new PriceTableResolver().resolveEligibleTables());
        if (candidateTables.isEmpty()) {
            BigDecimal configNuTab = FastchannelConfig.getInstance().getNuTab();
            if (configNuTab != null) {
                candidateTables.add(configNuTab);
            }
        }

        if (candidateTables.isEmpty()) return null;

        BigDecimal firstEligible = null;
        for (BigDecimal nuTab : candidateTables) {
            if (nuTab == null) continue;
            if (firstEligible == null) firstEligible = nuTab;
            if (productHasPriceInTableWithConn(codProd, nuTab, conn)) {
                return nuTab;
            }
        }
        return firstEligible;
    }

    private boolean productHasPriceInTableWithConn(BigDecimal codProd, BigDecimal nuTab, Connection conn) {
        if (codProd == null || nuTab == null || conn == null) return false;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(
                "SELECT TOP 1 1 FROM TGFEXC WHERE CODPROD = ? AND NUTAB = ? AND VLRVENDA > 0");
            stmt.setBigDecimal(1, codProd);
            stmt.setBigDecimal(2, nuTab);
            rs = stmt.executeQuery();
            return rs.next();
        } catch (Exception e) {
            log.fine("productHasPriceInTableWithConn falhou: " + e.getMessage());
            return false;
        } finally {
            DBUtil.closeAll(rs, stmt, null);
        }
    }

    private BigDecimal findEligibleNuTabForProduct(BigDecimal codProd) {
        if (codProd == null) {
            return null;
        }

        Set<BigDecimal> candidateTables = new LinkedHashSet<>(new PriceTableResolver().resolveEligibleTables());
        if (candidateTables.isEmpty()) {
            BigDecimal configNuTab = FastchannelConfig.getInstance().getNuTab();
            if (configNuTab != null) {
                candidateTables.add(configNuTab);
            }
        }

        if (candidateTables.isEmpty()) {
            return null;
        }

        // Filtra apenas tabelas onde o produto tem preco > 0
        BigDecimal firstEligible = null;
        for (BigDecimal nuTab : candidateTables) {
            if (nuTab == null) continue;
            if (firstEligible == null) firstEligible = nuTab;
            if (productHasPriceInTable(codProd, nuTab)) {
                return nuTab;
            }
        }
        return firstEligible;
    }

    private boolean productHasPriceInTable(BigDecimal codProd, BigDecimal nuTab) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            // Verificar via TGFEXC (mais rapido que SNK_GET_PRECO)
            stmt = conn.prepareStatement(
                "SELECT TOP 1 1 FROM TGFEXC WHERE CODPROD = ? AND NUTAB = ?");
            stmt.setBigDecimal(1, codProd);
            stmt.setBigDecimal(2, nuTab);
            rs = stmt.executeQuery();
            boolean found = rs.next();
            log.fine("productHasPriceInTable CODPROD=" + codProd + " NUTAB=" + nuTab + " -> " + found);
            return found;
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao verificar preco CODPROD " + codProd + " NUTAB " + nuTab, e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return false;
    }

    private BigDecimal getCodProdFromSku(String sku) throws Exception {
        String normalizedSku = normalizeSku(sku);
        BigDecimal mapped = DeparaService.getInstance().getCodProdBySkuOrEan(normalizedSku);
        if (mapped != null) {
            return mapped;
        }
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                "SELECT P.CODPROD FROM TGFPRO P WHERE P.REFERENCIA = ?"
            );
            stmt.setString(1, normalizedSku);
            rs = stmt.executeQuery();

            if (rs.next()) {
                return rs.getBigDecimal("CODPROD");
            }

            return null;

        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
    }

    private PriceInfo getPrecoSankhyaInfo(String sku, BigDecimal codProdHint,
                                          BigDecimal nuTabHint, String priceTableIdHint) throws Exception {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();
            String orderCol = supportsDtInic(conn) ? "E.DTINIC" : "E.NUTAB";
            String ativoFilter = supportsAtivo(conn) ? " AND E.ATIVO = 'S' " : " ";
            BigDecimal requestedNuTab = nuTabHint != null ? nuTabHint : resolveNuTabFromPriceTableId(priceTableIdHint);

            BigDecimal codProd = codProdHint != null ? codProdHint : getCodProdFromSku(sku);
            if (codProd != null) {
                stmt = conn.prepareStatement(
                    "SELECT CODPROD, NUTAB FROM ( " +
                    "  SELECT E.CODPROD, E.NUTAB, ROW_NUMBER() OVER (ORDER BY " + orderCol + " DESC) AS RN " +
                    "  FROM TGFEXC E " +
                    "  WHERE E.CODPROD = ? " +
                    (requestedNuTab != null ? " AND E.NUTAB = ? " : " ") +
                    ativoFilter +
                    ") X WHERE X.RN = 1"
                );
                stmt.setBigDecimal(1, codProd);
                if (requestedNuTab != null) {
                    stmt.setBigDecimal(2, requestedNuTab);
                }
            } else {
                stmt = conn.prepareStatement(
                    "SELECT CODPROD, NUTAB FROM ( " +
                    "  SELECT E.CODPROD, E.NUTAB, ROW_NUMBER() OVER (ORDER BY " + orderCol + " DESC) AS RN " +
                    "  FROM TGFEXC E " +
                    "  INNER JOIN TGFPRO P ON E.CODPROD = P.CODPROD " +
                    "  WHERE P.REFERENCIA = ? " +
                    (requestedNuTab != null ? " AND E.NUTAB = ? " : " ") +
                    ativoFilter +
                    ") X WHERE X.RN = 1"
                );
                stmt.setString(1, sku);
                if (requestedNuTab != null) {
                    stmt.setBigDecimal(2, requestedNuTab);
                }
            }
            rs = stmt.executeQuery();

            if (rs.next()) {
                BigDecimal codProdRow = rs.getBigDecimal("CODPROD");
                BigDecimal nuTab = rs.getBigDecimal("NUTAB");
                PriceResolver.PriceResult priceResult = new PriceResolver().resolve(codProdRow, nuTab);
                if (priceResult != null && priceResult.getPriceCentavos() != null) {
                    return new PriceInfo(priceResult.getPriceCentavos(), nuTab, resolvePriceTableId(nuTab));
                }
                return new PriceInfo(BigDecimal.ZERO, nuTab, resolvePriceTableId(nuTab));
            }

            return new PriceInfo(BigDecimal.ZERO, null, null);

        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
    }

    private BigDecimal getPrecoSankhya(String sku) throws Exception {
        return getPrecoSankhyaInfo(sku, null, null, null).getPriceCentavos();
    }

    private BigDecimal resolvePriceTableId(BigDecimal nuTab) {
        if (nuTab == null) return null;
        DeparaService depara = DeparaService.getInstance();
        String priceTableId = depara.getCodigoExterno(DeparaService.TIPO_TABELA_PRECO, nuTab);
        if (priceTableId == null || priceTableId.isEmpty()) {
            BigDecimal codTab = resolveCodTabFromNuTab(nuTab);
            if (codTab != null) {
                priceTableId = depara.getCodigoExterno(DeparaService.TIPO_TABELA_PRECO, codTab);
                if (priceTableId == null || priceTableId.isEmpty()) {
                    BigDecimal latestNuTab = resolveLatestNuTabByCodTab(codTab);
                    if (latestNuTab != null) {
                        priceTableId = depara.getCodigoExterno(DeparaService.TIPO_TABELA_PRECO, latestNuTab);
                    }
                }
            }
        }
        if (priceTableId == null || priceTableId.isEmpty()) {
            priceTableId = resolvePriceTableIdFromAdditionalField(nuTab);
        }
        if ((priceTableId == null || priceTableId.isEmpty())) {
            BigDecimal codTab = resolveCodTabFromNuTab(nuTab);
            if (codTab != null) {
                priceTableId = codTab.toPlainString();
            }
        }
        if (priceTableId == null || priceTableId.isEmpty()) return null;
        try {
            return new BigDecimal(priceTableId);
        } catch (NumberFormatException e) {
            log.log(Level.WARNING, "PriceTableId invalido para NUTAB " + nuTab + ": " + priceTableId, e);
            return null;
        }
    }

    private BigDecimal resolveNuTabFromPriceTableId(String priceTableId) {
        if (priceTableId == null || priceTableId.isEmpty()) return null;
        BigDecimal nuTab = DeparaService.getInstance()
                .getCodigoSankhya(DeparaService.TIPO_TABELA_PRECO, priceTableId);
        if (nuTab != null) {
            // nuTab here is the NUTAB from de-para (possibly old). Resolve CODTAB first, then latest NUTAB.
            BigDecimal codTab = resolveCodTabFromNuTab(nuTab);
            if (codTab != null) {
                BigDecimal latestNuTab = resolveLatestNuTabByCodTab(codTab);
                if (latestNuTab != null) {
                    return latestNuTab;
                }
            }
            return nuTab;
        }
        BigDecimal nuTabFromAdditional = resolveNuTabByAdditionalFastId(priceTableId);
        if (nuTabFromAdditional != null) {
            return nuTabFromAdditional;
        }
        BigDecimal codTab = toBigDecimal(priceTableId);
        if (codTab != null) {
            BigDecimal latestByCodTab = resolveLatestNuTabByCodTab(codTab);
            if (latestByCodTab != null) {
                return latestByCodTab;
            }
        }
        if (isDirectNuTabFallbackEnabled()) {
            try {
                return new BigDecimal(priceTableId);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        throw new IllegalArgumentException("PriceTableId " + priceTableId
                + " sem de-para ativo para NUTAB (TIPO_ENTIDADE=TABELA_PRECO)");
    }

    private void appendPriceTableFilter(StringBuilder where, List<Object> queryParams, String priceTableId, Connection conn) {
        BigDecimal mappedNuTab = resolveNuTabFromPriceTableId(priceTableId);
        if (mappedNuTab == null) {
            return;
        }

        BigDecimal codTab = resolveCodTabFromNuTab(mappedNuTab);
        if (codTab != null && supportsCodTab(conn)) {
            where.append(" AND E.NUTAB IN (SELECT T.NUTAB FROM TGFTAB T WHERE T.CODTAB = ?)");
            queryParams.add(codTab);
            return;
        }

        where.append(" AND E.NUTAB = ?");
        queryParams.add(mappedNuTab);
    }

    private BigDecimal resolveCodTabFromNuTab(BigDecimal nuTab) {
        if (nuTab == null) return null;
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement("SELECT CODTAB FROM TGFTAB WHERE NUTAB = ?");
            stmt.setBigDecimal(1, nuTab);
            rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODTAB");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver CODTAB por NUTAB " + nuTab, e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return null;
    }

    private BigDecimal resolveLatestNuTabByCodTab(BigDecimal codTab) {
        if (codTab == null) return null;
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                    "SELECT TOP 1 NUTAB FROM TGFTAB WHERE CODTAB = ? ORDER BY DTVIGOR DESC, NUTAB DESC");
            stmt.setBigDecimal(1, codTab);
            rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("NUTAB");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver NUTAB por CODTAB " + codTab, e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return null;
    }

    private boolean supportsCodTab(Connection conn) {
        return supportsColumn(conn, "TGFTAB", "CODTAB");
    }

    private boolean supportsTabAdIdFast(Connection conn) {
        return supportsColumn(conn, "TGFTAB", "AD_FAST");
    }

    private String resolvePriceTableIdFromAdditionalField(BigDecimal nuTab) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            if (!supportsTabAdIdFast(conn)) {
                return null;
            }
            stmt = conn.prepareStatement("SELECT AD_FAST FROM TGFTAB WHERE NUTAB = ?");
            stmt.setBigDecimal(1, nuTab);
            rs = stmt.executeQuery();
            if (!rs.next()) {
                return null;
            }
            String value = rs.getString("AD_FAST");
            if (value == null) {
                return null;
            }
            String trimmed = value.trim();
            return trimmed.isEmpty() ? null : trimmed;
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver TGFTAB.AD_FAST para NUTAB " + nuTab, e);
            return null;
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
    }

    private BigDecimal resolveNuTabByAdditionalFastId(String priceTableId) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            if (!supportsTabAdIdFast(conn)) {
                return null;
            }
            stmt = conn.prepareStatement(
                    "SELECT TOP 1 NUTAB FROM TGFTAB WHERE AD_FAST = ? ORDER BY DTVIGOR DESC, NUTAB DESC");
            try {
                stmt.setInt(1, Integer.parseInt(priceTableId));
            } catch (NumberFormatException nfe) {
                log.log(Level.FINE, "priceTableId nao e numerico para TGFTAB.AD_FAST (int): " + priceTableId);
                return null;
            }
            rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("NUTAB");
            }
            return null;
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver NUTAB por TGFTAB.AD_FAST " + priceTableId, e);
            return null;
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
    }

    private boolean isDirectNuTabFallbackEnabled() {
        String configured = System.getProperty("fastchannel.price.allowDirectNuTabFallback");
        if (configured == null || configured.trim().isEmpty()) {
            configured = System.getenv("FASTCHANNEL_PRICE_ALLOW_DIRECT_NUTAB_FALLBACK");
        }
        return configured != null && Boolean.parseBoolean(configured);
    }

    private String resolveOutboundSku(BigDecimal codProd, String fallbackSku) {
        if (codProd != null) {
            DeparaService depara = DeparaService.getInstance();
            // Priorizar regra da marca (mesma logica que PrecoListener/PriceFullSyncJob)
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

    private String normalizeSku(String sku) {
        if (sku == null) return null;
        String normalized = sku.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    // resolveSkuExpression and resolveMarcaJoin delegated to FastchannelProductFilter

    /**
     * Versao price-specific de appendConfiguredEmpresasFilter.
     * Quando TGFEXC nao tem CODEMP, usa EXISTS via TGFEST como fallback.
     */
    private void appendConfiguredEmpresasFilter(StringBuilder where, List<Object> queryParams, Connection conn, String columnExpr) {
        List<BigDecimal> empresas = FastchannelProductFilter.loadConfiguredEmpresas(conn);

        if (!supportsCodEmp(conn)) {
            // Fallback para schemas sem CODEMP em TGFEXC: restringe via TGFEST por produto/empresa.
            if (!empresas.isEmpty()) {
                where.append(" AND EXISTS (")
                        .append("SELECT 1 FROM TGFEST ESTCFG ")
                        .append("WHERE ESTCFG.CODPROD = E.CODPROD ")
                        .append("AND ESTCFG.CODPARC = 0 ")
                        .append("AND ESTCFG.CODEMP IN (");
                FastchannelProductFilter.appendInClauseValues(where, queryParams, empresas);
                where.append("))");
                return;
            }
            BigDecimal fallbackCodEmp = FastchannelConfig.getInstance().getCodemp();
            if (fallbackCodEmp != null) {
                where.append(" AND EXISTS (")
                        .append("SELECT 1 FROM TGFEST ESTCFG ")
                        .append("WHERE ESTCFG.CODPROD = E.CODPROD ")
                        .append("AND ESTCFG.CODPARC = 0 ")
                        .append("AND ESTCFG.CODEMP = ?)");
                queryParams.add(fallbackCodEmp);
            }
            return;
        }

        if (!empresas.isEmpty()) {
            where.append(" AND ").append(columnExpr).append(" IN (");
            FastchannelProductFilter.appendInClauseValues(where, queryParams, empresas);
            where.append(")");
            return;
        }

        BigDecimal fallbackCodEmp = FastchannelConfig.getInstance().getCodemp();
        if (fallbackCodEmp != null) {
            where.append(" AND ").append(columnExpr).append(" = ?");
            queryParams.add(fallbackCodEmp);
        }
    }

    private void appendConfiguredLocaisFilter(StringBuilder where, List<Object> queryParams, Connection conn,
                                              String codProdExpr, String codEmpExpr) {
        // Delegate to centralized filter (EXISTS subquery overload)
        FastchannelProductFilter.appendConfiguredLocaisFilter(where, queryParams, conn, codProdExpr, codEmpExpr);
    }

    private List<BigDecimal> loadConfiguredPriceTables(Connection conn) {
        List<BigDecimal> ids = FastchannelProductFilter.loadConfiguredPriceTables(conn);

        // Validacao: verificar se os IDs retornados existem como NUTAB em TGFTAB.
        // Se nao existirem, podem ser CODTABs - resolver para NUTABs vigentes.
        if (!ids.isEmpty()) {
            List<BigDecimal> validated = validateAndResolveNuTabs(conn, ids);
            if (!validated.isEmpty()) {
                return validated;
            }
        }

        return ids;
    }

    /**
     * Valida se os IDs sao NUTABs validos. Se algum nao for NUTAB mas for CODTAB,
     * resolve para o NUTAB vigente mais recente.
     */
    private List<BigDecimal> validateAndResolveNuTabs(Connection conn, List<BigDecimal> candidates) {
        List<BigDecimal> resolved = new ArrayList<>();
        for (BigDecimal candidate : candidates) {
            if (isValidNuTab(conn, candidate)) {
                resolved.add(candidate);
            } else {
                // Tentar como CODTAB - buscar NUTAB vigente mais recente
                BigDecimal nuTab = resolveNuTabFromCodTab(conn, candidate);
                if (nuTab != null) {
                    log.info("COD_SANKHYA " + candidate + " resolvido como CODTAB -> NUTAB " + nuTab);
                    resolved.add(nuTab);
                } else {
                    log.warning("COD_SANKHYA " + candidate
                            + " do de-para TABELA_PRECO nao e NUTAB nem CODTAB valido. Ignorando.");
                }
            }
        }
        return resolved;
    }

    private boolean isValidNuTab(Connection conn, BigDecimal nuTab) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT TOP 1 1 FROM TGFTAB WHERE NUTAB = ?");
            stmt.setBigDecimal(1, nuTab);
            rs = stmt.executeQuery();
            return rs.next();
        } catch (Exception e) {
            return false;
        } finally {
            DBUtil.closeAll(rs, stmt, null);
        }
    }

    private BigDecimal resolveNuTabFromCodTab(Connection conn, BigDecimal codTab) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(
                    "SELECT TOP 1 NUTAB FROM TGFTAB WHERE CODTAB = ? ORDER BY DTVIGOR DESC, NUTAB DESC");
            stmt.setBigDecimal(1, codTab);
            rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("NUTAB");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver NUTAB para CODTAB " + codTab, e);
        } finally {
            DBUtil.closeAll(rs, stmt, null);
        }
        return null;
    }

    private void appendMappedProductFilter(StringBuilder where, List<Object> queryParams, Connection conn, String codProdExpr) {
        if (!hasConfiguredProductMapping(conn)) {
            return;
        }
        boolean hasIntegraAuto = FastchannelProductFilter.supportsColumn(conn, "AD_FCDEPARA", "INTEGRA_AUTO");
        where.append(" AND EXISTS (SELECT 1 FROM AD_FCDEPARA DPROD ");
        where.append("WHERE DPROD.TIPO_ENTIDADE = ? ");
        where.append("AND DPROD.COD_SANKHYA = ").append(codProdExpr).append(" ");
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
            log.log(Level.FINE, "Nao foi possivel validar de-para de produto para precos", e);
            return false;
        } finally {
            DBUtil.closeAll(rs, stmt, null);
        }
    }

    private static final class PriceInfo {
        private final BigDecimal priceCentavos;
        private final BigDecimal nuTab;
        private final BigDecimal priceTableId;

        private PriceInfo(BigDecimal priceCentavos, BigDecimal nuTab, BigDecimal priceTableId) {
            this.priceCentavos = priceCentavos;
            this.nuTab = nuTab;
            this.priceTableId = priceTableId;
        }

        private BigDecimal getPriceCentavos() {
            return priceCentavos;
        }

        private BigDecimal getNuTab() {
            return nuTab;
        }

        private BigDecimal getPriceTableId() {
            return priceTableId;
        }
    }

    private String resolveOrderBy(Map<String, Object> params, boolean useDtInic) {
        String sortBy = getString(params, "sortBy");
        String sortDir = getString(params, "sortDir");
        boolean desc = sortDir == null || !"asc".equalsIgnoreCase(sortDir);
        String dir = desc ? "DESC" : "ASC";

        if (sortBy != null) {
            switch (sortBy.toLowerCase()) {
                case "sku": return "REFERENCIA " + dir;
                case "descricao": return "P.DESCRPROD " + dir;
                case "nutab":
                case "tabela": return "E.NUTAB " + dir;
                case "vlrvenda":
                case "precovenda": return "E.VLRVENDA " + dir;
                case "codprod": return "E.CODPROD " + dir;
                case "pricetableid": return "E.NUTAB " + dir;
            }
        }
        return (useDtInic ? "E.DTINIC" : "E.NUTAB") + " DESC";
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

    private BigDecimal getBigDecimal(Map<String, Object> params, String key) {
        return toBigDecimal(params.get(key));
    }

    private boolean getBoolean(Map<String, Object> params, String key, boolean defaultValue) {
        Object value = params.get(key);
        if (value == null) return defaultValue;
        if (value instanceof Boolean) return (Boolean) value;
        String text = value.toString().trim();
        if (text.isEmpty()) return defaultValue;
        return "true".equalsIgnoreCase(text) || "1".equals(text) || "s".equalsIgnoreCase(text) || "sim".equalsIgnoreCase(text);
    }

    private BigDecimal toBigDecimal(Object value) {
        if (value == null) return null;
        if (value instanceof BigDecimal) return (BigDecimal) value;
        if (value instanceof Number) return BigDecimal.valueOf(((Number) value).doubleValue());
        try {
            String text = value.toString();
            if (text == null || text.trim().isEmpty()) return null;
            return new BigDecimal(text.trim());
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

    private void processQueueNowBestEffort() {
        try {
            new OutboxProcessorJob().executeScheduler();
        } catch (Exception e) {
            log.log(Level.WARNING, "Falha ao processar fila imediatamente (precos)", e);
        }
    }

    private PriceLookupResult findPriceInFastchannel(List<String> skuCandidates, BigDecimal nuTabHint) throws Exception {
        return findPriceInFastchannel(skuCandidates, nuTabHint, null);
    }

    private PriceLookupResult findPriceInFastchannel(List<String> skuCandidates, BigDecimal nuTabHint, BigDecimal preferredPriceTableId) throws Exception {
        if (skuCandidates == null || skuCandidates.isEmpty()) {
            return PriceLookupResult.empty();
        }

        FastchannelPriceClient.Channel preferredChannel = resolveChannelByNuTab(nuTabHint);
        List<FastchannelPriceClient.Channel> channels = new ArrayList<>();
        if (preferredChannel != null) {
            channels.add(preferredChannel);
        }
        if (!channels.contains(FastchannelPriceClient.Channel.CONSUMPTION)) {
            channels.add(FastchannelPriceClient.Channel.CONSUMPTION);
        }
        if (!channels.contains(FastchannelPriceClient.Channel.DISTRIBUTION)) {
            channels.add(FastchannelPriceClient.Channel.DISTRIBUTION);
        }

        Exception lastError = null;
        for (String skuCandidate : skuCandidates) {
            String normalized = normalizeSku(skuCandidate);
            if (normalized == null || normalized.isEmpty()) continue;

            for (FastchannelPriceClient.Channel channel : channels) {
                try {
                    FastchannelPriceClient client = new FastchannelPriceClient(channel);
                    PriceDTO dto = client.getPrice(normalized, preferredPriceTableId);
                    if (dto != null) {
                        return new PriceLookupResult(dto, normalized, channel);
                    }
                } catch (Exception e) {
                    lastError = e;
                }
            }
        }

        if (lastError != null) {
            log.log(Level.FINE, "Falha ao consultar preco em um ou mais canais Fastchannel", lastError);
        }
        return PriceLookupResult.empty();
    }

    private FastchannelPriceClient.Channel resolveChannelByNuTab(BigDecimal nuTab) {
        if (nuTab == null) return null;

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement("SELECT AD_TIPO_FAST FROM TGFTAB WHERE NUTAB = ?");
            stmt.setBigDecimal(1, nuTab);
            rs = stmt.executeQuery();
            if (rs.next()) {
                String tipoFast = rs.getString("AD_TIPO_FAST");
                if (tipoFast != null) {
                    String normalized = tipoFast.trim().toUpperCase();
                    if (normalized.contains("DIST") || "R".equals(normalized) || "REVENDA".equals(normalized)) {
                        return FastchannelPriceClient.Channel.DISTRIBUTION;
                    }
                    if (normalized.contains("CONS") || "C".equals(normalized)) {
                        return FastchannelPriceClient.Channel.CONSUMPTION;
                    }
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver canal por AD_TIPO_FAST para NUTAB " + nuTab, e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return null;
    }

    /**
     * Marca items com flag hasBatches=true/false consultando TGFDES+TGFDPQ em batch.
     */
    private void enrichItemsWithBatchFlag(List<Map<String, Object>> items, Connection conn) {
        if (items == null || items.isEmpty() || conn == null) return;
        try {
            // Coleta CODPRODs unicos
            Set<BigDecimal> codProds = new HashSet<>();
            for (Map<String, Object> item : items) {
                BigDecimal cp = toBigDecimal(item.get("codProd"));
                if (cp != null) codProds.add(cp);
            }
            if (codProds.isEmpty()) return;

            // Query batch: quais CODPRODs tem desconto por quantidade vigente
            StringBuilder inClause = new StringBuilder();
            for (int i = 0; i < codProds.size(); i++) {
                if (i > 0) inClause.append(", ");
                inClause.append("?");
            }
            String sql = "SELECT DISTINCT D.CODPROD FROM TGFDES D " +
                    "INNER JOIN TGFDPQ Q ON D.NUPROMOCAO = Q.NUPROMOCAO " +
                    "WHERE D.CODPROD IN (" + inClause + ") " +
                    "  AND ISNULL(D.NUVERSAO, 0) = ( " +
                    "    SELECT MAX(ISNULL(S.NUVERSAO, 0)) FROM TGFDES S WHERE S.NUPROMOCAO = D.NUPROMOCAO " +
                    "  )";

            PreparedStatement stmt = null;
            ResultSet rs = null;
            Set<BigDecimal> withBatches = new HashSet<>();
            try {
                stmt = conn.prepareStatement(sql);
                int idx = 1;
                for (BigDecimal cp : codProds) {
                    stmt.setBigDecimal(idx++, cp);
                }
                rs = stmt.executeQuery();
                while (rs.next()) {
                    withBatches.add(rs.getBigDecimal("CODPROD"));
                }
            } finally {
                DBUtil.closeAll(rs, stmt, null);
            }

            // Setar flag nos items
            for (Map<String, Object> item : items) {
                BigDecimal cp = toBigDecimal(item.get("codProd"));
                item.put("hasBatches", cp != null && withBatches.contains(cp));
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Erro ao verificar batch pricing em lote", e);
            // Em caso de erro, deixar sem flag (UI nao mostra botao)
        }
    }

    private void enrichItemsWithFastchannelPrice(List<Map<String, Object>> items) {
        if (items == null || items.isEmpty()) {
            return;
        }
        Map<String, PriceLookupResult> cache = new HashMap<>();

        for (Map<String, Object> item : items) {
            try {
                BigDecimal codProd = toBigDecimal(item.get("codProd"));
                String sku = normalizeSku(item.get("sku") != null ? item.get("sku").toString() : null);
                String skuLocal = normalizeSku(item.get("skuLocal") != null ? item.get("skuLocal").toString() : null);
                List<String> candidates = resolveSkuCandidates(codProd, sku, skuLocal);

                PriceLookupResult found = null;
                for (String candidate : candidates) {
                    if (candidate == null || candidate.isEmpty()) continue;
                    if (cache.containsKey(candidate)) {
                        PriceLookupResult cached = cache.get(candidate);
                        if (cached != null && cached.price != null) {
                            found = cached;
                            break;
                        }
                        continue;
                    }
                    PriceLookupResult fetched = findPriceInFastchannel(Collections.singletonList(candidate),
                            toBigDecimal(item.get("nuTab")), toBigDecimal(item.get("priceTableId")));
                    cache.put(candidate, fetched);
                    if (fetched != null && fetched.price != null) {
                        found = fetched;
                        break;
                    }
                }

                if (found != null && found.price != null) {
                    // FC retorna centavos, converter para reais para comparar com vlrVenda (que ja esta em reais)
                    BigDecimal fcPriceReais = found.price.getPrice() != null
                            ? found.price.getPrice().movePointLeft(2) : null;
                    BigDecimal fcListPriceReais = found.price.getListPrice() != null
                            ? found.price.getListPrice().movePointLeft(2) : null;
                    item.put("precoFastchannel", fcPriceReais);
                    item.put("listPrice", fcListPriceReais);
                    item.put("resellerId", found.price.getResellerId());
                    item.put("priceChannel", found.channel != null ? found.channel.name() : null);
                    if (found.price.getPriceTableId() != null) {
                        item.put("priceTableIdFastchannel", found.price.getPriceTableId());
                    }
                    BigDecimal localPrice = toBigDecimal(item.get("vlrVenda"));
                    item.put("delta", localPrice != null && fcPriceReais != null
                            ? localPrice.subtract(fcPriceReais)
                            : null);
                    if (found.usedSku != null && !found.usedSku.isEmpty()) {
                        item.put("sku", found.usedSku);
                    }
                    item.put("apiStatus", "OK");
                } else {
                    item.put("precoFastchannel", null);
                    item.put("delta", null);
                    item.put("apiStatus", "NAO_ENCONTRADO");
                }
            } catch (Exception e) {
                item.put("precoFastchannel", null);
                item.put("delta", null);
                item.put("apiStatus", "ERRO");
                item.put("apiError", e.getMessage());
            }
        }
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

    private static final class PriceLookupResult {
        private final PriceDTO price;
        private final String usedSku;
        private final FastchannelPriceClient.Channel channel;

        private PriceLookupResult(PriceDTO price, String usedSku, FastchannelPriceClient.Channel channel) {
            this.price = price;
            this.usedSku = usedSku;
            this.channel = channel;
        }

        private static PriceLookupResult empty() {
            return new PriceLookupResult(null, null, null);
        }
    }

    private Timestamp readTimestamp(ResultSet rs, String columnLabel) throws Exception {
        Object value = rs.getObject(columnLabel);
        if (value == null) return null;
        if (value instanceof Timestamp) return (Timestamp) value;
        if (value instanceof java.util.Date) return new Timestamp(((java.util.Date) value).getTime());
        if (value instanceof Number) {
            long epoch = ((Number) value).longValue();
            if (epoch > 0 && epoch < 100000000000L) {
                epoch *= 1000L;
            }
            return epoch > 0 ? new Timestamp(epoch) : null;
        }
        try {
            return Timestamp.valueOf(value.toString());
        } catch (Exception ignored) {
            return null;
        }
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
        return FastchannelProductFilter.supportsColumn(conn, table, column);
    }
}
