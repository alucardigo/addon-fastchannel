package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.dto.OrderItemDTO;
import br.com.bellube.fastchannel.util.DBUtil;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Servico de De-Para para mapeamento entre Sankhya e Fastchannel.
 *
 * Gerencia a tabela AD_FCDEPARA para:
 * - CODPROD <-> SKU
 * - CODPARC <-> CustomerId
 * - Outros mapeamentos
 *
 * Caracteristicas:
 * - Cache em memoria
 * - Thread-safe
 */
public class DeparaService {

    private static final Logger log = Logger.getLogger(DeparaService.class.getName());
    private static DeparaService instance;

    // Tipos de entidade
    public static final String TIPO_PRODUTO = "PRODUTO";
    public static final String TIPO_PARCEIRO = "PARCEIRO";
    public static final String TIPO_LOCAL = "LOCAL";
    public static final String TIPO_TABELA_PRECO = "TABELA_PRECO";
    public static final String TIPO_STOCK_STORAGE = "STOCK_STORAGE";
    public static final String TIPO_STOCK_RESELLER = "STOCK_RESELLER";
    public static final String TIPO_EMPRESA = "EMPRESA";
    public static final String TIPO_TOP_PEDIDO = "TOP_PEDIDO";
    public static final String TIPO_TIPNEG = "TIPNEG";

    // Cache: tipo -> (codSankhya -> codExterno)
    private final Map<String, Map<BigDecimal, String>> cacheSankhyaToExterno = new ConcurrentHashMap<>();
    private final Map<String, Map<String, BigDecimal>> cacheExternoToSankhya = new ConcurrentHashMap<>();

    // TTL do cache (10 minutos)
    private static final long CACHE_TTL_MS = 600_000;
    private long lastCacheLoad = 0;
    private static volatile Boolean hasIntegraAutoColumn;
    private volatile BigDecimal defaultOrderFallbackCodProd;

    private DeparaService() {
    }

    public static synchronized DeparaService getInstance() {
        if (instance == null) {
            instance = new DeparaService();
        }
        return instance;
    }

    /**
     * Obtem SKU a partir do CODPROD.
     */
    public String getSku(BigDecimal codProd) {
        return getCodigoExterno(TIPO_PRODUTO, codProd);
    }

    /**
     * Obtem CODPROD a partir do SKU.
     */
    public BigDecimal getCodProd(String sku) {
        return getCodigoSankhya(TIPO_PRODUTO, sku);
    }

    /**
     * Obtem codigo externo do parceiro.
     */
    public String getExternalCustomerId(BigDecimal codParc) {
        return getCodigoExterno(TIPO_PARCEIRO, codParc);
    }

    /**
     * Obtem CODPARC a partir do ID externo.
     */
    public BigDecimal getCodParc(String externalId) {
        return getCodigoSankhya(TIPO_PARCEIRO, externalId);
    }

    /**
     * Obtem CODEMP a partir do ID externo (ResellerId/StorageId).
     */
    public BigDecimal getCodEmp(String externalId) {
        return getCodigoSankhya(TIPO_EMPRESA, externalId);
    }

    /**
     * Obtem CODTIPOPER a partir do ID externo (ResellerId/StorageId).
     */
    public BigDecimal getCodTipOper(String externalId) {
        return getCodigoSankhya(TIPO_TOP_PEDIDO, externalId);
    }

    /**
     * Obtem CODTIPVENDA (TIPNEG) a partir do ID externo (ResellerId/StorageId).
     */
    public BigDecimal getCodTipVenda(String externalId) {
        return getCodigoSankhya(TIPO_TIPNEG, externalId);
    }

    /**
     * Busca codigo externo para entidade Sankhya.
     */
    public String getCodigoExterno(String tipo, BigDecimal codSankhya) {
        if (codSankhya == null) return null;

        checkCacheValidity();

        // Tentar cache primeiro
        Map<BigDecimal, String> typeCache = cacheSankhyaToExterno.get(tipo);
        if (typeCache != null && typeCache.containsKey(codSankhya)) {
            return typeCache.get(codSankhya);
        }

        // Buscar no banco
        String codExterno = fetchCodigoExterno(tipo, codSankhya);

        // Atualizar cache
        if (codExterno != null) {
            cacheSankhyaToExterno.computeIfAbsent(tipo, k -> new ConcurrentHashMap<>())
                    .put(codSankhya, codExterno);
        }

        return codExterno;
    }

    /**
     * Busca codigo Sankhya para codigo externo.
     */
    public BigDecimal getCodigoSankhya(String tipo, String codExterno) {
        if (codExterno == null || codExterno.isEmpty()) return null;

        checkCacheValidity();

        // Tentar cache primeiro
        Map<String, BigDecimal> typeCache = cacheExternoToSankhya.get(tipo);
        if (typeCache != null && typeCache.containsKey(codExterno)) {
            return typeCache.get(codExterno);
        }

        // Buscar no banco
        BigDecimal codSankhya = fetchCodigoSankhya(tipo, codExterno);

        // Atualizar cache
        if (codSankhya != null) {
            cacheExternoToSankhya.computeIfAbsent(tipo, k -> new ConcurrentHashMap<>())
                    .put(codExterno, codSankhya);
        }

        return codSankhya;
    }

    /**
     * Cadastra ou atualiza mapeamento.
     */
    public void setMapping(String tipo, BigDecimal codSankhya, String codExterno) {
        setMapping(tipo, codSankhya, codExterno, true);
    }

    /**
     * Cadastra ou atualiza mapeamento com controle de integracao automatica.
     */
    public void setMapping(String tipo, BigDecimal codSankhya, String codExterno, boolean integraAuto) {
        if (codSankhya == null || codExterno == null || codExterno.isEmpty()) {
            return;
        }

        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();
            BigDecimal resolvedCodSankhya = normalizeSankhyaCodeForMapping(jdbc, tipo, codSankhya);
            if (TIPO_TABELA_PRECO.equalsIgnoreCase(tipo)) {
                validateUniqueFastCodeForPriceTable(jdbc, resolvedCodSankhya, codExterno);
            }

            BigDecimal existingId = getMappingId(jdbc, tipo, resolvedCodSankhya);
            if (existingId == null && TIPO_TABELA_PRECO.equalsIgnoreCase(tipo)) {
                PriceTableFamilyMapping familyMapping = findPriceTableFamilyMapping(jdbc, resolvedCodSankhya);
                if (familyMapping != null) {
                    existingId = familyMapping.idDepara;
                }
            }
            boolean hasIntegraAuto = supportsIntegraAuto(jdbc);

            if (existingId != null) {
                // Atualizar
                NativeSql sql = new NativeSql(jdbc);
                sql.appendSql("UPDATE AD_FCDEPARA SET ");
                sql.appendSql("COD_SANKHYA = :codSankhya, ");
                sql.appendSql("COD_EXTERNO = :codExterno, ");
                if (hasIntegraAuto) {
                    sql.appendSql("INTEGRA_AUTO = :integraAuto, ");
                }
                sql.appendSql("DH_ALTERACAO = CURRENT_TIMESTAMP ");
                sql.appendSql("WHERE IDDEPARA = :id");

                sql.setNamedParameter("codSankhya", resolvedCodSankhya);
                sql.setNamedParameter("codExterno", codExterno);
                if (hasIntegraAuto) {
                    sql.setNamedParameter("integraAuto", integraAuto ? "S" : "N");
                }
                sql.setNamedParameter("id", existingId);
                sql.executeUpdate();

            } else {
                // Inserir
                NativeSql sql = new NativeSql(jdbc);
                sql.appendSql("INSERT INTO AD_FCDEPARA ");
                sql.appendSql("(TIPO_ENTIDADE, COD_SANKHYA, COD_EXTERNO");
                if (hasIntegraAuto) {
                    sql.appendSql(", INTEGRA_AUTO");
                }
                sql.appendSql(", DH_CRIACAO) ");
                sql.appendSql("VALUES (:tipo, :codSankhya, :codExterno");
                if (hasIntegraAuto) {
                    sql.appendSql(", :integraAuto");
                }
                sql.appendSql(", CURRENT_TIMESTAMP)");

                sql.setNamedParameter("tipo", tipo);
                sql.setNamedParameter("codSankhya", resolvedCodSankhya);
                sql.setNamedParameter("codExterno", codExterno);
                if (hasIntegraAuto) {
                    sql.setNamedParameter("integraAuto", integraAuto ? "S" : "N");
                }
                sql.executeUpdate();
                existingId = getMappingId(jdbc, tipo, resolvedCodSankhya);
            }

            if (TIPO_TABELA_PRECO.equalsIgnoreCase(tipo)) {
                cleanupDuplicatePriceTableFamilyMappings(jdbc, existingId, resolvedCodSankhya);
            }
            invalidateCache();

            log.fine("Mapeamento registrado: " + tipo + " " + resolvedCodSankhya + " <-> " + codExterno);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao registrar mapeamento", e);
            throw new RuntimeException("Erro ao registrar mapeamento: " + e.getMessage(), e);
        } finally {
            closeJdbc(jdbc);
        }
    }

    /**
     * Remove mapeamento.
     */
    public void removeMapping(String tipo, BigDecimal codSankhya) {
        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();
            BigDecimal resolvedCodSankhya = normalizeSankhyaCodeForMapping(jdbc, tipo, codSankhya);

            // Buscar codigo externo para limpar cache
            String codExterno = getCodigoExterno(tipo, resolvedCodSankhya);

            NativeSql sql = new NativeSql(jdbc);
            if (TIPO_TABELA_PRECO.equalsIgnoreCase(tipo)) {
                BigDecimal codTab = resolvePriceTableCodTab(jdbc, resolvedCodSankhya);
                if (codTab != null) {
                    sql.appendSql("DELETE FROM AD_FCDEPARA ");
                    sql.appendSql("WHERE TIPO_ENTIDADE = :tipo ");
                    sql.appendSql("AND CAST(COD_SANKHYA AS INT) IN (SELECT NUTAB FROM TGFTAB WHERE CODTAB = :codTab)");
                    sql.setNamedParameter("tipo", tipo);
                    sql.setNamedParameter("codTab", codTab);
                } else {
                    sql.appendSql("DELETE FROM AD_FCDEPARA ");
                    sql.appendSql("WHERE TIPO_ENTIDADE = :tipo AND COD_SANKHYA = :codSankhya");
                    sql.setNamedParameter("tipo", tipo);
                    sql.setNamedParameter("codSankhya", resolvedCodSankhya);
                }
            } else {
                sql.appendSql("DELETE FROM AD_FCDEPARA ");
                sql.appendSql("WHERE TIPO_ENTIDADE = :tipo AND COD_SANKHYA = :codSankhya");
                sql.setNamedParameter("tipo", tipo);
                sql.setNamedParameter("codSankhya", resolvedCodSankhya);
            }
            sql.executeUpdate();

            invalidateCache();

            log.fine("Mapeamento removido: " + tipo + " " + resolvedCodSankhya + " <-> " + codExterno);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao remover mapeamento", e);
            throw new RuntimeException("Erro ao remover mapeamento: " + e.getMessage(), e);
        } finally {
            closeJdbc(jdbc);
        }
    }

    /**
     * Busca SKU do produto para sincronizacao com Fastchannel.
     * Prioriza regra da marca (AD_FASTREF + AD_FAST='S'), depois AD_FCDEPARA.
     * NUNCA retorna EAN/CODBARRA/REFERENCIA como SKU FC.
     */
    public String getSkuWithFallback(BigDecimal codProd) {
        // 1. Regra da marca (fonte primaria - alinhado com legado Node.js)
        String skuByRule = normalizeSku(getSkuForStock(codProd));
        if (skuByRule != null && !skuByRule.isEmpty()) {
            return skuByRule;
        }

        // 2. De-Para explicito (AD_FCDEPARA)
        String sku = normalizeSku(getSku(codProd));
        if (sku != null) return sku;

        // NAO usar REFERENCIA/EAN como fallback - pode retornar codigo de barras
        // que nao e um SKU valido no Fastchannel
        log.warning("SKU nao resolvido para CODPROD " + codProd
                + ". Produto sem marca AD_FAST='S' e sem de-para cadastrado.");
        return null;
    }

    /**
     * Busca SKU para estoque/pricing priorizando regra da marca (AD_FASTREF).
     *
     * Alinhado com legado Node.js (fastChannel.js):
     * - JOIN TGFMAR M ON M.CODIGO = P.CODMARCA AND M.AD_FAST = 'S'
     * - Se AD_FASTREF = 'R' e REFFORN nao-vazio -> retorna REFFORN
     * - Senao -> retorna CODPROD (como string)
     * - Se marca nao tem AD_FAST = 'S' -> fallback para AD_FCDEPARA
     * - NUNCA retorna EAN/CODBARRA/REFERENCIA
     */
    public String getSkuForStock(BigDecimal codProd) {
        if (codProd == null) return null;

        // Tenta via JAPE primeiro
        ResultSet rs = null;
        JdbcWrapper jdbc = null;
        boolean japeOk = false;
        try {
            jdbc = openJdbc();

            // Query alinhada com legado: somente marcas com AD_FAST='S' participam
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT M.AD_FASTREF, P.REFFORN, P.CODPROD ");
            sql.appendSql("FROM TGFPRO P ");
            sql.appendSql("INNER JOIN TGFMAR M ON M.CODIGO = P.CODMARCA AND M.AD_FAST = 'S' ");
            sql.appendSql("WHERE P.CODPROD = :codProd");
            sql.setNamedParameter("codProd", codProd);

            rs = sql.executeQuery();
            japeOk = true;
            if (rs.next()) {
                String adFastRef = rs.getString("AD_FASTREF");
                String refForn = rs.getString("REFFORN");
                BigDecimal cod = rs.getBigDecimal("CODPROD");
                String skuByRule = normalizeSku(computeSkuFromBrandRule(adFastRef, cod, refForn));
                if (skuByRule != null && !skuByRule.isEmpty()) {
                    return skuByRule;
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Erro ao buscar SKU por regra de marca via JAPE", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }

        // Fallback JDBC direto quando JAPE nao inicializou (mge-core nao pronto)
        if (!japeOk) {
            String skuJdbc = getSkuForStockJdbc(codProd);
            if (skuJdbc != null) return skuJdbc;
        }

        // Fallback: AD_FCDEPARA (somente registros com integracao ativa)
        String sku = normalizeSku(getCodigoExternoAtivo(TIPO_PRODUTO, codProd));
        if (sku == null) {
            sku = normalizeSku(getSku(codProd));
        }
        return sku;
    }

    /**
     * Fallback JDBC direto para getSkuForStock quando JAPE/mge-core nao esta disponivel.
     */
    private String getSkuForStockJdbc(BigDecimal codProd) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                "SELECT M.AD_FASTREF, P.REFFORN, P.CODPROD " +
                "FROM TGFPRO P " +
                "INNER JOIN TGFMAR M ON M.CODIGO = P.CODMARCA AND M.AD_FAST = 'S' " +
                "WHERE P.CODPROD = ?");
            stmt.setBigDecimal(1, codProd);
            rs = stmt.executeQuery();
            if (rs.next()) {
                String adFastRef = rs.getString("AD_FASTREF");
                String refForn = rs.getString("REFFORN");
                BigDecimal cod = rs.getBigDecimal("CODPROD");
                String skuByRule = normalizeSku(computeSkuFromBrandRule(adFastRef, cod, refForn));
                if (skuByRule != null && !skuByRule.isEmpty()) {
                    return skuByRule;
                }
            }
            // Se nao encontrou pela marca, tenta AD_FCDEPARA via JDBC
            return getCodigoExternoAtivoJdbc(TIPO_PRODUTO, codProd, conn);
        } catch (Exception e) {
            log.log(Level.WARNING, "Fallback JDBC getSkuForStock falhou para CODPROD=" + codProd, e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return null;
    }

    /**
     * Fallback JDBC direto para getCodigoExternoAtivo quando JAPE nao esta disponivel.
     */
    private String getCodigoExternoAtivoJdbc(String tipo, BigDecimal codSankhya, Connection conn) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(
                "SELECT COD_EXTERNO FROM AD_FCDEPARA " +
                "WHERE TIPO_ENTIDADE = ? AND COD_SANKHYA = ? " +
                "AND COALESCE(INTEGRA_AUTO, 'S') = 'S'");
            stmt.setString(1, tipo);
            stmt.setBigDecimal(2, codSankhya);
            rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getString("COD_EXTERNO");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Fallback JDBC getCodigoExternoAtivo falhou", e);
        } finally {
            DBUtil.closeResultSet(rs);
            DBUtil.closeStatement(stmt);
        }
        return null;
    }

    public static String computeSkuFromBrandRule(String adFastRef, BigDecimal codProd, String refForn) {
        if ("R".equalsIgnoreCase(adFastRef)) {
            String normalizedRef = normalizeSku(refForn);
            if (normalizedRef != null && !normalizedRef.isEmpty()) {
                return normalizedRef;
            }
        }
        if (codProd == null) return null;
        return codProd.toPlainString();
    }

    private static String normalizeSku(String sku) {
        if (sku == null) return null;
        String normalized = sku.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    /**
     * Busca CODPROD usando SKU ou EAN.
     */
    public BigDecimal getCodProdBySkuOrEan(String skuOrEan) {
        if (skuOrEan == null || skuOrEan.isEmpty()) return null;

        // 1. Prioridade: REFFORN via marca FC (AD_FAST='S', AD_FASTREF='R')
        // Esta regra e a fonte de verdade para SKUs FC e deve sobrepor qualquer De-Para manual errado
        BigDecimal codProd = getCodProdByRefFornFcBrand(skuOrEan);
        if (codProd != null) return codProd;

        // 2. De-Para explicito (fallback/override quando nao ha REFFORN na marca FC)
        codProd = getCodProd(skuOrEan);
        if (codProd != null) return codProd;

        // 3. Tentar por REFERENCIA generica (sem filtro de marca)
        codProd = getCodProdByReferencia(skuOrEan);
        if (codProd != null) return codProd;

        // 4. REFFORN sem filtro de marca (qualquer produto ativo)
        codProd = getCodProdByRefForn(skuOrEan);
        if (codProd != null) return codProd;

        // 5. Tentar por EAN
        codProd = getCodProdByEan(skuOrEan);
        if (codProd != null) return codProd;

        // 6. Fallback final: SKU numerico pode representar o proprio CODPROD
        return getCodProdByCodigoInterno(skuOrEan);
    }

    /**
     * Resolve CODPROD para item de pedido com fallback por nome do produto.
     * Regras:
     * - tenta SKU/EAN/externalProductId
     * - tenta descricao exata
     * - tenta LIKE somente se houver candidato unico
     * Quando resolve por nome e SKU estiver presente, registra de-para automaticamente.
     */
    public BigDecimal resolveCodProdForOrderItem(OrderItemDTO item) {
        if (item == null) {
            return null;
        }

        Set<String> candidates = new LinkedHashSet<>();
        if (item.getSku() != null) {
            candidates.add(item.getSku().trim());
        }
        if (item.getEan() != null) {
            candidates.add(item.getEan().trim());
        }
        if (item.getExternalProductId() != null) {
            candidates.add(item.getExternalProductId().trim());
        }

        for (String code : candidates) {
            if (code == null || code.isEmpty()) {
                continue;
            }
            BigDecimal codProd = getCodProdBySkuOrEan(code);
            if (codProd != null) {
                return codProd;
            }
        }

        String productName = item.getProductName() != null ? item.getProductName().trim() : null;
        if (productName == null || productName.isEmpty()) {
            BigDecimal fallbackNoName = getDefaultOrderFallbackCodProd();
            if (fallbackNoName != null) {
                log.warning("Item sem SKU/EAN/nome mapeavel. Aplicando CODPROD fallback="
                        + fallbackNoName + " sku=" + item.getSku() + " externalProductId=" + item.getExternalProductId());
            }
            return fallbackNoName;
        }

        BigDecimal byExactName = getCodProdByDescricaoExata(productName);
        if (byExactName != null) {
            tryPersistProductMapping(item.getSku(), byExactName, "descricao exata");
            return byExactName;
        }

        BigDecimal byLikeUnique = getCodProdByDescricaoLikeUnica(productName);
        if (byLikeUnique != null) {
            tryPersistProductMapping(item.getSku(), byLikeUnique, "descricao similar unica");
            return byLikeUnique;
        }
        BigDecimal fallback = getDefaultOrderFallbackCodProd();
        if (fallback != null) {
            log.warning("SKU sem mapeamento no de-para para importacao de pedido. Aplicando CODPROD fallback="
                    + fallback + " sku=" + item.getSku() + " produto=" + productName);
        }
        return fallback;
    }

    /**
     * Invalida cache (forca recarga).
     */
    public void invalidateCache() {
        cacheSankhyaToExterno.clear();
        cacheExternoToSankhya.clear();
        lastCacheLoad = 0;
        log.info("Cache de De-Para invalidado");
    }

    private void checkCacheValidity() {
        if (System.currentTimeMillis() - lastCacheLoad > CACHE_TTL_MS) {
            // Nao limpar, apenas marcar para eventual refresh
            lastCacheLoad = System.currentTimeMillis();
        }
    }

    private String fetchCodigoExterno(String tipo, BigDecimal codSankhya) {
        ResultSet rs = null;
        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT COD_EXTERNO FROM AD_FCDEPARA ");
            sql.appendSql("WHERE TIPO_ENTIDADE = :tipo AND COD_SANKHYA = :codSankhya");

            sql.setNamedParameter("tipo", tipo);
            sql.setNamedParameter("codSankhya", codSankhya);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getString("COD_EXTERNO");
            }
            if (TIPO_TABELA_PRECO.equalsIgnoreCase(tipo)) {
                PriceTableFamilyMapping familyMapping = findPriceTableFamilyMapping(jdbc, codSankhya);
                if (familyMapping != null) {
                    return familyMapping.codExterno;
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Erro ao buscar codigo externo via JAPE, tentando JDBC", e);
            // Fallback JDBC quando JAPE nao inicializou
            Connection conn = null;
            try {
                conn = DBUtil.getConnection();
                return getCodigoExternoAtivoJdbc(tipo, codSankhya, conn);
            } catch (Exception jdbcEx) {
                log.log(Level.WARNING, "Fallback JDBC fetchCodigoExterno tambem falhou", jdbcEx);
            } finally {
                DBUtil.closeConnection(conn);
            }
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    /**
     * Busca codigo externo somente quando integracao automatica esta habilitada.
     */
    public String getCodigoExternoAtivo(String tipo, BigDecimal codSankhya) {
        if (codSankhya == null) return null;

        ResultSet rs = null;
        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT COD_EXTERNO FROM AD_FCDEPARA ");
            sql.appendSql("WHERE TIPO_ENTIDADE = :tipo AND COD_SANKHYA = :codSankhya ");
            if (supportsIntegraAuto(jdbc)) {
                sql.appendSql("AND COALESCE(INTEGRA_AUTO, 'S') = 'S'");
            }

            sql.setNamedParameter("tipo", tipo);
            sql.setNamedParameter("codSankhya", codSankhya);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getString("COD_EXTERNO");
            }
            if (TIPO_TABELA_PRECO.equalsIgnoreCase(tipo)) {
                PriceTableFamilyMapping familyMapping = findPriceTableFamilyMapping(jdbc, codSankhya);
                if (familyMapping != null && "S".equalsIgnoreCase(familyMapping.integraAuto)) {
                    return familyMapping.codExterno;
                }
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar codigo externo ativo via JAPE, tentando JDBC direto", e);
            // Fallback JDBC quando JAPE/mge-core nao inicializou
            Connection conn = null;
            try {
                conn = DBUtil.getConnection();
                return getCodigoExternoAtivoJdbc(tipo, codSankhya, conn);
            } catch (Exception jdbcEx) {
                log.log(Level.WARNING, "Fallback JDBC getCodigoExternoAtivo tambem falhou", jdbcEx);
            } finally {
                DBUtil.closeConnection(conn);
            }
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    /**
     * Retorna true quando o registro esta habilitado para integracao automatica.
     * Se nao houver registro no de-para, assume habilitado para manter compatibilidade.
     */
    public boolean isIntegracaoAutomaticaAtiva(String tipo, BigDecimal codSankhya) {
        if (codSankhya == null) return true;

        ResultSet rs = null;
        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();
            if (!supportsIntegraAuto(jdbc)) {
                return true;
            }

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT COALESCE(INTEGRA_AUTO, 'S') AS INTEGRA_AUTO ");
            sql.appendSql("FROM AD_FCDEPARA ");
            sql.appendSql("WHERE TIPO_ENTIDADE = :tipo AND COD_SANKHYA = :codSankhya");

            sql.setNamedParameter("tipo", tipo);
            sql.setNamedParameter("codSankhya", codSankhya);

            rs = sql.executeQuery();
            if (rs.next()) {
                return "S".equalsIgnoreCase(rs.getString("INTEGRA_AUTO"));
            }
            if (TIPO_TABELA_PRECO.equalsIgnoreCase(tipo)) {
                PriceTableFamilyMapping familyMapping = findPriceTableFamilyMapping(jdbc, codSankhya);
                if (familyMapping != null) {
                    return "S".equalsIgnoreCase(familyMapping.integraAuto);
                }
            }
            return true;
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao validar flag de integracao automatica", e);
            return true;
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
    }

    private BigDecimal fetchCodigoSankhya(String tipo, String codExterno) {
        ResultSet rs = null;
        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT COD_SANKHYA FROM AD_FCDEPARA ");
            sql.appendSql("WHERE TIPO_ENTIDADE = :tipo AND COD_EXTERNO = :codExterno");

            sql.setNamedParameter("tipo", tipo);
            sql.setNamedParameter("codExterno", codExterno);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("COD_SANKHYA");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar codigo Sankhya", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal normalizeSankhyaCodeForMapping(JdbcWrapper jdbc, String tipo, BigDecimal codSankhya) {
        if (!TIPO_TABELA_PRECO.equalsIgnoreCase(tipo) || codSankhya == null) {
            return codSankhya;
        }
        BigDecimal codTab = resolvePriceTableCodTab(jdbc, codSankhya);
        if (codTab == null) {
            return codSankhya;
        }
        BigDecimal latestNuTab = resolveLatestPriceTableNuTab(jdbc, codTab);
        return latestNuTab != null ? latestNuTab : codSankhya;
    }

    private void validateUniqueFastCodeForPriceTable(JdbcWrapper jdbc, BigDecimal codSankhya, String codExterno) throws Exception {
        if (codSankhya == null || codExterno == null || codExterno.trim().isEmpty()) {
            return;
        }
        BigDecimal codTab = resolvePriceTableCodTab(jdbc, codSankhya);
        if (codTab == null) {
            return;
        }

        ResultSet rs = null;
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 REF.CODTAB AS CODTAB, ULT.NUTAB AS NUTAB ");
            sql.appendSql("FROM AD_FCDEPARA D ");
            sql.appendSql("INNER JOIN TGFTAB REF ON REF.NUTAB = CAST(D.COD_SANKHYA AS INT) ");
            sql.appendSql("INNER JOIN (SELECT CODTAB, MAX(DTVIGOR) AS DTVIGOR FROM TGFTAB GROUP BY CODTAB) MX ");
            sql.appendSql("ON MX.CODTAB = REF.CODTAB ");
            sql.appendSql("INNER JOIN TGFTAB ULT ON ULT.CODTAB = MX.CODTAB AND ULT.DTVIGOR = MX.DTVIGOR ");
            sql.appendSql("WHERE D.TIPO_ENTIDADE = :tipo ");
            sql.appendSql("AND LTRIM(RTRIM(D.COD_EXTERNO)) = :codExterno ");
            sql.appendSql("AND REF.CODTAB <> :codTab ");
            sql.appendSql("ORDER BY ULT.NUTAB DESC");
            sql.setNamedParameter("tipo", TIPO_TABELA_PRECO);
            sql.setNamedParameter("codExterno", codExterno.trim());
            sql.setNamedParameter("codTab", codTab);

            rs = sql.executeQuery();
            if (rs.next()) {
                throw new IllegalArgumentException("O ID Fast " + codExterno.trim()
                        + " ja esta vinculado a outra tabela de preco Sankhya.");
            }
        } finally {
            closeQuietly(rs);
        }
    }

    private void cleanupDuplicatePriceTableFamilyMappings(JdbcWrapper jdbc, BigDecimal keepId, BigDecimal codSankhya) throws Exception {
        if (keepId == null || codSankhya == null) {
            return;
        }
        BigDecimal codTab = resolvePriceTableCodTab(jdbc, codSankhya);
        if (codTab == null) {
            return;
        }

        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("DELETE FROM AD_FCDEPARA ");
        sql.appendSql("WHERE TIPO_ENTIDADE = :tipo ");
        sql.appendSql("AND IDDEPARA <> :keepId ");
        sql.appendSql("AND CAST(COD_SANKHYA AS INT) IN (SELECT NUTAB FROM TGFTAB WHERE CODTAB = :codTab)");
        sql.setNamedParameter("tipo", TIPO_TABELA_PRECO);
        sql.setNamedParameter("keepId", keepId);
        sql.setNamedParameter("codTab", codTab);
        sql.executeUpdate();
    }

    private PriceTableFamilyMapping findPriceTableFamilyMapping(JdbcWrapper jdbc, BigDecimal codSankhya) throws Exception {
        if (codSankhya == null) {
            return null;
        }
        BigDecimal codTab = resolvePriceTableCodTab(jdbc, codSankhya);
        if (codTab == null) {
            return null;
        }

        ResultSet rs = null;
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 D.IDDEPARA, D.COD_SANKHYA, D.COD_EXTERNO, ");
            if (supportsIntegraAuto(jdbc)) {
                sql.appendSql("COALESCE(D.INTEGRA_AUTO, 'S') AS INTEGRA_AUTO ");
            } else {
                sql.appendSql("'S' AS INTEGRA_AUTO ");
            }
            sql.appendSql("FROM AD_FCDEPARA D ");
            sql.appendSql("INNER JOIN TGFTAB T ON T.NUTAB = CAST(D.COD_SANKHYA AS INT) ");
            sql.appendSql("WHERE D.TIPO_ENTIDADE = :tipo ");
            sql.appendSql("AND T.CODTAB = :codTab ");
            sql.appendSql("ORDER BY ISNULL(D.DH_ALTERACAO, D.DH_CRIACAO) DESC, D.IDDEPARA DESC");
            sql.setNamedParameter("tipo", TIPO_TABELA_PRECO);
            sql.setNamedParameter("codTab", codTab);

            rs = sql.executeQuery();
            if (rs.next()) {
                PriceTableFamilyMapping mapping = new PriceTableFamilyMapping();
                mapping.idDepara = rs.getBigDecimal("IDDEPARA");
                mapping.codSankhya = rs.getBigDecimal("COD_SANKHYA");
                mapping.codExterno = rs.getString("COD_EXTERNO");
                mapping.integraAuto = rs.getString("INTEGRA_AUTO");
                return mapping;
            }
        } finally {
            closeQuietly(rs);
        }
        return null;
    }

    private BigDecimal resolvePriceTableCodTab(JdbcWrapper jdbc, BigDecimal nuTab) {
        if (jdbc == null || nuTab == null) {
            return null;
        }
        ResultSet rs = null;
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT CODTAB FROM TGFTAB WHERE NUTAB = :nuTab");
            sql.setNamedParameter("nuTab", nuTab);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODTAB");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao resolver CODTAB para NUTAB " + nuTab, e);
        } finally {
            closeQuietly(rs);
        }
        return null;
    }

    private BigDecimal resolveLatestPriceTableNuTab(JdbcWrapper jdbc, BigDecimal codTab) {
        if (jdbc == null || codTab == null) {
            return null;
        }
        ResultSet rs = null;
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 NUTAB FROM TGFTAB WHERE CODTAB = :codTab ORDER BY DTVIGOR DESC, NUTAB DESC");
            sql.setNamedParameter("codTab", codTab);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("NUTAB");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao resolver NUTAB vigente para CODTAB " + codTab, e);
        } finally {
            closeQuietly(rs);
        }
        return null;
    }

    private BigDecimal getMappingId(JdbcWrapper jdbc, String tipo, BigDecimal codSankhya) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("SELECT IDDEPARA FROM AD_FCDEPARA ");
        sql.appendSql("WHERE TIPO_ENTIDADE = :tipo AND COD_SANKHYA = :codSankhya");

        sql.setNamedParameter("tipo", tipo);
        sql.setNamedParameter("codSankhya", codSankhya);

        ResultSet rs = sql.executeQuery();
        try {
            if (rs.next()) {
                return rs.getBigDecimal("IDDEPARA");
            }
        } finally {
            closeQuietly(rs);
        }
        return null;
    }

    private String getReferenciaFromProduct(BigDecimal codProd) {
        ResultSet rs = null;
        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT REFERENCIA FROM TGFPRO WHERE CODPROD = :codProd");
            sql.setNamedParameter("codProd", codProd);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getString("REFERENCIA");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Erro ao buscar REFERENCIA", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private boolean supportsIntegraAuto(JdbcWrapper jdbc) {
        Boolean cached = hasIntegraAutoColumn;
        if (cached != null) {
            return cached;
        }

        ResultSet rs = null;
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT COUNT(*) AS CNT ");
            sql.appendSql("FROM INFORMATION_SCHEMA.COLUMNS ");
            sql.appendSql("WHERE TABLE_NAME = 'AD_FCDEPARA' AND COLUMN_NAME = 'INTEGRA_AUTO'");
            rs = sql.executeQuery();
            boolean supported = rs.next() && rs.getInt("CNT") > 0;
            hasIntegraAutoColumn = supported;
            return supported;
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao validar coluna INTEGRA_AUTO em AD_FCDEPARA", e);
            hasIntegraAutoColumn = false;
            return false;
        } finally {
            closeQuietly(rs);
        }
    }

    private BigDecimal getCodProdByReferencia(String referencia) {
        ResultSet rs = null;
        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT CODPROD FROM TGFPRO WHERE REFERENCIA = :referencia AND ATIVO = 'S'");
            sql.setNamedParameter("referencia", referencia);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODPROD");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Erro ao buscar por REFERENCIA", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal getCodProdByRefForn(String refForn) {
        ResultSet rs = null;
        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT CODPROD FROM TGFPRO ");
            sql.appendSql("WHERE ATIVO = 'S' ");
            sql.appendSql("AND LTRIM(RTRIM(REFFORN)) = LTRIM(RTRIM(:refForn))");
            sql.setNamedParameter("refForn", refForn);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODPROD");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Erro ao buscar por REFFORN", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    /**
     * Busca CODPROD por REFFORN filtrando apenas marcas FC ativas (TGFMAR.AD_FAST='S').
     * Esta e a fonte de verdade primaria para SKUs FC, com prioridade sobre De-Para.
     */
    private BigDecimal getCodProdByRefFornFcBrand(String refForn) {
        ResultSet rs = null;
        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT P.CODPROD FROM TGFPRO P ");
            sql.appendSql("INNER JOIN TGFMAR M ON M.CODIGO = P.CODMARCA AND M.AD_FAST = 'S' ");
            sql.appendSql("WHERE P.ATIVO = 'S' ");
            sql.appendSql("AND LTRIM(RTRIM(P.REFFORN)) = LTRIM(RTRIM(:refForn))");
            sql.setNamedParameter("refForn", refForn);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODPROD");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Erro ao buscar por REFFORN em marca FC", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    /**
     * Sincroniza todos os De-Para de PRODUTO com base na regra de marca (AD_FASTREF + AD_FAST='S').
     * Corrige entradas erradas, cria ausentes e remove obsoletas.
     * Executado preventivamente no startup e pode ser acionado manualmente.
     */
    public void syncProductDeparaFromRefforn() {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        int updated = 0;
        int inserted = 0;
        int skipped = 0;
        try {
            jdbc = openJdbc();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT P.CODPROD, M.AD_FASTREF, LTRIM(RTRIM(ISNULL(P.REFFORN,''))) AS REFFORN, ");
            sql.appendSql("  CASE WHEN M.AD_FASTREF='R' AND LTRIM(RTRIM(ISNULL(P.REFFORN,''))) <> '' ");
            sql.appendSql("    THEN LTRIM(RTRIM(P.REFFORN)) ");
            sql.appendSql("    ELSE CAST(P.CODPROD AS VARCHAR(20)) ");
            sql.appendSql("  END AS SKU_CORRETO, ");
            sql.appendSql("  D.IDDEPARA, D.COD_EXTERNO AS SKU_ATUAL ");
            sql.appendSql("FROM TGFPRO P ");
            sql.appendSql("INNER JOIN TGFMAR M ON M.CODIGO = P.CODMARCA AND M.AD_FAST = 'S' ");
            sql.appendSql("LEFT JOIN AD_FCDEPARA D ON D.TIPO_ENTIDADE = 'PRODUTO' ");
            sql.appendSql("  AND CAST(D.COD_SANKHYA AS VARCHAR(20)) = CAST(P.CODPROD AS VARCHAR(20)) ");
            sql.appendSql("WHERE P.ATIVO = 'S' ");

            rs = sql.executeQuery();
            while (rs.next()) {
                BigDecimal codProd = rs.getBigDecimal("CODPROD");
                String skuCorreto = rs.getString("SKU_CORRETO");
                String skuAtual = rs.getString("SKU_ATUAL");
                BigDecimal iddepara = rs.getBigDecimal("IDDEPARA");

                if (skuCorreto == null || skuCorreto.isEmpty()) {
                    skipped++;
                    continue;
                }
                if (skuCorreto.equals(skuAtual)) {
                    skipped++;
                    continue;
                }

                if (iddepara != null) {
                    NativeSql upd = new NativeSql(jdbc);
                    upd.appendSql("UPDATE AD_FCDEPARA SET COD_EXTERNO = :sku, DH_ALTERACAO = CURRENT_TIMESTAMP ");
                    upd.appendSql("WHERE IDDEPARA = :id");
                    upd.setNamedParameter("sku", skuCorreto);
                    upd.setNamedParameter("id", iddepara);
                    upd.executeUpdate();
                    updated++;
                } else {
                    try {
                        setMapping(TIPO_PRODUTO, codProd, skuCorreto, true);
                        inserted++;
                    } catch (Exception e) {
                        log.log(Level.FINE, "Falha ao inserir De-Para CODPROD=" + codProd + " SKU=" + skuCorreto, e);
                    }
                }
            }

            NativeSql del = new NativeSql(jdbc);
            del.appendSql("DELETE FROM AD_FCDEPARA WHERE TIPO_ENTIDADE = 'PRODUTO' ");
            del.appendSql("AND NOT EXISTS (");
            del.appendSql("  SELECT 1 FROM TGFPRO P ");
            del.appendSql("  INNER JOIN TGFMAR M ON M.CODIGO = P.CODMARCA AND M.AD_FAST = 'S' ");
            del.appendSql("  WHERE P.ATIVO = 'S' ");
            del.appendSql("    AND CAST(P.CODPROD AS VARCHAR(20)) = CAST(AD_FCDEPARA.COD_SANKHYA AS VARCHAR(20))");
            del.appendSql(")");
            del.executeUpdate();

            cacheSankhyaToExterno.remove(TIPO_PRODUTO);
            cacheExternoToSankhya.remove(TIPO_PRODUTO);

            log.info("syncProductDeparaFromRefforn: " + inserted + " inseridos, "
                    + updated + " corrigidos, " + skipped + " sem alteracao.");
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao sincronizar De-Para de produtos com REFFORN", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
    }

    private BigDecimal getCodProdByEan(String ean) {
        ResultSet rs = null;
        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT P.CODPROD FROM TGFPRO P ");
            sql.appendSql("INNER JOIN TGFBAR B ON B.CODPROD = P.CODPROD ");
            sql.appendSql("WHERE B.CODBARRA = :ean AND P.ATIVO = 'S'");
            sql.setNamedParameter("ean", ean);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODPROD");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Erro ao buscar por EAN", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal getCodProdByCodigoInterno(String rawCode) {
        if (rawCode == null) return null;
        String normalized = rawCode.trim();
        if (normalized.isEmpty() || !normalized.matches("\\d+")) {
            return null;
        }

        ResultSet rs = null;
        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT CODPROD FROM TGFPRO WHERE CODPROD = :codProd AND ATIVO = 'S'");
            sql.setNamedParameter("codProd", new BigDecimal(normalized));
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODPROD");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Erro ao buscar por CODPROD numerico", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal getCodProdByDescricaoExata(String descricao) {
        ResultSet rs = null;
        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT CODPROD ");
            sql.appendSql("FROM TGFPRO ");
            sql.appendSql("WHERE ATIVO = 'S' ");
            sql.appendSql("AND UPPER(LTRIM(RTRIM(DESCRPROD))) = UPPER(LTRIM(RTRIM(:descr)))");
            sql.setNamedParameter("descr", descricao);
            rs = sql.executeQuery();
            if (rs.next()) {
                BigDecimal codProd = rs.getBigDecimal("CODPROD");
                if (!rs.next()) {
                    return codProd;
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Erro ao buscar produto por descricao exata", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal getCodProdByDescricaoLikeUnica(String descricao) {
        if (descricao == null || descricao.trim().isEmpty()) {
            return null;
        }

        String[] tokens = descricao.trim().split("\\s+");
        NativeSql sql = null;
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 2 CODPROD FROM TGFPRO WHERE ATIVO = 'S' ");
            for (int i = 0; i < tokens.length; i++) {
                String param = "token" + i;
                sql.appendSql("AND UPPER(DESCRPROD) LIKE :" + param + " ");
                sql.setNamedParameter(param, "%" + tokens[i].toUpperCase() + "%");
            }
            rs = sql.executeQuery();

            if (rs.next()) {
                BigDecimal codProd = rs.getBigDecimal("CODPROD");
                if (!rs.next()) {
                    return codProd;
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Erro ao buscar produto por descricao aproximada", e);
        } finally {
            closeQuietly(rs);
            if (sql != null) {
                try { sql.close(); } catch (Exception ignored) {}
            }
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal getDefaultOrderFallbackCodProd() {
        if (!isOrderFallbackEnabled()) {
            return null;
        }

        BigDecimal cached = defaultOrderFallbackCodProd;
        if (cached != null) {
            return cached;
        }

        ResultSet rs = null;
        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 CODPROD FROM TGFPRO WHERE ATIVO = 'S' ORDER BY CODPROD ASC");
            rs = sql.executeQuery();
            if (rs.next()) {
                defaultOrderFallbackCodProd = rs.getBigDecimal("CODPROD");
                return defaultOrderFallbackCodProd;
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar CODPROD fallback para importacao de pedidos", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private boolean isOrderFallbackEnabled() {
        String configured = System.getProperty("fastchannel.order.fallback.enabled");
        if (configured == null || configured.trim().isEmpty()) {
            configured = System.getenv("FASTCHANNEL_ORDER_FALLBACK_ENABLED");
        }
        if (configured == null || configured.trim().isEmpty()) {
            // Em producao, fallback implicito causa associacao indevida (ex.: CODPROD 1001).
            // So habilitar com flag explicita quando necessario para contingencia.
            return false;
        }
        return Boolean.parseBoolean(configured);
    }

    private void tryPersistProductMapping(String externalCode, BigDecimal codProd, String source) {
        if (externalCode == null || externalCode.trim().isEmpty() || codProd == null) {
            return;
        }
        String normalized = externalCode.trim();
        try {
            BigDecimal existingTarget = getCodProd(normalized);
            if (existingTarget != null && existingTarget.compareTo(codProd) != 0) {
                log.warning("De-para nao atualizado para codigo " + normalized +
                        " (ja aponta para CODPROD " + existingTarget + ", candidato " + codProd + ")");
                return;
            }

            String currentExternal = getSku(codProd);
            if (currentExternal != null && !currentExternal.trim().isEmpty() && !normalized.equals(currentExternal.trim())) {
                log.warning("De-para nao atualizado para CODPROD " + codProd +
                        " (ja possui codigo externo " + currentExternal + ", candidato " + normalized + ")");
                return;
            }

            setMapping(TIPO_PRODUTO, codProd, normalized, true);
            log.info("De-para de produto criado automaticamente (" + source + "): " + normalized + " -> " + codProd);
        } catch (Exception e) {
            log.log(Level.WARNING, "Falha ao persistir de-para automatico de produto", e);
        }
    }

    private static final class PriceTableFamilyMapping {
        private BigDecimal idDepara;
        private BigDecimal codSankhya;
        private String codExterno;
        private String integraAuto;
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
                log.log(Level.FINE, "Erro ao fechar session do JdbcWrapper", e);
            }
        }
    }

    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (Exception ignored) {}
        }
    }
}
