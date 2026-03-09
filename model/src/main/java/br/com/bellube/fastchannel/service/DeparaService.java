package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.dto.OrderItemDTO;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Servi?o de De-Para para mapeamento entre Sankhya e Fastchannel.
 *
 * Gerencia a tabela AD_FCDEPARA para:
 * - CODPROD <-> SKU
 * - CODPARC <-> CustomerId
 * - Outros mapeamentos
 *
 * Caracter?sticas:
 * - Cache em mem?ria
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
     * Obt?m SKU a partir do CODPROD.
     */
    public String getSku(BigDecimal codProd) {
        return getCodigoExterno(TIPO_PRODUTO, codProd);
    }

    /**
     * Obt?m CODPROD a partir do SKU.
     */
    public BigDecimal getCodProd(String sku) {
        return getCodigoSankhya(TIPO_PRODUTO, sku);
    }

    /**
     * Obt?m c?digo externo do parceiro.
     */
    public String getExternalCustomerId(BigDecimal codParc) {
        return getCodigoExterno(TIPO_PARCEIRO, codParc);
    }

    /**
     * Obt?m CODPARC a partir do ID externo.
     */
    public BigDecimal getCodParc(String externalId) {
        return getCodigoSankhya(TIPO_PARCEIRO, externalId);
    }

    /**
     * Obt?m CODEMP a partir do ID externo (ResellerId/StorageId).
     */
    public BigDecimal getCodEmp(String externalId) {
        return getCodigoSankhya(TIPO_EMPRESA, externalId);
    }

    /**
     * Obt?m CODTIPOPER a partir do ID externo (ResellerId/StorageId).
     */
    public BigDecimal getCodTipOper(String externalId) {
        return getCodigoSankhya(TIPO_TOP_PEDIDO, externalId);
    }

    /**
     * Obt?m CODTIPVENDA (TIPNEG) a partir do ID externo (ResellerId/StorageId).
     */
    public BigDecimal getCodTipVenda(String externalId) {
        return getCodigoSankhya(TIPO_TIPNEG, externalId);
    }

    /**
     * Busca c?digo externo para entidade Sankhya.
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
     * Busca c?digo Sankhya para c?digo externo.
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
     * Cadastra ou atualiza mapeamento com controle de integração automática.
     */
    public void setMapping(String tipo, BigDecimal codSankhya, String codExterno, boolean integraAuto) {
        if (codSankhya == null || codExterno == null || codExterno.isEmpty()) {
            return;
        }

        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();

            // Verificar se j? existe
            BigDecimal existingId = getMappingId(jdbc, tipo, codSankhya);
            boolean hasIntegraAuto = supportsIntegraAuto(jdbc);

            if (existingId != null) {
                // Atualizar
                NativeSql sql = new NativeSql(jdbc);
                sql.appendSql("UPDATE AD_FCDEPARA SET ");
                sql.appendSql("COD_EXTERNO = :codExterno, ");
                if (hasIntegraAuto) {
                    sql.appendSql("INTEGRA_AUTO = :integraAuto, ");
                }
                sql.appendSql("DH_ALTERACAO = CURRENT_TIMESTAMP ");
                sql.appendSql("WHERE IDDEPARA = :id");

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
                sql.setNamedParameter("codSankhya", codSankhya);
                sql.setNamedParameter("codExterno", codExterno);
                if (hasIntegraAuto) {
                    sql.setNamedParameter("integraAuto", integraAuto ? "S" : "N");
                }
                sql.executeUpdate();
            }

            // Atualizar cache
            cacheSankhyaToExterno.computeIfAbsent(tipo, k -> new ConcurrentHashMap<>())
                    .put(codSankhya, codExterno);
            cacheExternoToSankhya.computeIfAbsent(tipo, k -> new ConcurrentHashMap<>())
                    .put(codExterno, codSankhya);

            log.fine("Mapeamento registrado: " + tipo + " " + codSankhya + " <-> " + codExterno);

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

            // Buscar c?digo externo para limpar cache
            String codExterno = getCodigoExterno(tipo, codSankhya);

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("DELETE FROM AD_FCDEPARA ");
            sql.appendSql("WHERE TIPO_ENTIDADE = :tipo AND COD_SANKHYA = :codSankhya");

            sql.setNamedParameter("tipo", tipo);
            sql.setNamedParameter("codSankhya", codSankhya);
            sql.executeUpdate();

            // Limpar cache
            Map<BigDecimal, String> cache1 = cacheSankhyaToExterno.get(tipo);
            if (cache1 != null) cache1.remove(codSankhya);

            if (codExterno != null) {
                Map<String, BigDecimal> cache2 = cacheExternoToSankhya.get(tipo);
                if (cache2 != null) cache2.remove(codExterno);
            }

            log.fine("Mapeamento removido: " + tipo + " " + codSankhya);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao remover mapeamento", e);
            throw new RuntimeException("Erro ao remover mapeamento: " + e.getMessage(), e);
        } finally {
            closeJdbc(jdbc);
        }
    }

    /**
     * Busca SKU do produto usando TGFPRO.REFERENCIA como fallback.
     */
    public String getSkuWithFallback(BigDecimal codProd) {
        String skuByRule = normalizeSku(getSkuForStock(codProd));
        if (skuByRule != null && !skuByRule.isEmpty()) {
            return skuByRule;
        }

        // Primeiro tentar AD_FCDEPARA
        String sku = normalizeSku(getSku(codProd));
        if (sku != null) return sku;

        // Fallback: usar REFERENCIA do produto
        return normalizeSku(getReferenciaFromProduct(codProd));
    }

    /**
     * Busca SKU para estoque/pricing priorizando regra da marca (AD_FASTREF).
     */
    public String getSkuForStock(BigDecimal codProd) {
        if (codProd == null) return null;

        ResultSet rs = null;
        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT M.AD_FASTREF, P.REFFORN, P.CODPROD ");
            sql.appendSql("FROM TGFPRO P ");
            sql.appendSql("LEFT JOIN TGFMAR M ON M.CODIGO = P.CODMARCA ");
            sql.appendSql("WHERE P.CODPROD = :codProd");
            sql.setNamedParameter("codProd", codProd);

            rs = sql.executeQuery();
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
            log.log(Level.FINE, "Erro ao buscar SKU por regra de marca", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }

        String sku = normalizeSku(getCodigoExternoAtivo(TIPO_PRODUTO, codProd));
        if (sku == null) {
            sku = normalizeSku(getSku(codProd));
        }
        return sku;
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

        // Primeiro tentar De-Para
        BigDecimal codProd = getCodProd(skuOrEan);
        if (codProd != null) return codProd;

        // Tentar por REFERENCIA
        codProd = getCodProdByReferencia(skuOrEan);
        if (codProd != null) return codProd;

        // Legado/fastchannel pode trafegar ProductId baseado em REFFORN (AD_FASTREF='R')
        codProd = getCodProdByRefForn(skuOrEan);
        if (codProd != null) return codProd;

        // Tentar por EAN
        codProd = getCodProdByEan(skuOrEan);
        if (codProd != null) return codProd;

        // Fallback final: SKU numerico pode representar o proprio CODPROD
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
     * Invalida cache (for?a recarga).
     */
    public void invalidateCache() {
        cacheSankhyaToExterno.clear();
        cacheExternoToSankhya.clear();
        lastCacheLoad = 0;
        log.info("Cache de De-Para invalidado");
    }

    private void checkCacheValidity() {
        if (System.currentTimeMillis() - lastCacheLoad > CACHE_TTL_MS) {
            // N?o limpar, apenas marcar para eventual refresh
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
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar c?digo externo", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    /**
     * Busca código externo somente quando integração automática está habilitada.
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
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar codigo externo ativo", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    /**
     * Retorna true quando o registro está habilitado para integração automática.
     * Se não houver registro no de-para, assume habilitado para manter compatibilidade.
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
            return true;
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao validar flag de integração automática", e);
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
            log.log(Level.WARNING, "Erro ao buscar c?digo Sankhya", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
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



