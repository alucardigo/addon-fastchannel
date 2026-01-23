package br.com.bellube.fastchannel.service;

import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
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

    // Cache: tipo -> (codSankhya -> codExterno)
    private final Map<String, Map<BigDecimal, String>> cacheSankhyaToExterno = new ConcurrentHashMap<>();
    private final Map<String, Map<String, BigDecimal>> cacheExternoToSankhya = new ConcurrentHashMap<>();

    // TTL do cache (10 minutos)
    private static final long CACHE_TTL_MS = 600_000;
    private long lastCacheLoad = 0;

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
        if (codSankhya == null || codExterno == null || codExterno.isEmpty()) {
            return;
        }

        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            // Verificar se j? existe
            BigDecimal existingId = getMappingId(jdbc, tipo, codSankhya);

            if (existingId != null) {
                // Atualizar
                NativeSql sql = new NativeSql(jdbc);
                sql.appendSql("UPDATE AD_FCDEPARA SET ");
                sql.appendSql("COD_EXTERNO = :codExterno, DH_ALTERACAO = CURRENT_TIMESTAMP ");
                sql.appendSql("WHERE IDDEPARA = :id");

                sql.setNamedParameter("codExterno", codExterno);
                sql.setNamedParameter("id", existingId);
                sql.executeUpdate();

            } else {
                // Inserir
                NativeSql sql = new NativeSql(jdbc);
                sql.appendSql("INSERT INTO AD_FCDEPARA ");
                sql.appendSql("(TIPO_ENTIDADE, COD_SANKHYA, COD_EXTERNO, DH_CRIACAO) ");
                sql.appendSql("VALUES (:tipo, :codSankhya, :codExterno, CURRENT_TIMESTAMP)");

                sql.setNamedParameter("tipo", tipo);
                sql.setNamedParameter("codSankhya", codSankhya);
                sql.setNamedParameter("codExterno", codExterno);
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
        }
    }

    /**
     * Remove mapeamento.
     */
    public void removeMapping(String tipo, BigDecimal codSankhya) {
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

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
        }
    }

    /**
     * Busca SKU do produto usando TGFPRO.REFERENCIA como fallback.
     */
    public String getSkuWithFallback(BigDecimal codProd) {
        // Primeiro tentar AD_FCDEPARA
        String sku = getSku(codProd);
        if (sku != null) return sku;

        // Fallback: usar REFERENCIA do produto
        return getReferenciaFromProduct(codProd);
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

        // Tentar por EAN
        return getCodProdByEan(skuOrEan);
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
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

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
        }
        return null;
    }

    private BigDecimal fetchCodigoSankhya(String tipo, String codExterno) {
        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

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
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

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
        }
        return null;
    }

    private BigDecimal getCodProdByReferencia(String referencia) {
        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

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
        }
        return null;
    }

    private BigDecimal getCodProdByEan(String ean) {
        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

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
        }
        return null;
    }

    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (Exception ignored) {}
        }
    }
}

