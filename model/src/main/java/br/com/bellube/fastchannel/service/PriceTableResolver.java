package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Resolve tabelas de preco elegiveis para publicacao no Fastchannel.
 */
public class PriceTableResolver {

    private static final Logger log = Logger.getLogger(PriceTableResolver.class.getName());

    private final FastchannelConfig config;

    public PriceTableResolver() {
        this.config = FastchannelConfig.getInstance();
    }

    public List<BigDecimal> resolveEligibleTables() {
        List<BigDecimal> explicit = parseTableIds(config.getPriceTableIds());
        if (!explicit.isEmpty()) {
            return explicit;
        }

        List<String> tipos = parseTipos(config.getPriceTableTipos());
        if (tipos.isEmpty()) {
            BigDecimal nuTab = config.getNuTab();
            if (nuTab != null) {
                return Collections.singletonList(nuTab);
            }
            return Collections.emptyList();
        }

        return fetchByTipoFast(tipos);
    }

    private List<BigDecimal> fetchByTipoFast(List<String> tipos) {
        if (tipos.isEmpty()) return Collections.emptyList();

        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
            NativeSql sql = new NativeSql(jdbc);

            StringBuilder in = new StringBuilder();
            for (int i = 0; i < tipos.size(); i++) {
                if (i > 0) in.append(", ");
                in.append(":tipo").append(i);
            }

            sql.appendSql("SELECT T.NUTAB ");
            sql.appendSql("FROM TGFTAB T ");
            sql.appendSql("INNER JOIN (SELECT CODTAB, MAX(DTVIGOR) AS DTVIGOR FROM TGFTAB GROUP BY CODTAB) X ");
            sql.appendSql("ON X.CODTAB = T.CODTAB AND X.DTVIGOR = T.DTVIGOR ");
            sql.appendSql("WHERE T.AD_TIPO_FAST IN (" + in.toString() + ")");

            for (int i = 0; i < tipos.size(); i++) {
                sql.setNamedParameter("tipo" + i, tipos.get(i));
            }

            rs = sql.executeQuery();
            List<BigDecimal> result = new ArrayList<>();
            while (rs.next()) {
                BigDecimal nuTab = rs.getBigDecimal("NUTAB");
                if (nuTab != null) {
                    result.add(nuTab);
                }
            }
            return result;
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao resolver tabelas de preco por AD_TIPO_FAST", e);
            return Collections.emptyList();
        } finally {
            closeQuietly(rs);
        }
    }

    List<String> parseTipos(String raw) {
        if (raw == null || raw.trim().isEmpty()) return Collections.emptyList();
        String[] parts = raw.split("[;,\\s]+");
        List<String> tipos = new ArrayList<>();
        for (String part : parts) {
            String value = part.trim();
            if (!value.isEmpty()) {
                tipos.add(value);
            }
        }
        return tipos;
    }

    List<BigDecimal> parseTableIds(String raw) {
        if (raw == null || raw.trim().isEmpty()) return Collections.emptyList();
        String[] parts = raw.split("[;,\\s]+");
        List<BigDecimal> ids = new ArrayList<>();
        for (String part : parts) {
            String value = part.trim();
            if (value.isEmpty()) continue;
            // Prioridade 1: de-para TABELA_PRECO — busca TODOS os NUTABs que mapeiam
            // para o FC PriceTableId (1 PriceTableId FC pode ter N tabelas Sankhya)
            List<BigDecimal> mappedNuTabs = findAllNuTabsForPriceTableId(value);
            if (!mappedNuTabs.isEmpty()) {
                ids.addAll(mappedNuTabs);
                continue;
            }
            // Prioridade 2: valor numerico como NUTAB direto (so se existir no TGFTAB)
            try {
                BigDecimal numeric = new BigDecimal(value);
                BigDecimal byNuTab = resolveExistingNuTab(numeric);
                if (byNuTab != null) {
                    ids.add(byNuTab);
                    continue;
                }
                log.warning("PRICE_TABLE_IDS valor '" + value + "' nao encontrado no de-para TABELA_PRECO"
                        + " nem como NUTAB em TGFTAB. Ignorando.");
            } catch (NumberFormatException e) {
                log.warning("NUTAB invalido em PRICE_TABLE_IDS: " + value);
            }
        }
        return ids;
    }

    /**
     * Para cada NUTAB no De-Para que mapeia para o FC PriceTableId, resolve o CODTAB
     * e retorna o ULTIMO NUTAB ativo (MAX DTVIGOR) daquele CODTAB.
     *
     * Isso garante que sempre usamos a versao mais recente da tabela de precos,
     * mesmo que o De-Para tenha sido configurado com um NUTAB antigo.
     *
     * Ex: De-Para tem NUTAB 4321 (CODTAB 0, Jan/2026).
     *     CODTAB 0 tem NUTAB 4408 (Mar/2026) como ultimo.
     *     Retorna 4408 (preco atual), nao 4321 (preco antigo).
     */
    private List<BigDecimal> findAllNuTabsForPriceTableId(String fcPriceTableId) {
        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
            NativeSql sql = new NativeSql(jdbc);
            // Busca NUTABs no De-Para, resolve CODTAB, e pega o ULTIMO NUTAB de cada CODTAB
            sql.appendSql("SELECT DISTINCT ULT.NUTAB ");
            sql.appendSql("FROM AD_FCDEPARA D ");
            sql.appendSql("INNER JOIN TGFTAB REF ON REF.NUTAB = CAST(D.COD_SANKHYA AS INT) ");
            sql.appendSql("INNER JOIN ( ");
            sql.appendSql("  SELECT CODTAB, MAX(DTVIGOR) AS MAX_DT FROM TGFTAB GROUP BY CODTAB ");
            sql.appendSql(") MX ON MX.CODTAB = REF.CODTAB ");
            sql.appendSql("INNER JOIN TGFTAB ULT ON ULT.CODTAB = MX.CODTAB AND ULT.DTVIGOR = MX.MAX_DT ");
            sql.appendSql("WHERE D.TIPO_ENTIDADE = 'TABELA_PRECO' ");
            sql.appendSql("AND D.COD_EXTERNO = :fcId ");
            sql.appendSql("AND (D.INTEGRA_AUTO IS NULL OR D.INTEGRA_AUTO = 'S') ");
            sql.appendSql("ORDER BY ULT.NUTAB");
            sql.setNamedParameter("fcId", fcPriceTableId);
            rs = sql.executeQuery();
            List<BigDecimal> result = new ArrayList<>();
            while (rs.next()) {
                BigDecimal nuTab = rs.getBigDecimal("NUTAB");
                if (nuTab != null) {
                    result.add(nuTab);
                    log.info("PriceTableId FC " + fcPriceTableId + " -> NUTAB " + nuTab + " (ultimo ativo)");
                }
            }
            return result;
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar NUTABs para FC PriceTableId " + fcPriceTableId, e);
            return Collections.emptyList();
        } finally {
            closeQuietly(rs);
        }
    }

    private BigDecimal resolveExistingNuTab(BigDecimal nuTab) {
        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 NUTAB FROM TGFTAB WHERE NUTAB = :nuTab");
            sql.setNamedParameter("nuTab", nuTab);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("NUTAB");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Erro ao validar NUTAB " + nuTab + " em TGFTAB", e);
        } finally {
            closeQuietly(rs);
        }
        return null;
    }

    private BigDecimal resolveLatestNuTabByCodTab(BigDecimal codTab) {
        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 NUTAB FROM TGFTAB WHERE CODTAB = :codTab ORDER BY DTVIGOR DESC, NUTAB DESC");
            sql.setNamedParameter("codTab", codTab);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("NUTAB");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Erro ao resolver NUTAB por CODTAB " + codTab, e);
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
