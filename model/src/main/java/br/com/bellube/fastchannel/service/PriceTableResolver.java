package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.config.FastchannelConfig;
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
            try {
                ids.add(new BigDecimal(value));
            } catch (NumberFormatException e) {
                log.warning("NUTAB invalido em PRICE_TABLE_IDS: " + value);
            }
        }
        return ids;
    }

    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (Exception ignored) {}
        }
    }
}
