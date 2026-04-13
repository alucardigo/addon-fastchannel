package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.util.DBUtil;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Resolve tabelas de preco elegiveis para publicacao no Fastchannel.
 */
public class PriceTableResolver {

    private static final Logger log = Logger.getLogger(PriceTableResolver.class.getName());
    private static volatile Boolean hasIntegraAutoColumn;

    private final FastchannelConfig config;

    public PriceTableResolver() {
        this.config = FastchannelConfig.getInstance();
    }

    public List<BigDecimal> resolveEligibleTables() {
        Map<String, BigDecimal> mappedTables = resolveTableToNuTabMap();
        if (!mappedTables.isEmpty()) {
            return new ArrayList<>(new LinkedHashSet<>(mappedTables.values()));
        }

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

        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
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
            if (jdbc != null) { try { jdbc.closeSession(); } catch (Exception ignored) {} }
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

    /**
     * Retorna mapa FC PriceTableId -> ultimo NUTAB (1 NUTAB por tabela FC).
     * Para cada tabela FC configurada, pega o ULTIMO de-para ativo e resolve o NUTAB mais recente.
     * Garante exatamente 1 PUT por tabela FC no sync.
     */
    public Map<String, BigDecimal> resolveTableToNuTabMap() {
        List<String> fcTableIds = resolveConfiguredOrMappedFcTableIds();
        if (fcTableIds.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, BigDecimal> result = new LinkedHashMap<>();
        for (String fcTableId : fcTableIds) {
            // Para cada FC table ID, pegar o ULTIMO NUTAB ativo (mais recente por DTVIGOR)
            BigDecimal latestNuTab = findLatestNuTabForFcTable(fcTableId);
            if (latestNuTab != null) {
                result.put(fcTableId, latestNuTab);
                log.info("resolveTableToNuTabMap: FC " + fcTableId + " -> NUTAB " + latestNuTab);
            }
        }
        return result;
    }

    private List<String> resolveConfiguredOrMappedFcTableIds() {
        // O de-para é a fonte de verdade — qualquer tabela ativa no de-para é elegível.
        // PRICE_TABLE_IDS no config é ignorado para sync (existe apenas para compatibilidade).
        return fetchMappedFcTableIds();
    }

    private List<String> parseRawTableIds(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return Collections.emptyList();
        }
        String[] parts = raw.split("[;,\\s]+");
        List<String> ids = new ArrayList<>();
        for (String part : parts) {
            String value = part != null ? part.trim() : null;
            if (value != null && !value.isEmpty()) {
                ids.add(value);
            }
        }
        return ids;
    }

    private List<String> fetchMappedFcTableIds() {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT DISTINCT LTRIM(RTRIM(COD_EXTERNO)) AS COD_EXTERNO ");
            sql.appendSql("FROM AD_FCDEPARA ");
            sql.appendSql("WHERE TIPO_ENTIDADE = 'TABELA_PRECO' ");
            sql.appendSql("AND COD_EXTERNO IS NOT NULL ");
            sql.appendSql("AND LTRIM(RTRIM(COD_EXTERNO)) <> '' ");
            if (supportsIntegraAuto(jdbc)) {
                sql.appendSql("AND COALESCE(INTEGRA_AUTO, 'S') = 'S' ");
            }
            sql.appendSql("ORDER BY LTRIM(RTRIM(COD_EXTERNO))");

            rs = sql.executeQuery();
            List<String> result = new ArrayList<>();
            while (rs.next()) {
                String fcTableId = rs.getString("COD_EXTERNO");
                if (fcTableId != null) {
                    result.add(fcTableId.trim());
                }
            }
            return result;
        } catch (Exception e) {
            log.log(Level.FINE, "JAPE indisponivel para fetchMappedFcTableIds, usando JDBC direto", e);
            return fetchMappedFcTableIdsJdbc();
        } finally {
            closeQuietly(rs);
            if (jdbc != null) { try { jdbc.closeSession(); } catch (Exception ignored) {} }
        }
    }

    private List<String> fetchMappedFcTableIdsJdbc() {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                "SELECT DISTINCT LTRIM(RTRIM(COD_EXTERNO)) AS COD_EXTERNO " +
                "FROM AD_FCDEPARA " +
                "WHERE TIPO_ENTIDADE = 'TABELA_PRECO' " +
                "AND COD_EXTERNO IS NOT NULL " +
                "AND LTRIM(RTRIM(COD_EXTERNO)) <> '' " +
                "AND COALESCE(INTEGRA_AUTO, 'S') = 'S' " +
                "ORDER BY LTRIM(RTRIM(COD_EXTERNO))");
            rs = stmt.executeQuery();
            List<String> result = new ArrayList<>();
            while (rs.next()) {
                String fcTableId = rs.getString("COD_EXTERNO");
                if (fcTableId != null) {
                    result.add(fcTableId.trim());
                }
            }
            return result;
        } catch (Exception e) {
            log.log(Level.WARNING, "Fallback JDBC fetchMappedFcTableIds tambem falhou", e);
            return Collections.emptyList();
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
    }

    /**
     * Para uma FC PriceTableId, encontra o ULTIMO de-para (por DH_CRIACAO ou DH_ALTERACAO DESC)
     * e resolve o NUTAB mais recente do CODTAB correspondente.
     */
    private BigDecimal findLatestNuTabForFcTable(String fcTableId) {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 ULT.NUTAB ");
            sql.appendSql("FROM AD_FCDEPARA D ");
            sql.appendSql("INNER JOIN TGFTAB REF ON REF.NUTAB = CAST(D.COD_SANKHYA AS INT) ");
            sql.appendSql("INNER JOIN ( ");
            sql.appendSql("  SELECT CODTAB, MAX(DTVIGOR) AS MAX_DT FROM TGFTAB GROUP BY CODTAB ");
            sql.appendSql(") MX ON MX.CODTAB = REF.CODTAB ");
            sql.appendSql("INNER JOIN TGFTAB ULT ON ULT.CODTAB = MX.CODTAB AND ULT.DTVIGOR = MX.MAX_DT ");
            sql.appendSql("WHERE D.TIPO_ENTIDADE = 'TABELA_PRECO' ");
            sql.appendSql("AND D.COD_EXTERNO = :fcId ");
            sql.appendSql("AND (D.INTEGRA_AUTO IS NULL OR D.INTEGRA_AUTO = 'S') ");
            sql.appendSql("ORDER BY ISNULL(D.DH_ALTERACAO, D.DH_CRIACAO) DESC, ULT.NUTAB DESC");
            sql.setNamedParameter("fcId", fcTableId);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("NUTAB");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "JAPE indisponivel para findLatestNuTabForFcTable, usando JDBC direto", e);
            return findLatestNuTabForFcTableJdbc(fcTableId);
        } finally {
            closeQuietly(rs);
            if (jdbc != null) { try { jdbc.closeSession(); } catch (Exception ignored) {} }
        }
        return null;
    }

    private BigDecimal findLatestNuTabForFcTableJdbc(String fcTableId) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                "SELECT TOP 1 ULT.NUTAB " +
                "FROM AD_FCDEPARA D " +
                "INNER JOIN TGFTAB REF ON REF.NUTAB = CAST(D.COD_SANKHYA AS INT) " +
                "INNER JOIN ( " +
                "  SELECT CODTAB, MAX(DTVIGOR) AS MAX_DT FROM TGFTAB GROUP BY CODTAB " +
                ") MX ON MX.CODTAB = REF.CODTAB " +
                "INNER JOIN TGFTAB ULT ON ULT.CODTAB = MX.CODTAB AND ULT.DTVIGOR = MX.MAX_DT " +
                "WHERE D.TIPO_ENTIDADE = 'TABELA_PRECO' " +
                "AND D.COD_EXTERNO = ? " +
                "AND (D.INTEGRA_AUTO IS NULL OR D.INTEGRA_AUTO = 'S') " +
                "ORDER BY ISNULL(D.DH_ALTERACAO, D.DH_CRIACAO) DESC, ULT.NUTAB DESC");
            stmt.setString(1, fcTableId);
            rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("NUTAB");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Fallback JDBC findLatestNuTabForFcTable falhou para " + fcTableId, e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return null;
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
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
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
            log.log(Level.FINE, "JAPE indisponivel para findAllNuTabsForPriceTableId, usando JDBC direto", e);
            return findAllNuTabsForPriceTableIdJdbc(fcPriceTableId);
        } finally {
            closeQuietly(rs);
            if (jdbc != null) { try { jdbc.closeSession(); } catch (Exception ignored) {} }
        }
    }

    private List<BigDecimal> findAllNuTabsForPriceTableIdJdbc(String fcPriceTableId) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                "SELECT DISTINCT ULT.NUTAB " +
                "FROM AD_FCDEPARA D " +
                "INNER JOIN TGFTAB REF ON REF.NUTAB = CAST(D.COD_SANKHYA AS INT) " +
                "INNER JOIN ( " +
                "  SELECT CODTAB, MAX(DTVIGOR) AS MAX_DT FROM TGFTAB GROUP BY CODTAB " +
                ") MX ON MX.CODTAB = REF.CODTAB " +
                "INNER JOIN TGFTAB ULT ON ULT.CODTAB = MX.CODTAB AND ULT.DTVIGOR = MX.MAX_DT " +
                "WHERE D.TIPO_ENTIDADE = 'TABELA_PRECO' " +
                "AND D.COD_EXTERNO = ? " +
                "AND (D.INTEGRA_AUTO IS NULL OR D.INTEGRA_AUTO = 'S') " +
                "ORDER BY ULT.NUTAB");
            stmt.setString(1, fcPriceTableId);
            rs = stmt.executeQuery();
            List<BigDecimal> result = new ArrayList<>();
            while (rs.next()) {
                BigDecimal nuTab = rs.getBigDecimal("NUTAB");
                if (nuTab != null) {
                    result.add(nuTab);
                    log.info("(JDBC) PriceTableId FC " + fcPriceTableId + " -> NUTAB " + nuTab);
                }
            }
            return result;
        } catch (Exception e) {
            log.log(Level.WARNING, "Fallback JDBC findAllNuTabsForPriceTableId falhou", e);
            return Collections.emptyList();
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
    }

    private BigDecimal resolveExistingNuTab(BigDecimal nuTab) {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 NUTAB FROM TGFTAB WHERE NUTAB = :nuTab");
            sql.setNamedParameter("nuTab", nuTab);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("NUTAB");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "JAPE indisponivel para resolveExistingNuTab, usando JDBC", e);
            return resolveExistingNuTabJdbc(nuTab);
        } finally {
            closeQuietly(rs);
            if (jdbc != null) { try { jdbc.closeSession(); } catch (Exception ignored) {} }
        }
        return null;
    }

    private BigDecimal resolveExistingNuTabJdbc(BigDecimal nuTab) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement("SELECT TOP 1 NUTAB FROM TGFTAB WHERE NUTAB = ?");
            stmt.setBigDecimal(1, nuTab);
            rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("NUTAB");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Fallback JDBC resolveExistingNuTab falhou", e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return null;
    }

    private BigDecimal resolveLatestNuTabByCodTab(BigDecimal codTab) {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
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
            if (jdbc != null) { try { jdbc.closeSession(); } catch (Exception ignored) {} }
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
            if (jdbc != null) { try { jdbc.closeSession(); } catch (Exception ignored) {} }
        }
    }

    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (Exception ignored) {}
        }
    }
}
