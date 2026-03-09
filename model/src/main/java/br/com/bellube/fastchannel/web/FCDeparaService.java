package br.com.bellube.fastchannel.web;

import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.util.DBUtil;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FCDeparaService {

    private static final Logger log = Logger.getLogger(FCDeparaService.class.getName());

    public Map<String, Object> listEmpresas(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                    "SELECT CODEMP, RAZAOSOCIAL, NOMEFANTASIA FROM TSIEMP ORDER BY CODEMP");
            rs = stmt.executeQuery();

            List<Map<String, Object>> items = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> item = new HashMap<>();
                item.put("codSankhya", rs.getBigDecimal("CODEMP"));
                String nomeFantasia = rs.getString("NOMEFANTASIA");
                String razaoSocial = rs.getString("RAZAOSOCIAL");
                item.put("descricao", coalesce(nomeFantasia, razaoSocial));
                items.add(item);
            }

            result.put("items", items);
            result.put("total", items.size());
        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao listar empresas", e);
            result.put("error", e.getMessage());
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }

        return result;
    }

    public Map<String, Object> listLocais(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                    "SELECT CODLOCAL, DESCRLOCAL FROM TGFLOC ORDER BY CODLOCAL");
            rs = stmt.executeQuery();

            List<Map<String, Object>> items = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> item = new HashMap<>();
                item.put("codSankhya", rs.getBigDecimal("CODLOCAL"));
                item.put("descricao", rs.getString("DESCRLOCAL"));
                items.add(item);
            }

            result.put("items", items);
            result.put("total", items.size());
        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao listar locais", e);
            result.put("error", e.getMessage());
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }

        return result;
    }

    public Map<String, Object> listTabelasPreco(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();
            String sql = buildPriceTableQuery(conn);

            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();

            List<Map<String, Object>> items = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> item = new HashMap<>();
                item.put("codSankhya", rs.getBigDecimal("NUTAB"));
                item.put("nuTab", rs.getBigDecimal("NUTAB"));
                item.put("codTab", rs.getString("CODTAB"));
                item.put("descricao", trimToNull(rs.getString("DESCRTAB")));
                item.put("dtVigor", rs.getTimestamp("DTVIGOR"));
                items.add(item);
            }

            result.put("items", items);
            result.put("total", items.size());
        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao listar tabelas de preco", e);
            result.put("error", e.getMessage());
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }

        return result;
    }

    private String buildPriceTableQuery(Connection conn) {
        List<String> tabColumns = getTableColumns(conn, "TGFTAB");
        List<String> viewColumns = getTableColumns(conn, "VGFTAB");

        boolean canUseViewName =
                containsIgnoreCase(viewColumns, "NUTAB") &&
                containsIgnoreCase(viewColumns, "CODTAB") &&
                containsIgnoreCase(viewColumns, "NOMETAB");

        if (canUseViewName) {
            return "SELECT NUTAB, CODTAB, DESCRTAB, DTVIGOR FROM (" +
                    "  SELECT V.NUTAB, V.CODTAB, V.NOMETAB AS DESCRTAB, V.DTVIGOR, " +
                    "         ROW_NUMBER() OVER (PARTITION BY V.CODTAB ORDER BY V.DTVIGOR DESC, V.NUTAB DESC) AS RN " +
                    "  FROM VGFTAB V " +
                    ") X WHERE X.RN = 1 ORDER BY CODTAB";
        }

        String descrColumn = choosePriceTableDescriptionColumn(tabColumns);
        String dateColumn = choosePriceTableDateColumn(tabColumns);

        String selectDescr = descrColumn != null ? "T." + descrColumn + " AS DESCRTAB" : "CAST(T.CODTAB AS VARCHAR(100)) AS DESCRTAB";
        String selectDate = dateColumn != null ? "T." + dateColumn + " AS DTVIGOR" : "NULL AS DTVIGOR";
        String orderDate = dateColumn != null ? "T." + dateColumn : "T.NUTAB";

        return "SELECT NUTAB, CODTAB, DESCRTAB, DTVIGOR FROM (" +
                "  SELECT T.NUTAB, T.CODTAB, " + selectDescr + ", " + selectDate + ", " +
                "         ROW_NUMBER() OVER (PARTITION BY T.CODTAB ORDER BY " + orderDate + " DESC, T.NUTAB DESC) AS RN " +
                "  FROM TGFTAB T " +
                ") X WHERE X.RN = 1 ORDER BY CODTAB";
    }

    static String choosePriceTableDescriptionColumn(List<String> availableColumns) {
        if (availableColumns == null) return null;
        String[] candidates = new String[]{"DESCRTAB", "DESCRICAO", "DESCTAB", "DESCR", "NOMETAB", "NOME"};
        for (String candidate : candidates) {
            if (containsIgnoreCase(availableColumns, candidate)) {
                return candidate;
            }
        }
        return null;
    }

    private static String choosePriceTableDateColumn(List<String> availableColumns) {
        if (availableColumns == null) return null;
        String[] candidates = new String[]{"DTVIGOR", "DHALTER", "DTALTER", "DTREF"};
        for (String candidate : candidates) {
            if (containsIgnoreCase(availableColumns, candidate)) {
                return candidate;
            }
        }
        return null;
    }

    private static boolean containsIgnoreCase(List<String> list, String value) {
        for (String item : list) {
            if (item != null && item.equalsIgnoreCase(value)) {
                return true;
            }
        }
        return false;
    }

    private static List<String> getTableColumns(Connection conn, String table) {
        if (conn == null || table == null) return Collections.emptyList();
        List<String> columns = new ArrayList<>();
        try {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getColumns(null, null, table, null)) {
                while (rs.next()) {
                    String name = rs.getString("COLUMN_NAME");
                    if (name != null) {
                        columns.add(name);
                    }
                }
            }
        } catch (SQLException e) {
            log.log(Level.WARNING, "Erro ao ler colunas da tabela " + table, e);
        }
        return columns;
    }

    public Map<String, Object> listMappings(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        String tipo = getString(params, "tipo");
        if (tipo == null || tipo.isEmpty()) {
            tipo = getString(params, "tipoEntidade");
        }

        if (tipo == null || tipo.isEmpty()) {
            result.put("error", "tipo obrigatorio");
            return result;
        }

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();
            boolean hasIntegraAuto = containsIgnoreCase(getTableColumns(conn, "AD_FCDEPARA"), "INTEGRA_AUTO");
            String selectIntegraAuto = hasIntegraAuto
                    ? "COALESCE(INTEGRA_AUTO, 'S') AS INTEGRA_AUTO"
                    : "'S' AS INTEGRA_AUTO";
            stmt = conn.prepareStatement(
                    "SELECT COD_SANKHYA, COD_EXTERNO, " + selectIntegraAuto + ", DH_CRIACAO, DH_ALTERACAO " +
                            "FROM AD_FCDEPARA WHERE TIPO_ENTIDADE = ? ORDER BY COD_SANKHYA");
            stmt.setString(1, tipo);
            rs = stmt.executeQuery();

            List<Map<String, Object>> items = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> item = new HashMap<>();
                item.put("codSankhya", rs.getBigDecimal("COD_SANKHYA"));
                item.put("codExterno", rs.getString("COD_EXTERNO"));
                item.put("integracaoAutomatica", "S".equalsIgnoreCase(rs.getString("INTEGRA_AUTO")));
                item.put("dhCriacao", rs.getTimestamp("DH_CRIACAO"));
                item.put("dhAlteracao", rs.getTimestamp("DH_ALTERACAO"));
                items.add(item);
            }

            result.put("items", items);
            result.put("total", items.size());
        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao listar de-para", e);
            result.put("error", e.getMessage());
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> saveMappings(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        String tipo = getString(params, "tipo");
        if (tipo == null || tipo.isEmpty()) {
            tipo = getString(params, "tipoEntidade");
        }

        Object itemsObj = params.get("items");
        if (tipo == null || tipo.isEmpty()) {
            result.put("success", false);
            result.put("message", "tipo obrigatorio");
            return result;
        }
        if (!(itemsObj instanceof List)) {
            result.put("success", false);
            result.put("message", "items obrigatorio");
            return result;
        }

        int updated = 0;
        int removed = 0;
        List<String> errors = new ArrayList<>();
        DeparaService deparaService = DeparaService.getInstance();

        for (Object obj : (List<?>) itemsObj) {
            if (!(obj instanceof Map)) continue;
            Map<String, Object> item = (Map<String, Object>) obj;
            BigDecimal codSankhya = toBigDecimal(item.get("codSankhya"));
            String codExterno = getString(item, "codExterno");
            boolean integracaoAutomatica = toBoolean(item.get("integracaoAutomatica"), true);
            if (codSankhya == null) continue;

            try {
                if (codExterno == null || codExterno.trim().isEmpty()) {
                    deparaService.removeMapping(tipo, codSankhya);
                    removed++;
                } else {
                    deparaService.setMapping(tipo, codSankhya, codExterno.trim(), integracaoAutomatica);
                    updated++;
                }
            } catch (Exception e) {
                errors.add("" + codSankhya + ": " + e.getMessage());
            }
        }

        deparaService.invalidateCache();

        result.put("success", errors.isEmpty());
        result.put("updated", updated);
        result.put("removed", removed);
        if (!errors.isEmpty()) {
            result.put("errors", errors);
        }
        result.put("message", "Atualizados: " + updated + ", removidos: " + removed);

        return result;
    }

    static List<Map<String, Object>> merge(List<Map<String, Object>> base, List<Map<String, Object>> mappings) {
        Map<String, String> byCod = new HashMap<>();
        for (Map<String, Object> m : mappings) {
            if (m.get("codSankhya") != null && m.get("codExterno") != null) {
                byCod.put(m.get("codSankhya").toString(), m.get("codExterno").toString());
            }
        }
        for (Map<String, Object> b : base) {
            String key = b.get("codSankhya") != null ? b.get("codSankhya").toString() : null;
            if (key != null && byCod.containsKey(key)) {
                b.put("codExterno", byCod.get(key));
            }
        }
        return base;
    }

    private String getString(Map<String, Object> params, String key) {
        Object value = params.get(key);
        return value != null ? value.toString() : null;
    }

    private BigDecimal toBigDecimal(Object value) {
        if (value == null) return null;
        if (value instanceof BigDecimal) return (BigDecimal) value;
        if (value instanceof Number) return BigDecimal.valueOf(((Number) value).doubleValue());
        try { return new BigDecimal(value.toString()); } catch (Exception e) { return null; }
    }

    private boolean toBoolean(Object value, boolean defaultValue) {
        if (value == null) return defaultValue;
        if (value instanceof Boolean) return (Boolean) value;
        String text = value.toString();
        if (text == null) return defaultValue;
        text = text.trim();
        if (text.isEmpty()) return defaultValue;
        if ("S".equalsIgnoreCase(text) || "Y".equalsIgnoreCase(text) || "TRUE".equalsIgnoreCase(text) || "1".equals(text)) {
            return true;
        }
        if ("N".equalsIgnoreCase(text) || "FALSE".equalsIgnoreCase(text) || "0".equals(text)) {
            return false;
        }
        return defaultValue;
    }

    private String coalesce(String a, String b) {
        if (a != null && !a.trim().isEmpty()) return a;
        return b;
    }

    private String trimToNull(String value) {
        if (value == null) return null;
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
