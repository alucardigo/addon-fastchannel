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
            List<String> columns = getTableColumns(conn, "TGFTAB");
            String descrColumn = choosePriceTableDescriptionColumn(columns);
            String dateColumn = choosePriceTableDateColumn(columns);

            String selectDescr = descrColumn != null ? "T." + descrColumn + " AS DESCRTAB" : "T.CODTAB AS DESCRTAB";
            String selectDate = dateColumn != null ? "T." + dateColumn + " AS DTVIGOR" : "NULL AS DTVIGOR";
            String orderDate = dateColumn != null ? "T." + dateColumn : "T.NUTAB";

            String sql = "SELECT NUTAB, CODTAB, DESCRTAB, DTVIGOR FROM (" +
                    "  SELECT T.NUTAB, T.CODTAB, " + selectDescr + ", " + selectDate + ", " +
                    "         ROW_NUMBER() OVER (PARTITION BY T.CODTAB ORDER BY " + orderDate + " DESC, T.NUTAB DESC) AS RN " +
                    "  FROM TGFTAB T " +
                    ") X WHERE X.RN = 1 ORDER BY CODTAB";

            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();

            List<Map<String, Object>> items = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> item = new HashMap<>();
                item.put("nuTab", rs.getBigDecimal("NUTAB"));
                item.put("codTab", rs.getString("CODTAB"));
                item.put("descricao", rs.getString("DESCRTAB"));
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
            stmt = conn.prepareStatement(
                    "SELECT COD_SANKHYA, COD_EXTERNO, DH_CRIACAO, DH_ALTERACAO " +
                            "FROM AD_FCDEPARA WHERE TIPO_ENTIDADE = ? ORDER BY COD_SANKHYA");
            stmt.setString(1, tipo);
            rs = stmt.executeQuery();

            List<Map<String, Object>> items = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> item = new HashMap<>();
                item.put("codSankhya", rs.getBigDecimal("COD_SANKHYA"));
                item.put("codExterno", rs.getString("COD_EXTERNO"));
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
            if (codSankhya == null) continue;

            try {
                if (codExterno == null || codExterno.trim().isEmpty()) {
                    deparaService.removeMapping(tipo, codSankhya);
                    removed++;
                } else {
                    deparaService.setMapping(tipo, codSankhya, codExterno.trim());
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

    // ====================================================================
    // Metodos avancados (tela de-para.html) - suportam todos os 5 tipos
    // de entidade: EMPRESA, LOCAL, TABELA_PRECO, STOCK_STORAGE, STOCK_RESELLER
    // ====================================================================

    /**
     * Lista mapeamentos de-para filtrados por tipo, com descricao via JOIN.
     */
    public Map<String, Object> list(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();

            String tipo = getString(params, "tipo");
            if (tipo == null || tipo.isEmpty()) {
                tipo = DeparaService.TIPO_EMPRESA;
            }

            StringBuilder where = new StringBuilder("D.TIPO_ENTIDADE = ?");
            List<Object> queryParams = new ArrayList<>();
            queryParams.add(tipo);

            BigDecimal codSankhya = toBigDecimal(params.get("codSankhya"));
            if (codSankhya != null) {
                where.append(" AND D.COD_SANKHYA = ?");
                queryParams.add(codSankhya);
            }

            String descricao = getString(params, "descricao");
            if (descricao != null && !descricao.isEmpty()) {
                where.append(" AND ").append(getDescColumn(tipo)).append(" LIKE ?");
                queryParams.add("%" + descricao + "%");
            }

            String joinClause = getJoinClause(tipo);
            String descSelect = getDescSelect(tipo);

            String sql = "SELECT D.IDDEPARA, D.COD_SANKHYA, D.COD_EXTERNO, D.DH_CRIACAO, D.DH_ALTERACAO, "
                    + descSelect
                    + " FROM AD_FCDEPARA D "
                    + joinClause
                    + " WHERE " + where
                    + " ORDER BY D.COD_SANKHYA";

            stmt = conn.prepareStatement(sql);
            int idx = 1;
            for (Object param : queryParams) {
                if (param instanceof BigDecimal) {
                    stmt.setBigDecimal(idx++, (BigDecimal) param);
                } else {
                    stmt.setObject(idx++, param);
                }
            }

            rs = stmt.executeQuery();

            List<Map<String, Object>> items = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> item = new HashMap<>();
                item.put("id", rs.getBigDecimal("IDDEPARA"));
                item.put("codSankhya", rs.getBigDecimal("COD_SANKHYA"));
                item.put("codExterno", rs.getString("COD_EXTERNO"));
                item.put("descricao", rs.getString("DESCRICAO"));
                item.put("dhCriacao", rs.getTimestamp("DH_CRIACAO"));
                item.put("dhAlteracao", rs.getTimestamp("DH_ALTERACAO"));
                items.add(item);
            }

            result.put("items", items);
            result.put("total", items.size());
            result.put("tipo", tipo);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao listar de-para", e);
            result.put("error", e.getMessage());
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }

        return result;
    }

    /**
     * Salva (cria/atualiza) mapeamentos de-para.
     */
    public Map<String, Object> save(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        int ok = 0;
        int errors = 0;

        try {
            DeparaService deparaService = DeparaService.getInstance();
            List<Map<String, Object>> items = extractItems(params);

            if (items.isEmpty()) {
                String tipo = getString(params, "tipo");
                BigDecimal codSankhya = toBigDecimal(params.get("codSankhya"));
                String codExterno = getString(params, "codExterno");

                if (tipo != null && codSankhya != null && codExterno != null && !codExterno.isEmpty()) {
                    deparaService.setMapping(tipo, codSankhya, codExterno);
                    ok++;
                } else {
                    result.put("error", "Parametros obrigatorios: tipo, codSankhya, codExterno");
                    return result;
                }
            } else {
                for (Map<String, Object> item : items) {
                    try {
                        String tipo = asString(item.get("tipo"));
                        BigDecimal codSankhya = toBigDecimal(item.get("codSankhya"));
                        String codExterno = asString(item.get("codExterno"));

                        if (tipo == null || codSankhya == null || codExterno == null || codExterno.isEmpty()) {
                            errors++;
                            continue;
                        }

                        deparaService.setMapping(tipo, codSankhya, codExterno);
                        ok++;
                    } catch (Exception e) {
                        errors++;
                        log.log(Level.WARNING, "Erro ao salvar de-para", e);
                    }
                }
            }

            deparaService.invalidateCache();

            result.put("success", errors == 0);
            result.put("saved", ok);
            result.put("errors", errors);
            result.put("message", "Salvos: " + ok + ", erros: " + errors);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao salvar de-para", e);
            result.put("success", false);
            result.put("message", e.getMessage());
        }

        return result;
    }

    /**
     * Remove mapeamento de-para.
     */
    public Map<String, Object> remove(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();

        try {
            String tipo = getString(params, "tipo");
            BigDecimal codSankhya = toBigDecimal(params.get("codSankhya"));

            if (tipo == null || codSankhya == null) {
                result.put("error", "Parametros obrigatorios: tipo, codSankhya");
                return result;
            }

            DeparaService deparaService = DeparaService.getInstance();
            deparaService.removeMapping(tipo, codSankhya);
            deparaService.invalidateCache();

            result.put("success", true);
            result.put("message", "Mapeamento removido");

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao remover de-para", e);
            result.put("success", false);
            result.put("message", e.getMessage());
        }

        return result;
    }

    /**
     * Lista entidades Sankhya disponiveis para mapeamento (que ainda nao tem de-para).
     */
    public Map<String, Object> listDisponiveis(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();

            String tipo = getString(params, "tipo");
            if (tipo == null || tipo.isEmpty()) {
                tipo = DeparaService.TIPO_EMPRESA;
            }

            String sql = getAvailableQuery(tipo);
            if (sql == null) {
                result.put("items", new ArrayList<>());
                return result;
            }

            String descricao = getString(params, "descricao");
            List<Object> queryParams = new ArrayList<>();
            if (descricao != null && !descricao.isEmpty()) {
                sql = sql.replace("/*FILTER*/", " AND " + getSourceDescColumn(tipo) + " LIKE ?");
                queryParams.add("%" + descricao + "%");
            } else {
                sql = sql.replace("/*FILTER*/", "");
            }

            stmt = conn.prepareStatement(sql);
            int idx = 1;
            for (Object param : queryParams) {
                stmt.setObject(idx++, param);
            }

            rs = stmt.executeQuery();

            List<Map<String, Object>> items = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> item = new HashMap<>();
                item.put("codSankhya", rs.getBigDecimal("COD_SANKHYA"));
                item.put("descricao", rs.getString("DESCRICAO"));
                items.add(item);
            }

            result.put("items", items);
            result.put("tipo", tipo);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao listar disponiveis", e);
            result.put("error", e.getMessage());
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }

        return result;
    }

    // --- SQL helpers por tipo (usado pelo metodo list) ---

    private String getJoinClause(String tipo) {
        switch (tipo) {
            case DeparaService.TIPO_EMPRESA:
                return "LEFT JOIN TSIEMP E ON E.CODEMP = D.COD_SANKHYA ";
            case DeparaService.TIPO_LOCAL:
                return "LEFT JOIN TGFLOC L ON L.CODLOCAL = D.COD_SANKHYA ";
            case DeparaService.TIPO_TABELA_PRECO:
                return "LEFT JOIN TGFTAB T ON T.NUTAB = D.COD_SANKHYA ";
            case DeparaService.TIPO_STOCK_STORAGE:
                return "LEFT JOIN TGFLOC L ON L.CODLOCAL = D.COD_SANKHYA ";
            case DeparaService.TIPO_STOCK_RESELLER:
                return "LEFT JOIN TSIEMP E ON E.CODEMP = D.COD_SANKHYA ";
            default:
                return "";
        }
    }

    private String getDescSelect(String tipo) {
        switch (tipo) {
            case DeparaService.TIPO_EMPRESA:
            case DeparaService.TIPO_STOCK_RESELLER:
                return "ISNULL(E.RAZAOABREV, '(sem descricao)') AS DESCRICAO";
            case DeparaService.TIPO_LOCAL:
            case DeparaService.TIPO_STOCK_STORAGE:
                return "ISNULL(L.DESCRLOCAL, '(sem descricao)') AS DESCRICAO";
            case DeparaService.TIPO_TABELA_PRECO:
                return "ISNULL(T.DESCRTAB, '(sem descricao)') AS DESCRICAO";
            default:
                return "CAST(D.COD_SANKHYA AS VARCHAR(20)) AS DESCRICAO";
        }
    }

    private String getDescColumn(String tipo) {
        switch (tipo) {
            case DeparaService.TIPO_EMPRESA:
            case DeparaService.TIPO_STOCK_RESELLER:
                return "E.RAZAOABREV";
            case DeparaService.TIPO_LOCAL:
            case DeparaService.TIPO_STOCK_STORAGE:
                return "L.DESCRLOCAL";
            case DeparaService.TIPO_TABELA_PRECO:
                return "T.DESCRTAB";
            default:
                return "CAST(D.COD_SANKHYA AS VARCHAR(20))";
        }
    }

    private String getSourceDescColumn(String tipo) {
        switch (tipo) {
            case DeparaService.TIPO_EMPRESA:
            case DeparaService.TIPO_STOCK_RESELLER:
                return "E.RAZAOABREV";
            case DeparaService.TIPO_LOCAL:
            case DeparaService.TIPO_STOCK_STORAGE:
                return "L.DESCRLOCAL";
            case DeparaService.TIPO_TABELA_PRECO:
                return "T.DESCRTAB";
            default:
                return "1=1";
        }
    }

    private String getAvailableQuery(String tipo) {
        switch (tipo) {
            case DeparaService.TIPO_EMPRESA:
                return "SELECT E.CODEMP AS COD_SANKHYA, E.RAZAOABREV AS DESCRICAO "
                        + "FROM TSIEMP E "
                        + "WHERE NOT EXISTS (SELECT 1 FROM AD_FCDEPARA D WHERE D.TIPO_ENTIDADE = 'EMPRESA' AND D.COD_SANKHYA = E.CODEMP) "
                        + "/*FILTER*/ ORDER BY E.CODEMP";
            case DeparaService.TIPO_LOCAL:
            case DeparaService.TIPO_STOCK_STORAGE:
                return "SELECT L.CODLOCAL AS COD_SANKHYA, L.DESCRLOCAL AS DESCRICAO "
                        + "FROM TGFLOC L "
                        + "WHERE NOT EXISTS (SELECT 1 FROM AD_FCDEPARA D WHERE D.TIPO_ENTIDADE = '" + tipo + "' AND D.COD_SANKHYA = L.CODLOCAL) "
                        + "/*FILTER*/ ORDER BY L.CODLOCAL";
            case DeparaService.TIPO_TABELA_PRECO:
                return "SELECT T.NUTAB AS COD_SANKHYA, T.DESCRTAB AS DESCRICAO "
                        + "FROM TGFTAB T "
                        + "WHERE NOT EXISTS (SELECT 1 FROM AD_FCDEPARA D WHERE D.TIPO_ENTIDADE = 'TABELA_PRECO' AND D.COD_SANKHYA = T.NUTAB) "
                        + "/*FILTER*/ ORDER BY T.NUTAB";
            case DeparaService.TIPO_STOCK_RESELLER:
                return "SELECT E.CODEMP AS COD_SANKHYA, E.RAZAOABREV AS DESCRICAO "
                        + "FROM TSIEMP E "
                        + "WHERE NOT EXISTS (SELECT 1 FROM AD_FCDEPARA D WHERE D.TIPO_ENTIDADE = 'STOCK_RESELLER' AND D.COD_SANKHYA = E.CODEMP) "
                        + "/*FILTER*/ ORDER BY E.CODEMP";
            default:
                return null;
        }
    }

    @SuppressWarnings("unchecked")
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

    private String asString(Object value) {
        return value != null ? value.toString() : null;
    }

    // ====================================================================
    // Metodos utilitarios compartilhados
    // ====================================================================

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

    private String coalesce(String a, String b) {
        if (a != null && !a.trim().isEmpty()) return a;
        return b;
    }
}
