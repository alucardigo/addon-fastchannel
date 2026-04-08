package br.com.bellube.fastchannel.util;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.service.DeparaService;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Centraliza a logica de filtragem de produtos FC (marcas habilitadas, SKU, empresas, locais).
 * Classe utilitaria stateless com metodos estaticos e cache thread-safe de existencia de colunas.
 */
public final class FastchannelProductFilter {

    private static final Logger log = Logger.getLogger(FastchannelProductFilter.class.getName());

    /** Cache thread-safe para existencia de colunas: "TABLE.COLUMN" -> Boolean */
    private static final ConcurrentHashMap<String, Boolean> columnCache = new ConcurrentHashMap<>();

    private FastchannelProductFilter() {
    }

    // -------------------------------------------------------
    // Column existence checks (cached)
    // -------------------------------------------------------

    /**
     * Verifica se uma coluna existe em uma tabela, com cache em memoria.
     */
    public static boolean supportsColumn(Connection conn, String table, String column) {
        if (conn == null) return false;
        String key = table.toUpperCase() + "." + column.toUpperCase();
        Boolean cached = columnCache.get(key);
        if (cached != null) return cached;

        boolean found = false;
        try {
            try (ResultSet rs = conn.getMetaData().getColumns(null, null, table, column)) {
                if (rs.next()) { found = true; }
            }
            if (!found) {
                try (ResultSet rs = conn.getMetaData().getColumns(null, null, table.toUpperCase(), column.toUpperCase())) {
                    if (rs.next()) { found = true; }
                }
            }
            if (!found) {
                try (ResultSet rs = conn.getMetaData().getColumns(null, null, table.toLowerCase(), column.toLowerCase())) {
                    if (rs.next()) { found = true; }
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Erro ao validar coluna " + table + "." + column, e);
        }

        columnCache.put(key, found);
        return found;
    }

    // -------------------------------------------------------
    // Marca checks (AD_FAST, AD_FASTREF)
    // -------------------------------------------------------

    public static boolean hasMarcaFastColumn(Connection conn) {
        return supportsColumn(conn, "TGFMAR", "AD_FAST");
    }

    public static boolean hasMarcaFastRefColumn(Connection conn) {
        return supportsColumn(conn, "TGFMAR", "AD_FASTREF");
    }

    // -------------------------------------------------------
    // SKU expression
    // -------------------------------------------------------

    /**
     * Retorna expressao SQL CASE para resolver o SKU do produto.
     * Requer alias P para TGFPRO e M para TGFMAR no contexto da query.
     */
    public static String resolveSkuExpression(Connection conn) {
        if (hasMarcaFastRefColumn(conn)) {
            return "CASE WHEN M.AD_FASTREF = 'R' THEN LTRIM(RTRIM(CAST(P.REFFORN AS VARCHAR(50)))) " +
                    "ELSE LTRIM(RTRIM(CAST(P.CODPROD AS VARCHAR(50)))) END";
        }
        return "LTRIM(RTRIM(CAST(P.CODPROD AS VARCHAR(50))))";
    }

    // -------------------------------------------------------
    // Marca JOIN
    // -------------------------------------------------------

    /**
     * Retorna clausula INNER JOIN TGFMAR com filtros de marca FC habilitada.
     * Requer alias P para TGFPRO no contexto da query.
     */
    public static String resolveMarcaJoin(Connection conn) {
        boolean hasFast = hasMarcaFastColumn(conn);
        boolean hasFastRef = hasMarcaFastRefColumn(conn);
        if (hasFast && hasFastRef) {
            return " INNER JOIN TGFMAR M ON M.CODIGO = P.CODMARCA AND M.AD_FAST = 'S' AND M.AD_FASTREF IN ('C', 'R') ";
        }
        if (hasFast) {
            return " INNER JOIN TGFMAR M ON M.CODIGO = P.CODMARCA AND M.AD_FAST = 'S' ";
        }
        return " INNER JOIN TGFMAR M ON M.CODIGO = P.CODMARCA ";
    }

    // -------------------------------------------------------
    // Configured empresas filter
    // -------------------------------------------------------

    /**
     * Adiciona filtro de empresas configuradas no de-para a um WHERE clause.
     * Usado por FCEstoqueService e outros contextos com coluna direta de CODEMP.
     */
    public static void appendConfiguredEmpresasFilter(StringBuilder where, List<Object> params,
                                                       Connection conn, String columnExpr) {
        List<BigDecimal> empresas = loadConfiguredEmpresas(conn);
        if (!empresas.isEmpty()) {
            where.append(" AND ").append(columnExpr).append(" IN (");
            appendInClauseValues(where, params, empresas);
            where.append(")");
            return;
        }
        BigDecimal fallbackCodEmp = FastchannelConfig.getInstance().getCodemp();
        if (fallbackCodEmp != null) {
            where.append(" AND ").append(columnExpr).append(" = ?");
            params.add(fallbackCodEmp);
        }
    }

    // -------------------------------------------------------
    // Configured locais filter (stock - direct column)
    // -------------------------------------------------------

    /**
     * Adiciona filtro de locais configurados para estoque (coluna direta CODLOCAL).
     */
    public static void appendConfiguredLocaisFilter(StringBuilder where, List<Object> params,
                                                     Connection conn, String columnExpr) {
        List<BigDecimal> locais = loadConfiguredLocais(conn);
        if (!locais.isEmpty()) {
            where.append(" AND ").append(columnExpr).append(" IN (");
            appendInClauseValues(where, params, locais);
            where.append(")");
            return;
        }
        BigDecimal fallbackCodLocal = FastchannelConfig.getInstance().getCodLocal();
        if (fallbackCodLocal != null) {
            where.append(" AND ").append(columnExpr).append(" = ?");
            params.add(fallbackCodLocal);
        }
    }

    // -------------------------------------------------------
    // Configured locais filter (prices - EXISTS subquery)
    // -------------------------------------------------------

    /**
     * Adiciona filtro de locais configurados para precos via EXISTS subquery em TGFEST.
     * Usado quando a tabela principal (TGFEXC) nao tem CODLOCAL.
     */
    public static void appendConfiguredLocaisFilter(StringBuilder where, List<Object> params,
                                                     Connection conn, String codProdExpr,
                                                     String codEmpExpr) {
        List<BigDecimal> locais = loadConfiguredLocais(conn);
        boolean hasCodEmp = supportsColumn(conn, "TGFEXC", "CODEMP");
        if (locais.isEmpty()) {
            BigDecimal fallbackCodLocal = FastchannelConfig.getInstance().getCodLocal();
            if (fallbackCodLocal != null) {
                where.append(" AND EXISTS (SELECT 1 FROM TGFEST ESTL ");
                where.append("WHERE ESTL.CODPROD = ").append(codProdExpr).append(" ");
                where.append("AND ESTL.CODPARC = 0 ");
                if (hasCodEmp && codEmpExpr != null && !codEmpExpr.trim().isEmpty()) {
                    where.append("AND ESTL.CODEMP = ").append(codEmpExpr).append(" ");
                }
                where.append("AND ESTL.CODLOCAL = ?)");
                params.add(fallbackCodLocal);
            }
            return;
        }

        where.append(" AND EXISTS (SELECT 1 FROM TGFEST ESTL ");
        where.append("WHERE ESTL.CODPROD = ").append(codProdExpr).append(" ");
        where.append("AND ESTL.CODPARC = 0 ");
        if (hasCodEmp && codEmpExpr != null && !codEmpExpr.trim().isEmpty()) {
            where.append("AND ESTL.CODEMP = ").append(codEmpExpr).append(" ");
        }
        where.append("AND ESTL.CODLOCAL IN (");
        appendInClauseValues(where, params, locais);
        where.append("))");
    }

    // -------------------------------------------------------
    // Load configured entities from De-Para
    // -------------------------------------------------------

    public static List<BigDecimal> loadConfiguredEmpresas(Connection conn) {
        List<BigDecimal> ids = new ArrayList<>();
        if (conn == null) return ids;

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            boolean hasIntegraAuto = supportsColumn(conn, "AD_FCDEPARA", "INTEGRA_AUTO");
            String sql = "SELECT DISTINCT COD_SANKHYA FROM AD_FCDEPARA " +
                    "WHERE TIPO_ENTIDADE IN (?, ?) " +
                    (hasIntegraAuto ? "AND COALESCE(INTEGRA_AUTO, 'S') = 'S' " : "") +
                    "AND COD_SANKHYA IS NOT NULL";
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, DeparaService.TIPO_EMPRESA);
            stmt.setString(2, DeparaService.TIPO_STOCK_RESELLER);
            rs = stmt.executeQuery();
            while (rs.next()) {
                BigDecimal cod = rs.getBigDecimal("COD_SANKHYA");
                if (cod != null) {
                    ids.add(cod);
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel carregar empresas configuradas do de/para", e);
        } finally {
            DBUtil.closeAll(rs, stmt, null);
        }
        return ids;
    }

    public static List<BigDecimal> loadConfiguredLocais(Connection conn) {
        List<BigDecimal> ids = new ArrayList<>();
        if (conn == null) return ids;

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            boolean hasIntegraAuto = supportsColumn(conn, "AD_FCDEPARA", "INTEGRA_AUTO");
            String sql = "SELECT DISTINCT COD_SANKHYA FROM AD_FCDEPARA " +
                    "WHERE TIPO_ENTIDADE IN (?, ?) " +
                    (hasIntegraAuto ? "AND COALESCE(INTEGRA_AUTO, 'S') = 'S' " : "") +
                    "AND COD_SANKHYA IS NOT NULL";
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, DeparaService.TIPO_LOCAL);
            stmt.setString(2, DeparaService.TIPO_STOCK_STORAGE);
            rs = stmt.executeQuery();
            while (rs.next()) {
                BigDecimal cod = rs.getBigDecimal("COD_SANKHYA");
                if (cod != null) {
                    ids.add(cod);
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel carregar locais de estoque do de/para", e);
        } finally {
            DBUtil.closeAll(rs, stmt, null);
        }
        return ids;
    }

    public static List<BigDecimal> loadConfiguredPriceTables(Connection conn) {
        List<BigDecimal> ids = new ArrayList<>();
        if (conn == null) return ids;

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            boolean hasIntegraAuto = supportsColumn(conn, "AD_FCDEPARA", "INTEGRA_AUTO");
            String sql = "SELECT DISTINCT COD_SANKHYA FROM AD_FCDEPARA " +
                    "WHERE TIPO_ENTIDADE = ? " +
                    (hasIntegraAuto ? "AND COALESCE(INTEGRA_AUTO, 'S') = 'S' " : "") +
                    "AND COD_SANKHYA IS NOT NULL";
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, DeparaService.TIPO_TABELA_PRECO);
            rs = stmt.executeQuery();
            while (rs.next()) {
                BigDecimal cod = rs.getBigDecimal("COD_SANKHYA");
                if (cod != null) {
                    ids.add(cod);
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel carregar tabelas de preco do de/para", e);
        } finally {
            DBUtil.closeAll(rs, stmt, null);
        }
        return ids;
    }

    // -------------------------------------------------------
    // Active FC products SQL
    // -------------------------------------------------------

    /**
     * Retorna SQL para buscar produtos ativos habilitados para FC (via marca).
     * Faz fallback se coluna AD_FAST nao existir na TGFMAR.
     */
    public static String getActiveFcProductsSql(Connection conn) {
        if (hasMarcaFastColumn(conn)) {
            return "SELECT DISTINCT P.CODPROD FROM TGFPRO P " +
                    "INNER JOIN TGFMAR M ON P.CODMARCA = M.CODIGO AND M.AD_FAST = 'S' " +
                    "WHERE P.ATIVO = 'S'";
        }
        // Fallback: todos os produtos ativos
        return "SELECT DISTINCT P.CODPROD FROM TGFPRO P WHERE P.ATIVO = 'S'";
    }

    // -------------------------------------------------------
    // Helper
    // -------------------------------------------------------

    public static void appendInClauseValues(StringBuilder where, List<Object> params, List<BigDecimal> values) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) where.append(", ");
            where.append("?");
            params.add(values.get(i));
        }
    }
}
