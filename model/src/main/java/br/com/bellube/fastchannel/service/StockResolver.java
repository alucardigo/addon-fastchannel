package br.com.bellube.fastchannel.service;

import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StockResolver {

    private static final Logger log = Logger.getLogger(StockResolver.class.getName());

    private static final String SQL_WITH_BRAND_FILTER = ""
            + "SELECT SUM(E.ESTOQUE - E.RESERVADO) AS QTD "
            + "FROM TGFEST E "
            + "INNER JOIN TGFPRO P ON P.CODPROD = E.CODPROD "
            + "INNER JOIN TGFMAR M ON M.CODIGO = P.CODMARCA "
            + "WHERE E.CODPROD = :codProd "
            + "AND E.CODEMP = :codEmp "
            + "AND E.CODLOCAL = :codLocal "
            + "AND P.ATIVO = 'S' "
            + "AND M.AD_FAST = 'S' "
            + "AND M.AD_FASTREF IN ('C','R')";

    private static final String SQL_FALLBACK = ""
            + "SELECT SUM(E.ESTOQUE - E.RESERVADO) AS QTD "
            + "FROM TGFEST E "
            + "INNER JOIN TGFPRO P ON P.CODPROD = E.CODPROD "
            + "WHERE E.CODPROD = :codProd "
            + "AND E.CODEMP = :codEmp "
            + "AND E.CODLOCAL = :codLocal "
            + "AND P.ATIVO = 'S'";

    private static volatile Boolean supportsBrandFilter;
    private static volatile boolean loggedFallback;

    public BigDecimal resolve(BigDecimal codProd, BigDecimal codEmp, BigDecimal codLocal) {
        if (codProd == null || codEmp == null || codLocal == null) {
            return null;
        }

        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
            NativeSql sql = new NativeSql(jdbc);
            String sqlText = resolveSql(jdbc);
            sql.appendSql(sqlText);
            sql.setNamedParameter("codProd", codProd);
            sql.setNamedParameter("codEmp", codEmp);
            sql.setNamedParameter("codLocal", codLocal);

            rs = sql.executeQuery();
            if (rs.next()) {
                BigDecimal qtd = rs.getBigDecimal("QTD");
                return qtd != null ? qtd : BigDecimal.ZERO;
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao resolver estoque", e);
        } finally {
            closeQuietly(rs);
        }
        return BigDecimal.ZERO;
    }

    String getSql() {
        return SQL_WITH_BRAND_FILTER;
    }

    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (Exception ignored) {}
        }
    }

    private String resolveSql(JdbcWrapper jdbc) {
        if (supportsBrandFilter == null) {
            supportsBrandFilter = hasColumn(jdbc, "TGFMAR", "AD_FAST")
                    && hasColumn(jdbc, "TGFMAR", "AD_FASTREF")
                    && hasColumn(jdbc, "TGFPRO", "CODMARCA");
            if (!supportsBrandFilter && !loggedFallback) {
                log.warning("TGFMAR.AD_FAST/AD_FASTREF indisponiveis neste schema. Usando fallback de estoque sem filtro de marca.");
                loggedFallback = true;
            }
        }
        return supportsBrandFilter ? SQL_WITH_BRAND_FILTER : SQL_FALLBACK;
    }

    private boolean hasColumn(JdbcWrapper jdbc, String tableName, String columnName) {
        ResultSet rs = null;
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT COUNT(*) AS CNT ");
            sql.appendSql("FROM INFORMATION_SCHEMA.COLUMNS ");
            sql.appendSql("WHERE TABLE_NAME = :tableName AND COLUMN_NAME = :columnName");
            sql.setNamedParameter("tableName", tableName);
            sql.setNamedParameter("columnName", columnName);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getInt("CNT") > 0;
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel verificar coluna " + tableName + "." + columnName, e);
        } finally {
            closeQuietly(rs);
        }
        return false;
    }
}
