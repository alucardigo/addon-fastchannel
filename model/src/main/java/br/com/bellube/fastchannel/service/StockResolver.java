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

    private static final String SQL = ""
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

    public BigDecimal resolve(BigDecimal codProd, BigDecimal codEmp, BigDecimal codLocal) {
        if (codProd == null || codEmp == null || codLocal == null) {
            return null;
        }

        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql(SQL);
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
        return SQL;
    }

    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (Exception ignored) {}
        }
    }
}
