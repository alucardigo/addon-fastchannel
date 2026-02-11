package br.com.bellube.fastchannel.service;

import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

final class PartnerLookupUtil {

    private static final Logger log = Logger.getLogger(PartnerLookupUtil.class.getName());

    private PartnerLookupUtil() {
    }

    static BigDecimal findCodParcByDocument(String document) {
        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT CODPARC FROM TGFPAR ");
            sql.appendSql("WHERE REPLACE(REPLACE(REPLACE(CGC_CPF, '.', ''), '-', ''), '/', '') = :doc");
            sql.setNamedParameter("doc", document);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODPARC");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar CODPARC por documento", e);
        } finally {
            closeQuietly(rs);
        }
        return null;
    }

    static BigDecimal findCodVendByParc(BigDecimal codParc) {
        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT CODVEND FROM TGFPAR WHERE CODPARC = :codParc");
            sql.setNamedParameter("codParc", codParc);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODVEND");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar CODVEND do parceiro", e);
        } finally {
            closeQuietly(rs);
        }
        return null;
    }

    private static void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (Exception ignored) {}
        }
    }
}
