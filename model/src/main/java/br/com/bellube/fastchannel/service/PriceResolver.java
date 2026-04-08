package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.util.DBUtil;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Resolve precos usando SNK_GET_PRECO (SQL Server) e converte para centavos.
 */
public class PriceResolver {

    private static final Logger log = Logger.getLogger(PriceResolver.class.getName());

    public PriceResult resolve(BigDecimal codProd, BigDecimal nuTab) {
        // SalePrice = preco final calculado (com ajuste percentual se TIPO=P)
        BigDecimal salePrice = fetchPriceDecimal(codProd, nuTab);
        if (salePrice == null || salePrice.compareTo(BigDecimal.ZERO) <= 0) {
            return null;
        }
        // ListPrice = preco da tabela de ORIGEM (sem ajuste percentual)
        // Se a tabela tem CODTABORIG, busca o preco la; senao, ListPrice = SalePrice
        BigDecimal listPrice = fetchOriginPrice(codProd, nuTab);
        if (listPrice == null || listPrice.compareTo(BigDecimal.ZERO) <= 0) {
            listPrice = salePrice; // fallback: se nao tem origem, usa o mesmo
        }
        return new PriceResult(salePrice, listPrice, toCentavos(salePrice), toCentavos(listPrice));
    }

    BigDecimal toCentavos(BigDecimal value) {
        if (value == null) return null;
        return value.movePointRight(2).setScale(0, BigDecimal.ROUND_HALF_UP);
    }

    /**
     * Busca o preco da TABELA DE ORIGEM (ListPrice).
     * Se a tabela atual tem CODTABORIG (tabela dependente com percentual),
     * busca SNK_GET_PRECO na tabela origem para obter o preco base sem ajuste.
     */
    private BigDecimal fetchOriginPrice(BigDecimal codProd, BigDecimal nuTab) {
        if (codProd == null || nuTab == null) return null;

        ResultSet rs = null;
        JdbcWrapper jdbc = null;
        try {
            jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
            jdbc.openSession();
            NativeSql sql = new NativeSql(jdbc);

            sql.appendSql("SELECT TOP 1 TORIG.NUTAB AS NUTAB_ORIG ");
            sql.appendSql("FROM TGFTAB T ");
            sql.appendSql("INNER JOIN TGFTAB TORIG ON TORIG.CODTAB = T.CODTABORIG ");
            sql.appendSql("WHERE T.NUTAB = :nuTab ");
            sql.appendSql("AND T.CODTABORIG IS NOT NULL AND T.CODTABORIG > 0 ");
            sql.appendSql("ORDER BY TORIG.DTVIGOR DESC");
            sql.setNamedParameter("nuTab", nuTab);

            rs = sql.executeQuery();
            if (rs.next()) {
                BigDecimal nuTabOrig = rs.getBigDecimal("NUTAB_ORIG");
                if (nuTabOrig != null) {
                    return fetchPriceDecimal(codProd, nuTabOrig);
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "JAPE indisponivel para fetchOriginPrice, usando JDBC direto", e);
            return fetchOriginPriceJdbc(codProd, nuTab);
        } finally {
            closeQuietly(rs);
            if (jdbc != null) {
                try { jdbc.closeSession(); } catch (Exception ignored) {}
            }
        }
        return null;
    }

    private BigDecimal fetchOriginPriceJdbc(BigDecimal codProd, BigDecimal nuTab) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                "SELECT TOP 1 TORIG.NUTAB AS NUTAB_ORIG " +
                "FROM TGFTAB T " +
                "INNER JOIN TGFTAB TORIG ON TORIG.CODTAB = T.CODTABORIG " +
                "WHERE T.NUTAB = ? " +
                "AND T.CODTABORIG IS NOT NULL AND T.CODTABORIG > 0 " +
                "ORDER BY TORIG.DTVIGOR DESC");
            stmt.setBigDecimal(1, nuTab);
            rs = stmt.executeQuery();
            if (rs.next()) {
                BigDecimal nuTabOrig = rs.getBigDecimal("NUTAB_ORIG");
                if (nuTabOrig != null) {
                    return fetchPriceDecimal(codProd, nuTabOrig);
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Fallback JDBC fetchOriginPrice falhou", e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return null;
    }

    private BigDecimal fetchPriceDecimal(BigDecimal codProd, BigDecimal nuTab) {
        if (codProd == null || nuTab == null) return null;

        ResultSet rs = null;
        JdbcWrapper jdbc = null;
        try {
            jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
            jdbc.openSession();
            NativeSql sql = new NativeSql(jdbc);

            sql.appendSql("SELECT [sankhya].SNK_GET_PRECO(:nuTab, :codProd, GETDATE()) AS VLR_FINAL");

            sql.setNamedParameter("nuTab", nuTab);
            sql.setNamedParameter("codProd", codProd);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("VLR_FINAL");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "JAPE indisponivel para fetchPriceDecimal, usando JDBC direto", e);
            return fetchPriceDecimalJdbc(codProd, nuTab);
        } finally {
            closeQuietly(rs);
            if (jdbc != null) {
                try {
                    jdbc.closeSession();
                } catch (Exception e) {
                    log.log(Level.FINE, "Erro ao fechar session do JdbcWrapper", e);
                }
            }
        }

        return null;
    }

    private BigDecimal fetchPriceDecimalJdbc(BigDecimal codProd, BigDecimal nuTab) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement("SELECT [sankhya].SNK_GET_PRECO(?, ?, GETDATE()) AS VLR_FINAL");
            stmt.setBigDecimal(1, nuTab);
            stmt.setBigDecimal(2, codProd);
            rs = stmt.executeQuery();
            if (rs.next()) {
                BigDecimal price = rs.getBigDecimal("VLR_FINAL");
                log.info("(JDBC) SNK_GET_PRECO NUTAB=" + nuTab + " CODPROD=" + codProd + " = " + price);
                return price;
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Fallback JDBC fetchPriceDecimal falhou", e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return null;
    }

    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (Exception ignored) {}
        }
    }

    public static class PriceResult {
        private final BigDecimal priceDecimal;
        private final BigDecimal listPriceDecimal;
        private final BigDecimal priceCentavos;
        private final BigDecimal listPriceCentavos;

        public PriceResult(BigDecimal priceDecimal, BigDecimal listPriceDecimal,
                           BigDecimal priceCentavos, BigDecimal listPriceCentavos) {
            this.priceDecimal = priceDecimal;
            this.listPriceDecimal = listPriceDecimal;
            this.priceCentavos = priceCentavos;
            this.listPriceCentavos = listPriceCentavos;
        }

        public BigDecimal getPriceDecimal() {
            return priceDecimal;
        }

        public BigDecimal getListPriceDecimal() {
            return listPriceDecimal;
        }

        public BigDecimal getPriceCentavos() {
            return priceCentavos;
        }

        public BigDecimal getListPriceCentavos() {
            return listPriceCentavos;
        }
    }
}
