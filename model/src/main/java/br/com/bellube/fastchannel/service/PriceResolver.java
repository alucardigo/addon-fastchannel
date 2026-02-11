package br.com.bellube.fastchannel.service;

import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Resolve precos usando SNK_GET_PRECO (SQL Server) e converte para centavos.
 */
public class PriceResolver {

    private static final Logger log = Logger.getLogger(PriceResolver.class.getName());

    public PriceResult resolve(BigDecimal codProd, BigDecimal nuTab) {
        BigDecimal priceDecimal = fetchPriceDecimal(codProd, nuTab);
        BigDecimal listDecimal = priceDecimal;
        BigDecimal priceCentavos = toCentavos(priceDecimal);
        BigDecimal listCentavos = toCentavos(listDecimal);
        return new PriceResult(priceDecimal, listDecimal, priceCentavos, listCentavos);
    }

    BigDecimal toCentavos(BigDecimal value) {
        if (value == null) return null;
        return value.movePointRight(2).setScale(0, BigDecimal.ROUND_HALF_UP);
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
            log.log(Level.WARNING, "Erro ao buscar preco via SNK_GET_PRECO", e);
        } finally {
            closeQuietly(rs);
            if (jdbc != null) {
                try {
                    jdbc.closeSession();
                } catch (Exception e) {
                    log.log(Level.WARNING, "Erro ao fechar session do JdbcWrapper", e);
                }
            }
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
