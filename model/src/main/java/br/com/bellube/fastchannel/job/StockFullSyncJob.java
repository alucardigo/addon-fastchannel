package br.com.bellube.fastchannel.job;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.http.FastchannelStockClient;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.StockResolver;
import br.com.bellube.fastchannel.util.DBUtil;
import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sincronizacao completa de estoque (safety net diario).
 */
public class StockFullSyncJob implements EventoProgramavelJava {
    private static final Logger log = Logger.getLogger(StockFullSyncJob.class.getName());

    public void executeScheduler() throws Exception {
        FastchannelConfig config = FastchannelConfig.getInstance();
        if (!config.isAtivo()) {
            return;
        }

        FastchannelStockClient stockClient = new FastchannelStockClient();
        DeparaService depara = DeparaService.getInstance();
        StockResolver resolver = new StockResolver();

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                    "SELECT CODPROD, ATIVO FROM TGFPRO WHERE CODPROD IS NOT NULL");
            rs = stmt.executeQuery();
            while (rs.next()) {
                BigDecimal codProd = rs.getBigDecimal("CODPROD");
                String sku = depara.getSkuForStock(codProd);
                if (sku == null || sku.trim().isEmpty()) {
                    continue;
                }
                BigDecimal qty = "S".equalsIgnoreCase(rs.getString("ATIVO"))
                        ? resolver.resolve(codProd, config.getCodemp(), config.getCodLocal())
                        : BigDecimal.ZERO;
                try {
                    stockClient.updateStock(sku, qty != null ? qty : BigDecimal.ZERO);
                } catch (Exception e) {
                    log.log(Level.WARNING, "Falha no full sync de estoque para SKU " + sku, e);
                }
            }
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
    }

    @Override public void beforeInsert(PersistenceEvent event) {}
    @Override public void beforeUpdate(PersistenceEvent event) {}
    @Override public void beforeDelete(PersistenceEvent event) {}
    @Override public void afterInsert(PersistenceEvent event) {}
    @Override public void afterUpdate(PersistenceEvent event) {}
    @Override public void afterDelete(PersistenceEvent event) {}
    @Override public void beforeCommit(TransactionContext transactionContext) {}
}

