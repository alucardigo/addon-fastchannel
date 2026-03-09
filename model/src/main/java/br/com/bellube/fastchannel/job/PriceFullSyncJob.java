package br.com.bellube.fastchannel.job;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.service.PriceService;
import br.com.bellube.fastchannel.util.DBUtil;
import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sincronizacao completa de precos (safety net diario).
 */
public class PriceFullSyncJob implements EventoProgramavelJava {
    private static final Logger log = Logger.getLogger(PriceFullSyncJob.class.getName());

    public void executeScheduler() throws Exception {
        FastchannelConfig config = FastchannelConfig.getInstance();
        if (!config.isAtivo()) {
            return;
        }

        List<BigDecimal> codProds = new ArrayList<>();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement("SELECT DISTINCT CODPROD FROM TGFPRO WHERE ATIVO = 'S'");
            rs = stmt.executeQuery();
            while (rs.next()) {
                codProds.add(rs.getBigDecimal("CODPROD"));
            }
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }

        try {
            new PriceService().syncPriceBatch(codProds);
        } catch (Exception e) {
            log.log(Level.SEVERE, "Falha no full sync de preco", e);
            throw e;
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

