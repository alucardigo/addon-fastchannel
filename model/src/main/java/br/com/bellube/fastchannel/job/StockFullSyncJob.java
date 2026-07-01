package br.com.bellube.fastchannel.job;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.http.FastchannelStockClient;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.LogService;
import br.com.bellube.fastchannel.service.StockResolver;
import br.com.bellube.fastchannel.util.DBUtil;
import br.com.bellube.fastchannel.util.FastchannelProductFilter;
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
        LogService logService = LogService.getInstance();

        int sent = 0;
        int errors = 0;

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            String activeFcSql = FastchannelProductFilter.getActiveFcProductsSql(conn);

            // [FIX 2026-05-11] Filtrar por config.getCodemp() para alinhar com EstoqueListener.
            // Sem esse filtro o job iterava por TODAS empresas que tinham o produto em estoque
            // e fazia 1 PUT por (empresa, local) - cada PUT sobrescrevia o anterior no FC,
            // resultando em estoque do FC com valor da ULTIMA empresa enviada (que pode ser
            // outra empresa que NAO atende a loja FC). Bug reproduzido com CODPROD 11896:
            // CODEMP=26 (config FC) tinha estoque 0, mas o FC ficou com 6 (CODEMP=37 - ultima
            // empresa processada). Cliente DISMAR comprou 5 unidades sem estoque real na
            // empresa 26.
            StringBuilder sql = new StringBuilder(
                    "SELECT DISTINCT E.CODPROD, E.CODEMP, E.CODLOCAL, P.ATIVO " +
                            "FROM TGFEST E " +
                            "INNER JOIN TGFPRO P ON P.CODPROD = E.CODPROD " +
                            "WHERE E.CODPROD IS NOT NULL " +
                            "AND E.CODEMP IS NOT NULL " +
                            "AND E.CODLOCAL IS NOT NULL " +
                            "AND E.CODPROD IN (" + activeFcSql + ")");
            BigDecimal configCodEmp = config.getCodemp();
            BigDecimal configCodLocal = config.getCodLocal();
            if (configCodEmp != null && configCodEmp.compareTo(BigDecimal.ZERO) > 0) {
                sql.append(" AND E.CODEMP = ?");
            }
            if (configCodLocal != null && configCodLocal.compareTo(BigDecimal.ZERO) > 0) {
                sql.append(" AND E.CODLOCAL = ?");
            }
            stmt = conn.prepareStatement(sql.toString());
            int paramIdx = 1;
            if (configCodEmp != null && configCodEmp.compareTo(BigDecimal.ZERO) > 0) {
                stmt.setBigDecimal(paramIdx++, configCodEmp);
            }
            if (configCodLocal != null && configCodLocal.compareTo(BigDecimal.ZERO) > 0) {
                stmt.setBigDecimal(paramIdx++, configCodLocal);
            }
            log.info("[StockFullSyncJob] Filtros aplicados: codEmp=" + configCodEmp
                    + " codLocal=" + (configCodLocal != null ? configCodLocal : "TODOS"));
            rs = stmt.executeQuery();
            while (rs.next()) {
                BigDecimal codProd = rs.getBigDecimal("CODPROD");
                BigDecimal codEmp = rs.getBigDecimal("CODEMP");
                BigDecimal codLocal = rs.getBigDecimal("CODLOCAL");
                String sku = depara.getSkuForStock(codProd);
                if (sku == null || sku.trim().isEmpty()) {
                    continue;
                }
                String storageId = depara.getCodigoExternoAtivo(DeparaService.TIPO_STOCK_STORAGE, codLocal);
                if (storageId == null || storageId.trim().isEmpty()) {
                    storageId = config.getStorageId();
                }
                String resellerId = depara.getCodigoExternoAtivo(DeparaService.TIPO_STOCK_RESELLER, codEmp);
                if (resellerId == null || resellerId.trim().isEmpty()) {
                    resellerId = config.getResellerId();
                }
                if (storageId == null || storageId.trim().isEmpty()) {
                    logService.warning(LogService.OP_STOCK_SYNC,
                            "StockFullSyncJob ignorou SKU " + sku + ": StorageId nao mapeado para CODLOCAL=" + codLocal);
                    continue;
                }
                if (resellerId == null || resellerId.trim().isEmpty()) {
                    resellerId = "";
                }
                BigDecimal qty = "S".equalsIgnoreCase(rs.getString("ATIVO"))
                        ? resolver.resolve(codProd, codEmp, codLocal)
                        : BigDecimal.ZERO;
                BigDecimal effectiveQty = qty != null ? qty : BigDecimal.ZERO;
                try {
                    stockClient.updateStock(sku, effectiveQty, storageId, resellerId);
                    logService.logStockSync(sku, effectiveQty, true, null);
                    sent++;
                } catch (Exception e) {
                    log.log(Level.WARNING, "Falha no full sync de estoque para SKU " + sku, e);
                    logService.logStockSync(sku, effectiveQty, false, e.getMessage());
                    errors++;
                }
            }
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }

        logService.info(LogService.OP_STOCK_SYNC,
                String.format("StockFullSyncJob concluido. Enviados: %d, Erros: %d", sent, errors));
    }

    @Override public void beforeInsert(PersistenceEvent event) {}
    @Override public void beforeUpdate(PersistenceEvent event) {}
    @Override public void beforeDelete(PersistenceEvent event) {}
    @Override public void afterInsert(PersistenceEvent event) {}
    @Override public void afterUpdate(PersistenceEvent event) {}
    @Override public void afterDelete(PersistenceEvent event) {}
    @Override public void beforeCommit(TransactionContext transactionContext) {}
}
