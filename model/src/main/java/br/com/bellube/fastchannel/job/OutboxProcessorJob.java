package br.com.bellube.fastchannel.job;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.PriceBatchItemDTO;
import br.com.bellube.fastchannel.dto.QueueItemDTO;
import br.com.bellube.fastchannel.http.FastchannelPriceClient;
import br.com.bellube.fastchannel.http.FastchannelStockClient;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.LogService;
import br.com.bellube.fastchannel.service.PriceBatchResolver;
import br.com.bellube.fastchannel.service.PriceResolver;
import br.com.bellube.fastchannel.service.PriceTableResolver;
import br.com.bellube.fastchannel.service.QueueService;
import br.com.bellube.fastchannel.service.StockResolver;
import br.com.bellube.fastchannel.util.DBUtil;
import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import com.google.gson.Gson;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Job Agendado para Processamento da Fila de Sincroniza??o (Outbox).
 *
 * Processa itens pendentes na tabela AD_FCQUEUE:
 * - ESTOQUE: Envia atualiza??es de estoque para Fastchannel
 * - PRECO: Envia atualiza??es de pre?o para Fastchannel
 * - PRODUTO: Sincroniza informa??es de produto
 *
 * Configura??o no Sankhya:
 * - Eventos Program?veis > Agendamento
 * - Classe: br.com.bellube.fastchannel.job.OutboxProcessorJob
 * - Intervalo recomendado: 1-2 minutos
 */
public class OutboxProcessorJob implements EventoProgramavelJava {

    private static final Logger log = Logger.getLogger(OutboxProcessorJob.class.getName());
    private static final Gson gson = new Gson();

    @Override
    public void beforeInsert(PersistenceEvent event) throws Exception {}

    @Override
    public void beforeUpdate(PersistenceEvent event) throws Exception {}

    @Override
    public void beforeDelete(PersistenceEvent event) throws Exception {}

    @Override
    public void afterInsert(PersistenceEvent event) throws Exception {}

    @Override
    public void afterUpdate(PersistenceEvent event) throws Exception {}

    @Override
    public void afterDelete(PersistenceEvent event) throws Exception {}

    @Override
    public void beforeCommit(TransactionContext transactionContext) throws Exception {}

    public void executeScheduler() throws Exception {
        log.info("=== Iniciando Job de Processamento Outbox Fastchannel ===");

        LogService logService = LogService.getInstance();
        FastchannelConfig config = FastchannelConfig.getInstance();
        QueueService queueService = QueueService.getInstance();

        try {
            // Verificar se integra??o est? ativa
            if (!config.isAtivo()) {
                log.info("Integra??o Fastchannel desativada. Job ignorado.");
                return;
            }

            int batchSize = config.getBatchSize();
            int processed = 0;
            int errors = 0;

            // Reativar itens com erro para reprocessamento
            queueService.reactivateErrorItems(FastchannelConstants.DEFAULT_MAX_RETRIES);

            // Buscar itens pendentes
            List<QueueItemDTO> items = queueService.fetchPendingItems(batchSize);

            if (items.isEmpty()) {
                log.fine("Nenhum item pendente na fila.");
                return;
            }

            log.info("Processando " + items.size() + " itens da fila");
            logService.info(LogService.OP_QUEUE_PROCESS, "Iniciando processamento de " + items.size() + " item(ns) da fila");

            FastchannelStockClient stockClient = new FastchannelStockClient();
            DeparaService deparaService = DeparaService.getInstance();

            for (QueueItemDTO item : items) {
                try {
                    queueService.markAsProcessing(item.getIdQueue());

                    switch (item.getEntityType()) {
                        case FastchannelConstants.ENTITY_ESTOQUE:
                            processStockItem(item, stockClient, deparaService);
                            break;

                        case FastchannelConstants.ENTITY_PRECO:
                            processPriceItem(item, deparaService, config);
                            break;

                        case FastchannelConstants.ENTITY_PRODUTO:
                            processProductItem(item, deparaService);
                            break;

                        default:
                            log.warning("Tipo de entidade desconhecido: " + item.getEntityType());
                            queueService.markAsFatalError(item.getIdQueue(), "Tipo desconhecido");
                            continue;
                    }

                    queueService.markAsSuccess(item.getIdQueue());
                    processed++;

                } catch (Exception e) {
                    log.log(Level.WARNING, "Erro ao processar item " + item.getIdQueue(), e);
                    String detailedError = buildDetailedErrorMessage(item, e);
                    logService.error(resolveOperationByEntity(item), detailedError, item.getEntityKey(), e);

                    if (isNonPublishableSkuError(e)) {
                        String msg = "SKU nao publicado no Fastchannel. Item marcado como ENVIADO para evitar retry infinito: "
                                + item.getEntityKey();
                        log.warning(msg);
                        queueService.markAsSuccess(item.getIdQueue());
                        LogService.getInstance().logPriceSync(item.getEntityKey(), false, msg);
                        continue;
                    }

                    if (item.canRetry(FastchannelConstants.DEFAULT_MAX_RETRIES)) {
                        queueService.markAsError(item.getIdQueue(), detailedError);
                    } else {
                        queueService.markAsFatalError(item.getIdQueue(),
                                "Excedeu maximo de tentativas. " + detailedError);
                    }
                    errors++;
                }
            }

            String message = String.format("Job conclu?do. Processados: %d, Erros: %d", processed, errors);
            log.info(message);
            logService.info(LogService.OP_QUEUE_PROCESS, message);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro no Job Outbox", e);
            logService.error(LogService.OP_QUEUE_PROCESS, "Erro no job outbox", e);
            throw e;
        }

        log.info("=== Job de Processamento Outbox Finalizado ===");
    }

    private void processStockItem(QueueItemDTO item, FastchannelStockClient stockClient,
                                  DeparaService deparaService) throws Exception {

        // A fila já carrega a chave externa resolvida no enqueue; use-a como fonte primária.
        String sku = item.getEntityKey();
        if (sku == null || sku.isEmpty()) {
            sku = deparaService.getCodigoExternoAtivo(DeparaService.TIPO_PRODUTO, item.getEntityId());
        }
        if (sku == null || sku.isEmpty()) {
            sku = deparaService.getSkuWithFallback(item.getEntityId());
        }

        if (sku == null || sku.isEmpty()) {
            throw new Exception("SKU n?o encontrado para CODPROD " + item.getEntityId());
        }

        StockPayload payload = parseStockPayload(item.getPayload());
        if (payload == null || payload.codEmp == null || payload.codLocal == null
                || payload.storageId == null || payload.storageId.isEmpty()) {
            throw new Exception("Payload de estoque incompleto. SKU=" + sku + ", payload=" + item.getPayload());
        }

        // Buscar estoque atual do Sankhya
        BigDecimal quantity = new StockResolver().resolve(item.getEntityId(), payload.codEmp, payload.codLocal);
        if (quantity == null) {
            throw new Exception("Estoque nao encontrado no Sankhya para SKU " + sku
                    + " (CODPROD=" + item.getEntityId() + ", CODEMP=" + payload.codEmp + ", CODLOCAL=" + payload.codLocal + ")");
        }

        log.info("Atualizando estoque: SKU " + sku + " = " + quantity);
        stockClient.updateStock(sku, quantity, payload.storageId, payload.resellerId);

        LogService.getInstance().logStockSync(sku, quantity, true, null);
    }

    private void processPriceItem(QueueItemDTO item,
                                  DeparaService deparaService,
                                  FastchannelConfig config) throws Exception {

        // Mantém comportamento do legado: prioriza ProductId/EntityKey já resolvido no enqueue.
        String sku = item.getEntityKey();
        if (sku == null || sku.isEmpty()) {
            sku = deparaService.getCodigoExternoAtivo(DeparaService.TIPO_PRODUTO, item.getEntityId());
        }
        if (sku == null || sku.isEmpty()) {
            sku = deparaService.getSkuWithFallback(item.getEntityId());
        }

        if (sku == null || sku.isEmpty()) {
            throw new Exception("SKU n?o encontrado para CODPROD " + item.getEntityId());
        }

        PriceResolver priceResolver = new PriceResolver();
        PriceBatchResolver batchResolver = new PriceBatchResolver();
        PriceTableResolver tableResolver = new PriceTableResolver();

        List<BigDecimal> tables = new ArrayList<>(tableResolver.resolveEligibleTables());
        if (tables.isEmpty() && config.getNuTab() != null) {
            tables.add(config.getNuTab());
        }
        if (tables.isEmpty()) {
            BigDecimal fallbackNuTab = findAnyNuTabForProduct(item.getEntityId());
            if (fallbackNuTab != null) {
                tables.add(fallbackNuTab);
            }
        }
        if (tables.isEmpty()) {
            throw new Exception("Nenhuma tabela de preco elegivel configurada");
        }

        int sentCount = 0;
        int skippedNoIntegration = 0;
        int skippedNoPrice = 0;

        for (BigDecimal nuTab : tables) {
            if (!deparaService.isIntegracaoAutomaticaAtiva(DeparaService.TIPO_TABELA_PRECO, nuTab)) {
                log.info("Tabela de preço " + nuTab + " com integração automática desabilitada. Ignorando.");
                skippedNoIntegration++;
                continue;
            }
            PriceResolver.PriceResult priceResult = priceResolver.resolve(item.getEntityId(), nuTab);
            if (priceResult == null || priceResult.getPriceCentavos() == null) {
                log.warning("Preco nao encontrado para SKU " + sku + " (NUTAB " + nuTab + ")");
                skippedNoPrice++;
                continue;
            }

            BigDecimal priceTableId = resolvePriceTableId(deparaService, nuTab);
            BigDecimal price = priceResult.getPriceCentavos();
            BigDecimal listPrice = priceResult.getListPriceCentavos();

            FastchannelPriceClient priceClient = resolvePriceClient(item.getEntityId(), sku, nuTab);
            log.info("Atualizando pre?o: SKU " + sku + " NUTAB " + nuTab + " = " + price
                    + " canal=" + priceClient.getChannel());
            priceClient.updatePrice(sku, price, listPrice, priceTableId);
            sentCount++;

            List<PriceBatchItemDTO> batches = batchResolver.resolve(item.getEntityId(), nuTab, priceTableId);
            if (!batches.isEmpty()) {
                priceClient.updatePriceBatches(sku, priceTableId, batches);
            }
        }

        if (sentCount == 0) {
            throw new Exception("Nenhum preco enviado para SKU " + sku
                    + ". Tabelas analisadas=" + tables.size()
                    + ", semIntegracao=" + skippedNoIntegration
                    + ", semPreco=" + skippedNoPrice);
        }

        LogService.getInstance().logPriceSync(sku, true, null);
    }

    private FastchannelPriceClient resolvePriceClient(BigDecimal codProd, String sku, BigDecimal nuTab) {
        FastchannelPriceClient.Channel channel = resolvePriceChannel(codProd, sku, nuTab);
        return new FastchannelPriceClient(channel);
    }

    private FastchannelPriceClient.Channel resolvePriceChannel(BigDecimal codProd, String sku, BigDecimal nuTab) {
        String tipoFast = resolveTipoFastByNuTab(nuTab);
        if (tipoFast != null) {
            String normalizedTipo = tipoFast.trim().toUpperCase();
            if (normalizedTipo.contains("DIST") || normalizedTipo.equals("R")
                    || normalizedTipo.equals("REVENDA") || normalizedTipo.equals("REVENDA")) {
                return FastchannelPriceClient.Channel.DISTRIBUTION;
            }
            if (normalizedTipo.contains("CONS") || normalizedTipo.equals("C")) {
                return FastchannelPriceClient.Channel.CONSUMPTION;
            }
        }

        FastchannelPriceClient.Channel byRule = resolvePriceChannelByBrandRule(codProd);
        if (byRule != null) {
            return byRule;
        }

        if (sku != null && sku.toUpperCase().startsWith("D-")) {
            return FastchannelPriceClient.Channel.DISTRIBUTION;
        }

        return FastchannelPriceClient.Channel.CONSUMPTION;
    }

    private FastchannelPriceClient.Channel resolvePriceChannelByBrandRule(BigDecimal codProd) {
        if (codProd == null) return null;
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                    "SELECT M.AD_FASTREF AS AD_FASTREF " +
                            "FROM TGFPRO P " +
                            "LEFT JOIN TGFMAR M ON M.CODIGO = P.CODMARCA " +
                            "WHERE P.CODPROD = ?");
            stmt.setBigDecimal(1, codProd);
            rs = stmt.executeQuery();
            if (rs.next()) {
                String fastRef = rs.getString("AD_FASTREF");
                if (fastRef != null && fastRef.trim().equalsIgnoreCase("R")) {
                    return FastchannelPriceClient.Channel.DISTRIBUTION;
                }
                if (fastRef != null && fastRef.trim().equalsIgnoreCase("C")) {
                    return FastchannelPriceClient.Channel.CONSUMPTION;
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver canal de preço por AD_FASTREF para CODPROD " + codProd, e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return null;
    }

    private String resolveTipoFastByNuTab(BigDecimal nuTab) {
        if (nuTab == null) return null;
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement("SELECT AD_TIPO_FAST FROM TGFTAB WHERE NUTAB = ?");
            stmt.setBigDecimal(1, nuTab);
            rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getString("AD_TIPO_FAST");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver AD_TIPO_FAST para NUTAB " + nuTab, e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return null;
    }

    private void processProductItem(QueueItemDTO item, DeparaService deparaService) throws Exception {
        // Sincroniza??o de produto
        String sku = item.getEntityKey();

        if (sku == null || sku.isEmpty()) {
            sku = deparaService.getSkuWithFallback(item.getEntityId());
        }

        if (sku != null && !sku.isEmpty()) {
            // Registrar no De-Para
            deparaService.setMapping(DeparaService.TIPO_PRODUTO, item.getEntityId(), sku);
            log.info("Produto sincronizado: CODPROD " + item.getEntityId() + " <-> SKU " + sku);
        }
    }

    private StockPayload parseStockPayload(String payloadJson) {
        if (payloadJson == null || payloadJson.isEmpty()) {
            return null;
        }
        try {
            return gson.fromJson(payloadJson, StockPayload.class);
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao parsear payload de estoque", e);
            return null;
        }
    }

    private static final class StockPayload {
        private String sku;
        private BigDecimal quantity;
        private BigDecimal codEmp;
        private BigDecimal codLocal;
        private String storageId;
        private String resellerId;
    }

    private BigDecimal resolvePriceTableId(DeparaService deparaService, BigDecimal nuTab) {
        if (nuTab == null) return null;
        String priceTableId = deparaService.getCodigoExternoAtivo(DeparaService.TIPO_TABELA_PRECO, nuTab);
        if (priceTableId == null || priceTableId.trim().isEmpty()) {
            priceTableId = deparaService.getCodigoExterno(DeparaService.TIPO_TABELA_PRECO, nuTab);
        }
        if (priceTableId == null || priceTableId.trim().isEmpty()) {
            BigDecimal codTab = resolveCodTabFromNuTab(nuTab);
            if (codTab != null) {
                priceTableId = deparaService.getCodigoExternoAtivo(DeparaService.TIPO_TABELA_PRECO, codTab);
                if (priceTableId == null || priceTableId.trim().isEmpty()) {
                    priceTableId = deparaService.getCodigoExterno(DeparaService.TIPO_TABELA_PRECO, codTab);
                }
                if (priceTableId == null || priceTableId.trim().isEmpty()) {
                    BigDecimal latestNuTab = resolveLatestNuTabByCodTab(codTab);
                    if (latestNuTab != null) {
                        priceTableId = deparaService.getCodigoExternoAtivo(DeparaService.TIPO_TABELA_PRECO, latestNuTab);
                        if (priceTableId == null || priceTableId.trim().isEmpty()) {
                            priceTableId = deparaService.getCodigoExterno(DeparaService.TIPO_TABELA_PRECO, latestNuTab);
                        }
                    }
                }
            }
        }
        if (priceTableId == null || priceTableId.trim().isEmpty()) {
            priceTableId = resolvePriceTableIdFromAdIdFastByNuTab(nuTab);
        }
        if ((priceTableId == null || priceTableId.trim().isEmpty())) {
            BigDecimal codTab = resolveCodTabFromNuTab(nuTab);
            if (codTab != null) {
                priceTableId = resolvePriceTableIdFromAdIdFastByCodTab(codTab);
                if (priceTableId == null || priceTableId.trim().isEmpty()) {
                    // Mantem consistencia com a tela de precos: usa CODTAB quando nao houver de-para explicito.
                    priceTableId = codTab.toPlainString();
                }
            }
        }
        if ((priceTableId == null || priceTableId.trim().isEmpty()) && isDirectNuTabFallbackEnabled()) {
            priceTableId = nuTab.toPlainString();
        }
        if (priceTableId == null || priceTableId.trim().isEmpty()) {
            throw new IllegalStateException("PriceTableId Fastchannel nao mapeado para NUTAB " + nuTab
                    + ". Configure de-para TIPO_TABELA_PRECO.");
        }
        try {
            return new BigDecimal(priceTableId);
        } catch (NumberFormatException e) {
            log.warning("PriceTableId invalido para NUTAB " + nuTab + ": " + priceTableId);
            throw new IllegalStateException("PriceTableId invalido para NUTAB " + nuTab + ": " + priceTableId, e);
        }
    }

    private BigDecimal findAnyNuTabForProduct(BigDecimal codProd) {
        if (codProd == null) return null;

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                    "SELECT TOP 1 NUTAB FROM TGFEXC WHERE CODPROD = ? ORDER BY NUTAB");
            stmt.setBigDecimal(1, codProd);
            rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("NUTAB");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Falha ao resolver NUTAB fallback para CODPROD " + codProd, e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return null;
    }

    private boolean isNonPublishableSkuError(Exception e) {
        if (e == null || e.getMessage() == null) {
            return false;
        }
        String msg = e.getMessage().toLowerCase();
        return msg.contains("resourcenotfound")
                || msg.contains("sku do produto não existe")
                || msg.contains("sku do produto nao existe")
                || msg.contains("sku do produto") && msg.contains("incorreto");
    }

    private BigDecimal resolveCodTabFromNuTab(BigDecimal nuTab) {
        if (nuTab == null) return null;
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement("SELECT CODTAB FROM TGFTAB WHERE NUTAB = ?");
            stmt.setBigDecimal(1, nuTab);
            rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODTAB");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Falha ao resolver CODTAB para NUTAB " + nuTab, e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return null;
    }

    private BigDecimal resolveLatestNuTabByCodTab(BigDecimal codTab) {
        if (codTab == null) return null;
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                    "SELECT TOP 1 NUTAB FROM TGFTAB WHERE CODTAB = ? ORDER BY DTVIGOR DESC, NUTAB DESC");
            stmt.setBigDecimal(1, codTab);
            rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("NUTAB");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Falha ao resolver NUTAB vigente para CODTAB " + codTab, e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return null;
    }

    private String resolvePriceTableIdFromAdIdFastByNuTab(BigDecimal nuTab) {
        if (nuTab == null) return null;
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement("SELECT AD_IDFAST FROM TGFTAB WHERE NUTAB = ?");
            stmt.setBigDecimal(1, nuTab);
            rs = stmt.executeQuery();
            if (rs.next()) {
                String value = rs.getString("AD_IDFAST");
                if (value != null && !value.trim().isEmpty()) {
                    return value.trim();
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver TGFTAB.AD_IDFAST para NUTAB " + nuTab, e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return null;
    }

    private String resolvePriceTableIdFromAdIdFastByCodTab(BigDecimal codTab) {
        if (codTab == null) return null;
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement("SELECT TOP 1 AD_IDFAST FROM TGFTAB WHERE CODTAB = ? AND AD_IDFAST IS NOT NULL AND LTRIM(RTRIM(AD_IDFAST)) <> '' ORDER BY DTVIGOR DESC, NUTAB DESC");
            stmt.setBigDecimal(1, codTab);
            rs = stmt.executeQuery();
            if (rs.next()) {
                String value = rs.getString("AD_IDFAST");
                if (value != null && !value.trim().isEmpty()) {
                    return value.trim();
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver TGFTAB.AD_IDFAST para CODTAB " + codTab, e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return null;
    }

    private boolean isDirectNuTabFallbackEnabled() {
        String configured = System.getProperty("fastchannel.price.allowDirectNuTabFallback");
        if (configured == null || configured.trim().isEmpty()) {
            configured = System.getenv("FASTCHANNEL_PRICE_ALLOW_DIRECT_NUTAB_FALLBACK");
        }
        return configured != null && Boolean.parseBoolean(configured);
    }

    private String buildDetailedErrorMessage(QueueItemDTO item, Exception e) {
        StringBuilder sb = new StringBuilder();
        sb.append("Falha no processamento da fila");
        if (item != null) {
            sb.append(" [IDQUEUE=").append(item.getIdQueue());
            sb.append(", ENTITY=").append(item.getEntityType());
            sb.append(", OP=").append(item.getOperation());
            sb.append(", ENTITY_ID=").append(item.getEntityId());
            if (item.getEntityKey() != null && !item.getEntityKey().isEmpty()) {
                sb.append(", ENTITY_KEY=").append(item.getEntityKey());
            }
            sb.append("]");
        }
        if (e != null) {
            if (e.getClass() != null) {
                sb.append(" | ").append(e.getClass().getSimpleName());
            }
            if (e.getMessage() != null && !e.getMessage().isEmpty()) {
                sb.append(": ").append(e.getMessage());
            }
            Throwable cause = e.getCause();
            if (cause != null && cause.getMessage() != null && !cause.getMessage().isEmpty()) {
                sb.append(" | cause=").append(cause.getClass().getSimpleName()).append(": ").append(cause.getMessage());
            }
        }
        return sb.toString();
    }

    private String resolveOperationByEntity(QueueItemDTO item) {
        if (item == null || item.getEntityType() == null) {
            return LogService.OP_QUEUE_PROCESS;
        }
        if (FastchannelConstants.ENTITY_ESTOQUE.equals(item.getEntityType())) {
            return LogService.OP_STOCK_SYNC;
        }
        if (FastchannelConstants.ENTITY_PRECO.equals(item.getEntityType())) {
            return LogService.OP_PRICE_SYNC;
        }
        if (FastchannelConstants.ENTITY_PRODUTO.equals(item.getEntityType())) {
            return LogService.OP_PRODUCT_SYNC;
        }
        return LogService.OP_QUEUE_PROCESS;
    }
}

