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
import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import com.google.gson.Gson;

import java.math.BigDecimal;
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

            FastchannelStockClient stockClient = new FastchannelStockClient();
            FastchannelPriceClient priceClient = new FastchannelPriceClient();
            DeparaService deparaService = DeparaService.getInstance();

            for (QueueItemDTO item : items) {
                try {
                    queueService.markAsProcessing(item.getIdQueue());

                    switch (item.getEntityType()) {
                        case FastchannelConstants.ENTITY_ESTOQUE:
                            if (!processStockItem(item, stockClient, deparaService)) {
                                queueService.markAsFatalError(item.getIdQueue(), "Mapping de estoque ausente");
                                continue;
                            }
                            break;

                        case FastchannelConstants.ENTITY_PRECO:
                            processPriceItem(item, priceClient, deparaService);
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

                    if (item.canRetry(FastchannelConstants.DEFAULT_MAX_RETRIES)) {
                        queueService.markAsError(item.getIdQueue(), e.getMessage());
                    } else {
                        queueService.markAsFatalError(item.getIdQueue(),
                                "Excedeu m?ximo de tentativas: " + e.getMessage());
                    }
                    errors++;
                }
            }

            String message = String.format("Job conclu?do. Processados: %d, Erros: %d", processed, errors);
            log.info(message);
            logService.info(LogService.OP_STOCK_SYNC, message);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro no Job Outbox", e);
            logService.error(LogService.OP_STOCK_SYNC, "Erro no job", e);
            throw e;
        }

        log.info("=== Job de Processamento Outbox Finalizado ===");
    }

    private boolean processStockItem(QueueItemDTO item, FastchannelStockClient stockClient,
                                     DeparaService deparaService) throws Exception {

        String sku = item.getEntityKey();
        if (sku == null || sku.isEmpty()) {
            sku = deparaService.getSkuForStock(item.getEntityId());
        }

        if (sku == null || sku.isEmpty()) {
            throw new Exception("SKU n?o encontrado para CODPROD " + item.getEntityId());
        }

        StockPayload payload = parseStockPayload(item.getPayload());
        if (payload == null || payload.codEmp == null || payload.codLocal == null
                || payload.storageId == null || payload.storageId.isEmpty()) {
            log.warning("Estoque ignorado: payload incompleto para SKU " + sku);
            return false;
        }

        // Buscar estoque atual do Sankhya
        BigDecimal quantity = new StockResolver().resolve(item.getEntityId(), payload.codEmp, payload.codLocal);
        if (quantity == null) {
            log.warning("Estoque ignorado: sem dados para SKU " + sku);
            return false;
        }

        log.info("Atualizando estoque: SKU " + sku + " = " + quantity);
        stockClient.updateStock(sku, quantity, payload.storageId);

        LogService.getInstance().logStockSync(sku, quantity, true, null);
        return true;
    }

    private void processPriceItem(QueueItemDTO item, FastchannelPriceClient priceClient,
                                  DeparaService deparaService) throws Exception {

        String sku = item.getEntityKey();
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
        if (tables.isEmpty()) {
            throw new Exception("Nenhuma tabela de preco elegivel configurada");
        }

        for (BigDecimal nuTab : tables) {
            PriceResolver.PriceResult priceResult = priceResolver.resolve(item.getEntityId(), nuTab);
            if (priceResult == null || priceResult.getPriceCentavos() == null) {
                log.warning("Preco nao encontrado para SKU " + sku + " (NUTAB " + nuTab + ")");
                continue;
            }

            BigDecimal priceTableId = resolvePriceTableId(deparaService, nuTab);
            BigDecimal price = priceResult.getPriceCentavos();
            BigDecimal listPrice = priceResult.getListPriceCentavos();

            log.info("Atualizando pre?o: SKU " + sku + " NUTAB " + nuTab + " = " + price);
            priceClient.updatePrice(sku, price, listPrice, priceTableId);

            List<PriceBatchItemDTO> batches = batchResolver.resolve(item.getEntityId(), nuTab, priceTableId);
            if (!batches.isEmpty()) {
                priceClient.updatePriceBatches(sku, priceTableId, batches);
            }
        }

        LogService.getInstance().logPriceSync(sku, true, null);
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
        String priceTableId = deparaService.getCodigoExterno(DeparaService.TIPO_TABELA_PRECO, nuTab);
        if (priceTableId == null || priceTableId.trim().isEmpty()) {
            return nuTab;
        }
        try {
            return new BigDecimal(priceTableId);
        } catch (NumberFormatException e) {
            log.warning("PriceTableId invalido para NUTAB " + nuTab + ": " + priceTableId);
            return nuTab;
        }
    }
}

