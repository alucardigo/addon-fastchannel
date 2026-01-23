package br.com.bellube.fastchannel.job;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.QueueItemDTO;
import br.com.bellube.fastchannel.dto.StockDTO;
import br.com.bellube.fastchannel.http.FastchannelPriceClient;
import br.com.bellube.fastchannel.http.FastchannelStockClient;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.LogService;
import br.com.bellube.fastchannel.service.QueueService;
import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import com.google.gson.Gson;

import java.math.BigDecimal;
import java.sql.ResultSet;
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
                            processStockItem(item, stockClient, deparaService);
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

    private void processStockItem(QueueItemDTO item, FastchannelStockClient stockClient,
                                  DeparaService deparaService) throws Exception {

        String sku = item.getEntityKey();
        if (sku == null || sku.isEmpty()) {
            sku = deparaService.getSkuWithFallback(item.getEntityId());
        }

        if (sku == null || sku.isEmpty()) {
            throw new Exception("SKU n?o encontrado para CODPROD " + item.getEntityId());
        }

        // Buscar estoque atual do Sankhya
        BigDecimal quantity = getCurrentStock(item.getEntityId());

        log.info("Atualizando estoque: SKU " + sku + " = " + quantity);
        stockClient.updateStock(sku, quantity);

        LogService.getInstance().logStockSync(sku, quantity, true, null);
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

        // Buscar pre?o atual do Sankhya
        BigDecimal[] prices = getCurrentPrice(item.getEntityId());
        BigDecimal price = prices[0];
        BigDecimal listPrice = prices[1];

        log.info("Atualizando pre?o: SKU " + sku + " = " + price);
        priceClient.updatePrice(sku, price, listPrice);

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

    private BigDecimal getCurrentStock(BigDecimal codProd) {
        FastchannelConfig config = FastchannelConfig.getInstance();
        BigDecimal codLocal = config.getCodLocal();
        BigDecimal codEmp = config.getCodemp();

        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT ESTOQUE FROM TGFEST ");
            sql.appendSql("WHERE CODPROD = :codProd AND CODLOCAL = :codLocal AND CODEMP = :codEmp");

            sql.setNamedParameter("codProd", codProd);
            sql.setNamedParameter("codLocal", codLocal);
            sql.setNamedParameter("codEmp", codEmp);

            rs = sql.executeQuery();
            if (rs.next()) {
                BigDecimal estoque = rs.getBigDecimal("ESTOQUE");
                return estoque != null ? estoque : BigDecimal.ZERO;
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar estoque", e);
        } finally {
            closeQuietly(rs);
        }
        return BigDecimal.ZERO;
    }

    private BigDecimal[] getCurrentPrice(BigDecimal codProd) {
        FastchannelConfig config = FastchannelConfig.getInstance();
        BigDecimal nuTab = config.getNuTab();

        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT VLRVENDA, VLRTABELA FROM TGFEXC ");
            sql.appendSql("WHERE CODPROD = :codProd AND NUTAB = :nuTab AND ATIVO = 'S'");

            sql.setNamedParameter("codProd", codProd);
            sql.setNamedParameter("nuTab", nuTab);

            rs = sql.executeQuery();
            if (rs.next()) {
                BigDecimal vlrVenda = rs.getBigDecimal("VLRVENDA");
                BigDecimal vlrTabela = rs.getBigDecimal("VLRTABELA");
                return new BigDecimal[] {
                    vlrVenda != null ? vlrVenda : BigDecimal.ZERO,
                    vlrTabela != null ? vlrTabela : vlrVenda
                };
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar pre?o", e);
        } finally {
            closeQuietly(rs);
        }
        return new BigDecimal[] { BigDecimal.ZERO, BigDecimal.ZERO };
    }

    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (Exception ignored) {}
        }
    }
}

