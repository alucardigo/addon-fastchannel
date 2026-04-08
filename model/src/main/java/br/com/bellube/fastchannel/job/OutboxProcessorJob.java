package br.com.bellube.fastchannel.job;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.PriceBatchItemDTO;
import br.com.bellube.fastchannel.dto.QueueItemDTO;
import br.com.bellube.fastchannel.http.FastchannelPriceClient;
import br.com.bellube.fastchannel.http.FastchannelStockClient;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.LogService;
import br.com.bellube.fastchannel.service.PriceService;
import br.com.bellube.fastchannel.service.PriceBatchResolver;
import br.com.bellube.fastchannel.service.PriceResolver;
import br.com.bellube.fastchannel.service.PriceTableResolver;
import br.com.bellube.fastchannel.service.NotificationService;
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
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Job Agendado para Processamento da Fila de Sincronizacao (Outbox).
 *
 * Processa itens pendentes na tabela AD_FCQUEUE:
 * - ESTOQUE: Envia atualizacoes de estoque para Fastchannel
 * - PRECO: Envia atualizacoes de preco para Fastchannel
 * - PRODUTO: Sincroniza informacoes de produto
 *
 * Configuracao no Sankhya:
 * - Eventos Programaveis > Agendamento
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
            // Verificar se integracao esta ativa
            if (!config.isAtivo()) {
                log.info("Integracao Fastchannel desativada. Job ignorado.");
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

                        case FastchannelConstants.ENTITY_TRACKING:
                            processTrackingItem(item);
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

                    // HTTP 400 com erro de configuracao (tabela invalida, dados malformados)
                    // nao deve gastar retries - marcar como ERRO_FATAL imediatamente
                    if (isConfigurationError(e)) {
                        String msg = "Erro de configuracao (HTTP 400): " + detailedError;
                        log.severe(msg);
                        queueService.markAsFatalError(item.getIdQueue(), msg);
                        NotificationService.getInstance().notifyQueueFatalError(
                                item.getEntityType(), item.getEntityKey(), msg);
                        errors++;
                        continue;
                    }

                    if (item.canRetry(FastchannelConstants.DEFAULT_MAX_RETRIES)) {
                        queueService.markAsError(item.getIdQueue(), detailedError);
                    } else {
                        queueService.markAsFatalError(item.getIdQueue(),
                                "Excedeu maximo de tentativas. " + detailedError);
                        // Notify via email about fatal queue error
                        NotificationService.getInstance().notifyQueueFatalError(
                                item.getEntityType(), item.getEntityKey(), detailedError);
                    }
                    errors++;
                }
            }

            String message = String.format("Job concluido. Processados: %d, Erros: %d", processed, errors);
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

        // A fila ja carrega a chave externa resolvida no enqueue; use-a como fonte primaria.
        String sku = item.getEntityKey();
        if (sku == null || sku.isEmpty()) {
            sku = deparaService.getCodigoExternoAtivo(DeparaService.TIPO_PRODUTO, item.getEntityId());
        }
        if (sku == null || sku.isEmpty()) {
            sku = deparaService.getSkuWithFallback(item.getEntityId());
        }

        if (sku == null || sku.isEmpty()) {
            throw new Exception("SKU nao encontrado para CODPROD " + item.getEntityId());
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

        // Mantem comportamento do legado: prioriza ProductId/EntityKey ja resolvido no enqueue.
        String sku = item.getEntityKey();
        if (sku == null || sku.isEmpty()) {
            sku = deparaService.getCodigoExternoAtivo(DeparaService.TIPO_PRODUTO, item.getEntityId());
        }
        if (sku == null || sku.isEmpty()) {
            sku = deparaService.getSkuWithFallback(item.getEntityId());
        }

        if (sku == null || sku.isEmpty()) {
            throw new Exception("SKU nao encontrado para CODPROD " + item.getEntityId());
        }

        new PriceService().syncPrice(item.getEntityId(), sku);

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
            log.log(Level.FINE, "Falha ao resolver canal de preco por AD_FASTREF para CODPROD " + codProd, e);
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
        // Sincronizacao de produto
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

    private void processTrackingItem(QueueItemDTO item) throws Exception {
        if (item.getPayload() == null || item.getPayload().isEmpty()) {
            throw new Exception("Payload de tracking vazio para item " + item.getIdQueue());
        }

        Map<String, String> payload = gson.fromJson(item.getPayload(),
                new com.google.gson.reflect.TypeToken<Map<String, String>>(){}.getType());

        String orderId = payload.get("orderId");
        String trackingCode = payload.get("trackingCode");
        String carrierName = payload.get("carrierName");

        if (orderId == null || trackingCode == null) {
            throw new Exception("Dados de tracking incompletos: orderId=" + orderId + ", tracking=" + trackingCode);
        }

        br.com.bellube.fastchannel.http.FastchannelOrdersClient ordersClient =
                new br.com.bellube.fastchannel.http.FastchannelOrdersClient();

        br.com.bellube.fastchannel.dto.OrderTrackingDTO tracking =
                new br.com.bellube.fastchannel.dto.OrderTrackingDTO();
        tracking.setTrackingCode(trackingCode);
        if (carrierName != null) {
            tracking.setCarrierName(carrierName);
        }

        ordersClient.sendTracking(orderId, tracking);
        log.info("Tracking enviado para pedido " + orderId + ": " + trackingCode);
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
                // Removido fallback CODTAB como PriceTableId - CODTAB nao e um ID valido no Fastchannel.
                // Legado Node.js nao enviava PriceTableId quando nao havia mapeamento.
            }
        }
        // Removido fallback NUTAB como PriceTableId - NUTAB nao e um ID valido no Fastchannel.
        // Quando nao ha mapeamento valido, retorna null e o preco sera enviado SEM PriceTableId (comportamento legado).
        if (priceTableId == null || priceTableId.trim().isEmpty()) {
            log.info("Nenhum PriceTableId FC valido mapeado para NUTAB " + nuTab
                    + ". Preco sera enviado sem PriceTableId (comportamento legado).");
            return null;
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

    private boolean isConfigurationError(Exception e) {
        if (e == null || e.getMessage() == null) {
            return false;
        }
        String msg = e.getMessage().toLowerCase();
        // HTTP 400 com mensagens de configuracao invalida
        return (msg.contains("status=400") || msg.contains("http 400"))
                && (msg.contains("tabela de preco") || msg.contains("pricetableid")
                || msg.contains("nao e valido") || msg.contains("is not valid")
                || msg.contains("invalid") || msg.contains("bad request"));
    }

    private boolean isNonPublishableSkuError(Exception e) {
        if (e == null || e.getMessage() == null) {
            return false;
        }
        String msg = e.getMessage().toLowerCase();
        return msg.contains("resourcenotfound")
                || msg.contains("sku do produto nao existe")
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
            stmt = conn.prepareStatement("SELECT AD_FAST FROM TGFTAB WHERE NUTAB = ?");
            stmt.setBigDecimal(1, nuTab);
            rs = stmt.executeQuery();
            if (rs.next()) {
                String value = rs.getString("AD_FAST");
                if (value != null && !value.trim().isEmpty()) {
                    return value.trim();
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver TGFTAB.AD_FAST para NUTAB " + nuTab, e);
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
            stmt = conn.prepareStatement("SELECT TOP 1 AD_FAST FROM TGFTAB WHERE CODTAB = ? AND AD_FAST IS NOT NULL ORDER BY DTVIGOR DESC, NUTAB DESC");
            stmt.setBigDecimal(1, codTab);
            rs = stmt.executeQuery();
            if (rs.next()) {
                String value = rs.getString("AD_FAST");
                if (value != null && !value.trim().isEmpty()) {
                    return value.trim();
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver TGFTAB.AD_FAST para CODTAB " + codTab, e);
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
