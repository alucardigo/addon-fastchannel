package br.com.bellube.fastchannel.http;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.StockDTO;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Cliente especializado para Stock Management API do Fastchannel.
 *
 * Operações:
 * - Atualizar estoque de produto
 * - Consultar estoque atual
 */
public class FastchannelStockClient {

    private static final Logger log = Logger.getLogger(FastchannelStockClient.class.getName());
    private static final Gson gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd'T'HH:mm:ss")
            .create();

    private final FastchannelHttpClient httpClient;
    private final FastchannelConfig config;

    public FastchannelStockClient() {
        this.httpClient = new FastchannelHttpClient();
        this.config = FastchannelConfig.getInstance();
    }

    public FastchannelStockClient(FastchannelHttpClient httpClient) {
        this.httpClient = httpClient;
        this.config = FastchannelConfig.getInstance();
    }

    /**
     * Atualiza estoque de um SKU específico.
     *
     * @param sku código do produto
     * @param quantity quantidade disponível
     */
    public void updateStock(String sku, BigDecimal quantity) throws Exception {
        updateStock(sku, quantity, null);
    }

    /**
     * Atualiza estoque de um SKU específico informando o StorageId.
     */
    public void updateStock(String sku, BigDecimal quantity, String storageIdOverride) throws Exception {
        updateStock(sku, quantity, storageIdOverride, null);
    }

    /**
     * Atualiza estoque de um SKU específico informando StorageId e ResellerId.
     */
    public void updateStock(String sku, BigDecimal quantity, String storageIdOverride, String resellerIdOverride) throws Exception {
        String storageId = (storageIdOverride != null && !storageIdOverride.isEmpty())
                ? storageIdOverride
                : config.getStorageId();
        if (storageId == null || storageId.isEmpty()) {
            throw new Exception("Storage ID não configurado para atualização de estoque.");
        }
        String resellerId = (resellerIdOverride != null && !resellerIdOverride.isEmpty())
                ? resellerIdOverride
                : config.getResellerId();

        String endpoint = String.format(FastchannelConstants.ENDPOINT_STOCK, sku);
        String json = gson.toJson(buildLegacyCompatiblePayload(sku, quantity, storageId, resellerId));
        log.info("Atualizando estoque do SKU " + sku + ": " + quantity);

        FastchannelHttpClient.HttpResult result = httpClient.putStock(endpoint, json);

        if (!result.isSuccess()) {
            log.warning("Erro ao atualizar estoque: HTTP " + result.getStatusCode() + " - " + result.getBody());
            throw new Exception(buildHttpError("PUT", endpoint, sku, result));
        }

        log.info("Estoque do SKU " + sku + " atualizado com sucesso.");
    }

    /**
     * Atualiza estoque com dados completos.
     *
     * @param stockDto dados completos de estoque
     */
    public void updateStock(StockDTO stockDto) throws Exception {
        if (stockDto.getSku() == null || stockDto.getSku().isEmpty()) {
            throw new IllegalArgumentException("SKU é obrigatório");
        }

        String storageId = stockDto.getStorageId();
        if (storageId == null || storageId.isEmpty()) {
            storageId = config.getStorageId();
            stockDto.setStorageId(storageId);
        }

        String endpoint = String.format(FastchannelConstants.ENDPOINT_STOCK, stockDto.getSku());
        String json = gson.toJson(stockDto);

        log.info("Atualizando estoque completo do SKU " + stockDto.getSku());

        FastchannelHttpClient.HttpResult result = httpClient.putStock(endpoint, json);

        if (!result.isSuccess()) {
            log.warning("Erro ao atualizar estoque: HTTP " + result.getStatusCode() + " - " + result.getBody());
            throw new Exception(buildHttpError("PUT", endpoint, stockDto.getSku(), result));
        }

        log.info("Estoque do SKU " + stockDto.getSku() + " atualizado com sucesso.");
    }

    private Map<String, Object> buildLegacyCompatiblePayload(String sku, BigDecimal quantity,
                                                             String storageId, String resellerId) {
        BigDecimal safeQty = quantity == null ? BigDecimal.ZERO : quantity;
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("ProductId", sku);
        payload.put("IsAvailable", safeQty.compareTo(BigDecimal.ZERO) > 0);
        payload.put("StorageId", storageId);
        payload.put("StorageName", storageId);
        if (resellerId != null && !resellerId.isEmpty()) {
            payload.put("ResellerId", resellerId);
            payload.put("ResellerName", resellerId);
        }
        payload.put("ProductDefinitionId", sku);
        payload.put("ProductName", sku);
        payload.put("Quantity", safeQty);
        payload.put("MinimumQuantity", BigDecimal.ZERO);
        payload.put("HandlingTime", BigDecimal.ZERO);
        payload.put("IsExternalStockEnabled", false);
        payload.put("ExternalStockHandlingTime", BigDecimal.ZERO);
        return payload;
    }

    /**
     * Consulta estoque atual de um SKU.
     *
     * @param sku código do produto
     * @return dados de estoque ou null se não encontrado
     */
    public StockDTO getStock(String sku) throws Exception {
        String endpoint = String.format(FastchannelConstants.ENDPOINT_STOCK, sku);

        FastchannelHttpClient.HttpResult result = httpClient.getStock(endpoint);

        if (result.getStatusCode() == 404) {
            log.info("SKU " + sku + " não encontrado no Fastchannel.");
            return null;
        }

        if (!result.isSuccess()) {
            log.warning("Erro ao consultar estoque: HTTP " + result.getStatusCode());
            throw new Exception(buildHttpError("GET", endpoint, sku, result));
        }

        return gson.fromJson(result.getBody(), StockDTO.class);
    }

    /**
     * Zera o estoque de um SKU (usado quando produto é inativado).
     *
     * @param sku código do produto
     */
    public void zeroStock(String sku) throws Exception {
        updateStock(sku, BigDecimal.ZERO);
    }

    private String buildHttpError(String method, String endpoint, String sku, FastchannelHttpClient.HttpResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append("Erro ").append(method).append(" estoque Fastchannel");
        if (sku != null && !sku.isEmpty()) {
            sb.append(" [SKU=").append(sku).append("]");
        }
        sb.append(" endpoint=").append(endpoint);
        sb.append(" status=").append(result.getStatusCode());
        String body = result.getBody();
        if (body != null && !body.trim().isEmpty()) {
            sb.append(" body=").append(truncate(body.trim(), 1500));
        }
        return sb.toString();
    }

    private String truncate(String value, int max) {
        if (value == null) return null;
        return value.length() > max ? value.substring(0, max) : value;
    }
}
