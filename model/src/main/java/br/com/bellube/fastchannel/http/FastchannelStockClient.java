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
 * Operacoes:
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
     * Atualiza estoque de um SKU especifico.
     *
     * @param sku codigo do produto
     * @param quantity quantidade disponivel
     */
    public void updateStock(String sku, BigDecimal quantity) throws Exception {
        updateStock(sku, quantity, null);
    }

    /**
     * Atualiza estoque de um SKU especifico informando o StorageId.
     */
    public void updateStock(String sku, BigDecimal quantity, String storageIdOverride) throws Exception {
        updateStock(sku, quantity, storageIdOverride, null);
    }

    /**
     * Atualiza estoque de um SKU especifico informando StorageId e ResellerId.
     */
    public void updateStock(String sku, BigDecimal quantity, String storageIdOverride, String resellerIdOverride) throws Exception {
        String storageId = (storageIdOverride != null && !storageIdOverride.isEmpty())
                ? storageIdOverride
                : config.getStorageId();
        if (storageId == null || storageId.isEmpty()) {
            throw new Exception("Storage ID nao configurado para atualizacao de estoque.");
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
            throw new IllegalArgumentException("SKU e obrigatorio");
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
        payload.put("StorageId", parseIntSafe(storageId));
        payload.put("Quantity", safeQty.intValue());
        payload.put("MinimumQuantity", 1);
        payload.put("HandlingTime", 0);
        payload.put("IsExternalStockEnabled", false);
        payload.put("ExternalStockHandlingTime", 0);
        return payload;
    }

    private static int parseIntSafe(String value) {
        if (value == null || value.trim().isEmpty()) return 0;
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /**
     * Consulta estoque atual de um SKU.
     *
     * @param sku codigo do produto
     * @return dados de estoque ou null se nao encontrado
     */
    public StockDTO getStock(String sku) throws Exception {
        return getStock(sku, config.getStorageId());
    }

    public StockDTO getStock(String sku, String storageId) throws Exception {
        String endpoint = String.format(FastchannelConstants.ENDPOINT_STOCK, sku);
        if (storageId != null && !storageId.trim().isEmpty()) {
            endpoint += "?StorageId=" + storageId.trim();
        }

        FastchannelHttpClient.HttpResult result = httpClient.getStock(endpoint);

        if (result.getStatusCode() == 404) {
            log.info("SKU " + sku + " nao encontrado no Fastchannel.");
            return null;
        }

        if (!result.isSuccess()) {
            log.warning("Erro ao consultar estoque: HTTP " + result.getStatusCode());
            throw new Exception(buildHttpError("GET", endpoint, sku, result));
        }

        return parseStockFromResponse(result.getBody());
    }

    /**
     * Extrai StockDTO do response da API FC.
     * A API retorna wrapper: {"Success":true, "Payload":[{...dados do estoque...}]}
     * Payload e um array - pegamos o primeiro elemento.
     */
    private StockDTO parseStockFromResponse(String body) {
        if (body == null || body.trim().isEmpty()) return null;
        String trimmed = body.trim();
        if (!trimmed.startsWith("{")) return null;

        try {
            // Usar JsonParser.parse() ao inves de gson.fromJson(String, JsonObject.class)
            // porque o WildFly classloader causa gson.fromJson retornar objeto vazio
            com.google.gson.JsonElement rootElement = new com.google.gson.JsonParser().parse(trimmed);
            if (!rootElement.isJsonObject()) return null;
            com.google.gson.JsonObject root = rootElement.getAsJsonObject();
            if (root.has("Payload") && !root.get("Payload").isJsonNull()) {
                com.google.gson.JsonElement payload = root.get("Payload");
                if (payload.isJsonArray()) {
                    com.google.gson.JsonArray arr = payload.getAsJsonArray();
                    if (arr.size() > 0) {
                        return gson.fromJson(arr.get(0), StockDTO.class);
                    }
                    return null;
                } else if (payload.isJsonObject()) {
                    return gson.fromJson(payload, StockDTO.class);
                }
            }
            // Fallback: tenta parsear direto
            if (root.has("ProductId") || root.has("Quantity")) {
                return gson.fromJson(trimmed, StockDTO.class);
            }
            return null;
        } catch (Exception e) {
            log.warning("Erro ao parsear resposta de estoque: " + e.getMessage());
            return null;
        }
    }

    /**
     * Zera o estoque de um SKU (usado quando produto e inativado).
     *
     * @param sku codigo do produto
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
