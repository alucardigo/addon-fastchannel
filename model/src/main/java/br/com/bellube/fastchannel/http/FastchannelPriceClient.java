package br.com.bellube.fastchannel.http;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.PriceBatchDTO;
import br.com.bellube.fastchannel.dto.PriceBatchItemDTO;
import br.com.bellube.fastchannel.dto.PriceDTO;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Cliente especializado para Price Management API do Fastchannel.
 *
 * Operacoes:
 * - Atualizar preco de produto individual
 * - Atualizar precos em lote (batch)
 * - Consultar preco atual
 */
public class FastchannelPriceClient {
    public enum Channel {
        DISTRIBUTION,
        CONSUMPTION
    }

    private static final Logger log = Logger.getLogger(FastchannelPriceClient.class.getName());
    private static final Gson gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd'T'HH:mm:ss")
            .create();

    private final FastchannelHttpClient httpClient;
    private final FastchannelConfig config;
    private final Channel channel;

    public FastchannelPriceClient() {
        this(new FastchannelHttpClient(), Channel.CONSUMPTION);
    }

    public FastchannelPriceClient(Channel channel) {
        this(new FastchannelHttpClient(), channel);
    }

    public FastchannelPriceClient(FastchannelHttpClient httpClient) {
        this(httpClient, Channel.CONSUMPTION);
    }

    public FastchannelPriceClient(FastchannelHttpClient httpClient, Channel channel) {
        this.httpClient = httpClient;
        this.config = FastchannelConfig.getInstance();
        this.channel = channel != null ? channel : Channel.CONSUMPTION;
    }

    /**
     * Atualiza preco de um SKU especifico.
     *
     * @param sku codigo do produto
     * @param price preco de venda
     * @param listPrice preco de lista (opcional)
     */
    public void updatePrice(String sku, BigDecimal price, BigDecimal listPrice) throws Exception {
        updatePrice(sku, price, listPrice, null);
    }

    /**
     * Atualiza preco de um SKU especifico (com tabela de preco).
     *
     * @param sku codigo do produto
     * @param price preco de venda (centavos)
     * @param listPrice preco de lista (centavos)
     * @param priceTableId ID da tabela de preco (opcional)
     */
    public void updatePrice(String sku, BigDecimal price, BigDecimal listPrice, BigDecimal priceTableId) throws Exception {
        String resellerId = config.getResellerId();

        String endpoint = String.format(FastchannelConstants.ENDPOINT_PRICE, sku);

        BigDecimal effectiveListPrice = listPrice != null ? listPrice : price;
        // FC rejeita SalePrice > ListPrice; garantir que ListPrice >= SalePrice
        if (price != null && effectiveListPrice != null && price.compareTo(effectiveListPrice) > 0) {
            log.info("SalePrice (" + price + ") > ListPrice (" + effectiveListPrice + ") para SKU " + sku
                    + ". Ajustando ListPrice = SalePrice para evitar rejeicao FC.");
            effectiveListPrice = price;
        }

        Map<String, Object> payload = new java.util.LinkedHashMap<>();
        if (resellerId != null && !resellerId.isEmpty()) {
            payload.put("ResellerId", resellerId);
        }
        if (priceTableId != null) {
            payload.put("PriceTableId", priceTableId.intValue());
        }
        payload.put("SalePrice", price != null ? price.intValue() : 0);
        payload.put("ListPrice", effectiveListPrice != null ? effectiveListPrice.intValue() : 0);

        String json = gson.toJson(payload);
        log.info("Atualizando preco do SKU " + sku + ": " + price);

        FastchannelHttpClient.HttpResult result = httpClient.putPrice(endpoint, json, getSubscriptionKeyForChannel());

        if (!result.isSuccess()) {
            log.warning("Erro ao atualizar preco: HTTP " + result.getStatusCode() + " - " + result.getBody());
            throw new Exception(buildHttpError("PUT", endpoint, sku, result));
        }

        log.info("Preco do SKU " + sku + " atualizado com sucesso.");
    }

    /**
     * Atualiza preco com dados completos.
     *
     * @param priceDto dados completos de preco
     */
    public void updatePrice(PriceDTO priceDto) throws Exception {
        if (priceDto.getSku() == null || priceDto.getSku().isEmpty()) {
            throw new IllegalArgumentException("SKU e obrigatorio");
        }

        String resellerId = priceDto.getResellerId();
        if (resellerId == null || resellerId.isEmpty()) {
            resellerId = config.getResellerId();
        }

        String endpoint = String.format(FastchannelConstants.ENDPOINT_PRICE, priceDto.getSku());

        Map<String, Object> payload = new java.util.LinkedHashMap<>();
        if (resellerId != null && !resellerId.isEmpty()) {
            payload.put("ResellerId", resellerId);
        }
        if (priceDto.getPriceTableId() != null) {
            payload.put("PriceTableId", priceDto.getPriceTableId().intValue());
        }
        BigDecimal salePrice = priceDto.getPrice();
        BigDecimal listPrice = priceDto.getListPrice();
        // FC rejeita SalePrice > ListPrice; garantir que ListPrice >= SalePrice
        if (salePrice != null && listPrice != null && salePrice.compareTo(listPrice) > 0) {
            log.info("SalePrice (" + salePrice + ") > ListPrice (" + listPrice + ") para SKU " + priceDto.getSku()
                    + ". Ajustando ListPrice = SalePrice para evitar rejeicao FC.");
            listPrice = salePrice;
        }
        if (salePrice != null) {
            payload.put("SalePrice", salePrice.intValue());
        }
        if (listPrice != null) {
            payload.put("ListPrice", listPrice.intValue());
        }
        if (priceDto.getPromotionalPrice() != null) {
            payload.put("PromotionalPrice", priceDto.getPromotionalPrice().intValue());
        }
        if (priceDto.getPromotionStartDate() != null) {
            payload.put("PromotionStartDate", priceDto.getPromotionStartDate());
        }
        if (priceDto.getPromotionEndDate() != null) {
            payload.put("PromotionEndDate", priceDto.getPromotionEndDate());
        }
        if (priceDto.getCurrency() != null) {
            payload.put("Currency", priceDto.getCurrency());
        }

        String json = gson.toJson(payload);

        log.info("Atualizando preco do SKU " + priceDto.getSku() + " payload=" + json);

        FastchannelHttpClient.HttpResult result = httpClient.putPrice(endpoint, json, getSubscriptionKeyForChannel());

        if (!result.isSuccess()) {
            log.warning("Erro ao atualizar preco: HTTP " + result.getStatusCode() + " - " + result.getBody());
            throw new Exception(buildHttpError("PUT", endpoint, priceDto.getSku(), result));
        }

        log.info("Preco do SKU " + priceDto.getSku() + " atualizado com sucesso.");
    }

    /**
     * Atualiza precos em lote (batch).
     * Mais eficiente para grandes volumes.
     *
     * @param resellerId ID do revendedor
     * @param prices lista de precos a atualizar
     */
    public void updatePricesBatch(String resellerId, List<PriceDTO> prices) throws Exception {
        if (prices == null || prices.isEmpty()) {
            log.info("Nenhum preco para atualizar em batch.");
            return;
        }

        if (resellerId == null || resellerId.isEmpty()) {
            resellerId = config.getResellerId();
        }

        String endpoint = String.format(FastchannelConstants.ENDPOINT_PRICE_BATCHES, resellerId);

        PriceBatchDTO batch = new PriceBatchDTO();
        batch.setResellerId(resellerId);
        batch.setPrices(prices);

        String json = gson.toJson(batch);
        log.info("Atualizando " + prices.size() + " precos em batch para reseller " + resellerId);

        FastchannelHttpClient.HttpResult result = httpClient.postPrice(endpoint, json, getSubscriptionKeyForChannel());

        if (!result.isSuccess()) {
            log.warning("Erro ao atualizar precos em batch: HTTP " + result.getStatusCode() + " - " + result.getBody());
            throw new Exception(buildHttpError("POST", endpoint, null, result));
        }

        log.info("Batch de " + prices.size() + " precos atualizado com sucesso.");
    }

    /**
     * Atualiza precos escalonados (batches) de um SKU.
     *
     * @param sku codigo do produto
     * @param priceTableId ID da tabela de preco (opcional)
     * @param batches lista de faixas
     */
    public void updatePriceBatches(String sku, BigDecimal priceTableId, List<PriceBatchItemDTO> batches) throws Exception {
        if (batches == null) {
            return;
        }
        String endpoint = String.format(FastchannelConstants.ENDPOINT_PRICE_BATCHES, sku);
        List<PriceBatchItemDTO> desiredBatches = normalizeDesiredBatches(priceTableId, batches);
        List<PriceBatchItemDTO> currentBatches = listPriceBatches(sku);

        for (PriceBatchItemDTO currentBatch : currentBatches) {
            if (currentBatch == null || currentBatch.getBatchId() == null || currentBatch.getBatchId().trim().isEmpty()) {
                continue;
            }
            if (!containsEquivalentBatch(desiredBatches, currentBatch)) {
                deletePriceBatch(sku, currentBatch.getBatchId());
            }
        }

        for (PriceBatchItemDTO desiredBatch : desiredBatches) {
            if (containsEquivalentBatch(currentBatches, desiredBatch)) {
                continue;
            }

            String json = gson.toJson(desiredBatch);
            FastchannelHttpClient.HttpResult result = httpClient.postPrice(endpoint, json, getSubscriptionKeyForChannel());
            if (!result.isSuccess()) {
                log.warning("Erro ao atualizar batch de preco: HTTP " + result.getStatusCode() + " - " + result.getBody());
                throw new Exception(buildHttpError("POST", endpoint, sku, result));
            }
        }
    }

    List<PriceBatchItemDTO> listPriceBatches(String sku) throws Exception {
        String endpoint = String.format(FastchannelConstants.ENDPOINT_PRICE_BATCHES, sku);
        FastchannelHttpClient.HttpResult result = httpClient.getPrice(endpoint, getSubscriptionKeyForChannel());

        if (result.getStatusCode() == 404) {
            return new ArrayList<>();
        }
        if (!result.isSuccess()) {
            throw new Exception(buildHttpError("GET", endpoint, sku, result));
        }

        String body = result.getBody();
        if (body == null || body.trim().isEmpty()) {
            return new ArrayList<>();
        }

        if (body.trim().startsWith("[")) {
            PriceBatchItemDTO[] arr = gson.fromJson(body, PriceBatchItemDTO[].class);
            List<PriceBatchItemDTO> batches = new ArrayList<>();
            if (arr != null) {
                for (PriceBatchItemDTO item : arr) {
                    if (item != null) {
                        batches.add(item);
                    }
                }
            }
            return batches;
        }

        BatchListResponse response = gson.fromJson(body, BatchListResponse.class);
        if (response == null || response.getProductPriceBatch() == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(response.getProductPriceBatch());
    }

    void deletePriceBatch(String sku, String batchId) throws Exception {
        String endpoint = String.format(FastchannelConstants.ENDPOINT_PRICE_BATCHES, sku) + "/" + batchId;
        FastchannelHttpClient.HttpResult result = httpClient.deletePrice(endpoint, getSubscriptionKeyForChannel());
        if (result.getStatusCode() == 404) {
            return;
        }
        if (!result.isSuccess()) {
            throw new Exception(buildHttpError("DELETE", endpoint, sku, result));
        }
    }

    private List<PriceBatchItemDTO> normalizeDesiredBatches(BigDecimal priceTableId, List<PriceBatchItemDTO> batches) {
        List<PriceBatchItemDTO> normalized = new ArrayList<>();
        for (PriceBatchItemDTO batch : batches) {
            if (batch == null) {
                continue;
            }
            if (batch.getPriceTableId() == null && priceTableId != null) {
                batch.setPriceTableId(priceTableId);
            }
            normalized.add(batch);
        }
        return normalized;
    }

    private boolean containsEquivalentBatch(List<PriceBatchItemDTO> candidates, PriceBatchItemDTO reference) {
        if (candidates == null || candidates.isEmpty() || reference == null) {
            return false;
        }
        for (PriceBatchItemDTO candidate : candidates) {
            if (isEquivalentBatch(candidate, reference)) {
                return true;
            }
        }
        return false;
    }

    private boolean isEquivalentBatch(PriceBatchItemDTO left, PriceBatchItemDTO right) {
        if (left == null || right == null) {
            return false;
        }
        return equalsDecimal(left.getPriceTableId(), right.getPriceTableId())
                && equalsDecimal(left.getMinimumBatchSize(), right.getMinimumBatchSize())
                && equalsDecimal(left.getMaximumBatchSize(), right.getMaximumBatchSize())
                && equalsDecimal(left.getUnitaryPriceForBatch(), right.getUnitaryPriceForBatch())
                && equalsBoolean(left.getBatchDisabled(), right.getBatchDisabled());
    }

    private boolean equalsDecimal(BigDecimal left, BigDecimal right) {
        if (left == null) {
            return right == null;
        }
        return right != null && left.compareTo(right) == 0;
    }

    private boolean equalsBoolean(Boolean left, Boolean right) {
        if (left == null) {
            return right == null;
        }
        return left.equals(right);
    }

    /**
     * Lista tabelas de preco disponiveis na API Fastchannel.
     * Util para discovery de PriceTableIds validos.
     *
     * @return lista de tabelas de preco (cada item com id e nome)
     */
    public List<Map<String, Object>> listPriceTables() throws Exception {
        // Tenta endpoint /prices/tables (discovery)
        String endpoint = "/tables";
        FastchannelHttpClient.HttpResult result = httpClient.getPrice(endpoint, getSubscriptionKeyForChannel());

        List<Map<String, Object>> tables = new ArrayList<>();

        if (result.isSuccess() && result.getBody() != null && !result.getBody().trim().isEmpty()) {
            String body = result.getBody().trim();
            // Parse array ou objeto
            if (body.startsWith("[")) {
                Object[] arr = gson.fromJson(body, Object[].class);
                if (arr != null) {
                    for (Object item : arr) {
                        if (item instanceof Map) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> map = (Map<String, Object>) item;
                            tables.add(map);
                        }
                    }
                }
            } else if (body.startsWith("{")) {
                @SuppressWarnings("unchecked")
                Map<String, Object> wrapper = gson.fromJson(body, Map.class);
                if (wrapper != null) {
                    // Tenta pegar lista de dentro do wrapper
                    for (Object value : wrapper.values()) {
                        if (value instanceof List) {
                            @SuppressWarnings("unchecked")
                            List<Object> list = (List<Object>) value;
                            for (Object item : list) {
                                if (item instanceof Map) {
                                    @SuppressWarnings("unchecked")
                                    Map<String, Object> map = (Map<String, Object>) item;
                                    tables.add(map);
                                }
                            }
                            break;
                        }
                    }
                    if (tables.isEmpty()) {
                        tables.add(wrapper);
                    }
                }
            }
        } else {
            log.warning("Endpoint /prices/tables retornou HTTP " + result.getStatusCode()
                    + ". Corpo: " + (result.getBody() != null ? result.getBody() : "<vazio>"));
        }

        return tables;
    }

    /**
     * Consulta preco atual de um SKU.
     *
     * @param sku codigo do produto
     * @return dados de preco ou null se nao encontrado
     */
    public PriceDTO getPrice(String sku) throws Exception {
        return getPrice(sku, null);
    }

    public PriceDTO getPrice(String sku, BigDecimal preferredPriceTableId) throws Exception {
        String endpoint = String.format(FastchannelConstants.ENDPOINT_PRICE, sku);

        FastchannelHttpClient.HttpResult result = httpClient.getPrice(endpoint, getSubscriptionKeyForChannel());

        if (result.getStatusCode() == 404) {
            log.fine("SKU " + sku + " nao tem preco cadastrado no Fastchannel.");
            return null;
        }

        if (!result.isSuccess()) {
            log.warning("Erro ao consultar preco: HTTP " + result.getStatusCode());
            throw new Exception(buildHttpError("GET", endpoint, sku, result));
        }

        return parsePriceFromResponse(result.getBody(), preferredPriceTableId);
    }

    /**
     * Extrai PriceDTO do response da API FC.
     * A API retorna um wrapper: {"Success":true, "Payload":[{...}, ...]} (array)
     * ou em alguns endpoints: {"Success":true, "Payload":{...}} (objeto unico).
     * Precisamos extrair o Payload antes de parsear como PriceDTO.
     */
    /**
     * Extrai PriceDTO do response da API FC.
     * A API retorna Payload como array quando multiplas tabelas de preco existem.
     * Se preferredTableId for informado, tenta encontrar o preco da tabela correspondente.
     */
    private PriceDTO parsePriceFromResponse(String body) {
        return parsePriceFromResponse(body, null);
    }

    private PriceDTO parsePriceFromResponse(String body, BigDecimal preferredTableId) {
        if (body == null || body.trim().isEmpty()) return null;
        String trimmed = body.trim();
        if (!trimmed.startsWith("{")) return null;

        try {
            com.google.gson.JsonElement rootElement = new com.google.gson.JsonParser().parse(trimmed);
            if (!rootElement.isJsonObject()) return null;
            com.google.gson.JsonObject root = rootElement.getAsJsonObject();

            if (root.has("Payload") && !root.get("Payload").isJsonNull()) {
                com.google.gson.JsonElement payload = root.get("Payload");
                if (payload.isJsonArray()) {
                    com.google.gson.JsonArray arr = payload.getAsJsonArray();
                    if (arr.size() == 0) return null;
                    // Se preferredTableId informado, buscar o item correspondente
                    if (preferredTableId != null) {
                        for (int i = 0; i < arr.size(); i++) {
                            PriceDTO candidate = gson.fromJson(arr.get(i), PriceDTO.class);
                            if (candidate != null && candidate.getPriceTableId() != null
                                    && candidate.getPriceTableId().intValue() == preferredTableId.intValue()) {
                                log.fine("parsePriceFromResponse: matched priceTableId=" + preferredTableId);
                                return candidate;
                            }
                        }
                    }
                    // Fallback: retorna primeiro item
                    return gson.fromJson(arr.get(0), PriceDTO.class);
                }
                if (payload.isJsonObject()) {
                    return gson.fromJson(payload, PriceDTO.class);
                }
            }
            // Fallback: tenta parsear direto como PriceDTO
            PriceDTO direct = gson.fromJson(trimmed, PriceDTO.class);
            if (direct != null && direct.getSku() != null && !direct.getSku().isEmpty()) {
                return direct;
            }
            if (root.has("ProductId")) {
                return gson.fromJson(trimmed, PriceDTO.class);
            }
            return null;
        } catch (Exception e) {
            log.log(Level.WARNING, "parsePriceFromResponse error: " + e.getMessage(), e);
            return null;
        }
    }

    private String buildHttpError(String method, String endpoint, String sku, FastchannelHttpClient.HttpResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append("Erro ").append(method).append(" preco Fastchannel");
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

    public Channel getChannel() {
        return channel;
    }

    private String getSubscriptionKeyForChannel() {
        if (channel == Channel.DISTRIBUTION) {
            return config.getSubscriptionKeyDistribution();
        }
        return config.getSubscriptionKeyConsumption();
    }

    private static final class BatchListResponse {
        @SerializedName("ProductPriceBatch")
        private List<PriceBatchItemDTO> productPriceBatch;

        private List<PriceBatchItemDTO> getProductPriceBatch() {
            return productPriceBatch;
        }
    }
}
