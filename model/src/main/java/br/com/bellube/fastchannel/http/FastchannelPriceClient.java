package br.com.bellube.fastchannel.http;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.PriceBatchDTO;
import br.com.bellube.fastchannel.dto.PriceBatchItemDTO;
import br.com.bellube.fastchannel.dto.PriceDTO;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

    /**
     * [NO-BATCH-CACHE] Cache de pares SKU+priceTableId sem batches confirmados na API FC.
     *
     * PROBLEMA: listPriceBatches() faz GET para cada produto × cada tabela em cada sync.
     * Para 1500 produtos × 8 tabelas = 12.000 chamadas GET. Se 1% delas timeout em 30s+
     * com 3 retries = 120+ horas. Na pratica: sync nunca termina.
     *
     * SOLUCAO: se GET retornou vazio (0 batches) → guardar no cache.
     * Na proxima sync, se desiredBatches tbm esta vazio → pular GET (ja sabemos que nao tem).
     * Se desiredBatches nao-vazio → sempre GET (precisamos verificar o estado atual).
     *
     * Chave: "sku|priceTableId"
     * Ciclo de vida: reset no restart do addon (aceito — primeira sync reconstroi o cache).
     *
     * INCIDENTE 2026-04-16: sync de 1500 produtos via syncAll causava HTTP 504 no proxy
     * porque as chamadas GET /prices/{sku}/batches somavam horas de execucao.
     */
    private static final java.util.Set<String> NO_BATCH_SKU_TABLE_CACHE =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());

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

        // [NO-BATCH-CACHE] Pular GET quando desiredBatches e vazio E cache confirma que nao ha batches na FC.
        // Evita 12.000+ chamadas GET em um full-sync (1500 produtos x 8 tabelas), a maioria desnecessarias.
        String cacheKey = sku + "|" + (priceTableId != null ? priceTableId.toPlainString() : "null");
        if (desiredBatches.isEmpty() && NO_BATCH_SKU_TABLE_CACHE.contains(cacheKey)) {
            log.fine("[NO-BATCH-CACHE] Pulando GET batches SKU=" + sku + " tabela=" + priceTableId
                    + " (cache: sem batches confirmado)");
            return;
        }

        List<PriceBatchItemDTO> currentBatches = listPriceBatches(sku);
        currentBatches = filterBatchesByPriceTable(currentBatches, priceTableId);

        // Atualizar cache baseado no estado atual da FC
        if (desiredBatches.isEmpty() && currentBatches.isEmpty()) {
            NO_BATCH_SKU_TABLE_CACHE.add(cacheKey);
            log.fine("[NO-BATCH-CACHE] Adicionado ao cache sem-batch: " + cacheKey);
            return; // nada a fazer
        } else if (!currentBatches.isEmpty() || !desiredBatches.isEmpty()) {
            NO_BATCH_SKU_TABLE_CACHE.remove(cacheKey); // tem (ou vai ter) batches — remover do cache
        }

        // [DEDUP-CLEANUP 2026-05-25] Reconciliacao completa entre o que a FC TEM (currentBatches —
        // agora visiveis gracas ao fix do parser de Payload) e o que DEVE ter (desiredBatches):
        //   - manter UMA unica ocorrencia de cada faixa desejada;
        //   - deletar lixo (Min/Max=0, Disabled, preco<=0);
        //   - deletar faixas obsoletas (promocao expirada/alterada => nao constam em desiredBatches);
        //   - deletar DUPLICATAS (mesma faixa repetida) — residuo de syncs concorrentes/repetidos que,
        //     com o parser quebrado, nunca eram detectados. Incidente 2026-05-25: SKU 31251453 teve a
        //     mesma faixa 1-3 enviada 6x porque a FC NAO deduplica POST (testado: 2 POST = 2 batches).
        // 'kept' acumula as faixas mantidas para que o POST abaixo nao recrie o que ja existe.
        List<PriceBatchItemDTO> kept = new ArrayList<>();
        for (PriceBatchItemDTO currentBatch : currentBatches) {
            if (currentBatch == null || currentBatch.getBatchId() == null || currentBatch.getBatchId().trim().isEmpty()) {
                continue;
            }
            boolean isGarbage = isGarbageBatch(currentBatch);
            boolean matchesDesired = !isGarbage && containsEquivalentBatch(desiredBatches, currentBatch);
            boolean duplicate = matchesDesired && containsEquivalentBatch(kept, currentBatch);
            if (matchesDesired && !duplicate) {
                kept.add(currentBatch); // primeira ocorrencia da faixa desejada: manter
                continue;
            }
            if (isGarbage) {
                log.info("[CLEANUP] Removendo batch lixo SKU=" + sku + " batchId=" + currentBatch.getBatchId()
                        + " (Min=" + currentBatch.getMinimumBatchSize() + " Max=" + currentBatch.getMaximumBatchSize()
                        + " Disabled=" + currentBatch.getBatchDisabled() + ")");
            } else if (duplicate) {
                log.info("[CLEANUP] Removendo batch DUPLICADO SKU=" + sku + " batchId=" + currentBatch.getBatchId()
                        + " (Min=" + currentBatch.getMinimumBatchSize() + " Max=" + currentBatch.getMaximumBatchSize()
                        + " price=" + currentBatch.getUnitaryPriceForBatch() + " tableId=" + currentBatch.getPriceTableId() + ")");
            } else {
                log.info("[CLEANUP] Removendo batch obsoleto SKU=" + sku + " batchId=" + currentBatch.getBatchId()
                        + " (Min=" + currentBatch.getMinimumBatchSize() + " Max=" + currentBatch.getMaximumBatchSize()
                        + " tableId=" + currentBatch.getPriceTableId() + ") — nao consta nas faixas vigentes.");
            }
            deletePriceBatch(sku, currentBatch.getBatchId());
        }

        for (PriceBatchItemDTO desiredBatch : desiredBatches) {
            // [VALIDATE 2026-04-27] Nao enviar batches invalidos (Min<=0, Max<=0 ou Min>Max)
            // Evita poluir o FC com batches "lixo" que ficam pra sempre.
            if (!isValidBatchPayload(desiredBatch)) {
                log.warning("[VALIDATE] Pulando batch invalido SKU=" + sku
                        + " Min=" + desiredBatch.getMinimumBatchSize()
                        + " Max=" + desiredBatch.getMaximumBatchSize()
                        + " Price=" + desiredBatch.getUnitaryPriceForBatch());
                continue;
            }
            // Ja existe na FC (mantido acima) OU ja foi enviado nesta chamada: nao duplicar.
            if (containsEquivalentBatch(kept, desiredBatch)) {
                continue;
            }

            String json = gson.toJson(desiredBatch);
            log.info("POST batch SKU=" + sku + " payload=" + json);
            FastchannelHttpClient.HttpResult result = httpClient.postPrice(endpoint, json, getSubscriptionKeyForChannel());
            if (!result.isSuccess()) {
                log.warning("Erro batch POST: HTTP " + result.getStatusCode()
                        + " SKU=" + sku + " payload=" + json + " response=" + result.getBody());
                continue;
            }
            kept.add(desiredBatch); // registra para evitar repost caso a mesma faixa apareca 2x na lista
            log.info("Batch OK: SKU=" + sku + " tableId=" + desiredBatch.getPriceTableId());
        }
    }

    /** Batch lixo = Min=0 OU Max=0 OU Disabled=true OU UnitaryPriceForBatch<=0 (sao todos invalidos comercialmente). */
    private boolean isGarbageBatch(PriceBatchItemDTO b) {
        if (b == null) return true;
        BigDecimal min = b.getMinimumBatchSize();
        BigDecimal max = b.getMaximumBatchSize();
        BigDecimal price = b.getUnitaryPriceForBatch();
        Boolean disabled = b.getBatchDisabled();
        if (Boolean.TRUE.equals(disabled)) return true;
        if (min == null || min.compareTo(BigDecimal.ZERO) <= 0) return true;
        if (max == null || max.compareTo(BigDecimal.ZERO) <= 0) return true;
        if (price == null || price.compareTo(BigDecimal.ZERO) <= 0) return true;
        return false;
    }

    /** Valida payload antes de POST: Min>=1, Max>=Min, UnitaryPriceForBatch>0, PriceTableId definido. */
    private boolean isValidBatchPayload(PriceBatchItemDTO b) {
        if (b == null) return false;
        BigDecimal min = b.getMinimumBatchSize();
        BigDecimal max = b.getMaximumBatchSize();
        BigDecimal price = b.getUnitaryPriceForBatch();
        if (min == null || min.compareTo(BigDecimal.ONE) < 0) return false;
        if (max == null || max.compareTo(min) < 0) return false;
        if (price == null || price.compareTo(BigDecimal.ZERO) <= 0) return false;
        if (b.getPriceTableId() == null) return false;
        return true;
    }

    /**
     * Limpeza explicita de batches "lixo" (Min=0/Max=0/Disabled=true) para um SKU
     * em todas as tabelas. Util como ferramenta admin pra limpar residuos sem precisar
     * fazer sync completo.
     *
     * @return numero de batches deletados
     */
    public int cleanupGarbageBatches(String sku) throws Exception {
        if (sku == null || sku.trim().isEmpty()) return 0;
        List<PriceBatchItemDTO> all = listPriceBatches(sku);
        int removed = 0;
        for (PriceBatchItemDTO b : all) {
            if (b == null || b.getBatchId() == null) continue;
            if (isGarbageBatch(b)) {
                try {
                    deletePriceBatch(sku, b.getBatchId());
                    removed++;
                } catch (Exception e) {
                    log.log(java.util.logging.Level.WARNING, "Falha ao deletar batch lixo SKU=" + sku
                            + " batchId=" + b.getBatchId(), e);
                }
            }
        }
        return removed;
    }

    private List<PriceBatchItemDTO> filterBatchesByPriceTable(List<PriceBatchItemDTO> batches, BigDecimal priceTableId) {
        if (batches == null || batches.isEmpty()) {
            return new ArrayList<>();
        }
        List<PriceBatchItemDTO> filtered = new ArrayList<>();
        for (PriceBatchItemDTO batch : batches) {
            if (batch == null) {
                continue;
            }
            if (matchesPriceTable(batch.getPriceTableId(), priceTableId)) {
                filtered.add(batch);
            }
        }
        return filtered;
    }

    private boolean matchesPriceTable(BigDecimal currentPriceTableId, BigDecimal targetPriceTableId) {
        if (targetPriceTableId == null) {
            return currentPriceTableId == null;
        }
        return currentPriceTableId != null && currentPriceTableId.compareTo(targetPriceTableId) == 0;
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

        // [FC-JSON-WRAPPER-FIX 2026-05-25] A API FC (ServiceStack) responde, com Accept: application/json,
        // no formato {"Success":true,"Payload":[ {batch...}, ... ],"TotalRecords":N}.
        // O parser antigo procurava o campo "ProductPriceBatch" (nome do ELEMENTO no XML), que NUNCA
        // existe no JSON => listPriceBatches retornava SEMPRE lista vazia. Consequencia: o dedup e a
        // limpeza em updatePriceBatches nunca enxergavam os batches ja existentes na FC, gerando POSTs
        // duplicados a cada sync (6x no incidente 2026-05-25) e jamais removendo faixas obsoletas.
        // Agora lemos o array "Payload" (mesmo padrao de parsePriceFromResponse/listPricesForTable).
        // Confirmado contra a API real em 2026-05-25 (GET /price-management/v1/prices/{sku}/batches).
        String trimmed = stripBom(body.trim());
        List<PriceBatchItemDTO> batches = new ArrayList<>();
        try {
            com.google.gson.JsonElement rootEl = new com.google.gson.JsonParser().parse(trimmed);
            com.google.gson.JsonArray arr = null;
            if (rootEl.isJsonArray()) {
                arr = rootEl.getAsJsonArray();
            } else if (rootEl.isJsonObject()) {
                com.google.gson.JsonObject root = rootEl.getAsJsonObject();
                if (root.has("Payload") && root.get("Payload").isJsonArray()) {
                    arr = root.getAsJsonArray("Payload");
                } else if (root.has("ProductPriceBatch") && root.get("ProductPriceBatch").isJsonArray()) {
                    arr = root.getAsJsonArray("ProductPriceBatch"); // fallback formato legado
                }
            }
            if (arr != null) {
                for (int i = 0; i < arr.size(); i++) {
                    if (arr.get(i) != null && arr.get(i).isJsonObject()) {
                        PriceBatchItemDTO item = gson.fromJson(arr.get(i), PriceBatchItemDTO.class);
                        if (item != null) {
                            batches.add(item);
                        }
                    }
                }
            } else {
                log.warning("listPriceBatches: resposta sem array de batches reconhecivel SKU=" + sku
                        + " body=" + truncate(trimmed, 300));
            }
        } catch (Exception e) {
            // NUNCA mascarar silenciosamente como lista vazia: logar WARNING para que uma eventual
            // regressao de formato (XML/erro) seja visivel e nao reabra o bug de duplicacao.
            log.log(Level.WARNING, "listPriceBatches: falha ao parsear resposta SKU=" + sku
                    + " body=" + truncate(trimmed, 300), e);
            return new ArrayList<>();
        }
        return batches;
    }

    /** Remove BOM (UTF-8 BOM) inicial que alguns gateways adicionam e que quebra o parser JSON. */
    private static String stripBom(String s) {
        if (s != null && !s.isEmpty() && s.charAt(0) == '\uFEFF') {
            return s.substring(1);
        }
        return s;
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
            // [FC-INT-QTY-FIX 2026-05-26] A API FC grava MinimumBatchSize/MaximumBatchSize como 0
            // quando recebe notacao decimal ("1.0","3.0") em vez de inteiro ("1","3"). Como a origem
            // (TGFDPQ.QTDE) e float no SQL, o gson serializava "1.0". Forcamos escala 0 aqui — chokepoint
            // unico de todo POST de batch — para garantir notacao inteira independente da origem
            // (resolver ou edicao manual na tela). Confirmado na API real 2026-05-26 (2.0->0, 2->2).
            if (batch.getMinimumBatchSize() != null) {
                batch.setMinimumBatchSize(batch.getMinimumBatchSize().setScale(0, RoundingMode.HALF_UP));
            }
            if (batch.getMaximumBatchSize() != null) {
                batch.setMaximumBatchSize(batch.getMaximumBatchSize().setScale(0, RoundingMode.HALF_UP));
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
        // Tratar null como false: FC pode nao retornar BatchDisabled explicitamente para batches ativos.
        // null == false == nao-desabilitado para evitar re-criacao desnecessaria de batches ativos.
        boolean leftVal = left != null && left;
        boolean rightVal = right != null && right;
        return leftVal == rightVal;
    }

    /**
     * Lista tabelas de preco disponiveis na API Fastchannel.
     *
     * <p>REFACTOR 2026-04-20: A API FastChannel NAO EXPOE endpoint de discovery
     * de tabelas (vide OpenAPI spec Pricing-v1.json: so existem /prices, /prices/{sku},
     * /prices/{sku}/batches). Endpoint /tables retorna 404 "Resource not found".
     *
     * <p>Nova estrategia: descobrir PriceTableIds via GET /prices?PageSize=5000 e
     * extrair valores unicos de PriceTableId + PriceTableName do Payload.
     * Se isso nao retornar nada (tabelas vazias), cai em fallback para IDs
     * configurados em AD_FCCONFIG.PRICE_TABLE_IDS.
     *
     * @return lista de tabelas de preco com {id, name}
     */
    public List<Map<String, Object>> listPriceTables() throws Exception {
        Map<String, Map<String, Object>> tablesById = new LinkedHashMap<>();

        // FASE 1: discovery de tabelas com preços cadastrados
        // PageSize alto cobre catalogo inteiro numa chamada. Retorna PriceTableId + PriceTableName
        // para cada item do Payload. Deduplica com tablesById. Cobre 100% das tabelas POPULADAS.
        try {
            String endpoint = "/prices?PageSize=5000&PageNumber=1";
            FastchannelHttpClient.HttpResult result = httpClient.getPrice(endpoint, getSubscriptionKeyForChannel());

            if (result.isSuccess() && result.getBody() != null && !result.getBody().trim().isEmpty()) {
                String body = result.getBody().trim();
                @SuppressWarnings("unchecked")
                Map<String, Object> wrapper = gson.fromJson(body, Map.class);
                if (wrapper != null && wrapper.get("Payload") instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> payload = (List<Map<String, Object>>) wrapper.get("Payload");
                    for (Map<String, Object> item : payload) {
                        Object ptid = item.get("PriceTableId");
                        Object ptname = item.get("PriceTableName");
                        if (ptid != null) {
                            String key = String.valueOf(ptid);
                            tablesById.computeIfAbsent(key, k -> {
                                Map<String, Object> t = new HashMap<>();
                                t.put("id", ptid);
                                t.put("name", ptname != null ? ptname : ("Tabela " + ptid));
                                return t;
                            });
                        }
                    }
                }
            } else {
                log.warning("GET /prices retornou HTTP " + result.getStatusCode()
                        + ". Corpo: " + (result.getBody() != null ? result.getBody() : "<vazio>"));
            }
        } catch (Exception e) {
            log.log(java.util.logging.Level.WARNING, "Erro ao descobrir tabelas via /prices", e);
        }

        // FASE 2: complementar com IDs explicitamente configurados em AD_FCCONFIG.PRICE_TABLE_IDS.
        // JUSTIFICATIVA: a API FC retorna 200 TotalRecords=0 tanto pra tabela VAZIA quanto
        // pra tabela INEXISTENTE (testado com IDs 28 e 9999 — ambos respondem igual), entao
        // brute-force scan dá falso positivo e adiciona fantasmas. A unica forma confiavel
        // de listar tabelas vazias e o cliente informar os IDs que quer exibir.
        // Se PRICE_TABLE_IDS nao estiver configurado, a tela mostra so as populadas (fase 1),
        // o que e o comportamento correto pra API FC que nao expõe endpoint de discovery.
        String configuredIds = config.getPriceTableIds();
        if (configuredIds != null && !configuredIds.trim().isEmpty()) {
            for (String idStr : configuredIds.split(",")) {
                idStr = idStr.trim();
                if (idStr.isEmpty() || tablesById.containsKey(idStr)) continue;
                try {
                    Integer id = Integer.parseInt(idStr);
                    Map<String, Object> t = new HashMap<>();
                    t.put("id", id);
                    t.put("name", "Tabela " + id); // tabela vazia, sem produtos pra extrair PriceTableName
                    tablesById.put(idStr, t);
                } catch (NumberFormatException nfe) {
                    log.warning("PRICE_TABLE_IDS contem valor nao numerico: " + idStr);
                }
            }
        }

        return new ArrayList<>(tablesById.values());
    }

    /**
     * Lista TODOS os precos de uma tabela especifica na Fastchannel.
     *
     * <p>Usa o endpoint {@code GET /prices?PriceTableId=X&PageNumber=1&PageSize=5000}
     * que retorna JSON com {@code Payload} array de {@code ProductPrice} contendo
     * {@code ProductId} (=SKU), {@code SalePrice}, {@code ListPrice}, {@code PriceTableId},
     * {@code PriceTableName}, {@code SellerName}.</p>
     *
     * <p>Usado pelo {@code FCPrecosService.mirrorCleanup} para comparacao server-side
     * completa FC vs Sankhya sem depender do frontend.</p>
     *
     * @param priceTableId ID numerico da tabela FC (ex.: 31)
     * @return lista de PriceDTO com SKU, price, listPrice, priceTableId
     */
    public List<PriceDTO> listPricesForTable(BigDecimal priceTableId) throws Exception {
        if (priceTableId == null) {
            return new ArrayList<>();
        }

        List<PriceDTO> allPrices = new ArrayList<>();
        int pageNumber = 1;
        int pageSize = 500; // FC max documented is 5000, usamos 500 para ser conservador
        int totalPages = 1;

        while (pageNumber <= totalPages) {
            String endpoint = "/prices?PriceTableId=" + priceTableId.intValue()
                + "&PageNumber=" + pageNumber + "&PageSize=" + pageSize;

            FastchannelHttpClient.HttpResult result = httpClient.getPrice(endpoint, getSubscriptionKeyForChannel());

            if (!result.isSuccess()) {
                if (result.getStatusCode() == 404) {
                    log.info("listPricesForTable: tabela " + priceTableId + " nao encontrada na FC.");
                    return allPrices;
                }
                throw new Exception("Erro ao listar precos FC tabela " + priceTableId
                    + ": HTTP " + result.getStatusCode() + " " + result.getBody());
            }

            String body = result.getBody();
            if (body == null || body.trim().isEmpty()) break;

            com.google.gson.JsonObject root = new com.google.gson.JsonParser()
                .parse(body.trim()).getAsJsonObject();

            if (root.has("TotalPages") && !root.get("TotalPages").isJsonNull()) {
                totalPages = root.get("TotalPages").getAsInt();
            }

            if (root.has("Payload") && root.get("Payload").isJsonArray()) {
                com.google.gson.JsonArray arr = root.getAsJsonArray("Payload");
                for (int i = 0; i < arr.size(); i++) {
                    com.google.gson.JsonObject item = arr.get(i).getAsJsonObject();
                    PriceDTO dto = new PriceDTO();
                    dto.setSku(item.has("ProductId") ? item.get("ProductId").getAsString() : null);
                    dto.setPriceTableId(priceTableId);
                    dto.setPrice(item.has("SalePrice") ? item.get("SalePrice").getAsBigDecimal() : null);
                    dto.setListPrice(item.has("ListPrice") ? item.get("ListPrice").getAsBigDecimal() : null);
                    if (dto.getSku() != null && !dto.getSku().trim().isEmpty()) {
                        allPrices.add(dto);
                    }
                }
            }

            pageNumber++;
        }

        log.info("listPricesForTable: tabela " + priceTableId + " retornou " + allPrices.size()
            + " precos em " + (pageNumber - 1) + " pagina(s).");
        return allPrices;
    }

    /**
     * [SYNC-OPT 2026-06-01] Retorna o conjunto de SKUs que EXISTEM no catalogo da FC,
     * agregando {@link #listPricesForTable} de todas as tabelas informadas.
     *
     * <p>Motivacao: o full sync tenta empurrar preco de ~2500 produtos (todos da marca
     * AD_FAST='S'), mas apenas ~630 existem na FC. Os ~1900 inexistentes geram HTTP 404
     * "O SKU do produto nao existe" — 75% das chamadas sao desperdicio. Como o addon NUNCA
     * cria produtos no catalogo FC (processProductItem so grava De-Para local), um produto
     * ausente da FC jamais sera criado por aqui — logo e seguro pula-lo no full/large sync.</p>
     *
     * <p>Falha de uma tabela nao aborta as demais (best-effort). Se NENHUMA tabela responder,
     * retorna conjunto vazio — o chamador deve tratar vazio como "fail-open" (nao pular nada)
     * para nunca zerar o sync por causa de um hiccup transitorio da API.</p>
     *
     * @param priceTableIds tabelas FC a consultar (ex.: De-Para TABELA_PRECO)
     * @return SKUs (ProductId) distintos que tem preco em pelo menos uma tabela
     */
    public java.util.Set<String> listExistingSkus(List<BigDecimal> priceTableIds) {
        java.util.Set<String> skus = new java.util.HashSet<>();
        if (priceTableIds == null || priceTableIds.isEmpty()) {
            return skus;
        }
        for (BigDecimal tableId : priceTableIds) {
            if (tableId == null) {
                continue;
            }
            try {
                List<PriceDTO> prices = listPricesForTable(tableId);
                for (PriceDTO p : prices) {
                    if (p != null && p.getSku() != null && !p.getSku().trim().isEmpty()) {
                        skus.add(p.getSku().trim());
                    }
                }
            } catch (Exception e) {
                log.warning("listExistingSkus: falha ao listar tabela " + tableId + ": " + e.getMessage());
            }
        }
        log.info("listExistingSkus: " + skus.size() + " SKU(s) existentes na FC em "
                + priceTableIds.size() + " tabela(s) consultada(s).");
        return skus;
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
}
