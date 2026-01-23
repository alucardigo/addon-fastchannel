package br.com.bellube.fastchannel.http;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.PriceDTO;
import br.com.bellube.fastchannel.dto.PriceBatchDTO;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.math.BigDecimal;
import java.util.List;
import java.util.logging.Logger;

/**
 * Cliente especializado para Price Management API do Fastchannel.
 *
 * Operações:
 * - Atualizar preço de produto individual
 * - Atualizar preços em lote (batch)
 * - Consultar preço atual
 */
public class FastchannelPriceClient {

    private static final Logger log = Logger.getLogger(FastchannelPriceClient.class.getName());
    private static final Gson gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd'T'HH:mm:ss")
            .create();

    private final FastchannelHttpClient httpClient;
    private final FastchannelConfig config;

    public FastchannelPriceClient() {
        this.httpClient = new FastchannelHttpClient();
        this.config = FastchannelConfig.getInstance();
    }

    public FastchannelPriceClient(FastchannelHttpClient httpClient) {
        this.httpClient = httpClient;
        this.config = FastchannelConfig.getInstance();
    }

    /**
     * Atualiza preço de um SKU específico.
     *
     * @param sku código do produto
     * @param price preço de venda
     * @param listPrice preço de lista (opcional)
     */
    public void updatePrice(String sku, BigDecimal price, BigDecimal listPrice) throws Exception {
        String resellerId = config.getResellerId();
        if (resellerId == null || resellerId.isEmpty()) {
            throw new Exception("Reseller ID não configurado para atualização de preço.");
        }

        String endpoint = String.format(FastchannelConstants.ENDPOINT_PRICE, sku);

        PriceDTO priceDto = new PriceDTO();
        priceDto.setSku(sku);
        priceDto.setResellerId(resellerId);
        priceDto.setPrice(price);
        priceDto.setListPrice(listPrice != null ? listPrice : price);

        String json = gson.toJson(priceDto);
        log.info("Atualizando preço do SKU " + sku + ": " + price);

        FastchannelHttpClient.HttpResult result = httpClient.putPrice(endpoint, json);

        if (!result.isSuccess()) {
            log.warning("Erro ao atualizar preço: HTTP " + result.getStatusCode() + " - " + result.getBody());
            throw new Exception("Erro ao atualizar preço: " + result.getErrorMessage());
        }

        log.info("Preço do SKU " + sku + " atualizado com sucesso.");
    }

    /**
     * Atualiza preço com dados completos.
     *
     * @param priceDto dados completos de preço
     */
    public void updatePrice(PriceDTO priceDto) throws Exception {
        if (priceDto.getSku() == null || priceDto.getSku().isEmpty()) {
            throw new IllegalArgumentException("SKU é obrigatório");
        }

        String resellerId = priceDto.getResellerId();
        if (resellerId == null || resellerId.isEmpty()) {
            resellerId = config.getResellerId();
            priceDto.setResellerId(resellerId);
        }

        String endpoint = String.format(FastchannelConstants.ENDPOINT_PRICE, priceDto.getSku());
        String json = gson.toJson(priceDto);

        log.info("Atualizando preço completo do SKU " + priceDto.getSku());

        FastchannelHttpClient.HttpResult result = httpClient.putPrice(endpoint, json);

        if (!result.isSuccess()) {
            log.warning("Erro ao atualizar preço: HTTP " + result.getStatusCode() + " - " + result.getBody());
            throw new Exception("Erro ao atualizar preço: " + result.getErrorMessage());
        }

        log.info("Preço do SKU " + priceDto.getSku() + " atualizado com sucesso.");
    }

    /**
     * Atualiza preços em lote (batch).
     * Mais eficiente para grandes volumes.
     *
     * @param resellerId ID do revendedor
     * @param prices lista de preços a atualizar
     */
    public void updatePricesBatch(String resellerId, List<PriceDTO> prices) throws Exception {
        if (prices == null || prices.isEmpty()) {
            log.info("Nenhum preço para atualizar em batch.");
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
        log.info("Atualizando " + prices.size() + " preços em batch para reseller " + resellerId);

        FastchannelHttpClient.HttpResult result = httpClient.postPrice(endpoint, json);

        if (!result.isSuccess()) {
            log.warning("Erro ao atualizar preços em batch: HTTP " + result.getStatusCode() + " - " + result.getBody());
            throw new Exception("Erro ao atualizar preços em batch: " + result.getErrorMessage());
        }

        log.info("Batch de " + prices.size() + " preços atualizado com sucesso.");
    }

    /**
     * Consulta preço atual de um SKU.
     *
     * @param sku código do produto
     * @return dados de preço ou null se não encontrado
     */
    public PriceDTO getPrice(String sku) throws Exception {
        String endpoint = String.format(FastchannelConstants.ENDPOINT_PRICE, sku);

        FastchannelHttpClient.HttpResult result = httpClient.getPrice(endpoint);

        if (result.getStatusCode() == 404) {
            log.info("SKU " + sku + " não tem preço cadastrado no Fastchannel.");
            return null;
        }

        if (!result.isSuccess()) {
            log.warning("Erro ao consultar preço: HTTP " + result.getStatusCode());
            throw new Exception("Erro ao consultar preço: " + result.getErrorMessage());
        }

        return gson.fromJson(result.getBody(), PriceDTO.class);
    }
}
