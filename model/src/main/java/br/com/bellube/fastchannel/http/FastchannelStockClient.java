package br.com.bellube.fastchannel.http;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.StockDTO;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.math.BigDecimal;
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
        String storageId = config.getStorageId();
        if (storageId == null || storageId.isEmpty()) {
            throw new Exception("Storage ID não configurado para atualização de estoque.");
        }

        String endpoint = String.format(FastchannelConstants.ENDPOINT_STOCK, sku);

        StockDTO stockDto = new StockDTO();
        stockDto.setSku(sku);
        stockDto.setStorageId(storageId);
        stockDto.setQuantity(quantity);

        String json = gson.toJson(stockDto);
        log.info("Atualizando estoque do SKU " + sku + ": " + quantity);

        FastchannelHttpClient.HttpResult result = httpClient.putStock(endpoint, json);

        if (!result.isSuccess()) {
            log.warning("Erro ao atualizar estoque: HTTP " + result.getStatusCode() + " - " + result.getBody());
            throw new Exception("Erro ao atualizar estoque: " + result.getErrorMessage());
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
            throw new Exception("Erro ao atualizar estoque: " + result.getErrorMessage());
        }

        log.info("Estoque do SKU " + stockDto.getSku() + " atualizado com sucesso.");
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
            throw new Exception("Erro ao consultar estoque: " + result.getErrorMessage());
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
}
