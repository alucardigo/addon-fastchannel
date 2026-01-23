package br.com.bellube.fastchannel.dto;

import java.util.ArrayList;
import java.util.List;

/**
 * DTO para operações de preço em lote (batch) com a API Fastchannel.
 *
 * Permite enviar múltiplos preços em uma única requisição,
 * otimizando a comunicação com a API.
 */
public class PriceBatchDTO {

    private String resellerId;
    private List<PriceDTO> prices;

    public PriceBatchDTO() {
        this.prices = new ArrayList<>();
    }

    public PriceBatchDTO(String resellerId, List<PriceDTO> prices) {
        this.resellerId = resellerId;
        this.prices = prices != null ? prices : new ArrayList<>();
    }

    public String getResellerId() {
        return resellerId;
    }

    public void setResellerId(String resellerId) {
        this.resellerId = resellerId;
    }

    public List<PriceDTO> getPrices() {
        return prices;
    }

    public void setPrices(List<PriceDTO> prices) {
        this.prices = prices;
    }

    public void addPrice(PriceDTO price) {
        if (this.prices == null) {
            this.prices = new ArrayList<>();
        }
        this.prices.add(price);
    }

    public int size() {
        return prices != null ? prices.size() : 0;
    }

    @Override
    public String toString() {
        return "PriceBatchDTO{" +
                "resellerId='" + resellerId + '\'' +
                ", pricesCount=" + size() +
                '}';
    }
}
