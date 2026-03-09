package br.com.bellube.fastchannel.dto;

import com.google.gson.annotations.SerializedName;

import java.math.BigDecimal;

/**
 * DTO para envio de faixas de preco escalonado (batches) ao Fastchannel.
 */
public class PriceBatchItemDTO {

    @SerializedName("PriceTableId")
    private BigDecimal priceTableId;

    @SerializedName("MinimumBatchSize")
    private BigDecimal minimumBatchSize;

    @SerializedName("MaximumBatchSize")
    private BigDecimal maximumBatchSize;

    @SerializedName("UnitaryPriceForBatch")
    private BigDecimal unitaryPriceForBatch;

    @SerializedName("BatchDisabled")
    private Boolean batchDisabled;

    public BigDecimal getPriceTableId() {
        return priceTableId;
    }

    public void setPriceTableId(BigDecimal priceTableId) {
        this.priceTableId = priceTableId;
    }

    public BigDecimal getMinimumBatchSize() {
        return minimumBatchSize;
    }

    public void setMinimumBatchSize(BigDecimal minimumBatchSize) {
        this.minimumBatchSize = minimumBatchSize;
    }

    public BigDecimal getMaximumBatchSize() {
        return maximumBatchSize;
    }

    public void setMaximumBatchSize(BigDecimal maximumBatchSize) {
        this.maximumBatchSize = maximumBatchSize;
    }

    public BigDecimal getUnitaryPriceForBatch() {
        return unitaryPriceForBatch;
    }

    public void setUnitaryPriceForBatch(BigDecimal unitaryPriceForBatch) {
        this.unitaryPriceForBatch = unitaryPriceForBatch;
    }

    public Boolean getBatchDisabled() {
        return batchDisabled;
    }

    public void setBatchDisabled(Boolean batchDisabled) {
        this.batchDisabled = batchDisabled;
    }
}
