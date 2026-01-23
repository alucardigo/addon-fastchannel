package br.com.bellube.fastchannel.dto;

import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * DTO para operações de estoque com a API Fastchannel.
 *
 * Representa a estrutura de dados para envio/recebimento
 * de informações de estoque.
 */
public class StockDTO {

    private String sku;
    private String storageId;
    private BigDecimal quantity;
    private BigDecimal reservedQuantity;
    private BigDecimal availableQuantity;
    private Timestamp lastUpdate;

    // Campos adicionais Sankhya (para mapeamento interno)
    private transient BigDecimal codProd;
    private transient BigDecimal codLocal;

    public StockDTO() {
    }

    public StockDTO(String sku, String storageId, BigDecimal quantity) {
        this.sku = sku;
        this.storageId = storageId;
        this.quantity = quantity;
    }

    // ==================== GETTERS e SETTERS ====================

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public String getStorageId() {
        return storageId;
    }

    public void setStorageId(String storageId) {
        this.storageId = storageId;
    }

    public BigDecimal getQuantity() {
        return quantity;
    }

    public void setQuantity(BigDecimal quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getReservedQuantity() {
        return reservedQuantity;
    }

    public void setReservedQuantity(BigDecimal reservedQuantity) {
        this.reservedQuantity = reservedQuantity;
    }

    public BigDecimal getAvailableQuantity() {
        return availableQuantity;
    }

    public void setAvailableQuantity(BigDecimal availableQuantity) {
        this.availableQuantity = availableQuantity;
    }

    public Timestamp getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(Timestamp lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public BigDecimal getCodProd() {
        return codProd;
    }

    public void setCodProd(BigDecimal codProd) {
        this.codProd = codProd;
    }

    public BigDecimal getCodLocal() {
        return codLocal;
    }

    public void setCodLocal(BigDecimal codLocal) {
        this.codLocal = codLocal;
    }

    @Override
    public String toString() {
        return "StockDTO{" +
                "sku='" + sku + '\'' +
                ", storageId='" + storageId + '\'' +
                ", quantity=" + quantity +
                ", availableQuantity=" + availableQuantity +
                '}';
    }
}
