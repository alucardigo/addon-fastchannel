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

    // Compatibilidade com payloads PascalCase retornados pela API.
    private String ProductId;
    private String StorageId;
    private BigDecimal Quantity;
    private BigDecimal ReservedQuantity;
    private BigDecimal AvailableQuantity;
    private Timestamp LastUpdate;

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
        if (sku != null && !sku.isEmpty()) return sku;
        return ProductId;
    }

    public void setSku(String sku) {
        this.sku = sku;
        this.ProductId = sku;
    }

    public String getStorageId() {
        if (storageId != null && !storageId.isEmpty()) return storageId;
        return StorageId;
    }

    public void setStorageId(String storageId) {
        this.storageId = storageId;
        this.StorageId = storageId;
    }

    public BigDecimal getQuantity() {
        if (quantity != null) return quantity;
        return Quantity;
    }

    public void setQuantity(BigDecimal quantity) {
        this.quantity = quantity;
        this.Quantity = quantity;
    }

    public BigDecimal getReservedQuantity() {
        if (reservedQuantity != null) return reservedQuantity;
        return ReservedQuantity;
    }

    public void setReservedQuantity(BigDecimal reservedQuantity) {
        this.reservedQuantity = reservedQuantity;
        this.ReservedQuantity = reservedQuantity;
    }

    public BigDecimal getAvailableQuantity() {
        if (availableQuantity != null) return availableQuantity;
        return AvailableQuantity;
    }

    public void setAvailableQuantity(BigDecimal availableQuantity) {
        this.availableQuantity = availableQuantity;
        this.AvailableQuantity = availableQuantity;
    }

    public Timestamp getLastUpdate() {
        if (lastUpdate != null) return lastUpdate;
        return LastUpdate;
    }

    public void setLastUpdate(Timestamp lastUpdate) {
        this.lastUpdate = lastUpdate;
        this.LastUpdate = lastUpdate;
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
