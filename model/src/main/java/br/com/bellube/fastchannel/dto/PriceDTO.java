package br.com.bellube.fastchannel.dto;

import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * DTO para operações de preço com a API Fastchannel.
 *
 * Representa a estrutura de dados para envio/recebimento
 * de informações de preço.
 */
public class PriceDTO {

    private String sku;
    private String resellerId;
    private BigDecimal price;
    private BigDecimal listPrice;
    private BigDecimal promotionalPrice;
    private Timestamp promotionStartDate;
    private Timestamp promotionEndDate;
    private String currency;
    private Timestamp lastUpdate;

    // Campos adicionais Sankhya (para mapeamento interno)
    private transient BigDecimal codProd;
    private transient BigDecimal nuTab;
    private transient BigDecimal codEmp;

    public PriceDTO() {
        this.currency = "BRL";
    }

    public PriceDTO(String sku, String resellerId, BigDecimal price) {
        this.sku = sku;
        this.resellerId = resellerId;
        this.price = price;
        this.listPrice = price;
        this.currency = "BRL";
    }

    // ==================== GETTERS e SETTERS ====================

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public String getResellerId() {
        return resellerId;
    }

    public void setResellerId(String resellerId) {
        this.resellerId = resellerId;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public BigDecimal getListPrice() {
        return listPrice;
    }

    public void setListPrice(BigDecimal listPrice) {
        this.listPrice = listPrice;
    }

    public BigDecimal getPromotionalPrice() {
        return promotionalPrice;
    }

    public void setPromotionalPrice(BigDecimal promotionalPrice) {
        this.promotionalPrice = promotionalPrice;
    }

    public Timestamp getPromotionStartDate() {
        return promotionStartDate;
    }

    public void setPromotionStartDate(Timestamp promotionStartDate) {
        this.promotionStartDate = promotionStartDate;
    }

    public Timestamp getPromotionEndDate() {
        return promotionEndDate;
    }

    public void setPromotionEndDate(Timestamp promotionEndDate) {
        this.promotionEndDate = promotionEndDate;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
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

    public BigDecimal getNuTab() {
        return nuTab;
    }

    public void setNuTab(BigDecimal nuTab) {
        this.nuTab = nuTab;
    }

    public BigDecimal getCodEmp() {
        return codEmp;
    }

    public void setCodEmp(BigDecimal codEmp) {
        this.codEmp = codEmp;
    }

    @Override
    public String toString() {
        return "PriceDTO{" +
                "sku='" + sku + '\'' +
                ", resellerId='" + resellerId + '\'' +
                ", price=" + price +
                ", listPrice=" + listPrice +
                '}';
    }
}
