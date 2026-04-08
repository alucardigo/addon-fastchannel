package br.com.bellube.fastchannel.dto;

import com.google.gson.annotations.SerializedName;

import java.math.BigDecimal;

/**
 * DTO para itens de pedido do Fastchannel.
 *
 * Mapeia para TGFITE no Sankhya.
 */
public class OrderItemDTO {

    @SerializedName("ProductId")
    private String sku;
    @SerializedName("ProductName")
    private String productName;
    @SerializedName("Quantity")
    private BigDecimal quantity;
    @SerializedName("SalePrice")
    private BigDecimal unitPrice;
    @SerializedName("Discount")
    private BigDecimal discount;
    @SerializedName("TotalProductCost")
    private BigDecimal totalPrice;
    @SerializedName("ListPrice")
    private BigDecimal listPrice;
    @SerializedName("IdVolume")
    private String volumeId;
    @SerializedName("VolumeId")
    private String volumeIdAlt;
    @SerializedName("VolumeCode")
    private String volumeCode;
    @SerializedName("CodVol")
    private String codVolExternal;

    @SerializedName("IdGradeControl")
    private String gradeControlId;
    @SerializedName("GradeControlId")
    private String gradeControlIdAlt;
    @SerializedName("Control")
    private String gradeControlIdControl;
    @SerializedName("Controle")
    private String gradeControlIdControle;

    @SerializedName("AssociationDiscount")
    private BigDecimal associationDiscount;
    @SerializedName("ManualDiscount")
    private BigDecimal manualDiscount;
    @SerializedName("CatalogDiscount")
    private BigDecimal catalogDiscount;
    @SerializedName("QuotaDiscount")
    private BigDecimal quotaDiscount;
    @SerializedName("CouponDiscount")
    private BigDecimal couponDiscount;
    @SerializedName("PaymentDiscount")
    private BigDecimal paymentDiscount;

    // Identificadores externos
    private String externalProductId;
    private String ean;

    // Campos Sankhya (transient)
    private transient BigDecimal codProd;
    private transient BigDecimal sequencia;
    private transient BigDecimal codVol;

    public OrderItemDTO() {
    }

    public OrderItemDTO(String sku, BigDecimal quantity, BigDecimal unitPrice) {
        this.sku = sku;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
        this.totalPrice = quantity.multiply(unitPrice);
    }

    // ==================== GETTERS e SETTERS ====================

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public BigDecimal getQuantity() {
        return quantity;
    }

    public void setQuantity(BigDecimal quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(BigDecimal unitPrice) {
        this.unitPrice = unitPrice;
    }

    public BigDecimal getDiscount() {
        if (discount != null && discount.compareTo(BigDecimal.ZERO) != 0) {
            return discount;
        }
        BigDecimal sum = BigDecimal.ZERO;
        boolean has = false;
        if (associationDiscount != null) {
            sum = sum.add(associationDiscount);
            has = true;
        }
        if (manualDiscount != null) {
            sum = sum.add(manualDiscount);
            has = true;
        }
        if (catalogDiscount != null) {
            sum = sum.add(catalogDiscount);
            has = true;
        }
        if (quotaDiscount != null) {
            sum = sum.add(quotaDiscount);
            has = true;
        }
        if (couponDiscount != null) {
            sum = sum.add(couponDiscount);
            has = true;
        }
        if (paymentDiscount != null) {
            sum = sum.add(paymentDiscount);
            has = true;
        }
        return has ? sum : discount;
    }

    public void setDiscount(BigDecimal discount) {
        this.discount = discount;
    }

    public BigDecimal getTotalPrice() {
        return totalPrice;
    }

    public void setTotalPrice(BigDecimal totalPrice) {
        this.totalPrice = totalPrice;
    }

    public String getExternalProductId() {
        return externalProductId;
    }

    public void setExternalProductId(String externalProductId) {
        this.externalProductId = externalProductId;
    }

    public String getEan() {
        return ean;
    }

    public void setEan(String ean) {
        this.ean = ean;
    }

    public BigDecimal getListPrice() {
        return listPrice;
    }

    public void setListPrice(BigDecimal listPrice) {
        this.listPrice = listPrice;
    }

    public String getVolumeId() {
        if (volumeId != null && !volumeId.trim().isEmpty()) return volumeId;
        if (volumeIdAlt != null && !volumeIdAlt.trim().isEmpty()) return volumeIdAlt;
        if (volumeCode != null && !volumeCode.trim().isEmpty()) return volumeCode;
        if (codVolExternal != null && !codVolExternal.trim().isEmpty()) return codVolExternal;
        return volumeId;
    }

    public void setVolumeId(String volumeId) {
        this.volumeId = volumeId;
    }

    public String getGradeControlId() {
        if (gradeControlId != null && !gradeControlId.trim().isEmpty()) return gradeControlId;
        if (gradeControlIdAlt != null && !gradeControlIdAlt.trim().isEmpty()) return gradeControlIdAlt;
        if (gradeControlIdControl != null && !gradeControlIdControl.trim().isEmpty()) return gradeControlIdControl;
        if (gradeControlIdControle != null && !gradeControlIdControle.trim().isEmpty()) return gradeControlIdControle;
        return gradeControlId;
    }

    public void setGradeControlId(String gradeControlId) {
        this.gradeControlId = gradeControlId;
    }

    public BigDecimal getAssociationDiscount() {
        return associationDiscount;
    }

    public void setAssociationDiscount(BigDecimal associationDiscount) {
        this.associationDiscount = associationDiscount;
    }

    public BigDecimal getManualDiscount() {
        return manualDiscount;
    }

    public void setManualDiscount(BigDecimal manualDiscount) {
        this.manualDiscount = manualDiscount;
    }

    public BigDecimal getCatalogDiscount() {
        return catalogDiscount;
    }

    public void setCatalogDiscount(BigDecimal catalogDiscount) {
        this.catalogDiscount = catalogDiscount;
    }

    public BigDecimal getQuotaDiscount() {
        return quotaDiscount;
    }

    public void setQuotaDiscount(BigDecimal quotaDiscount) {
        this.quotaDiscount = quotaDiscount;
    }

    public BigDecimal getCouponDiscount() {
        return couponDiscount;
    }

    public void setCouponDiscount(BigDecimal couponDiscount) {
        this.couponDiscount = couponDiscount;
    }

    public BigDecimal getPaymentDiscount() {
        return paymentDiscount;
    }

    public void setPaymentDiscount(BigDecimal paymentDiscount) {
        this.paymentDiscount = paymentDiscount;
    }

    public BigDecimal getCodProd() {
        return codProd;
    }

    public void setCodProd(BigDecimal codProd) {
        this.codProd = codProd;
    }

    public BigDecimal getSequencia() {
        return sequencia;
    }

    public void setSequencia(BigDecimal sequencia) {
        this.sequencia = sequencia;
    }

    public BigDecimal getCodVol() {
        return codVol;
    }

    public void setCodVol(BigDecimal codVol) {
        this.codVol = codVol;
    }

    /**
     * Calcula o total do item.
     */
    public BigDecimal calculateTotal() {
        BigDecimal qty = quantity != null ? quantity : BigDecimal.ZERO;
        BigDecimal price = unitPrice != null ? unitPrice : BigDecimal.ZERO;
        BigDecimal disc = getDiscount();
        if (disc == null) {
            disc = BigDecimal.ZERO;
        }
        return qty.multiply(price).subtract(disc);
    }

    @Override
    public String toString() {
        return "OrderItemDTO{" +
                "sku='" + sku + '\'' +
                ", quantity=" + quantity +
                ", unitPrice=" + unitPrice +
                '}';
    }
}
