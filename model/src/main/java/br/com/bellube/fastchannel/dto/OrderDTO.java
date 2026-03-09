package br.com.bellube.fastchannel.dto;

import com.google.gson.annotations.SerializedName;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * DTO principal para pedidos recebidos do Fastchannel.
 *
 * Estrutura completa do pedido incluindo:
 * - Dados do pedido
 * - Cliente
 * - Endereço de entrega
 * - Itens
 * - Valores e descontos
 */
public class OrderDTO {

    // Identificação
    @SerializedName("OrderId")
    private String orderId;
    @SerializedName("OrderCode")
    private String externalOrderId;
    @SerializedName("ResellerId")
    private String resellerId;
    @SerializedName("StorageId")
    private String storageId;
    @SerializedName("CurrentStatusId")
    private int status;
    @SerializedName("CurrentStatusDescription")
    private String statusDescription;

    // Datas
    @SerializedName("CreatedAt")
    private Timestamp createdAt;
    @SerializedName("UpdatedAt")
    private Timestamp updatedAt;
    @SerializedName("ApprovedAt")
    private Timestamp approvedAt;

    // Cliente
    @SerializedName("Customer")
    private OrderCustomerDTO customer;

    // Endereços
    @SerializedName("ShippingData")
    private OrderAddressDTO shippingAddress;
    @SerializedName("BillingData")
    private OrderAddressDTO billingAddress;

    // Itens
    @SerializedName("Items")
    private List<OrderItemDTO> items;

    // Valores
    @SerializedName("Subtotal")
    private BigDecimal subtotal;
    @SerializedName("SubtotalProducts")
    private BigDecimal subtotalProducts;
    @SerializedName("Discount")
    private BigDecimal discount;
    @SerializedName("ProductDiscount")
    private BigDecimal productDiscount;
    @SerializedName("ProductDiscountQuota")
    private BigDecimal productDiscountQuota;
    @SerializedName("ProductDiscountCoupon")
    private BigDecimal productDiscountCoupon;
    @SerializedName("ProductDiscountManual")
    private BigDecimal productDiscountManual;
    @SerializedName("ProductDiscountPayment")
    private BigDecimal productDiscountPayment;
    @SerializedName("ProductDiscountAssociation")
    private BigDecimal productDiscountAssociation;
    @SerializedName("ShippingCost")
    private BigDecimal shippingCost;
    @SerializedName("Total")
    private BigDecimal total;
    @SerializedName("TotalOrderValue")
    private BigDecimal totalOrderValue;
    @SerializedName("OrderTotal")
    private BigDecimal orderTotal;
    @SerializedName("ShippingDiscount")
    private BigDecimal shippingDiscount;
    @SerializedName("ShippingDiscountCoupon")
    private BigDecimal shippingDiscountCoupon;
    @SerializedName("ShippingDiscountManual")
    private BigDecimal shippingDiscountManual;
    @SerializedName("ShippingDiscountAmount")
    private BigDecimal shippingDiscountAmount;

    // Pagamento
    @SerializedName("PaymentMethodName")
    private String paymentMethod;
    @SerializedName("PaymentMethodId")
    private Integer paymentMethodId;
    @SerializedName("CurrentPaymentDetails")
    private OrderPaymentDetailsDTO currentPaymentDetails;
    @SerializedName("Parcels")
    private int installments;

    // Frete
    @SerializedName("ShippingMethod")
    private String shippingMethod;
    @SerializedName("CarrierName")
    private String carrierName;

    // Observações
    @SerializedName("Notes")
    private String notes;
    @SerializedName("SellerNotes")
    private String sellerNotes;

    // Campos Sankhya (transient)
    private transient BigDecimal nuNota;
    private transient BigDecimal codParc;
    private transient BigDecimal codTipOper;
    private transient BigDecimal codEmp;
    private transient BigDecimal codLocal;

    public OrderDTO() {
        this.items = new ArrayList<>();
    }

    // ==================== GETTERS e SETTERS ====================

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getExternalOrderId() {
        return externalOrderId;
    }

    public void setExternalOrderId(String externalOrderId) {
        this.externalOrderId = externalOrderId;
    }

    public String getResellerId() {
        return resellerId;
    }

    public void setResellerId(String resellerId) {
        this.resellerId = resellerId;
    }

    public String getStorageId() {
        return storageId;
    }

    public void setStorageId(String storageId) {
        this.storageId = storageId;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getStatusDescription() {
        return statusDescription;
    }

    public void setStatusDescription(String statusDescription) {
        this.statusDescription = statusDescription;
    }

    public Timestamp getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Timestamp createdAt) {
        this.createdAt = createdAt;
    }

    public Timestamp getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Timestamp updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Timestamp getApprovedAt() {
        return approvedAt;
    }

    public void setApprovedAt(Timestamp approvedAt) {
        this.approvedAt = approvedAt;
    }

    public OrderCustomerDTO getCustomer() {
        return customer;
    }

    public void setCustomer(OrderCustomerDTO customer) {
        this.customer = customer;
    }

    public OrderAddressDTO getShippingAddress() {
        return shippingAddress;
    }

    public void setShippingAddress(OrderAddressDTO shippingAddress) {
        this.shippingAddress = shippingAddress;
    }

    public OrderAddressDTO getBillingAddress() {
        return billingAddress;
    }

    public void setBillingAddress(OrderAddressDTO billingAddress) {
        this.billingAddress = billingAddress;
    }

    public List<OrderItemDTO> getItems() {
        return items;
    }

    public void setItems(List<OrderItemDTO> items) {
        this.items = items;
    }

    public void addItem(OrderItemDTO item) {
        if (this.items == null) {
            this.items = new ArrayList<>();
        }
        this.items.add(item);
    }

    public BigDecimal getSubtotal() {
        return subtotal != null ? subtotal : subtotalProducts;
    }

    public void setSubtotal(BigDecimal subtotal) {
        this.subtotal = subtotal;
    }

    public BigDecimal getDiscount() {
        if (discount != null && discount.compareTo(BigDecimal.ZERO) != 0) {
            return discount;
        }
        BigDecimal sum = BigDecimal.ZERO;
        boolean has = false;
        if (productDiscount != null) {
            sum = sum.add(productDiscount);
            has = true;
        }
        if (productDiscountQuota != null) {
            sum = sum.add(productDiscountQuota);
            has = true;
        }
        if (productDiscountCoupon != null) {
            sum = sum.add(productDiscountCoupon);
            has = true;
        }
        if (productDiscountManual != null) {
            sum = sum.add(productDiscountManual);
            has = true;
        }
        if (productDiscountPayment != null) {
            sum = sum.add(productDiscountPayment);
            has = true;
        }
        if (productDiscountAssociation != null) {
            sum = sum.add(productDiscountAssociation);
            has = true;
        }
        if (shippingDiscount != null) {
            sum = sum.add(shippingDiscount);
            has = true;
        }
        if (shippingDiscountCoupon != null) {
            sum = sum.add(shippingDiscountCoupon);
            has = true;
        }
        if (shippingDiscountManual != null) {
            sum = sum.add(shippingDiscountManual);
            has = true;
        }
        if (shippingDiscountAmount != null) {
            sum = sum.add(shippingDiscountAmount);
            has = true;
        }
        return has ? sum : discount;
    }

    public void setDiscount(BigDecimal discount) {
        this.discount = discount;
    }

    public BigDecimal getShippingCost() {
        return shippingCost;
    }

    public void setShippingCost(BigDecimal shippingCost) {
        this.shippingCost = shippingCost;
    }

    public BigDecimal getTotal() {
        if (total != null) {
            return total;
        }
        if (totalOrderValue != null) {
            return totalOrderValue;
        }
        return orderTotal;
    }

    public void setTotal(BigDecimal total) {
        this.total = total;
    }

    public BigDecimal getSubtotalProducts() {
        return subtotalProducts;
    }

    public void setSubtotalProducts(BigDecimal subtotalProducts) {
        this.subtotalProducts = subtotalProducts;
    }

    public BigDecimal getProductDiscount() {
        return productDiscount;
    }

    public void setProductDiscount(BigDecimal productDiscount) {
        this.productDiscount = productDiscount;
    }

    public BigDecimal getProductDiscountQuota() {
        return productDiscountQuota;
    }

    public void setProductDiscountQuota(BigDecimal productDiscountQuota) {
        this.productDiscountQuota = productDiscountQuota;
    }

    public BigDecimal getProductDiscountCoupon() {
        return productDiscountCoupon;
    }

    public void setProductDiscountCoupon(BigDecimal productDiscountCoupon) {
        this.productDiscountCoupon = productDiscountCoupon;
    }

    public BigDecimal getProductDiscountManual() {
        return productDiscountManual;
    }

    public void setProductDiscountManual(BigDecimal productDiscountManual) {
        this.productDiscountManual = productDiscountManual;
    }

    public BigDecimal getProductDiscountPayment() {
        return productDiscountPayment;
    }

    public void setProductDiscountPayment(BigDecimal productDiscountPayment) {
        this.productDiscountPayment = productDiscountPayment;
    }

    public BigDecimal getProductDiscountAssociation() {
        return productDiscountAssociation;
    }

    public void setProductDiscountAssociation(BigDecimal productDiscountAssociation) {
        this.productDiscountAssociation = productDiscountAssociation;
    }

    public BigDecimal getTotalOrderValue() {
        return totalOrderValue;
    }

    public void setTotalOrderValue(BigDecimal totalOrderValue) {
        this.totalOrderValue = totalOrderValue;
    }

    public BigDecimal getOrderTotal() {
        return orderTotal;
    }

    public void setOrderTotal(BigDecimal orderTotal) {
        this.orderTotal = orderTotal;
    }

    public BigDecimal getShippingDiscount() {
        return shippingDiscount;
    }

    public void setShippingDiscount(BigDecimal shippingDiscount) {
        this.shippingDiscount = shippingDiscount;
    }

    public BigDecimal getShippingDiscountCoupon() {
        return shippingDiscountCoupon;
    }

    public void setShippingDiscountCoupon(BigDecimal shippingDiscountCoupon) {
        this.shippingDiscountCoupon = shippingDiscountCoupon;
    }

    public BigDecimal getShippingDiscountManual() {
        return shippingDiscountManual;
    }

    public void setShippingDiscountManual(BigDecimal shippingDiscountManual) {
        this.shippingDiscountManual = shippingDiscountManual;
    }

    public BigDecimal getShippingDiscountAmount() {
        return shippingDiscountAmount;
    }

    public void setShippingDiscountAmount(BigDecimal shippingDiscountAmount) {
        this.shippingDiscountAmount = shippingDiscountAmount;
    }

    public String getPaymentMethod() {
        return paymentMethod;
    }

    public void setPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
    }

    public Integer getPaymentMethodId() {
        if (currentPaymentDetails != null && currentPaymentDetails.getPaymentMethodId() != null) {
            return currentPaymentDetails.getPaymentMethodId();
        }
        return paymentMethodId;
    }

    public void setPaymentMethodId(Integer paymentMethodId) {
        this.paymentMethodId = paymentMethodId;
    }

    public OrderPaymentDetailsDTO getCurrentPaymentDetails() {
        return currentPaymentDetails;
    }

    public void setCurrentPaymentDetails(OrderPaymentDetailsDTO currentPaymentDetails) {
        this.currentPaymentDetails = currentPaymentDetails;
    }

    public int getInstallments() {
        return installments;
    }

    public void setInstallments(int installments) {
        this.installments = installments;
    }

    public String getShippingMethod() {
        return shippingMethod;
    }

    public void setShippingMethod(String shippingMethod) {
        this.shippingMethod = shippingMethod;
    }

    public String getCarrierName() {
        return carrierName;
    }

    public void setCarrierName(String carrierName) {
        this.carrierName = carrierName;
    }

    public String getNotes() {
        return notes;
    }

    public void setNotes(String notes) {
        this.notes = notes;
    }

    public String getSellerNotes() {
        return sellerNotes;
    }

    public void setSellerNotes(String sellerNotes) {
        this.sellerNotes = sellerNotes;
    }

    public BigDecimal getNuNota() {
        return nuNota;
    }

    public void setNuNota(BigDecimal nuNota) {
        this.nuNota = nuNota;
    }

    public BigDecimal getCodParc() {
        return codParc;
    }

    public void setCodParc(BigDecimal codParc) {
        this.codParc = codParc;
    }

    public BigDecimal getCodTipOper() {
        return codTipOper;
    }

    public void setCodTipOper(BigDecimal codTipOper) {
        this.codTipOper = codTipOper;
    }

    public BigDecimal getCodEmp() {
        return codEmp;
    }

    public void setCodEmp(BigDecimal codEmp) {
        this.codEmp = codEmp;
    }

    public BigDecimal getCodLocal() {
        return codLocal;
    }

    public void setCodLocal(BigDecimal codLocal) {
        this.codLocal = codLocal;
    }

    public int getItemCount() {
        return items != null ? items.size() : 0;
    }

    @Override
    public String toString() {
        return "OrderDTO{" +
                "orderId='" + orderId + '\'' +
                ", status=" + status +
                ", total=" + total +
                ", items=" + getItemCount() +
                '}';
    }
}
