package br.com.bellube.fastchannel.dto;

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
    private String orderId;
    private String externalOrderId;
    private String resellerId;
    private int status;
    private String statusDescription;

    // Datas
    private Timestamp createdAt;
    private Timestamp updatedAt;
    private Timestamp approvedAt;

    // Cliente
    private OrderCustomerDTO customer;

    // Endereços
    private OrderAddressDTO shippingAddress;
    private OrderAddressDTO billingAddress;

    // Itens
    private List<OrderItemDTO> items;

    // Valores
    private BigDecimal subtotal;
    private BigDecimal discount;
    private BigDecimal shippingCost;
    private BigDecimal total;

    // Pagamento
    private String paymentMethod;
    private int installments;

    // Frete
    private String shippingMethod;
    private String carrierName;

    // Observações
    private String notes;
    private String sellerNotes;

    // Campos Sankhya (transient)
    private transient BigDecimal nuNota;
    private transient BigDecimal codParc;
    private transient BigDecimal codTipOper;
    private transient BigDecimal codEmp;

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
        return subtotal;
    }

    public void setSubtotal(BigDecimal subtotal) {
        this.subtotal = subtotal;
    }

    public BigDecimal getDiscount() {
        return discount;
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
        return total;
    }

    public void setTotal(BigDecimal total) {
        this.total = total;
    }

    public String getPaymentMethod() {
        return paymentMethod;
    }

    public void setPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
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
