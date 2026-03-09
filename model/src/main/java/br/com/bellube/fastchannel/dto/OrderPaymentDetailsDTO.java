package br.com.bellube.fastchannel.dto;

import com.google.gson.annotations.SerializedName;

public class OrderPaymentDetailsDTO {

    @SerializedName("PaymentMethodId")
    private Integer paymentMethodId;
    @SerializedName("PaymentMethodTypeId")
    private Integer paymentMethodTypeId;
    @SerializedName("PaymentMethodTypeName")
    private String paymentMethodTypeName;
    @SerializedName("PaymentMethodName")
    private String paymentMethodName;

    public Integer getPaymentMethodId() {
        return paymentMethodId;
    }

    public void setPaymentMethodId(Integer paymentMethodId) {
        this.paymentMethodId = paymentMethodId;
    }

    public Integer getPaymentMethodTypeId() {
        return paymentMethodTypeId;
    }

    public void setPaymentMethodTypeId(Integer paymentMethodTypeId) {
        this.paymentMethodTypeId = paymentMethodTypeId;
    }

    public String getPaymentMethodTypeName() {
        return paymentMethodTypeName;
    }

    public void setPaymentMethodTypeName(String paymentMethodTypeName) {
        this.paymentMethodTypeName = paymentMethodTypeName;
    }

    public String getPaymentMethodName() {
        return paymentMethodName;
    }

    public void setPaymentMethodName(String paymentMethodName) {
        this.paymentMethodName = paymentMethodName;
    }
}
