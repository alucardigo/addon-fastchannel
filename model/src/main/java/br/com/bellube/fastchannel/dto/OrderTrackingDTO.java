package br.com.bellube.fastchannel.dto;

import java.sql.Timestamp;

/**
 * DTO para envio de rastreamento de pedido ao Fastchannel.
 */
public class OrderTrackingDTO {

    private String trackingCode;
    private String carrierName;
    private String carrierCode;
    private String trackingUrl;
    private Timestamp shippedAt;
    private Timestamp estimatedDelivery;

    public OrderTrackingDTO() {
    }

    public OrderTrackingDTO(String trackingCode, String carrierName) {
        this.trackingCode = trackingCode;
        this.carrierName = carrierName;
    }

    // ==================== GETTERS e SETTERS ====================

    public String getTrackingCode() {
        return trackingCode;
    }

    public void setTrackingCode(String trackingCode) {
        this.trackingCode = trackingCode;
    }

    public String getCarrierName() {
        return carrierName;
    }

    public void setCarrierName(String carrierName) {
        this.carrierName = carrierName;
    }

    public String getCarrierCode() {
        return carrierCode;
    }

    public void setCarrierCode(String carrierCode) {
        this.carrierCode = carrierCode;
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public void setTrackingUrl(String trackingUrl) {
        this.trackingUrl = trackingUrl;
    }

    public Timestamp getShippedAt() {
        return shippedAt;
    }

    public void setShippedAt(Timestamp shippedAt) {
        this.shippedAt = shippedAt;
    }

    public Timestamp getEstimatedDelivery() {
        return estimatedDelivery;
    }

    public void setEstimatedDelivery(Timestamp estimatedDelivery) {
        this.estimatedDelivery = estimatedDelivery;
    }

    @Override
    public String toString() {
        return "OrderTrackingDTO{" +
                "trackingCode='" + trackingCode + '\'' +
                ", carrierName='" + carrierName + '\'' +
                '}';
    }
}
