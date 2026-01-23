package br.com.bellube.fastchannel.dto;

import java.sql.Timestamp;

/**
 * DTO para atualização de status de pedido no Fastchannel.
 */
public class OrderStatusDTO {

    private int status;
    private String message;
    private Timestamp timestamp;

    public OrderStatusDTO() {
        this.timestamp = new Timestamp(System.currentTimeMillis());
    }

    public OrderStatusDTO(int status, String message) {
        this.status = status;
        this.message = message;
        this.timestamp = new Timestamp(System.currentTimeMillis());
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "OrderStatusDTO{" +
                "status=" + status +
                ", message='" + message + '\'' +
                '}';
    }
}
