package br.com.bellube.fastchannel.dto;

import com.google.gson.annotations.SerializedName;
import java.sql.Timestamp;

/**
 * DTO para atualizacao de status de pedido no Fastchannel.
 *
 * <p>[FIX 2026-05-19 v1.2.83] Adicionados {@code @SerializedName} para mapear os campos
 * Java para os nomes capitalizados que a API Fastchannel espera. Sem isso, o Gson
 * serializava como {@code {"status":201,"message":"..."}}, mas a API FC exige
 * {@code {"OrderStatusId":201,"Message":"..."}}. Como o campo "OrderStatusId" chegava
 * NULL/0 na request, o FC respondia HTTP 400 com a mensagem confusa:
 * "O codigo de status informado no parametro OrderStatusId nao e um codigo valido"
 * (mensagem essa que sugeria erro de maquina de estado quando na verdade era apenas
 * o nome do campo errado no JSON).
 *
 * <p>Confirmado consultando o legado Node.js (gbi-app-integrador
 * {@code models/mssqlModels/sankhya/Pedidos.js:144-147}) que monta o payload como:
 * <pre>{@code
 * { "OrderId": <int>, "OrderStatusId": <int> }
 * }</pre>
 */
public class OrderStatusDTO {

    @SerializedName("OrderStatusId")
    private int status;

    @SerializedName("Message")
    private String message;

    @SerializedName("Timestamp")
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
