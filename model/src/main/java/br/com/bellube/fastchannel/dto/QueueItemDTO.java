package br.com.bellube.fastchannel.dto;

import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * DTO para itens da fila de sincronização (Outbox Pattern).
 *
 * Representa um registro na tabela AD_FCQUEUE.
 */
public class QueueItemDTO {

    private BigDecimal idQueue;
    private String entityType;      // PRODUTO, ESTOQUE, PRECO, PEDIDO_STATUS
    private String operation;       // CREATE, UPDATE, DELETE
    private BigDecimal entityId;    // CODPROD, CODPARC, etc
    private String entityKey;       // SKU ou ID alternativo
    private String payload;         // JSON com dados
    private String status;          // PENDENTE, PROCESSANDO, ENVIADO, ERRO
    private int retryCount;
    private String lastError;
    private Timestamp createdAt;
    private Timestamp processedAt;
    private BigDecimal priority;

    public QueueItemDTO() {
        this.status = "PENDENTE";
        this.retryCount = 0;
        this.priority = BigDecimal.ZERO;
        this.createdAt = new Timestamp(System.currentTimeMillis());
    }

    public QueueItemDTO(String entityType, String operation, BigDecimal entityId) {
        this();
        this.entityType = entityType;
        this.operation = operation;
        this.entityId = entityId;
    }

    // ==================== GETTERS e SETTERS ====================

    public BigDecimal getIdQueue() {
        return idQueue;
    }

    public void setIdQueue(BigDecimal idQueue) {
        this.idQueue = idQueue;
    }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public BigDecimal getEntityId() {
        return entityId;
    }

    public void setEntityId(BigDecimal entityId) {
        this.entityId = entityId;
    }

    public String getEntityKey() {
        return entityKey;
    }

    public void setEntityKey(String entityKey) {
        this.entityKey = entityKey;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public Timestamp getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Timestamp createdAt) {
        this.createdAt = createdAt;
    }

    public Timestamp getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(Timestamp processedAt) {
        this.processedAt = processedAt;
    }

    public BigDecimal getPriority() {
        return priority;
    }

    public void setPriority(BigDecimal priority) {
        this.priority = priority;
    }

    /**
     * Verifica se pode ser reprocessado.
     */
    public boolean canRetry(int maxRetries) {
        return retryCount < maxRetries && !"ERRO_FATAL".equals(status);
    }

    /**
     * Incrementa contador de tentativas.
     */
    public void incrementRetry() {
        this.retryCount++;
    }

    @Override
    public String toString() {
        return "QueueItemDTO{" +
                "idQueue=" + idQueue +
                ", entityType='" + entityType + '\'' +
                ", operation='" + operation + '\'' +
                ", entityId=" + entityId +
                ", status='" + status + '\'' +
                '}';
    }
}
