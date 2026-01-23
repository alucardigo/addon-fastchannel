package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.QueueItemDTO;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import com.google.gson.Gson;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Servi?o de Fila (Transactional Outbox Pattern).
 *
 * Gerencia a tabela AD_FCQUEUE para sincroniza??o ass?ncrona
 * entre Sankhya e Fastchannel.
 *
 * Caracter?sticas:
 * - Thread-safe
 * - Debouncing autom?tico (evita duplicatas)
 * - Prioriza??o de itens
 * - Controle de retry
 */
public class QueueService {

    private static final Logger log = Logger.getLogger(QueueService.class.getName());
    private static final Gson gson = new Gson();

    private static QueueService instance;
    private final FastchannelConfig config;

    // Debounce window em milissegundos (evita duplicatas)
    private static final long DEBOUNCE_WINDOW_MS = 5000; // 5 segundos

    private QueueService() {
        this.config = FastchannelConfig.getInstance();
    }

    public static synchronized QueueService getInstance() {
        if (instance == null) {
            instance = new QueueService();
        }
        return instance;
    }

    /**
     * Enfileira item para sincroniza??o (com debounce).
     *
     * @param entityType tipo da entidade (PRODUTO, ESTOQUE, PRECO)
     * @param operation opera??o (CREATE, UPDATE, DELETE)
     * @param entityId ID da entidade no Sankhya
     * @param entityKey chave alternativa (SKU)
     * @param payload dados JSON (opcional)
     */
    public void enqueue(String entityType, String operation, BigDecimal entityId,
                        String entityKey, String payload) {
        enqueue(entityType, operation, entityId, entityKey, payload, BigDecimal.ZERO);
    }

    /**
     * Enfileira item com prioridade.
     */
    public void enqueue(String entityType, String operation, BigDecimal entityId,
                        String entityKey, String payload, BigDecimal priority) {

        JdbcWrapper jdbc = null;
        ResultSet rs = null;

        try {
            jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            // Debounce: verificar se j? existe item similar recente
            if (hasPendingItem(jdbc, entityType, entityId, entityKey)) {
                log.fine("Item j? na fila (debounce): " + entityType + "/" + entityId);
                return;
            }

            // Inserir novo item na fila
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("INSERT INTO AD_FCQUEUE ");
            sql.appendSql("(ENTITY_TYPE, OPERATION, ENTITY_ID, ENTITY_KEY, PAYLOAD, STATUS, ");
            sql.appendSql("RETRY_COUNT, PRIORITY, DH_CRIACAO) ");
            sql.appendSql("VALUES (:entityType, :operation, :entityId, :entityKey, :payload, ");
            sql.appendSql(":status, 0, :priority, CURRENT_TIMESTAMP)");

            sql.setNamedParameter("entityType", entityType);
            sql.setNamedParameter("operation", operation);
            sql.setNamedParameter("entityId", entityId);
            sql.setNamedParameter("entityKey", entityKey);
            sql.setNamedParameter("payload", payload);
            sql.setNamedParameter("status", FastchannelConstants.QUEUE_STATUS_PENDENTE);
            sql.setNamedParameter("priority", priority);

            sql.executeUpdate();

            log.info("Enfileirado: " + entityType + "/" + operation + " - " + entityKey);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao enfileirar item", e);
        }
    }

    /**
     * Verifica se h? item pendente similar (debounce).
     */
    private boolean hasPendingItem(JdbcWrapper jdbc, String entityType,
                                   BigDecimal entityId, String entityKey) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("SELECT 1 FROM AD_FCQUEUE WHERE ");
        sql.appendSql("ENTITY_TYPE = :entityType AND STATUS IN ('PENDENTE', 'PROCESSANDO') ");
        sql.appendSql("AND (ENTITY_ID = :entityId OR ENTITY_KEY = :entityKey) ");
        sql.appendSql("AND DH_CRIACAO > :debounceTime");

        sql.setNamedParameter("entityType", entityType);
        sql.setNamedParameter("entityId", entityId);
        sql.setNamedParameter("entityKey", entityKey);

        Timestamp debounceTime = new Timestamp(System.currentTimeMillis() - DEBOUNCE_WINDOW_MS);
        sql.setNamedParameter("debounceTime", debounceTime);

        ResultSet rs = sql.executeQuery();
        try {
            return rs.next();
        } finally {
            closeQuietly(rs);
        }
    }

    /**
     * Busca pr?ximos itens pendentes para processamento.
     *
     * @param batchSize quantidade m?xima de itens
     * @return lista de itens pendentes ordenados por prioridade
     */
    public List<QueueItemDTO> fetchPendingItems(int batchSize) {
        List<QueueItemDTO> items = new ArrayList<>();
        JdbcWrapper jdbc = null;
        ResultSet rs = null;

        try {
            jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT IDQUEUE, ENTITY_TYPE, OPERATION, ENTITY_ID, ENTITY_KEY, ");
            sql.appendSql("PAYLOAD, STATUS, RETRY_COUNT, LAST_ERROR, DH_CRIACAO, PRIORITY ");
            sql.appendSql("FROM AD_FCQUEUE ");
            sql.appendSql("WHERE STATUS = :status ");
            sql.appendSql("ORDER BY PRIORITY DESC, DH_CRIACAO ASC ");
            sql.appendSql("FETCH FIRST :limit ROWS ONLY");

            sql.setNamedParameter("status", FastchannelConstants.QUEUE_STATUS_PENDENTE);
            sql.setNamedParameter("limit", batchSize);

            rs = sql.executeQuery();

            while (rs.next()) {
                QueueItemDTO item = new QueueItemDTO();
                item.setIdQueue(rs.getBigDecimal("IDQUEUE"));
                item.setEntityType(rs.getString("ENTITY_TYPE"));
                item.setOperation(rs.getString("OPERATION"));
                item.setEntityId(rs.getBigDecimal("ENTITY_ID"));
                item.setEntityKey(rs.getString("ENTITY_KEY"));
                item.setPayload(rs.getString("PAYLOAD"));
                item.setStatus(rs.getString("STATUS"));
                item.setRetryCount(rs.getInt("RETRY_COUNT"));
                item.setLastError(rs.getString("LAST_ERROR"));
                item.setCreatedAt(rs.getTimestamp("DH_CRIACAO"));
                item.setPriority(rs.getBigDecimal("PRIORITY"));
                items.add(item);
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao buscar itens pendentes", e);
        } finally {
            closeQuietly(rs);
        }

        return items;
    }

    /**
     * Busca itens pendentes por tipo de entidade.
     */
    public List<QueueItemDTO> fetchPendingByType(String entityType, int batchSize) {
        List<QueueItemDTO> items = new ArrayList<>();
        JdbcWrapper jdbc = null;
        ResultSet rs = null;

        try {
            jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT IDQUEUE, ENTITY_TYPE, OPERATION, ENTITY_ID, ENTITY_KEY, ");
            sql.appendSql("PAYLOAD, STATUS, RETRY_COUNT, LAST_ERROR, DH_CRIACAO, PRIORITY ");
            sql.appendSql("FROM AD_FCQUEUE ");
            sql.appendSql("WHERE STATUS = :status AND ENTITY_TYPE = :entityType ");
            sql.appendSql("ORDER BY PRIORITY DESC, DH_CRIACAO ASC ");
            sql.appendSql("FETCH FIRST :limit ROWS ONLY");

            sql.setNamedParameter("status", FastchannelConstants.QUEUE_STATUS_PENDENTE);
            sql.setNamedParameter("entityType", entityType);
            sql.setNamedParameter("limit", batchSize);

            rs = sql.executeQuery();

            while (rs.next()) {
                QueueItemDTO item = new QueueItemDTO();
                item.setIdQueue(rs.getBigDecimal("IDQUEUE"));
                item.setEntityType(rs.getString("ENTITY_TYPE"));
                item.setOperation(rs.getString("OPERATION"));
                item.setEntityId(rs.getBigDecimal("ENTITY_ID"));
                item.setEntityKey(rs.getString("ENTITY_KEY"));
                item.setPayload(rs.getString("PAYLOAD"));
                item.setStatus(rs.getString("STATUS"));
                item.setRetryCount(rs.getInt("RETRY_COUNT"));
                item.setLastError(rs.getString("LAST_ERROR"));
                item.setCreatedAt(rs.getTimestamp("DH_CRIACAO"));
                item.setPriority(rs.getBigDecimal("PRIORITY"));
                items.add(item);
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao buscar itens por tipo", e);
        } finally {
            closeQuietly(rs);
        }

        return items;
    }

    /**
     * Marca item como "em processamento".
     */
    public void markAsProcessing(BigDecimal idQueue) {
        updateStatus(idQueue, FastchannelConstants.QUEUE_STATUS_PROCESSANDO, null);
    }

    /**
     * Marca item como enviado com sucesso.
     */
    public void markAsSuccess(BigDecimal idQueue) {
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("UPDATE AD_FCQUEUE SET ");
            sql.appendSql("STATUS = :status, DH_PROCESSAMENTO = CURRENT_TIMESTAMP, LAST_ERROR = NULL ");
            sql.appendSql("WHERE IDQUEUE = :idQueue");

            sql.setNamedParameter("status", FastchannelConstants.QUEUE_STATUS_ENVIADO);
            sql.setNamedParameter("idQueue", idQueue);

            sql.executeUpdate();

            log.fine("Item " + idQueue + " marcado como ENVIADO");

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao marcar item como sucesso", e);
        }
    }

    /**
     * Marca item como erro (para retry).
     */
    public void markAsError(BigDecimal idQueue, String errorMessage) {
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("UPDATE AD_FCQUEUE SET ");
            sql.appendSql("STATUS = :status, RETRY_COUNT = RETRY_COUNT + 1, ");
            sql.appendSql("LAST_ERROR = :error, DH_ALTERACAO = CURRENT_TIMESTAMP ");
            sql.appendSql("WHERE IDQUEUE = :idQueue");

            sql.setNamedParameter("status", FastchannelConstants.QUEUE_STATUS_ERRO);
            sql.setNamedParameter("error", truncate(errorMessage, 4000));
            sql.setNamedParameter("idQueue", idQueue);

            sql.executeUpdate();

            log.warning("Item " + idQueue + " marcado como ERRO: " + errorMessage);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao marcar item como erro", e);
        }
    }

    /**
     * Marca item como erro fatal (n?o vai mais ser reprocessado).
     */
    public void markAsFatalError(BigDecimal idQueue, String errorMessage) {
        updateStatus(idQueue, FastchannelConstants.QUEUE_STATUS_ERRO_FATAL, errorMessage);
        log.severe("Item " + idQueue + " marcado como ERRO_FATAL: " + errorMessage);
    }

    /**
     * Reativa itens com erro para reprocessamento.
     */
    public int reactivateErrorItems(int maxRetries) {
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("UPDATE AD_FCQUEUE SET ");
            sql.appendSql("STATUS = :newStatus, DH_ALTERACAO = CURRENT_TIMESTAMP ");
            sql.appendSql("WHERE STATUS = :errorStatus AND RETRY_COUNT < :maxRetries");

            sql.setNamedParameter("newStatus", FastchannelConstants.QUEUE_STATUS_PENDENTE);
            sql.setNamedParameter("errorStatus", FastchannelConstants.QUEUE_STATUS_ERRO);
            sql.setNamedParameter("maxRetries", maxRetries);

            boolean updated = sql.executeUpdate();
            if (updated) {
                log.info("Reativa??o de itens para reprocessamento executada");
                return 1;
            }
            return 0;

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao reativar itens", e);
            return 0;
        }
    }

    /**
     * Conta todos os itens pendentes.
     */
    public int countPending() {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;

        try {
            jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT COUNT(*) AS TOTAL FROM AD_FCQUEUE ");
            sql.appendSql("WHERE STATUS = :status");

            sql.setNamedParameter("status", FastchannelConstants.QUEUE_STATUS_PENDENTE);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getInt("TOTAL");
            }

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao contar itens pendentes", e);
        } finally {
            closeQuietly(rs);
        }

        return 0;
    }

    /**
     * Conta todos os itens com erro.
     */
    public int countErrors() {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;

        try {
            jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT COUNT(*) AS TOTAL FROM AD_FCQUEUE ");
            sql.appendSql("WHERE STATUS = :status");

            sql.setNamedParameter("status", FastchannelConstants.QUEUE_STATUS_ERRO);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getInt("TOTAL");
            }

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao contar itens com erro", e);
        } finally {
            closeQuietly(rs);
        }

        return 0;
    }

    /**
     * Reseta item para reprocessamento (zera tentativas e status para PENDENTE).
     */
    public void resetForRetry(BigDecimal codQueue) {
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("UPDATE AD_FCQUEUE SET ");
            sql.appendSql("STATUS = :status, RETRY_COUNT = 0, LAST_ERROR = NULL, ");
            sql.appendSql("DH_ALTERACAO = CURRENT_TIMESTAMP ");
            sql.appendSql("WHERE IDQUEUE = :idQueue");

            sql.setNamedParameter("status", FastchannelConstants.QUEUE_STATUS_PENDENTE);
            sql.setNamedParameter("idQueue", codQueue);

            sql.executeUpdate();

            log.info("Item " + codQueue + " resetado para reprocessamento");

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao resetar item para retry", e);
            throw new RuntimeException("Falha ao resetar item: " + e.getMessage(), e);
        }
    }

    /**
     * Conta itens pendentes por tipo.
     */
    public int countPendingByType(String entityType) {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;

        try {
            jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT COUNT(*) AS TOTAL FROM AD_FCQUEUE ");
            sql.appendSql("WHERE STATUS = :status AND ENTITY_TYPE = :entityType");

            sql.setNamedParameter("status", FastchannelConstants.QUEUE_STATUS_PENDENTE);
            sql.setNamedParameter("entityType", entityType);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getInt("TOTAL");
            }

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao contar itens pendentes", e);
        } finally {
            closeQuietly(rs);
        }

        return 0;
    }

    /**
     * Limpa itens antigos j? processados.
     */
    public int cleanupOldItems(int daysToKeep) {
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("DELETE FROM AD_FCQUEUE ");
            sql.appendSql("WHERE STATUS IN ('ENVIADO', 'ERRO_FATAL', 'CANCELADO') ");
            sql.appendSql("AND DH_CRIACAO < DATEADD(DAY, -:days, CURRENT_TIMESTAMP)");

            sql.setNamedParameter("days", daysToKeep);

            boolean deleted = sql.executeUpdate();
            if (deleted) {
                log.info("Remo??o de itens antigos da fila executada");
                return 1;
            }
            return 0;

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao limpar itens antigos", e);
            return 0;
        }
    }

    private void updateStatus(BigDecimal idQueue, String status, String errorMessage) {
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("UPDATE AD_FCQUEUE SET ");
            sql.appendSql("STATUS = :status, LAST_ERROR = :error, DH_ALTERACAO = CURRENT_TIMESTAMP ");
            sql.appendSql("WHERE IDQUEUE = :idQueue");

            sql.setNamedParameter("status", status);
            sql.setNamedParameter("error", truncate(errorMessage, 4000));
            sql.setNamedParameter("idQueue", idQueue);

            sql.executeUpdate();

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao atualizar status", e);
        }
    }

    private String truncate(String str, int maxLength) {
        if (str == null) return null;
        return str.length() > maxLength ? str.substring(0, maxLength) : str;
    }

    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (Exception ignored) {}
        }
    }

    // ==================== M?TODOS DE CONVENI?NCIA ====================

    /**
     * Enfileira atualiza??o de produto.
     */
    public void enqueueProduct(BigDecimal codProd, String sku, String operation) {
        enqueue(FastchannelConstants.ENTITY_PRODUTO, operation, codProd, sku, null);
    }

    /**
     * Enfileira atualiza??o de estoque (alta prioridade).
     */
    public void enqueueStock(BigDecimal codProd, String sku, BigDecimal quantity) {
        String payload = gson.toJson(new Object[]{ sku, quantity });
        enqueue(FastchannelConstants.ENTITY_ESTOQUE, FastchannelConstants.OPERATION_UPDATE,
                codProd, sku, payload, new BigDecimal(10)); // Prioridade alta
    }

    /**
     * Enfileira atualiza??o de pre?o.
     */
    public void enqueuePrice(BigDecimal codProd, String sku) {
        enqueue(FastchannelConstants.ENTITY_PRECO, FastchannelConstants.OPERATION_UPDATE,
                codProd, sku, null, new BigDecimal(5)); // Prioridade m?dia
    }

    /**
     * Enfileira atualiza??o de status de pedido (prioridade m?xima).
     */
    public void enqueueOrderStatus(BigDecimal nuNota, String orderId, int status) {
        String payload = gson.toJson(new Object[]{ orderId, status });
        enqueue(FastchannelConstants.ENTITY_PEDIDO_STATUS, FastchannelConstants.OPERATION_UPDATE,
                nuNota, orderId, payload, new BigDecimal(100)); // Prioridade m?xima
    }
}

