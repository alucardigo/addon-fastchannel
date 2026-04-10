package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.QueueItemDTO;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.util.DBUtil;
import com.google.gson.Gson;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Servico de Fila (Transactional Outbox Pattern).
 *
 * Gerencia a tabela AD_FCQUEUE para sincronizacao assincrona
 * entre Sankhya e Fastchannel.
 *
 * Usa JDBC direto (DBUtil/JNDI java:/MGEDS) em vez de JAPE,
 * para funcionar imediatamente apos WildFly boot sem depender
 * da inicializacao do mge-core.
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
     * Enfileira item para sincronizacao (com debounce).
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

        String normalizedEntityKey = normalizeEntityKey(entityKey);
        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            conn = DBUtil.getConnection();

            // Debounce: verificar se ja existe item similar recente
            if (hasPendingItem(conn, entityType, entityId, normalizedEntityKey)) {
                log.fine("Item ja na fila (debounce): " + entityType + "/" + entityId);
                return;
            }

            stmt = conn.prepareStatement(
                "INSERT INTO AD_FCQUEUE " +
                "(ENTITY_TYPE, OPERATION, ENTITY_ID, ENTITY_KEY, PAYLOAD, STATUS, " +
                "RETRY_COUNT, PRIORITY, DH_CRIACAO) " +
                "VALUES (?, ?, ?, ?, ?, ?, 0, ?, CURRENT_TIMESTAMP)");

            stmt.setString(1, entityType);
            stmt.setString(2, operation);
            stmt.setBigDecimal(3, entityId);
            stmt.setString(4, normalizedEntityKey);
            stmt.setString(5, payload);
            stmt.setString(6, FastchannelConstants.QUEUE_STATUS_PENDENTE);
            stmt.setBigDecimal(7, priority);

            stmt.executeUpdate();

            log.info("Enfileirado: " + entityType + "/" + operation + " - " + normalizedEntityKey);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao enfileirar item", e);
        } finally {
            DBUtil.closeAll(null, stmt, conn);
        }
    }

    /**
     * Verifica se ha item pendente similar (debounce).
     */
    private boolean hasPendingItem(Connection conn, String entityType,
                                   BigDecimal entityId, String entityKey) throws Exception {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(
                "SELECT 1 FROM AD_FCQUEUE WHERE " +
                "ENTITY_TYPE = ? AND STATUS IN ('PENDENTE', 'PROCESSANDO') " +
                "AND (ENTITY_ID = ? OR ENTITY_KEY = ?) " +
                "AND DH_CRIACAO > ?");

            stmt.setString(1, entityType);
            stmt.setBigDecimal(2, entityId);
            stmt.setString(3, entityKey);
            stmt.setTimestamp(4, new Timestamp(System.currentTimeMillis() - DEBOUNCE_WINDOW_MS));

            rs = stmt.executeQuery();
            return rs.next();
        } finally {
            DBUtil.closeResultSet(rs);
            DBUtil.closeStatement(stmt);
        }
    }

    private String normalizeEntityKey(String entityKey) {
        if (entityKey == null) return null;
        String normalized = entityKey.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    /**
     * Busca proximos itens pendentes para processamento.
     */
    public List<QueueItemDTO> fetchPendingItems(int batchSize) {
        List<QueueItemDTO> items = new ArrayList<>();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();

            stmt = conn.prepareStatement(
                "SELECT TOP (?) IDQUEUE, ENTITY_TYPE, OPERATION, ENTITY_ID, ENTITY_KEY, " +
                "PAYLOAD, STATUS, RETRY_COUNT, LAST_ERROR, DH_CRIACAO, PRIORITY " +
                "FROM AD_FCQUEUE " +
                "WHERE STATUS = ? " +
                "ORDER BY PRIORITY DESC, DH_CRIACAO ASC");

            stmt.setInt(1, batchSize);
            stmt.setString(2, FastchannelConstants.QUEUE_STATUS_PENDENTE);

            rs = stmt.executeQuery();

            while (rs.next()) {
                items.add(mapQueueItem(rs));
            }

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar itens pendentes", e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }

        return items;
    }

    /**
     * Busca itens pendentes por tipo de entidade.
     */
    public List<QueueItemDTO> fetchPendingByType(String entityType, int batchSize) {
        List<QueueItemDTO> items = new ArrayList<>();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();

            stmt = conn.prepareStatement(
                "SELECT TOP (?) IDQUEUE, ENTITY_TYPE, OPERATION, ENTITY_ID, ENTITY_KEY, " +
                "PAYLOAD, STATUS, RETRY_COUNT, LAST_ERROR, DH_CRIACAO, PRIORITY " +
                "FROM AD_FCQUEUE " +
                "WHERE STATUS = ? AND ENTITY_TYPE = ? " +
                "ORDER BY PRIORITY DESC, DH_CRIACAO ASC");

            stmt.setInt(1, batchSize);
            stmt.setString(2, FastchannelConstants.QUEUE_STATUS_PENDENTE);
            stmt.setString(3, entityType);

            rs = stmt.executeQuery();

            while (rs.next()) {
                items.add(mapQueueItem(rs));
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao buscar itens por tipo", e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
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
     * [TASK-2] Claim atomico: marca PENDENTE -> PROCESSANDO apenas se o item ainda estiver PENDENTE.
     *
     * Resolve race condition quando OutboxProcessorJob executa em paralelo (schedulerFixedDelay
     * nao garante serializacao entre execucoes se uma ficar lenta). O SELECT + UPDATE em
     * duas etapas permitia dois workers pegarem o mesmo IDQUEUE.
     *
     * Retorna true se o claim foi efetivo (este caller assumiu processamento),
     * false se outro thread venceu o race (caller deve pular o item silenciosamente).
     */
    public boolean tryMarkAsProcessing(BigDecimal idQueue) {
        if (idQueue == null) return false;
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                "UPDATE AD_FCQUEUE SET STATUS = ?, DH_PROCESSAMENTO = CURRENT_TIMESTAMP, " +
                "DH_ALTERACAO = CURRENT_TIMESTAMP " +
                "WHERE IDQUEUE = ? AND STATUS = ?");
            stmt.setString(1, FastchannelConstants.QUEUE_STATUS_PROCESSANDO);
            stmt.setBigDecimal(2, idQueue);
            stmt.setString(3, FastchannelConstants.QUEUE_STATUS_PENDENTE);
            int rows = stmt.executeUpdate();
            return rows > 0;
        } catch (Exception e) {
            log.log(Level.WARNING, "tryMarkAsProcessing falhou para IDQUEUE=" + idQueue, e);
            return false;
        } finally {
            DBUtil.closeAll(null, stmt, conn);
        }
    }

    /**
     * Marca item como enviado com sucesso.
     */
    public void markAsSuccess(BigDecimal idQueue) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DBUtil.getConnection();

            stmt = conn.prepareStatement(
                "UPDATE AD_FCQUEUE SET " +
                "STATUS = ?, DH_PROCESSAMENTO = CURRENT_TIMESTAMP, LAST_ERROR = NULL " +
                "WHERE IDQUEUE = ?");

            stmt.setString(1, FastchannelConstants.QUEUE_STATUS_ENVIADO);
            stmt.setBigDecimal(2, idQueue);

            stmt.executeUpdate();

            log.fine("Item " + idQueue + " marcado como ENVIADO");

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao marcar item como sucesso", e);
        } finally {
            DBUtil.closeAll(null, stmt, conn);
        }
    }

    /**
     * Marca item como erro (para retry).
     */
    public void markAsError(BigDecimal idQueue, String errorMessage) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DBUtil.getConnection();

            stmt = conn.prepareStatement(
                "UPDATE AD_FCQUEUE SET " +
                "STATUS = ?, RETRY_COUNT = RETRY_COUNT + 1, " +
                "LAST_ERROR = ?, DH_ALTERACAO = CURRENT_TIMESTAMP " +
                "WHERE IDQUEUE = ?");

            stmt.setString(1, FastchannelConstants.QUEUE_STATUS_ERRO);
            stmt.setString(2, truncate(errorMessage, 4000));
            stmt.setBigDecimal(3, idQueue);

            stmt.executeUpdate();

            log.warning("Item " + idQueue + " marcado como ERRO: " + errorMessage);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao marcar item como erro", e);
        } finally {
            DBUtil.closeAll(null, stmt, conn);
        }
    }

    /**
     * Marca item como erro fatal (nao vai mais ser reprocessado).
     */
    public void markAsFatalError(BigDecimal idQueue, String errorMessage) {
        updateStatus(idQueue, FastchannelConstants.QUEUE_STATUS_ERRO_FATAL, errorMessage);
        log.severe("Item " + idQueue + " marcado como ERRO_FATAL: " + errorMessage);
    }

    /**
     * Reativa itens com erro para reprocessamento.
     */
    public int reactivateErrorItems(int maxRetries) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DBUtil.getConnection();

            stmt = conn.prepareStatement(
                "UPDATE AD_FCQUEUE SET " +
                "STATUS = ?, DH_ALTERACAO = CURRENT_TIMESTAMP " +
                "WHERE STATUS = ? AND RETRY_COUNT < ?");

            stmt.setString(1, FastchannelConstants.QUEUE_STATUS_PENDENTE);
            stmt.setString(2, FastchannelConstants.QUEUE_STATUS_ERRO);
            stmt.setInt(3, maxRetries);

            int updated = stmt.executeUpdate();
            if (updated > 0) {
                log.info("Reativados " + updated + " itens para reprocessamento");
            }
            return updated;

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao reativar itens", e);
            return 0;
        } finally {
            DBUtil.closeAll(null, stmt, conn);
        }
    }

    /**
     * Conta todos os itens pendentes.
     */
    public int countPending() {
        return countByStatus(FastchannelConstants.QUEUE_STATUS_PENDENTE);
    }

    /**
     * Conta todos os itens com erro.
     */
    public int countErrors() {
        return countByStatus(FastchannelConstants.QUEUE_STATUS_ERRO);
    }

    private int countByStatus(String status) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();

            stmt = conn.prepareStatement(
                "SELECT COUNT(*) AS TOTAL FROM AD_FCQUEUE WHERE STATUS = ?");
            stmt.setString(1, status);

            rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("TOTAL");
            }

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao contar itens com status " + status, e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }

        return 0;
    }

    /**
     * Reseta item para reprocessamento (zera tentativas e status para PENDENTE).
     */
    public void resetForRetry(BigDecimal codQueue) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DBUtil.getConnection();

            stmt = conn.prepareStatement(
                "UPDATE AD_FCQUEUE SET " +
                "STATUS = ?, RETRY_COUNT = 0, LAST_ERROR = NULL, " +
                "DH_ALTERACAO = CURRENT_TIMESTAMP " +
                "WHERE IDQUEUE = ?");

            stmt.setString(1, FastchannelConstants.QUEUE_STATUS_PENDENTE);
            stmt.setBigDecimal(2, codQueue);

            stmt.executeUpdate();

            log.info("Item " + codQueue + " resetado para reprocessamento");

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao resetar item para retry", e);
            throw new RuntimeException("Falha ao resetar item: " + e.getMessage(), e);
        } finally {
            DBUtil.closeAll(null, stmt, conn);
        }
    }

    /**
     * Conta itens pendentes por tipo.
     */
    public int countPendingByType(String entityType) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();

            stmt = conn.prepareStatement(
                "SELECT COUNT(*) AS TOTAL FROM AD_FCQUEUE " +
                "WHERE STATUS = ? AND ENTITY_TYPE = ?");

            stmt.setString(1, FastchannelConstants.QUEUE_STATUS_PENDENTE);
            stmt.setString(2, entityType);

            rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("TOTAL");
            }

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao contar itens pendentes por tipo", e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }

        return 0;
    }

    /**
     * Limpa itens antigos ja processados.
     */
    public int cleanupOldItems(int daysToKeep) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DBUtil.getConnection();

            stmt = conn.prepareStatement(
                "DELETE FROM AD_FCQUEUE " +
                "WHERE STATUS IN ('ENVIADO', 'ERRO_FATAL', 'CANCELADO') " +
                "AND DH_CRIACAO < DATEADD(DAY, -?, CURRENT_TIMESTAMP)");

            stmt.setInt(1, daysToKeep);

            int deleted = stmt.executeUpdate();
            if (deleted > 0) {
                log.info("Removidos " + deleted + " itens antigos da fila");
            }
            return deleted;

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao limpar itens antigos", e);
            return 0;
        } finally {
            DBUtil.closeAll(null, stmt, conn);
        }
    }

    private void updateStatus(BigDecimal idQueue, String status, String errorMessage) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DBUtil.getConnection();

            stmt = conn.prepareStatement(
                "UPDATE AD_FCQUEUE SET " +
                "STATUS = ?, LAST_ERROR = ?, DH_ALTERACAO = CURRENT_TIMESTAMP " +
                "WHERE IDQUEUE = ?");

            stmt.setString(1, status);
            stmt.setString(2, truncate(errorMessage, 4000));
            stmt.setBigDecimal(3, idQueue);

            stmt.executeUpdate();

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao atualizar status", e);
        } finally {
            DBUtil.closeAll(null, stmt, conn);
        }
    }

    private QueueItemDTO mapQueueItem(ResultSet rs) throws Exception {
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
        return item;
    }

    private String truncate(String str, int maxLength) {
        if (str == null) return null;
        return str.length() > maxLength ? str.substring(0, maxLength) : str;
    }

    // ==================== METODOS DE CONVENIENCIA ====================

    /**
     * Enfileira atualizacao de produto.
     */
    public void enqueueProduct(BigDecimal codProd, String sku, String operation) {
        enqueue(FastchannelConstants.ENTITY_PRODUTO, operation, codProd, sku, null);
    }

    /**
     * Enfileira atualizacao de estoque (alta prioridade).
     */
    public void enqueueStock(BigDecimal codProd, String sku, BigDecimal quantity) {
        BigDecimal codEmp = config.getCodemp();
        BigDecimal codLocal = config.getCodLocal();
        enqueueStock(codProd, sku, quantity, codEmp, codLocal);
    }

    /**
     * Enfileira atualizacao de estoque com contexto de empresa/local.
     */
    public void enqueueStock(BigDecimal codProd, String sku, BigDecimal quantity,
                             BigDecimal codEmp, BigDecimal codLocal) {
        if (codEmp == null || codLocal == null) {
            log.warning("Estoque ignorado: CODEMP/CODLOCAL nao informados para SKU " + sku);
            return;
        }

        DeparaService deparaService = DeparaService.getInstance();
        String storageId = resolveStorageId(deparaService, codLocal);
        String resellerId = resolveResellerId(deparaService, codEmp);

        if (storageId == null || storageId.isEmpty()) {
            log.warning("Estoque ignorado: StorageId nao mapeado para CODLOCAL " + codLocal);
            return;
        }
        if (resellerId == null || resellerId.isEmpty()) {
            log.warning("Estoque ignorado: ResellerId nao mapeado para CODEMP " + codEmp);
            return;
        }

        String payload = buildStockPayload(sku, quantity, codEmp, codLocal, storageId, resellerId);
        enqueue(FastchannelConstants.ENTITY_ESTOQUE, FastchannelConstants.OPERATION_UPDATE,
                codProd, sku, payload, new BigDecimal(10)); // Prioridade alta
    }

    private String resolveStorageId(DeparaService deparaService, BigDecimal codLocal) {
        String storageId = deparaService.getCodigoExternoAtivo(DeparaService.TIPO_STOCK_STORAGE, codLocal);
        if ((storageId == null || storageId.isEmpty()) && isGlobalStorageFallbackEnabled()) {
            storageId = config.getStorageId();
        }
        return storageId;
    }

    private String resolveResellerId(DeparaService deparaService, BigDecimal codEmp) {
        String resellerId = deparaService.getCodigoExternoAtivo(DeparaService.TIPO_STOCK_RESELLER, codEmp);
        if ((resellerId == null || resellerId.isEmpty()) && isGlobalResellerFallbackEnabled()) {
            resellerId = config.getResellerId();
        }
        return resellerId;
    }

    private boolean isGlobalStorageFallbackEnabled() {
        String configured = System.getProperty("fastchannel.stock.allowGlobalStorageFallback");
        if (configured == null || configured.trim().isEmpty()) {
            configured = System.getenv("FASTCHANNEL_STOCK_ALLOW_GLOBAL_STORAGE_FALLBACK");
        }
        if (configured == null || configured.trim().isEmpty()) {
            return true;
        }
        return Boolean.parseBoolean(configured);
    }

    private boolean isGlobalResellerFallbackEnabled() {
        String configured = System.getProperty("fastchannel.stock.allowGlobalResellerFallback");
        if (configured == null || configured.trim().isEmpty()) {
            configured = System.getenv("FASTCHANNEL_STOCK_ALLOW_GLOBAL_RESELLER_FALLBACK");
        }
        if (configured == null || configured.trim().isEmpty()) {
            return true;
        }
        return Boolean.parseBoolean(configured);
    }

    /**
     * Enfileira atualizacao de preco.
     */
    public void enqueuePrice(BigDecimal codProd, String sku) {
        enqueue(FastchannelConstants.ENTITY_PRECO, FastchannelConstants.OPERATION_UPDATE,
                codProd, sku, null, new BigDecimal(5)); // Prioridade media
    }

    /**
     * Enfileira atualizacao de status de pedido (prioridade maxima).
     */
    public void enqueueOrderStatus(BigDecimal nuNota, String orderId, int status) {
        String payload = gson.toJson(new Object[]{ orderId, status });
        enqueue(FastchannelConstants.ENTITY_PEDIDO_STATUS, FastchannelConstants.OPERATION_UPDATE,
                nuNota, orderId, payload, new BigDecimal(100)); // Prioridade maxima
    }

    String buildStockPayload(String sku, BigDecimal quantity, BigDecimal codEmp, BigDecimal codLocal,
                             String storageId, String resellerId) {
        StockPayload payload = new StockPayload();
        payload.sku = sku;
        payload.quantity = quantity;
        payload.codEmp = codEmp;
        payload.codLocal = codLocal;
        payload.storageId = storageId;
        payload.resellerId = resellerId;
        return gson.toJson(payload);
    }

    private static final class StockPayload {
        private String sku;
        private BigDecimal quantity;
        private BigDecimal codEmp;
        private BigDecimal codLocal;
        private String storageId;
        private String resellerId;
    }
}
