package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.util.DBUtil;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Servico de Log para integracao Fastchannel.
 *
 * Registra operacoes na tabela AD_FCLOG para auditoria.
 * Usa JDBC direto (DBUtil) para funcionar sem JAPE/mge-core.
 */
public class LogService {

    private static final Logger log = Logger.getLogger(LogService.class.getName());
    private static LogService instance;

    // Niveis de log
    public static final String LEVEL_INFO = "INFO";
    public static final String LEVEL_WARNING = "WARNING";
    public static final String LEVEL_ERROR = "ERROR";
    public static final String LEVEL_DEBUG = "DEBUG";

    // Tipos de operacao
    public static final String OP_ORDER_IMPORT = "ORDER_IMPORT";
    public static final String OP_STOCK_SYNC = "STOCK_SYNC";
    public static final String OP_PRICE_SYNC = "PRICE_SYNC";
    public static final String OP_PRODUCT_SYNC = "PRODUCT_SYNC";
    public static final String OP_QUEUE_PROCESS = "QUEUE_PROCESS";
    public static final String OP_AUTH = "AUTH";
    public static final String OP_HTTP = "HTTP";
    public static final String OP_GENERAL = "GENERAL";

    private LogService() {
    }

    public static synchronized LogService getInstance() {
        if (instance == null) {
            instance = new LogService();
        }
        return instance;
    }

    public void info(String operation, String message) {
        logEntry(LEVEL_INFO, operation, message, null, null);
    }

    public void info(String operation, String message, String reference) {
        logEntry(LEVEL_INFO, operation, message, reference, null);
    }

    public void warning(String operation, String message) {
        logEntry(LEVEL_WARNING, operation, message, null, null);
    }

    public void warning(String operation, String message, String reference) {
        logEntry(LEVEL_WARNING, operation, message, reference, null);
    }

    public void error(String operation, String message, Throwable exception) {
        String stackTrace = exception != null ? getStackTrace(exception) : null;
        logEntry(LEVEL_ERROR, operation, message, null, stackTrace);
    }

    public void error(String operation, String message, String reference, Throwable exception) {
        String stackTrace = exception != null ? getStackTrace(exception) : null;
        logEntry(LEVEL_ERROR, operation, message, reference, stackTrace);
    }

    public void debug(String operation, String message) {
        logEntry(LEVEL_DEBUG, operation, message, null, null);
    }

    public void logOrderImport(String orderId, BigDecimal nuNota, boolean success, String details) {
        String message = success ?
                "Pedido " + orderId + " importado como NUNOTA " + nuNota :
                "Falha ao importar pedido " + orderId + ": " + details;
        logEntry(success ? LEVEL_INFO : LEVEL_ERROR, OP_ORDER_IMPORT, message, orderId, details);
    }

    public void logOrderRetrySkipped(String orderId, String reason) {
        String message = "Retry ignorado para pedido " + orderId + ": " + reason;
        logEntry(LEVEL_INFO, OP_ORDER_IMPORT, message, orderId, null);
    }

    public void logStockSync(String sku, BigDecimal quantity, boolean success, String details) {
        String message = success ?
                "Estoque do SKU " + sku + " atualizado: " + quantity :
                "Falha ao atualizar estoque do SKU " + sku;
        logEntry(success ? LEVEL_INFO : LEVEL_ERROR, OP_STOCK_SYNC, message, sku, details);
    }

    public void logPriceSync(String sku, boolean success, String details) {
        String message = success ?
                "Preco do SKU " + sku + " atualizado" :
                "Falha ao atualizar preco do SKU " + sku;
        logEntry(success ? LEVEL_INFO : LEVEL_ERROR, OP_PRICE_SYNC, message, sku, details);
    }

    public void logHttpRequest(String method, String url, int statusCode, String responseBody) {
        String level = statusCode >= 200 && statusCode < 300 ? LEVEL_DEBUG : LEVEL_ERROR;
        String message = method + " " + url + " -> " + statusCode;
        logEntry(level, OP_HTTP, message, String.valueOf(statusCode), truncate(responseBody, 2000));
    }

    private void logEntry(String level, String operation, String message,
                          String reference, String details) {
        // Log no console Java
        if (LEVEL_ERROR.equals(level)) {
            log.severe("[" + operation + "] " + message);
        } else if (LEVEL_WARNING.equals(level)) {
            log.warning("[" + operation + "] " + message);
        } else if (LEVEL_INFO.equals(level)) {
            log.info("[" + operation + "] " + message);
        } else {
            log.fine("[" + operation + "] " + message);
        }

        // Persistir na tabela via JDBC direto
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DBUtil.getConnection();

            stmt = conn.prepareStatement(
                "INSERT INTO AD_FCLOG " +
                "(NIVEL, OPERACAO, MENSAGEM, REFERENCIA, DETALHES, DH_REGISTRO) " +
                "VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)");

            stmt.setString(1, level);
            stmt.setString(2, operation);
            stmt.setString(3, truncate(message, 500));
            stmt.setString(4, truncate(reference, 100));
            stmt.setString(5, truncate(details, 4000));

            stmt.executeUpdate();

        } catch (Exception e) {
            // Nao propagar erro de log - apenas registrar no console
            log.log(Level.FINE, "Falha ao persistir log na AD_FCLOG: " + e.getMessage());
        } finally {
            DBUtil.closeAll(null, stmt, conn);
        }
    }

    private String getStackTrace(Throwable t) {
        StringBuilder sb = new StringBuilder();
        sb.append(t.getClass().getName()).append(": ").append(t.getMessage()).append("\n");
        for (StackTraceElement elem : t.getStackTrace()) {
            sb.append("  at ").append(elem.toString()).append("\n");
            if (sb.length() > 3500) {
                sb.append("  ... truncated");
                break;
            }
        }
        return sb.toString();
    }

    private String truncate(String str, int maxLength) {
        if (str == null) return null;
        return str.length() > maxLength ? str.substring(0, maxLength) : str;
    }

    /**
     * Limpa logs antigos.
     */
    public int cleanupOldLogs(int daysToKeep) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DBUtil.getConnection();

            stmt = conn.prepareStatement(
                "DELETE FROM AD_FCLOG " +
                "WHERE DH_REGISTRO < DATEADD(DAY, -?, CURRENT_TIMESTAMP)");

            stmt.setInt(1, daysToKeep);

            int deleted = stmt.executeUpdate();
            if (deleted > 0) {
                log.info("Removidos " + deleted + " logs antigos");
            }
            return deleted;

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao limpar logs antigos", e);
            return 0;
        } finally {
            DBUtil.closeAll(null, stmt, conn);
        }
    }
}
