package br.com.bellube.fastchannel.service;

import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Servi?o de Log para integra??o Fastchannel.
 *
 * Registra opera??es na tabela AD_FCLOG para auditoria.
 */
public class LogService {

    private static final Logger log = Logger.getLogger(LogService.class.getName());
    private static LogService instance;

    // N?veis de log
    public static final String LEVEL_INFO = "INFO";
    public static final String LEVEL_WARNING = "WARNING";
    public static final String LEVEL_ERROR = "ERROR";
    public static final String LEVEL_DEBUG = "DEBUG";

    // Tipos de opera??o
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

    /**
     * Registra log de informa??o.
     */
    public void info(String operation, String message) {
        logEntry(LEVEL_INFO, operation, message, null, null);
    }

    /**
     * Registra log de informa??o com refer?ncia.
     */
    public void info(String operation, String message, String reference) {
        logEntry(LEVEL_INFO, operation, message, reference, null);
    }

    /**
     * Registra log de aviso.
     */
    public void warning(String operation, String message) {
        logEntry(LEVEL_WARNING, operation, message, null, null);
    }

    /**
     * Registra log de aviso com refer?ncia.
     */
    public void warning(String operation, String message, String reference) {
        logEntry(LEVEL_WARNING, operation, message, reference, null);
    }

    /**
     * Registra log de erro.
     */
    public void error(String operation, String message, Throwable exception) {
        String stackTrace = exception != null ? getStackTrace(exception) : null;
        logEntry(LEVEL_ERROR, operation, message, null, stackTrace);
    }

    /**
     * Registra log de erro com refer?ncia.
     */
    public void error(String operation, String message, String reference, Throwable exception) {
        String stackTrace = exception != null ? getStackTrace(exception) : null;
        logEntry(LEVEL_ERROR, operation, message, reference, stackTrace);
    }

    /**
     * Registra log de debug.
     */
    public void debug(String operation, String message) {
        logEntry(LEVEL_DEBUG, operation, message, null, null);
    }

    /**
     * Registra importa??o de pedido.
     */
    public void logOrderImport(String orderId, BigDecimal nuNota, boolean success, String details) {
        String message = success ?
                "Pedido " + orderId + " importado como NUNOTA " + nuNota :
                "Falha ao importar pedido " + orderId + ": " + details;
        logEntry(success ? LEVEL_INFO : LEVEL_ERROR, OP_ORDER_IMPORT, message, orderId, details);
    }

    /**
     * Registra sincroniza??o de estoque.
     */
    public void logStockSync(String sku, BigDecimal quantity, boolean success, String details) {
        String message = success ?
                "Estoque do SKU " + sku + " atualizado: " + quantity :
                "Falha ao atualizar estoque do SKU " + sku;
        logEntry(success ? LEVEL_INFO : LEVEL_ERROR, OP_STOCK_SYNC, message, sku, details);
    }

    /**
     * Registra sincroniza??o de pre?o.
     */
    public void logPriceSync(String sku, boolean success, String details) {
        String message = success ?
                "Pre?o do SKU " + sku + " atualizado" :
                "Falha ao atualizar pre?o do SKU " + sku;
        logEntry(success ? LEVEL_INFO : LEVEL_ERROR, OP_PRICE_SYNC, message, sku, details);
    }

    /**
     * Registra requisi??o HTTP.
     */
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

        // Persistir na tabela
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("INSERT INTO AD_FCLOG ");
            sql.appendSql("(NIVEL, OPERACAO, MENSAGEM, REFERENCIA, DETALHES, DH_REGISTRO) ");
            sql.appendSql("VALUES (:nivel, :operacao, :mensagem, :referencia, :detalhes, CURRENT_TIMESTAMP)");

            sql.setNamedParameter("nivel", level);
            sql.setNamedParameter("operacao", operation);
            sql.setNamedParameter("mensagem", truncate(message, 500));
            sql.setNamedParameter("referencia", truncate(reference, 100));
            sql.setNamedParameter("detalhes", truncate(details, 4000));

            sql.executeUpdate();

        } catch (Exception e) {
            // N?o propagar erro de log
            log.log(Level.WARNING, "Erro ao persistir log: " + e.getMessage());
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
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("DELETE FROM AD_FCLOG ");
            sql.appendSql("WHERE DH_REGISTRO < DATEADD(DAY, -:days, CURRENT_TIMESTAMP)");

            sql.setNamedParameter("days", daysToKeep);

            boolean success = sql.executeUpdate();
            if (success) {
                log.info("Remo??o de logs antigos executada com sucesso");
                // Contagem n?o dispon?vel na assinatura atual
                return 1;
            }
            return 0;

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao limpar logs antigos", e);
            return 0;
        }
    }
}

