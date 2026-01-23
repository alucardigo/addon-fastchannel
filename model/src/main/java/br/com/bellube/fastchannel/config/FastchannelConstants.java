package br.com.bellube.fastchannel.config;

/**
 * Constantes para integração Fastchannel Commerce.
 *
 * Centraliza URLs, endpoints, e configurações estáticas.
 * Baseado no legado gbi-app-integrador.
 */
public final class FastchannelConstants {

    private FastchannelConstants() {
        // Utility class
    }

    // ==================== URLS BASE ====================

    /** URL de autenticação Azure AD para Fastchannel */
    public static final String AUTH_URL = "https://login.microsoftonline.com/fastchannel.com/oauth2/v2.0/token";

    /** URL base da API de Order Management */
    public static final String ORDER_API_BASE = "https://api.commerce.fastchannel.com/order-management/v1";

    /** URL base da API de Stock Management */
    public static final String STOCK_API_BASE = "https://api.commerce.fastchannel.com/stock-management/v1";

    /** URL base da API de Price Management */
    public static final String PRICE_API_BASE = "https://api.commerce.fastchannel.com/price-management/v1";

    // ==================== ENDPOINTS ====================

    // Orders
    public static final String ENDPOINT_ORDERS = "/orders";
    public static final String ENDPOINT_ORDER_STATUS = "/orders/%s/status";
    public static final String ENDPOINT_ORDER_SYNC = "/orders/%s/sync";
    public static final String ENDPOINT_ORDER_INVOICES = "/orders/%s/invoices";
    public static final String ENDPOINT_ORDER_TRACKING = "/orders/%s/tracking";

    // Stock
    public static final String ENDPOINT_STOCK = "/stock/%s";

    // Price
    public static final String ENDPOINT_PRICE = "/prices/%s";
    public static final String ENDPOINT_PRICE_BATCHES = "/prices/%s/batches";

    // ==================== STATUS CODES FASTCHANNEL ====================

    public static final int STATUS_CREATED = 200;
    public static final int STATUS_APPROVED = 201;
    public static final int STATUS_INVOICE_CREATED = 300;
    public static final int STATUS_DELIVERED = 301;
    public static final int STATUS_RETURNED = 303;
    public static final int STATUS_DENIED = 400;

    // ==================== TABELAS SANKHYA ====================

    public static final String TABLE_CONFIG = "AD_FCCONFIG";
    public static final String TABLE_QUEUE = "AD_FCQUEUE";
    public static final String TABLE_DEPARA = "AD_FCDEPARA";
    public static final String TABLE_PEDIDO = "AD_FCPEDIDO";
    public static final String TABLE_LOG = "AD_FCLOG";

    // ==================== DEFAULTS ====================

    public static final int DEFAULT_BATCH_SIZE = 50;
    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final int DEFAULT_TIMEOUT_SECONDS = 30;
    public static final int DEFAULT_RATE_LIMIT_PER_MINUTE = 30;
    public static final int TOKEN_REFRESH_BUFFER_SECONDS = 300; // 5 minutos antes de expirar

    // ==================== TIPOS DE ENTIDADE (FILA) ====================

    public static final String ENTITY_PRODUTO = "PRODUTO";
    public static final String ENTITY_ESTOQUE = "ESTOQUE";
    public static final String ENTITY_PRECO = "PRECO";
    public static final String ENTITY_PEDIDO_STATUS = "PEDIDO_STATUS";
    public static final String ENTITY_PARCEIRO = "PARCEIRO";

    // ==================== STATUS DA FILA ====================

    public static final String QUEUE_STATUS_PENDENTE = "PENDENTE";
    public static final String QUEUE_STATUS_PROCESSANDO = "PROCESSANDO";
    public static final String QUEUE_STATUS_ENVIADO = "ENVIADO";
    public static final String QUEUE_STATUS_ERRO = "ERRO";
    public static final String QUEUE_STATUS_ERRO_FATAL = "ERRO_FATAL";
    public static final String QUEUE_STATUS_CANCELADO = "CANCELADO";

    // ==================== OPERAÇÕES ====================

    public static final String OPERATION_CREATE = "CREATE";
    public static final String OPERATION_UPDATE = "UPDATE";
    public static final String OPERATION_DELETE = "DELETE";
}
