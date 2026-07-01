package br.com.bellube.fastchannel.util;

import br.com.sankhya.modelcore.util.MGECoreParameter;

import java.math.BigDecimal;

/**
 * Acessor centralizado para parametros do addon Fastchannel lidos via
 * {@link MGECoreParameter} (parameter.xml).
 *
 * <p>Cada parametro possui um default hardcoded como fallback caso o registro
 * ainda nao tenha sido provisionado na base (primeiro deploy, ambiente novo,
 * etc.). Em producao, o valor efetivo e sempre o resolvido pelo
 * {@code MGECoreParameter}.</p>
 *
 * <p>IMPORTANTE: configs <b>runtime/secretas</b> (client_id, client_secret,
 * tokens renovaveis, endpoints por tenant) continuam em {@code AD_FCCONFIG}.
 * Esta classe cobre apenas os parametros <b>imutaveis/versionados</b> que
 * acompanham o addon.</p>
 */
public final class FastchannelParameters {

    private FastchannelParameters() {
        // Utility class
    }

    // ===================== STRING =====================

    public static String getApiUrl() {
        return getString("FC_API_URL", "https://api.commerce.fastchannel.com");
    }

    public static String getApiUrlSandbox() {
        return getString("FC_API_URL_SANDBOX", "https://api-sandbox.commerce.fastchannel.com");
    }

    public static String getOAuthUrl() {
        return getString("FC_OAUTH_URL",
                "https://login.microsoftonline.com/fastchannel.com/oauth2/v2.0/token");
    }

    public static String getOAuthScope() {
        return getString("FC_OAUTH_SCOPE", "api://fastchannel-commerce/.default");
    }

    // ===================== INTEGER =====================

    public static int getHttpTimeoutConnectMs() {
        return getInt("FC_HTTP_TIMEOUT_CONNECT_MS", 30_000);
    }

    public static int getHttpTimeoutReadMs() {
        return getInt("FC_HTTP_TIMEOUT_READ_MS", 60_000);
    }

    public static int getRetryMaxAttempts() {
        return getInt("FC_RETRY_MAX_ATTEMPTS", 3);
    }

    public static int getRetryBaseBackoffMs() {
        return getInt("FC_RETRY_BASE_BACKOFF_MS", 500);
    }

    public static int getBatchOrderImportSize() {
        return getInt("FC_BATCH_ORDER_IMPORT_SIZE", 50);
    }

    public static int getBatchPriceSyncSize() {
        return getInt("FC_BATCH_PRICE_SYNC_SIZE", 100);
    }

    public static int getBatchStockSyncSize() {
        return getInt("FC_BATCH_STOCK_SYNC_SIZE", 100);
    }

    public static int getCacheDeparaTtlMinutes() {
        return getInt("FC_CACHE_DEPARA_TTL_MINUTES", 15);
    }

    public static int getTokenRefreshBufferSeconds() {
        return getInt("FC_TOKEN_REFRESH_BUFFER_SECONDS", 300);
    }

    public static int getCircuitBreakerThreshold() {
        return getInt("FC_CIRCUIT_BREAKER_THRESHOLD", 5);
    }

    public static int getCircuitBreakerResetMs() {
        return getInt("FC_CIRCUIT_BREAKER_RESET_MS", 60_000);
    }

    public static int getRateLimitPerMinute() {
        return getInt("FC_RATE_LIMIT_PER_MINUTE", 30);
    }

    // ===================== HELPERS =====================

    /** Le uma String do MGECoreParameter; em qualquer erro retorna o default. */
    public static String getString(String name, String defaultValue) {
        try {
            String v = MGECoreParameter.getParameterAsString(name);
            if (v == null || v.trim().isEmpty()) {
                return defaultValue;
            }
            return v;
        } catch (Throwable t) {
            return defaultValue;
        }
    }

    /** Le um int do MGECoreParameter; em qualquer erro retorna o default. */
    public static int getInt(String name, int defaultValue) {
        try {
            String v = MGECoreParameter.getParameterAsString(name);
            if (v == null || v.trim().isEmpty()) {
                return defaultValue;
            }
            return Integer.parseInt(v.trim());
        } catch (Throwable t) {
            return defaultValue;
        }
    }

    /** Le um BigDecimal do MGECoreParameter; em qualquer erro retorna o default. */
    public static BigDecimal getBigDecimal(String name, BigDecimal defaultValue) {
        try {
            String v = MGECoreParameter.getParameterAsString(name);
            if (v == null || v.trim().isEmpty()) {
                return defaultValue;
            }
            return new BigDecimal(v.trim());
        } catch (Throwable t) {
            return defaultValue;
        }
    }

    /** Le um boolean do MGECoreParameter (S/N, true/false); default em erro. */
    public static boolean getBoolean(String name, boolean defaultValue) {
        try {
            String v = MGECoreParameter.getParameterAsString(name);
            if (v == null || v.trim().isEmpty()) {
                return defaultValue;
            }
            String t = v.trim();
            return "S".equalsIgnoreCase(t) || "Y".equalsIgnoreCase(t)
                    || "TRUE".equalsIgnoreCase(t) || "1".equals(t);
        } catch (Throwable ex) {
            return defaultValue;
        }
    }
}
