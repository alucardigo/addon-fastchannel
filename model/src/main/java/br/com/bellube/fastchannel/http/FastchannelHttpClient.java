package br.com.bellube.fastchannel.http;

import br.com.bellube.fastchannel.auth.FastchannelTokenManager;
import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.exception.FastchannelAuthException;
import br.com.bellube.fastchannel.exception.FastchannelException;
import br.com.bellube.fastchannel.exception.FastchannelFatalException;
import br.com.bellube.fastchannel.exception.FastchannelRateLimitException;
import br.com.bellube.fastchannel.exception.FastchannelTransientException;
import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Cliente HTTP para comunicacao com a API Fastchannel Commerce.
 *
 * Caracteristicas:
 * - Rate Limiting com sliding window
 * - Retry com exponential backoff
 * - Renovacao automatica de token em 401
 * - Headers Ocp-Apim-Subscription-Key
 * - Thread-safe
 */
public class FastchannelHttpClient {

    private static final Logger log = Logger.getLogger(FastchannelHttpClient.class.getName());
    private static final Gson gson = new Gson();

    private final FastchannelTokenManager tokenManager;
    private final FastchannelConfig config;
    private final int timeoutMs;

    // Rate Limiting - sliding window
    private final AtomicInteger requestCount = new AtomicInteger(0);
    private final AtomicLong windowStart = new AtomicLong(System.currentTimeMillis());
    private static final long WINDOW_SIZE_MS = 60_000; // 1 minuto

    // Retry config
    private static final int MAX_RETRIES = FastchannelConstants.DEFAULT_MAX_RETRIES;
    private static final long INITIAL_BACKOFF_MS = 500;
    private static final double BACKOFF_MULTIPLIER = 2.0;
    private static final double JITTER_FACTOR = 0.2; // +/-20%

    // Observabilidade - contadores por metodo
    private static final AtomicInteger totalRetriesCount = new AtomicInteger(0);
    private static final AtomicInteger totalRequestsCount = new AtomicInteger(0);
    private static volatile SSLSocketFactory insecureSslSocketFactory;
    private static volatile HostnameVerifier insecureHostnameVerifier;

    public FastchannelHttpClient() {
        this(FastchannelConstants.DEFAULT_TIMEOUT_SECONDS);
    }

    public FastchannelHttpClient(int timeoutSeconds) {
        this.timeoutMs = timeoutSeconds * 1000;
        this.tokenManager = FastchannelTokenManager.getInstance();
        this.config = FastchannelConfig.getInstance();
    }

    /**
     * GET request para Order Management API.
     */
    public HttpResult getOrders(String endpoint) throws Exception {
        String url = buildOrderUrl(endpoint);
        return executeWithRetry("GET", url, null, config.getSubscriptionKeyDistribution());
    }

    /**
     * POST request para Order Management API.
     */
    public HttpResult postOrders(String endpoint, String jsonBody) throws Exception {
        String url = buildOrderUrl(endpoint);
        return executeWithRetry("POST", url, jsonBody, config.getSubscriptionKeyDistribution());
    }

    /**
     * PUT request para Order Management API.
     */
    public HttpResult putOrders(String endpoint, String jsonBody) throws Exception {
        String url = buildOrderUrl(endpoint);
        return executeWithRetry("PUT", url, jsonBody, config.getSubscriptionKeyDistribution());
    }

    private String buildOrderUrl(String endpoint) {
        String configuredBase = config.getBaseUrl();
        if (configuredBase != null && configuredBase.contains("/order-management/")) {
            return configuredBase + endpoint;
        }
        return FastchannelConstants.ORDER_API_BASE + endpoint;
    }

    /**
     * GET request para Stock Management API.
     */
    public HttpResult getStock(String endpoint) throws Exception {
        String url = FastchannelConstants.STOCK_API_BASE + endpoint;
        // Legado usa chave de distribuicao para rotas de estoque.
        return executeWithRetry("GET", url, null, config.getSubscriptionKeyDistribution());
    }

    /**
     * PUT request para Stock Management API.
     */
    public HttpResult putStock(String endpoint, String jsonBody) throws Exception {
        String url = FastchannelConstants.STOCK_API_BASE + endpoint;
        // Legado usa chave de distribuicao para rotas de estoque.
        return executeWithRetry("PUT", url, jsonBody, config.getSubscriptionKeyDistribution());
    }

    /**
     * GET request para Price Management API.
     */
    public HttpResult getPrice(String endpoint) throws Exception {
        return getPrice(endpoint, config.getSubscriptionKeyConsumption());
    }

    /**
     * PUT request para Price Management API.
     */
    public HttpResult putPrice(String endpoint, String jsonBody) throws Exception {
        return putPrice(endpoint, jsonBody, config.getSubscriptionKeyConsumption());
    }

    /**
     * POST request para Price Management API (batches).
     */
    public HttpResult postPrice(String endpoint, String jsonBody) throws Exception {
        return postPrice(endpoint, jsonBody, config.getSubscriptionKeyConsumption());
    }

    public HttpResult getPrice(String endpoint, String subscriptionKey) throws Exception {
        String url = FastchannelConstants.PRICE_API_BASE + endpoint;
        return executeWithRetry("GET", url, null, subscriptionKey);
    }

    public HttpResult putPrice(String endpoint, String jsonBody, String subscriptionKey) throws Exception {
        String url = FastchannelConstants.PRICE_API_BASE + endpoint;
        return executeWithRetry("PUT", url, jsonBody, subscriptionKey);
    }

    public HttpResult postPrice(String endpoint, String jsonBody, String subscriptionKey) throws Exception {
        String url = FastchannelConstants.PRICE_API_BASE + endpoint;
        return executeWithRetry("POST", url, jsonBody, subscriptionKey);
    }

    public HttpResult deletePrice(String endpoint, String subscriptionKey) throws Exception {
        String url = FastchannelConstants.PRICE_API_BASE + endpoint;
        return executeWithRetry("DELETE", url, null, subscriptionKey);
    }

    /**
     * Executa requisicao com retry e exponential backoff + jitter.
     * Apenas erros TRANSIENTES sao retentados (5xx, 429, timeouts, connection reset).
     * Erros CLIENTES (4xx != 429) retornam imediatamente sem retry.
     */
    private HttpResult executeWithRetry(String method, String url, String jsonBody, String subscriptionKey) throws Exception {
        Exception lastException = null;
        totalRequestsCount.incrementAndGet();
        long startTime = System.currentTimeMillis();

        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            try {
                // Rate limiting
                waitForRateLimit();

                String token = tokenManager.getValidToken();
                long callStart = System.currentTimeMillis();
                HttpResult result = doHttpCall(method, url, token, jsonBody, subscriptionKey);
                long callMs = System.currentTimeMillis() - callStart;

                log.info("HTTP " + method + " " + url + " -> " + result.getStatusCode() + " em " + callMs + "ms (attempt " + (attempt + 1) + ")");

                // 401 -> renovar token e retry (nao conta como erro transiente retryable)
                if (result.getStatusCode() == 401 && attempt < MAX_RETRIES) {
                    log.warning("Recebido 401. Renovando token Fastchannel...");
                    tokenManager.forceRenew();
                    totalRetriesCount.incrementAndGet();
                    continue;
                }

                // Erros transientes HTTP (429, 5xx) -> retry com backoff
                if (isRetryable(null, result.getStatusCode()) && attempt < MAX_RETRIES) {
                    long delay = calculateBackoff(attempt);
                    log.warning(String.format("HTTP retry %d/%d em %dms para %s %s (status=%d)",
                        attempt + 1, MAX_RETRIES, delay, method, url, result.getStatusCode()));
                    totalRetriesCount.incrementAndGet();
                    Thread.sleep(delay);
                    continue;
                }

                // Sucesso ou erro cliente (4xx != 429) -> retorna sem retry
                return result;

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw ie;
            } catch (Exception e) {
                lastException = e;
                // So retenta se for excecao transiente (timeout, connect, reset)
                if (!isRetryable(e, null)) {
                    log.log(Level.WARNING, "Erro NAO retryable em " + method + " " + url, e);
                    throw e;
                }
                if (attempt < MAX_RETRIES) {
                    long delay = calculateBackoff(attempt);
                    log.log(Level.WARNING, String.format("HTTP retry %d/%d em %dms para %s %s (excecao=%s)",
                        attempt + 1, MAX_RETRIES, delay, method, url, e.getClass().getSimpleName()), e);
                    totalRetriesCount.incrementAndGet();
                    Thread.sleep(delay);
                }
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        log.severe(String.format("HTTP FALHA %s %s apos %d tentativas em %dms (retries=%d/requests=%d)",
            method, url, MAX_RETRIES + 1, elapsed,
            totalRetriesCount.get(), totalRequestsCount.get()));
        String msg = "Falha apos " + MAX_RETRIES + " tentativas: " +
            (lastException != null ? lastException.getMessage() : "unknown error");
        // Preserva tipo original se for FastchannelException; caso contrario classifica como transient.
        if (lastException instanceof FastchannelException) {
            throw (FastchannelException) lastException;
        }
        throw new FastchannelTransientException(msg, lastException);
    }

    /**
     * Classifica status HTTP em excecao tipada.
     * 401/403 -> Auth, 429 -> RateLimit, 5xx -> Transient, 4xx -> Fatal.
     */
    private FastchannelException classifyHttpError(int statusCode, String body, Long retryAfterSeconds) {
        String safeBody = body == null ? "" : body;
        if (statusCode == 401 || statusCode == 403) {
            return new FastchannelAuthException("Auth falhou: " + statusCode + " " + safeBody, statusCode);
        }
        if (statusCode == 429) {
            return new FastchannelRateLimitException("Rate limited: " + safeBody, retryAfterSeconds);
        }
        if (statusCode >= 500 && statusCode < 600) {
            return new FastchannelTransientException("Servidor Fastchannel erro " + statusCode + ": " + safeBody, statusCode, null);
        }
        if (statusCode >= 400) {
            return new FastchannelFatalException("Request invalido " + statusCode + ": " + safeBody, statusCode);
        }
        return new FastchannelException("Status inesperado " + statusCode + ": " + safeBody, statusCode);
    }

    /**
     * Classifica se um erro eh transiente (retryable) ou nao.
     * Transientes: 5xx, 429, SocketTimeoutException, ConnectException, "Connection reset".
     * Nao retryable: 4xx (exceto 429), erros de negocio.
     */
    private boolean isRetryable(Exception ex, Integer statusCode) {
        if (ex != null) {
            // Tipagem nova: decide pelo tipo da excecao Fastchannel
            if (ex instanceof FastchannelTransientException) return true;
            if (ex instanceof FastchannelRateLimitException) return true;
            if (ex instanceof FastchannelAuthException) return false; // token refresh e fluxo separado
            if (ex instanceof FastchannelFatalException) return false;
            // Rede baixa-nivel
            if (ex instanceof java.net.SocketTimeoutException) return true;
            if (ex instanceof java.net.ConnectException) return true;
            if (ex instanceof java.net.NoRouteToHostException) return true;
            if (ex instanceof java.net.UnknownHostException) return true;
            String msg = ex.getMessage();
            if (ex instanceof java.io.IOException && msg != null &&
                (msg.contains("Connection reset") || msg.contains("Connection refused")
                 || msg.contains("timed out") || msg.contains("Broken pipe"))) {
                return true;
            }
        }
        if (statusCode != null) {
            if (statusCode == 429) return true;
            if (statusCode >= 500 && statusCode < 600) return true;
        }
        return false;
    }

    /**
     * Backoff exponencial com jitter (+/-20%).
     * attempt=0 -> ~500ms, 1 -> ~1000ms, 2 -> ~2000ms, 3 -> ~4000ms
     */
    private long calculateBackoff(int attempt) {
        long base = (long) (INITIAL_BACKOFF_MS * Math.pow(BACKOFF_MULTIPLIER, Math.min(attempt, 5)));
        double jitter = (1.0 - JITTER_FACTOR) + (Math.random() * 2 * JITTER_FACTOR);
        return (long) (base * jitter);
    }

    /**
     * Retorna metricas de retry para observabilidade.
     */
    public static String getRetryStats() {
        return String.format("requests=%d retries=%d", totalRequestsCount.get(), totalRetriesCount.get());
    }

    /**
     * Implementa rate limiting com sliding window.
     */
    private synchronized void waitForRateLimit() throws InterruptedException {
        long now = System.currentTimeMillis();
        long windowStartTime = windowStart.get();

        // Reset window se passou de 1 minuto
        if (now - windowStartTime >= WINDOW_SIZE_MS) {
            windowStart.set(now);
            requestCount.set(0);
        }

        int maxRequests = config.getMaxRequestsPerMinute();
        int currentCount = requestCount.get();

        // Se atingiu limite, esperar ate proxima janela
        if (currentCount >= maxRequests) {
            long waitTime = WINDOW_SIZE_MS - (now - windowStart.get());
            if (waitTime > 0) {
                log.info("Rate limit atingido. Aguardando " + waitTime + "ms...");
                Thread.sleep(waitTime);
                windowStart.set(System.currentTimeMillis());
                requestCount.set(0);
            }
        }

        requestCount.incrementAndGet();
    }

    private HttpResult doHttpCall(String method, String urlString, String token, String jsonBody, String subscriptionKey) throws Exception {
        HttpURLConnection connection = null;
        try {
            // Append subscription-key as query param (required by FC APIM gateway)
            String finalUrl = urlString + (urlString.contains("?") ? "&" : "?") + "subscription-key=" + subscriptionKey;
            URL url = new URL(finalUrl);
            connection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
            configureSslIfNeeded(connection);

            connection.setRequestMethod(method);
            connection.setConnectTimeout(timeoutMs);
            connection.setReadTimeout(timeoutMs);

            // Headers padrao Fastchannel
            connection.setRequestProperty("Authorization", "Bearer " + token);
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty(getSubscriptionHeaderName(), subscriptionKey);  // "Subscription-Key"
            connection.setRequestProperty("Ocp-Apim-Subscription-Key", subscriptionKey);
            // NOTA 2026-04-20: removido setRequestProperty("subscription-key", ...) pois HTTP
            // header names sao case-insensitive => "Subscription-Key" + "subscription-key"
            // viram o MESMO header com valor duplicado "X,X", que o gateway Vertis rejeita
            // com 403 "chave de assinatura '...,...' informada nao e valida".
            // Confirmado via curl: com o header UNICO, API responde 200 em /prices?PriceTableId=X.

            if (jsonBody != null) {
                connection.setRequestProperty("Content-Type", "application/json");
                connection.setDoOutput(true);
                try (OutputStream os = connection.getOutputStream()) {
                    byte[] input = jsonBody.getBytes(StandardCharsets.UTF_8);
                    os.write(input, 0, input.length);
                }
            }

            int statusCode = connection.getResponseCode();
            String responseBody;

            if (statusCode >= 200 && statusCode < 300) {
                responseBody = readStream(connection.getInputStream());
            } else {
                responseBody = readStream(connection.getErrorStream());
            }

            // Parse Retry-After header (segundos). Ignora formato HTTP-date por enquanto.
            Long retryAfterSeconds = null;
            String retryAfter = connection.getHeaderField("Retry-After");
            if (retryAfter != null && !retryAfter.trim().isEmpty()) {
                try {
                    retryAfterSeconds = Long.parseLong(retryAfter.trim());
                } catch (NumberFormatException nfe) {
                    // ignore - formato date nao suportado aqui
                }
            }

            log.fine(method + " " + urlString + " -> " + statusCode);

            // Classifica e loga tipo do erro (nao lanca — mantem fluxo antigo via HttpResult)
            if (statusCode >= 400) {
                FastchannelException classified = classifyHttpError(statusCode, responseBody, retryAfterSeconds);
                log.warning("HTTP " + statusCode + " classificado como " + classified.getClass().getSimpleName()
                    + " em " + method + " " + urlString);
            }
            return new HttpResult(statusCode, responseBody, retryAfterSeconds);

        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static String getSubscriptionHeaderName() {
        return "Subscription-Key";
    }

    private void configureSslIfNeeded(HttpURLConnection connection) throws Exception {
        if (!(connection instanceof HttpsURLConnection)) {
            return;
        }

        String configured = System.getProperty("fastchannel.ssl.insecure");
        if (configured == null || configured.trim().isEmpty()) {
            configured = System.getenv("FASTCHANNEL_SSL_INSECURE");
        }
        boolean insecure = configured != null && !configured.trim().isEmpty() && Boolean.parseBoolean(configured);
        if (!insecure) {
            return;
        }

        if (insecureSslSocketFactory == null || insecureHostnameVerifier == null) {
            synchronized (FastchannelHttpClient.class) {
                if (insecureSslSocketFactory == null || insecureHostnameVerifier == null) {
                    TrustManager[] trustAll = new TrustManager[]{new X509TrustManager() {
                        public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                        public void checkClientTrusted(X509Certificate[] chain, String authType) {}
                        public void checkServerTrusted(X509Certificate[] chain, String authType) {}
                    }};
                    SSLContext sc = SSLContext.getInstance("TLS");
                    sc.init(null, trustAll, new SecureRandom());
                    insecureSslSocketFactory = sc.getSocketFactory();
                    insecureHostnameVerifier = (hostname, session) -> true;
                }
            }
        }

        HttpsURLConnection https = (HttpsURLConnection) connection;
        https.setSSLSocketFactory(insecureSslSocketFactory);
        https.setHostnameVerifier(insecureHostnameVerifier);
    }

    private String readStream(java.io.InputStream stream) throws Exception {
        if (stream == null) return "";
        try (BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            return br.lines().collect(Collectors.joining());
        }
    }

    /**
     * Wrapper para resultado HTTP.
     */
    public static class HttpResult {
        private final int statusCode;
        private final String body;
        private final Long retryAfterSeconds;

        public HttpResult(int statusCode, String body) {
            this(statusCode, body, null);
        }

        public HttpResult(int statusCode, String body, Long retryAfterSeconds) {
            this.statusCode = statusCode;
            this.body = body;
            this.retryAfterSeconds = retryAfterSeconds;
        }

        public Long getRetryAfterSeconds() {
            return retryAfterSeconds;
        }

        public int getStatusCode() {
            return statusCode;
        }

        public String getBody() {
            return body;
        }

        public boolean isSuccess() {
            return statusCode >= 200 && statusCode < 300;
        }

        public boolean isClientError() {
            return statusCode >= 400 && statusCode < 500;
        }

        public boolean isServerError() {
            return statusCode >= 500;
        }

        public String getErrorMessage() {
            if (body == null || body.isEmpty()) {
                return "HTTP " + statusCode;
            }
            return body;
        }
    }
}
