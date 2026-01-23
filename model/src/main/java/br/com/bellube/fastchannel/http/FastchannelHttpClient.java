package br.com.bellube.fastchannel.http;

import br.com.bellube.fastchannel.auth.FastchannelTokenManager;
import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Cliente HTTP para comunicação com a API Fastchannel Commerce.
 *
 * Características:
 * - Rate Limiting com sliding window
 * - Retry com exponential backoff
 * - Renovação automática de token em 401
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
    private static final long INITIAL_BACKOFF_MS = 1000;
    private static final double BACKOFF_MULTIPLIER = 2.0;

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
        String url = config.getBaseUrl() + endpoint;
        return executeWithRetry("GET", url, null, config.getSubscriptionKeyDistribution());
    }

    /**
     * POST request para Order Management API.
     */
    public HttpResult postOrders(String endpoint, String jsonBody) throws Exception {
        String url = config.getBaseUrl() + endpoint;
        return executeWithRetry("POST", url, jsonBody, config.getSubscriptionKeyDistribution());
    }

    /**
     * PUT request para Order Management API.
     */
    public HttpResult putOrders(String endpoint, String jsonBody) throws Exception {
        String url = config.getBaseUrl() + endpoint;
        return executeWithRetry("PUT", url, jsonBody, config.getSubscriptionKeyDistribution());
    }

    /**
     * GET request para Stock Management API.
     */
    public HttpResult getStock(String endpoint) throws Exception {
        String url = FastchannelConstants.STOCK_API_BASE + endpoint;
        return executeWithRetry("GET", url, null, config.getSubscriptionKeyConsumption());
    }

    /**
     * PUT request para Stock Management API.
     */
    public HttpResult putStock(String endpoint, String jsonBody) throws Exception {
        String url = FastchannelConstants.STOCK_API_BASE + endpoint;
        return executeWithRetry("PUT", url, jsonBody, config.getSubscriptionKeyConsumption());
    }

    /**
     * GET request para Price Management API.
     */
    public HttpResult getPrice(String endpoint) throws Exception {
        String url = FastchannelConstants.PRICE_API_BASE + endpoint;
        return executeWithRetry("GET", url, null, config.getSubscriptionKeyConsumption());
    }

    /**
     * PUT request para Price Management API.
     */
    public HttpResult putPrice(String endpoint, String jsonBody) throws Exception {
        String url = FastchannelConstants.PRICE_API_BASE + endpoint;
        return executeWithRetry("PUT", url, jsonBody, config.getSubscriptionKeyConsumption());
    }

    /**
     * POST request para Price Management API (batches).
     */
    public HttpResult postPrice(String endpoint, String jsonBody) throws Exception {
        String url = FastchannelConstants.PRICE_API_BASE + endpoint;
        return executeWithRetry("POST", url, jsonBody, config.getSubscriptionKeyConsumption());
    }

    /**
     * Executa requisição com retry e exponential backoff.
     */
    private HttpResult executeWithRetry(String method, String url, String jsonBody, String subscriptionKey) throws Exception {
        Exception lastException = null;
        long backoff = INITIAL_BACKOFF_MS;

        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            try {
                // Rate limiting
                waitForRateLimit();

                String token = tokenManager.getValidToken();
                HttpResult result = doHttpCall(method, url, token, jsonBody, subscriptionKey);

                // Se 401, tentar renovar token e repetir UMA vez
                if (result.getStatusCode() == 401 && attempt < MAX_RETRIES) {
                    log.warning("Recebido 401. Renovando token Fastchannel...");
                    tokenManager.forceRenew();
                    continue;
                }

                // Se 429 (rate limited), esperar e tentar novamente
                if (result.getStatusCode() == 429 && attempt < MAX_RETRIES) {
                    log.warning("Rate limited (429). Aguardando " + backoff + "ms...");
                    Thread.sleep(backoff);
                    backoff = (long) (backoff * BACKOFF_MULTIPLIER);
                    continue;
                }

                // Se 5xx, retry com backoff
                if (result.getStatusCode() >= 500 && attempt < MAX_RETRIES) {
                    log.warning("Erro servidor (" + result.getStatusCode() + "). Retry em " + backoff + "ms...");
                    Thread.sleep(backoff);
                    backoff = (long) (backoff * BACKOFF_MULTIPLIER);
                    continue;
                }

                return result;

            } catch (Exception e) {
                lastException = e;
                if (attempt < MAX_RETRIES) {
                    log.warning("Erro na tentativa " + (attempt + 1) + ": " + e.getMessage() + ". Retry em " + backoff + "ms...");
                    Thread.sleep(backoff);
                    backoff = (long) (backoff * BACKOFF_MULTIPLIER);
                }
            }
        }

        throw new Exception("Falha após " + MAX_RETRIES + " tentativas: " +
            (lastException != null ? lastException.getMessage() : "unknown error"), lastException);
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

        // Se atingiu limite, esperar até próxima janela
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
            URL url = new URL(urlString);
            connection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);

            connection.setRequestMethod(method);
            connection.setConnectTimeout(timeoutMs);
            connection.setReadTimeout(timeoutMs);

            // Headers padrão Fastchannel
            connection.setRequestProperty("Authorization", "Bearer " + token);
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Ocp-Apim-Subscription-Key", subscriptionKey);

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

            log.fine(method + " " + urlString + " -> " + statusCode);
            return new HttpResult(statusCode, responseBody);

        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
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

        public HttpResult(int statusCode, String body) {
            this.statusCode = statusCode;
            this.body = body;
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
