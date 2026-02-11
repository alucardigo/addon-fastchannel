package br.com.bellube.fastchannel.auth;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.net.URLEncoder;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Gerenciador de Token OAuth2 para Fastchannel Commerce API.
 *
 * Implementa padrão Singleton com:
 * - Cache em memória do access_token
 * - Renovação proativa antes da expiração (5 min buffer)
 * - Thread-safe com synchronized
 * - Retry em caso de falha
 */
public class FastchannelTokenManager {

    private static final Logger log = Logger.getLogger(FastchannelTokenManager.class.getName());
    private static final Gson gson = new Gson();

    private static FastchannelTokenManager instance;

    // Cache de Token
    private String accessToken;
    private long expiresAt; // Timestamp em milissegundos

    // Timeout em ms
    private static final int CONNECT_TIMEOUT_MS = 30000;
    private static final int READ_TIMEOUT_MS = 30000;

    private FastchannelTokenManager() {
        this.accessToken = null;
        this.expiresAt = 0;
    }

    /**
     * Obtém instância Singleton do TokenManager.
     */
    public static synchronized FastchannelTokenManager getInstance() {
        if (instance == null) {
            instance = new FastchannelTokenManager();
        }
        return instance;
    }

    /**
     * Obtém token válido para uso em requisições.
     * Renova automaticamente se necessário.
     *
     * @return Access token Bearer válido
     * @throws Exception se falhar na autenticação
     */
    public synchronized String getValidToken() throws Exception {
        long now = System.currentTimeMillis();
        long bufferMs = FastchannelConstants.TOKEN_REFRESH_BUFFER_SECONDS * 1000L;

        // Verifica se token ainda é válido (com margem de segurança)
        if (accessToken != null && now < (expiresAt - bufferMs)) {
            log.fine("Usando token Fastchannel em cache. Expira em: " + ((expiresAt - now) / 1000) + "s");
            return accessToken;
        }

        // Token expirado ou inexistente - renovar
        log.info("Token Fastchannel expirado ou inexistente. Renovando...");
        return renewToken();
    }

    /**
     * Força renovação do token (útil após erro 401).
     */
    public synchronized String forceRenew() throws Exception {
        log.info("Forçando renovação de token Fastchannel...");
        return renewToken();
    }

    /**
     * Invalida token atual.
     */
    public synchronized void invalidate() {
        this.accessToken = null;
        this.expiresAt = 0;
        log.info("Token Fastchannel invalidado.");
    }

    /**
     * Verifica se há token válido em cache.
     */
    public synchronized boolean hasValidToken() {
        long bufferMs = FastchannelConstants.TOKEN_REFRESH_BUFFER_SECONDS * 1000L;
        return accessToken != null && System.currentTimeMillis() < (expiresAt - bufferMs);
    }

    /**
     * Retorna tempo restante do token em segundos, ou 0 se expirado.
     */
    public synchronized long getRemainingSeconds() {
        if (accessToken == null) return 0;
        long remaining = (expiresAt - System.currentTimeMillis()) / 1000;
        return Math.max(0, remaining);
    }

    private String renewToken() throws Exception {
        FastchannelConfig config = FastchannelConfig.getInstance();

        // Validar configuração
        if (config.getClientId() == null || config.getClientId().isEmpty()) {
            throw new Exception("Fastchannel Client ID não configurado.");
        }
        if (config.getClientSecret() == null || config.getClientSecret().isEmpty()) {
            throw new Exception("Fastchannel Client Secret não configurado.");
        }
        if (config.getScope() == null || config.getScope().isEmpty()) {
            throw new Exception("Fastchannel Scope não configurado.");
        }

        // Montar body form-urlencoded
        String formBody = String.format(
                "grant_type=%s&client_id=%s&client_secret=%s&scope=%s",
                encode("client_credentials"),
                encode(config.getClientId()),
                encode(config.getClientSecret()),
                encode(config.getScope()));

        HttpURLConnection connection = null;
        try {
            URL url = new URL(config.getAuthUrl());
            connection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            connection.setConnectTimeout(CONNECT_TIMEOUT_MS);
            connection.setReadTimeout(READ_TIMEOUT_MS);
            connection.setDoOutput(true);

            // Enviar body
            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = formBody.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = connection.getResponseCode();

            if (responseCode == 200) {
                String responseBody = readResponse(connection);
                TokenData tokenData = parseTokenResponse(responseBody);
                if (tokenData.accessToken == null || tokenData.accessToken.isEmpty()) {
                    String snippet = responseBody == null ? "" : responseBody.substring(0, Math.min(200, responseBody.length()));
                    log.severe("Resposta OAuth2 invalida. error=" + tokenData.error + " desc=" + tokenData.errorDescription + " body=" + snippet);
                    String msg = "Resposta OAuth2 invalida";
                    if ((tokenData.error != null && !tokenData.error.isEmpty()) || (tokenData.errorDescription != null && !tokenData.errorDescription.isEmpty())) {
                        msg += ": " + (tokenData.error == null ? "" : tokenData.error)
                                + ((tokenData.errorDescription == null || tokenData.errorDescription.isEmpty()) ? "" : " - " + tokenData.errorDescription);
                    } else if (!snippet.isEmpty()) {
                        msg += ": " + snippet;
                    }
                    throw new Exception(msg);
                }

                this.accessToken = tokenData.accessToken;
                int expiresIn = tokenData.expiresIn;
                this.expiresAt = System.currentTimeMillis() + (expiresIn * 1000L);

                log.info("Token Fastchannel renovado com sucesso. Expira em: " + expiresIn + " segundos.");
                return this.accessToken;

            } else {
                String errorBody = readErrorResponse(connection);
                String snippet = errorBody == null ? "" : errorBody.substring(0, Math.min(200, errorBody.length()));
                log.severe("Falha na autenticacao Fastchannel. HTTP " + responseCode + ": " + snippet);
                String msg = "Falha na autenticacao Fastchannel: HTTP " + responseCode;
                if (snippet != null && !snippet.isEmpty()) {
                    msg += " - " + snippet;
                }
                throw new Exception(msg);
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro critico na autenticacao Fastchannel", e);
            String msg = e.getMessage();
            if (msg == null || msg.trim().isEmpty()) {
                msg = e.getClass().getSimpleName();
            }
            throw new Exception("Falha de Autenticacao Fastchannel: " + msg, e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static class TokenData {
        private final String accessToken;
        private final int expiresIn;
        private final String error;
        private final String errorDescription;

        private TokenData(String accessToken, int expiresIn, String error, String errorDescription) {
            this.accessToken = accessToken;
            this.expiresIn = expiresIn;
            this.error = error;
            this.errorDescription = errorDescription;
        }
    }

    private static TokenData parseTokenResponse(String responseBody) {
        if (responseBody == null || responseBody.trim().isEmpty()) {
            return new TokenData(null, 0, null, null);
        }

        JsonObject json;
        try {
            json = gson.fromJson(responseBody, JsonObject.class);
        } catch (Exception e) {
            return new TokenData(null, 0, null, null);
        }

        if (json == null) {
            return new TokenData(null, 0, null, null);
        }

        String accessToken = getValueByNormalizedKey(json, "access_token");
        String expiresInValue = getValueByNormalizedKey(json, "expires_in");
        String error = getValueByNormalizedKey(json, "error");
        String errorDescription = getValueByNormalizedKey(json, "error_description");

        String normalizedBody = responseBody
                .replace("\uFEFF", "")
                .replace("\u200B", "")
                .replace("\\uFEFF", "");
        if (accessToken == null && normalizedBody.contains("\"access_token\"")) {
            accessToken = extractByRegex(normalizedBody, "\"access_token\"\\s*:\\s*\"([^\"]+)\"");
        }
        if ((expiresInValue == null || expiresInValue.isEmpty()) && normalizedBody.contains("\"expires_in\"")) {
            expiresInValue = extractByRegex(normalizedBody, "\"expires_in\"\\s*:\\s*(\\d+)");
        }

        int expiresIn = 0;
        if (expiresInValue != null && !expiresInValue.isEmpty()) {
            try {
                expiresIn = Integer.parseInt(expiresInValue);
            } catch (NumberFormatException e) {
                expiresIn = 0;
            }
        }

        return new TokenData(accessToken, expiresIn, error, errorDescription);
    }

    private static String getValueByNormalizedKey(JsonObject json, String expectedKey) {
        for (java.util.Map.Entry<String, com.google.gson.JsonElement> entry : json.entrySet()) {
            String key = entry.getKey();
            String normalized = normalizeKey(key).replaceAll("\\s+", "");
            if (expectedKey.equalsIgnoreCase(normalized)) {
                com.google.gson.JsonElement value = entry.getValue();
                if (value == null || value.isJsonNull()) {
                    return null;
                }
                return value.getAsString();
            }
        }
        return null;
    }

    private static String normalizeKey(String key) {
        if (key == null) return "";
        return key.replace("\uFEFF", "").replace("\u200B", "").trim();
    }

    private static String extractByRegex(String text, String pattern) {
        try {
            java.util.regex.Matcher matcher = java.util.regex.Pattern.compile(pattern).matcher(text);
            if (matcher.find()) {
                return matcher.group(1);
            }
        } catch (Exception e) {
            // Ignorar
        }
        return null;
    }

    private String readResponse(HttpURLConnection connection) throws Exception {
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
            return br.lines().collect(Collectors.joining());
        }
    }

    private String readErrorResponse(HttpURLConnection connection) {
        try {
            if (connection.getErrorStream() != null) {
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(connection.getErrorStream(), StandardCharsets.UTF_8))) {
                    return br.lines().collect(Collectors.joining());
                }
            }
        } catch (Exception e) {
            // Ignorar
        }
        return "";
    }

    private String encode(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            return value;
        }
    }
}
