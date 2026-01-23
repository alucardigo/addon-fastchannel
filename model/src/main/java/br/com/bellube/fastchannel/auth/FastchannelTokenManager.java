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
                JsonObject json = gson.fromJson(responseBody, JsonObject.class);

                this.accessToken = json.get("access_token").getAsString();
                int expiresIn = json.get("expires_in").getAsInt();
                this.expiresAt = System.currentTimeMillis() + (expiresIn * 1000L);

                log.info("Token Fastchannel renovado com sucesso. Expira em: " + expiresIn + " segundos.");
                return this.accessToken;

            } else {
                String errorBody = readErrorResponse(connection);
                log.severe("Falha na autenticação Fastchannel. HTTP " + responseCode + ": " + errorBody);
                throw new Exception("Falha na autenticação Fastchannel: HTTP " + responseCode);
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro crítico na autenticação Fastchannel", e);
            throw new Exception("Falha de Autenticação Fastchannel: " + e.getMessage(), e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
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
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
