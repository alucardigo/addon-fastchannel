package br.com.bellube.fastchannel.service.auth;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Gerenciador de autenticacao OAuth2 (client_credentials) contra a API Oficial
 * do Sankhya Om (Gateway) — {@code POST /mge/oauth/token}.
 *
 * <p>Metodo de autenticacao OFICIAL/documentado da plataforma (Portal do Desenvolvedor
 * Sankhya, Area do Desenvolvedor {@literal >} Minhas solucoes), em contraste com o login
 * legado (MobileLoginSP.login + JSESSIONID raspado de body/header) usado por
 * {@link SankhyaAuthManager}. Vantagens praticas:
 * <ul>
 *   <li>Token e CACHEADO e reutilizado ate perto de expirar — evita o round-trip de
 *       login+logout que o {@code HttpServiceStrategy} paga a CADA pedido criado;</li>
 *   <li>Contrato de autenticacao estavel entre versoes do servidor (nao depende de
 *       parsing de resposta XML/headers proprietarios que ja quebrou silenciosamente
 *       em atualizacoes anteriores do Sankhya — ver incidente v4670000 desregistrando
 *       a base).</li>
 * </ul>
 *
 * <p>Thread-safe: cache do token protegido por lock, com margem de seguranca antes do
 * vencimento para nunca usar um token expirado numa chamada em andamento.
 */
public class SankhyaOAuthManager {

    private static final Logger log = Logger.getLogger(SankhyaOAuthManager.class.getName());

    /** Margem de seguranca: renova o token esse tanto de segundos antes do vencimento real. */
    private static final long EXPIRY_SAFETY_MARGIN_SECONDS = 60;

    private final FastchannelConfig config;
    private final Object tokenLock = new Object();

    private volatile String cachedAccessToken;
    private volatile long cachedTokenExpiresAtEpochMs;

    public SankhyaOAuthManager() {
        this.config = FastchannelConfig.getInstance();
    }

    /**
     * Retorna um access token valido, reutilizando o cache se ainda nao estiver
     * proximo do vencimento. Faz fetch (ou refresh) de forma sincronizada.
     */
    public String getAccessToken() throws Exception {
        String token = cachedAccessToken;
        if (token != null && System.currentTimeMillis() < cachedTokenExpiresAtEpochMs) {
            return token;
        }
        synchronized (tokenLock) {
            // Outra thread pode ja ter renovado enquanto esperavamos o lock.
            if (cachedAccessToken != null && System.currentTimeMillis() < cachedTokenExpiresAtEpochMs) {
                return cachedAccessToken;
            }
            return fetchNewToken();
        }
    }

    /** Forca a renovacao do token, ignorando o cache. Usado apos um 401 inesperado. */
    public String forceRefreshToken() throws Exception {
        synchronized (tokenLock) {
            return fetchNewToken();
        }
    }

    private String fetchNewToken() throws Exception {
        String clientId = config.getSankhyaOAuthClientId();
        String clientSecret = config.getSankhyaOAuthClientSecret();
        String xToken = config.getSankhyaGatewayXToken();

        if (clientId == null || clientSecret == null || xToken == null
                || clientId.trim().isEmpty() || clientSecret.trim().isEmpty() || xToken.trim().isEmpty()) {
            throw new Exception("API Oficial Sankhya nao configurada "
                    + "(SANKHYA_OAUTH_CLIENT_ID/SECRET/GATEWAY_X_TOKEN ausentes)");
        }

        // [VALIDADO 2026-07-03] Host FIXO do Gateway (fornecedor), NAO o host do ERP do
        // cliente. Header X-Token obrigatorio (Token de Integracao da tela Configuracoes
        // Gateway) — sem ele o /authenticate rejeita mesmo com client_id/secret corretos.
        String tokenUrl = FastchannelConstants.SANKHYA_GATEWAY_BASE_URL + FastchannelConstants.SANKHYA_GATEWAY_AUTH_PATH;
        log.info("[SankhyaOAuth] Solicitando novo access token em " + tokenUrl);

        String body = "grant_type=client_credentials"
                + "&client_id=" + URLEncoder.encode(clientId, "UTF-8")
                + "&client_secret=" + URLEncoder.encode(clientSecret, "UTF-8");

        HttpURLConnection conn = null;
        try {
            URL url = new URL(tokenUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            conn.setRequestProperty("X-Token", xToken);
            conn.setDoOutput(true);
            conn.setConnectTimeout(15000);
            conn.setReadTimeout(30000);

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = body.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int status = conn.getResponseCode();
            String response = readStream(status >= 200 && status < 300 ? conn.getInputStream() : conn.getErrorStream());

            if (status < 200 || status >= 300) {
                throw new Exception("Falha ao obter token OAuth2 Sankhya: HTTP " + status
                        + (response != null && !response.trim().isEmpty() ? " body=" + response : ""));
            }
            if (response == null || response.trim().isEmpty()) {
                throw new Exception("Resposta vazia do endpoint de token OAuth2 Sankhya");
            }

            String accessToken = extractJsonField(response, "access_token");
            if (accessToken == null || accessToken.trim().isEmpty()) {
                throw new Exception("access_token nao encontrado na resposta OAuth2: " + response);
            }

            // [VALIDADO 2026-07-03] expires_in real do Gateway = 300s (5min), nao 3600 como
            // em outras APIs OAuth2 genericas — usar como default apenas se o campo faltar.
            long expiresInSeconds = 300;
            String expiresInRaw = extractJsonField(response, "expires_in");
            if (expiresInRaw != null) {
                try {
                    expiresInSeconds = Long.parseLong(expiresInRaw.trim());
                } catch (NumberFormatException ignored) {
                    // mantem default
                }
            }

            long safeExpirySeconds = Math.max(1, expiresInSeconds - EXPIRY_SAFETY_MARGIN_SECONDS);
            this.cachedAccessToken = accessToken;
            this.cachedTokenExpiresAtEpochMs = System.currentTimeMillis() + (safeExpirySeconds * 1000L);

            log.info("[SankhyaOAuth] Token obtido com sucesso (expira em " + expiresInSeconds + "s, cache valido por "
                    + safeExpirySeconds + "s).");
            return accessToken;

        } catch (Exception e) {
            log.log(Level.WARNING, "[SankhyaOAuth] Erro ao obter token OAuth2", e);
            throw e;
        } finally {
            if (conn != null) {
                try {
                    conn.disconnect();
                } catch (Exception ignored) {}
            }
        }
    }

    /** Extracao simples de campo JSON de 1o nivel (sem dependencia de parser externo). */
    private String extractJsonField(String json, String field) {
        Pattern pattern = Pattern.compile("\"" + Pattern.quote(field) + "\"\\s*:\\s*\"?([^\",}]+)\"?");
        Matcher matcher = pattern.matcher(json);
        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        return null;
    }

    private String readStream(InputStream stream) throws Exception {
        if (stream == null) return "";
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[4096];
            int read;
            while ((read = stream.read(buffer)) != -1) {
                baos.write(buffer, 0, read);
            }
            return new String(baos.toByteArray(), StandardCharsets.UTF_8);
        }
    }
}
