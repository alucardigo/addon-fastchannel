package br.com.bellube.fastchannel.service.auth;

import br.com.bellube.fastchannel.config.FastchannelConfig;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Gerenciador de autenticacao no Sankhya via servicos web.
 * Responsavel por login/logout e gerenciamento de JSESSIONID.
 */
public class SankhyaAuthManager {

    private static final Logger log = Logger.getLogger(SankhyaAuthManager.class.getName());

    private static final String LOGIN_SERVICE = "MobileLoginSP.login";
    private static final String LOGOUT_SERVICE = "MobileLoginSP.logout";
    private static final String SERVICE_PATH = "/mge/service.sbr";
    private static final Charset LEGACY_CHARSET = Charset.forName("ISO-8859-1");

    private final FastchannelConfig config;

    public SankhyaAuthManager() {
        this.config = FastchannelConfig.getInstance();
    }

    /**
     * Contexto de autenticacao com JSESSIONID.
     */
    public static class AuthContext {
        private final String jsessionId;
        private final String serverUrl;

        public AuthContext(String jsessionId, String serverUrl) {
            this.jsessionId = jsessionId;
            this.serverUrl = serverUrl;
        }

        public String getJsessionId() {
            return jsessionId;
        }

        public String getServerUrl() {
            return serverUrl;
        }
    }

    /**
     * Faz login no Sankhya e retorna contexto de autenticacao.
     *
     * @return AuthContext com JSESSIONID
     * @throws Exception se login falhar
     */
    public AuthContext login() throws Exception {
        return login(SERVICE_PATH);
    }

    public AuthContext login(String servicePath) throws Exception {
        String serverUrl = normalizeServerBaseUrl(config.getSankhyaServerUrl());
        String username = config.getSankhyaUser();
        String password = config.getSankhyaPassword();

        if (serverUrl == null || serverUrl.isEmpty()) {
            throw new Exception("SANKHYA_SERVER_URL nao configurado");
        }
        if (username == null || username.isEmpty()) {
            throw new Exception("SANKHYA_USER nao configurado");
        }
        if (password == null) {
            // Ambiente Sankhya pode usar senha interna vazia (ex.: usuario SUP em dev local)
            password = "";
        }

        log.info("Fazendo login no Sankhya: " + serverUrl + " (usuario: " + username + ")");

        String loginXml = buildLoginXml(username, password);
        String effectivePath = normalizeServicePath(servicePath);
        String fullUrl = serverUrl + effectivePath + "?serviceName=" + LOGIN_SERVICE;

        HttpURLConnection conn = null;
        try {
            URL url = new URL(fullUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "text/xml; charset=ISO-8859-1");
            conn.setRequestProperty("charset", "ISO-8859-1");
            conn.setDoOutput(true);
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(60000);

            // Enviar XML de login
            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = loginXml.getBytes(LEGACY_CHARSET);
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                throw new Exception("Login falhou com HTTP " + responseCode);
            }

            // Ler resposta
            String response = readStream(conn.getInputStream());

            // Extrair JSESSIONID (prioridade: body XML/JSON, fallback: header Set-Cookie).
            // Em alguns ambientes clusterizados o cookie vem com sufixo de rota (ex.: ".master"),
            // enquanto o mgeSession válido para service.sbr é o valor puro retornado no body.
            String jsessionId = extractJsessionId(response);
            boolean fromBody = jsessionId != null && !jsessionId.isEmpty();
            if (jsessionId == null || jsessionId.isEmpty()) {
                jsessionId = extractJsessionIdFromHeaders(conn);
            }
            if (jsessionId == null || jsessionId.isEmpty()) {
                throw new Exception("JSESSIONID nao encontrado na resposta de login");
            }

            log.warning("Login bem-sucedido. JSESSIONID obtido via " + (fromBody ? "body" : "header") + ".");
            return new AuthContext(jsessionId, serverUrl);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao fazer login no Sankhya", e);
            throw new Exception("Falha no login: " + e.getMessage(), e);
        } finally {
            if (conn != null) {
                try {
                    conn.disconnect();
                } catch (Exception ignored) {}
            }
        }
    }

    /**
     * Faz logout no Sankhya, encerrando a sessao.
     *
     * @param authContext Contexto de autenticacao
     */
    public void logout(AuthContext authContext) {
        logout(authContext, SERVICE_PATH);
    }

    public void logout(AuthContext authContext, String servicePath) {
        if (authContext == null) {
            return;
        }

        try {
            log.info("Fazendo logout no Sankhya");

            String effectivePath = normalizeServicePath(servicePath);
            String fullUrl = authContext.getServerUrl() + effectivePath +
                           "?serviceName=" + LOGOUT_SERVICE +
                           "&mgeSession=" + authContext.getJsessionId();

            HttpURLConnection conn = null;
            try {
                URL url = new URL(fullUrl);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("Cookie", "JSESSIONID=" + authContext.getJsessionId());
                conn.setRequestProperty("JSESSIONID", authContext.getJsessionId());
                conn.setConnectTimeout(10000);
                conn.setReadTimeout(10000);

                int responseCode = conn.getResponseCode();
                if (responseCode == 200) {
                    log.info("Logout bem-sucedido");
                } else {
                    log.warning("Logout retornou HTTP " + responseCode);
                }

            } finally {
                if (conn != null) {
                    try {
                        conn.disconnect();
                    } catch (Exception ignored) {}
                }
            }

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao fazer logout (nao critico)", e);
        }
    }

    /**
     * Constroi XML de login.
     */
    private String buildLoginXml(String username, String password) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
               "<serviceRequest serviceName=\"" + LOGIN_SERVICE + "\">\n" +
               "  <requestBody>\n" +
               "    <NOMUSU>" + xmlEscape(username) + "</NOMUSU>\n" +
               "    <INTERNO>" + xmlEscape(password) + "</INTERNO>\n" +
               "    <KEEPCONNECTED>S</KEEPCONNECTED>\n" +
               "  </requestBody>\n" +
               "</serviceRequest>";
    }

    /**
     * Extrai JSESSIONID da resposta XML de login.
     */
    private String extractJsessionId(String responseXml) {
        try {
            // Formato esperado: <jsessionid>ABC123</jsessionid>
            Pattern pattern = Pattern.compile("<jsessionid>([^<]+)</jsessionid>", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(responseXml);
            if (matcher.find()) {
                return matcher.group(1);
            }

            // Formato alternativo: <JSESSIONID>ABC123</JSESSIONID>
            pattern = Pattern.compile("<JSESSIONID>([^<]+)</JSESSIONID>");
            matcher = pattern.matcher(responseXml);
            if (matcher.find()) {
                return matcher.group(1);
            }

            // Formato JSON/alternativo: "mgeSession":"ABC123"
            pattern = Pattern.compile("\"mgeSession\"\\s*:\\s*\"([^\"]+)\"", Pattern.CASE_INSENSITIVE);
            matcher = pattern.matcher(responseXml);
            if (matcher.find()) {
                return matcher.group(1);
            }

            // Formato simples: mgeSession=ABC123
            pattern = Pattern.compile("mgeSession=([A-Za-z0-9\\-_.]+)", Pattern.CASE_INSENSITIVE);
            matcher = pattern.matcher(responseXml);
            if (matcher.find()) {
                return matcher.group(1);
            }

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao extrair JSESSIONID", e);
        }
        return null;
    }

    private String extractJsessionIdFromHeaders(HttpURLConnection conn) {
        try {
            String setCookie = conn.getHeaderField("Set-Cookie");
            if (setCookie == null || setCookie.isEmpty()) {
                return null;
            }
            Pattern pattern = Pattern.compile("JSESSIONID=([^;]+)", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(setCookie);
            if (matcher.find()) {
                return matcher.group(1);
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel extrair JSESSIONID do header", e);
        }
        return null;
    }

    private String xmlEscape(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace("\"", "&quot;")
                   .replace("'", "&apos;");
    }

    private String readStream(InputStream stream) throws Exception {
        if (stream == null) return "";
        byte[] bytes = readAllBytes(stream);
        String utf8 = new String(bytes, StandardCharsets.UTF_8);
        if (looksLikeDecodingProblem(utf8)) {
            return new String(bytes, LEGACY_CHARSET);
        }
        return utf8;
    }

    private byte[] readAllBytes(InputStream stream) throws Exception {
        try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream()) {
            byte[] buffer = new byte[4096];
            int read;
            while ((read = stream.read(buffer)) != -1) {
                baos.write(buffer, 0, read);
            }
            return baos.toByteArray();
        }
    }

    private boolean looksLikeDecodingProblem(String text) {
        if (text == null || text.isEmpty()) {
            return false;
        }
        return text.contains("\uFFFD");
    }

    private String normalizeServerBaseUrl(String rawUrl) {
        if (rawUrl == null) {
            return null;
        }
        String normalized = rawUrl.trim();
        if (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        if (normalized.endsWith("/mge")) {
            normalized = normalized.substring(0, normalized.length() - 4);
        } else if (normalized.endsWith("/mgecom")) {
            normalized = normalized.substring(0, normalized.length() - 7);
        }
        return normalized;
    }

    private String normalizeServicePath(String rawPath) {
        if (rawPath == null || rawPath.trim().isEmpty()) {
            return SERVICE_PATH;
        }
        String path = rawPath.trim();
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        if (!path.endsWith("/service.sbr")) {
            if (path.endsWith("/")) {
                path = path + "service.sbr";
            } else {
                path = path + "/service.sbr";
            }
        }
        return path;
    }
}
