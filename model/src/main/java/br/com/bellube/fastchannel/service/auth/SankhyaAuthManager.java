package br.com.bellube.fastchannel.service.auth;

import br.com.bellube.fastchannel.config.FastchannelConfig;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
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
        String serverUrl = config.getSankhyaServerUrl();
        String username = config.getSankhyaUser();
        String password = config.getSankhyaPassword();

        if (serverUrl == null || serverUrl.isEmpty()) {
            throw new Exception("SANKHYA_SERVER_URL nao configurado");
        }
        if (username == null || username.isEmpty()) {
            throw new Exception("SANKHYA_USER nao configurado");
        }
        if (password == null || password.isEmpty()) {
            throw new Exception("SANKHYA_PASSWORD nao configurado");
        }

        log.info("Fazendo login no Sankhya: " + serverUrl + " (usuario: " + username + ")");

        String loginXml = buildLoginXml(username, password);
        String fullUrl = serverUrl + SERVICE_PATH + "?serviceName=" + LOGIN_SERVICE;

        HttpURLConnection conn = null;
        try {
            URL url = new URL(fullUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/xml; charset=UTF-8");
            conn.setDoOutput(true);
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(60000);

            // Enviar XML de login
            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = loginXml.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                throw new Exception("Login falhou com HTTP " + responseCode);
            }

            // Ler resposta
            String response = readStream(conn.getInputStream());

            // Extrair JSESSIONID da resposta
            String jsessionId = extractJsessionId(response);
            if (jsessionId == null || jsessionId.isEmpty()) {
                throw new Exception("JSESSIONID nao encontrado na resposta de login");
            }

            log.info("Login bem-sucedido. JSESSIONID obtido.");
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
        if (authContext == null) {
            return;
        }

        try {
            log.info("Fazendo logout no Sankhya");

            String fullUrl = authContext.getServerUrl() + SERVICE_PATH +
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

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao extrair JSESSIONID", e);
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
        try (BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        }
    }
}
