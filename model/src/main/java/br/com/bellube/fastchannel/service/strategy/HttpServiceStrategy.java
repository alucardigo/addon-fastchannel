package br.com.bellube.fastchannel.service.strategy;

import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.service.OrderXmlBuilder;
import br.com.bellube.fastchannel.service.auth.SankhyaAuthManager;
import br.com.bellube.fastchannel.service.auth.SankhyaAuthManager.AuthContext;

import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Estrategia FALLBACK 2 (ultimo recurso): Chamada HTTP com autenticacao completa.
 * Usa login/logout para obter JSESSIONID antes de chamar servico.
 */
public class HttpServiceStrategy implements OrderCreationStrategy {

    private static final Logger log = Logger.getLogger(HttpServiceStrategy.class.getName());
    private static final Charset LEGACY_CHARSET = Charset.forName("ISO-8859-1");

    private static final String SERVICE_PATH_MGECOM = "/mgecom/service.sbr";
    private static final String SERVICE_PATH_MGE = "/mge/service.sbr";
    private static final String[][] LOGIN_SERVICE_COMBINATIONS = {
            // Fluxo legado: service em /mgecom
            {SERVICE_PATH_MGECOM, SERVICE_PATH_MGECOM},
            // Ambientes mistos: login no /mgecom e servico no /mge
            {SERVICE_PATH_MGECOM, SERVICE_PATH_MGE},
            // Ambientes em que login so funciona via /mge
            {SERVICE_PATH_MGE, SERVICE_PATH_MGECOM},
            // Ultimo recurso totalmente em /mge
            {SERVICE_PATH_MGE, SERVICE_PATH_MGE}
    };

    private final OrderXmlBuilder xmlBuilder;
    private final SankhyaAuthManager authManager;

    public HttpServiceStrategy() {
        this.xmlBuilder = new OrderXmlBuilder();
        this.authManager = new SankhyaAuthManager();
    }

    @Override
    public String getStrategyName() {
        return "HTTP";
    }

    @Override
    public boolean isAvailable() {
        FastchannelConfig cfg = FastchannelConfig.getInstance();
        String url = cfg.getSankhyaServerUrl();
        String user = cfg.getSankhyaUser();
        boolean available = url != null && !url.trim().isEmpty()
                && user != null && !user.trim().isEmpty();
        if (!available) {
            log.info("[HTTP] Estrategia indisponivel por configuracao incompleta (SANKHYA_SERVER_URL/SANKHYA_USER).");
        }
        return available;
    }

    @Override
    public BigDecimal createOrder(OrderDTO order, BigDecimal codParc,
                                  BigDecimal codTipVenda, BigDecimal codVend,
                                  BigDecimal codNat, BigDecimal codCenCus) throws Exception {

        log.info("[HTTP] Criando pedido " + order.getOrderId() + " via HTTP com autenticacao");

        try {
            // 1. Construir XML
            String requestXml = xmlBuilder.buildIncluirNotaXml(order, codParc, codTipVenda, codVend, codNat, codCenCus);

            log.fine("[HTTP] XML: " + requestXml);

            // 2. Chamar servico com login no mesmo contexto do endpoint
            String responseXml = invokeService(requestXml);

            log.fine("[HTTP] Resposta: " + responseXml);

            // 3. Extrair NUNOTA
            BigDecimal nuNota = parseNuNotaFromResponse(responseXml);
            if (nuNota == null) {
                throw new Exception("NUNOTA nao encontrado na resposta");
            }

            log.info("[HTTP] Pedido " + order.getOrderId() + " criado como NUNOTA " + nuNota);
            return nuNota;

        } catch (Exception e) {
            log.log(Level.SEVERE, "[HTTP] Erro ao criar pedido", e);
            throw new Exception("Falha no HTTP: " + e.getMessage(), e);
        }
    }

    /**
     * Invoca servico CACSP.incluirNota via HTTP com autenticacao.
     */
    private String invokeService(String requestXml) throws Exception {
        Exception lastError = null;
        for (String[] combination : LOGIN_SERVICE_COMBINATIONS) {
            String loginPath = combination[0];
            String servicePath = combination[1];
            AuthContext authContext = null;
            try {
                authContext = authManager.login(loginPath);
                return invokeServiceAtPath(authContext, requestXml, servicePath);
            } catch (Exception e) {
                lastError = e;
                log.warning("[HTTP] Falha ao invocar CACSP.incluirNota loginPath=" + loginPath +
                        " servicePath=" + servicePath + ": " + e.getMessage());
            } finally {
                if (authContext != null) {
                    authManager.logout(authContext, loginPath);
                }
            }
        }

        if (lastError != null) {
            throw lastError;
        }
        throw new Exception("Falha ao invocar CACSP.incluirNota em todos os endpoints");
    }

    private String invokeServiceAtPath(AuthContext authContext, String requestXml, String servicePath) throws Exception {
        String fullUrl = authContext.getServerUrl() + servicePath +
                "?serviceName=CACSP.incluirNota" +
                "&mgeSession=" + authContext.getJsessionId();

        HttpURLConnection conn = null;
        try {
            URL url = new URL(fullUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "text/xml; charset=ISO-8859-1");
            conn.setRequestProperty("charset", "ISO-8859-1");
            conn.setRequestProperty("Cookie", "JSESSIONID=" + authContext.getJsessionId());
            conn.setRequestProperty("JSESSIONID", authContext.getJsessionId());
            conn.setDoOutput(true);
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(60000);

            // Enviar XML
            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = requestXml.getBytes(LEGACY_CHARSET);
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                String errorBody = readStream(conn.getErrorStream());
                if (errorBody == null || errorBody.trim().isEmpty()) {
                    errorBody = readStream(conn.getInputStream());
                }
                throw new Exception("HTTP error code: " + responseCode + (errorBody != null && !errorBody.trim().isEmpty()
                        ? " body=" + errorBody
                        : ""));
            }

            // Ler resposta
            String response = readStream(conn.getInputStream());

            if (response == null || response.trim().isEmpty()) {
                throw new Exception("Resposta vazia do servico HTTP");
            }

            // Verificar se houve erro na resposta
            if (response.contains("status=\"0\"")) {
                String errorMsg = extractErrorMessage(response);
                throw new Exception("Erro do Sankhya: " + errorMsg);
            }

            return response;

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro na chamada HTTP", e);
            throw e;
        } finally {
            if (conn != null) {
                try {
                    conn.disconnect();
                } catch (Exception ignored) {}
            }
        }
    }

    private BigDecimal parseNuNotaFromResponse(String responseXml) {
        try {
            // Formato: <NUNOTA>12345</NUNOTA>
            java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("<NUNOTA>(\\d+)</NUNOTA>");
            java.util.regex.Matcher matcher = pattern.matcher(responseXml);
            if (matcher.find()) {
                return new BigDecimal(matcher.group(1));
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao parsear NUNOTA", e);
        }
        return null;
    }

    private String extractErrorMessage(String responseXml) {
        try {
            // Formato: <statusMessage>BASE64_ENCODED_MESSAGE</statusMessage>
            java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("<statusMessage>([^<]+)</statusMessage>");
            java.util.regex.Matcher matcher = pattern.matcher(responseXml);
            if (matcher.find()) {
                String rawMsg = matcher.group(1);
                if (rawMsg != null && !rawMsg.trim().isEmpty()) {
                    String trimmed = rawMsg.trim();
                    try {
                        byte[] decoded = java.util.Base64.getDecoder().decode(trimmed);
                        String decodedMsg = new String(decoded, StandardCharsets.UTF_8);
                        if (!decodedMsg.trim().isEmpty()) {
                            return decodedMsg;
                        }
                    } catch (IllegalArgumentException ignored) {
                        // Mensagem nao esta em base64; seguir com texto bruto
                    }
                    return sanitizeErrorText(trimmed);
                }
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao extrair mensagem de erro", e);
        }
        try {
            java.util.regex.Pattern p = java.util.regex.Pattern.compile("<message>([^<]+)</message>");
            java.util.regex.Matcher m = p.matcher(responseXml);
            if (m.find()) {
                String sanitized = sanitizeErrorText(m.group(1));
                if (sanitized != null && !sanitized.trim().isEmpty()) {
                    return sanitized;
                }
            }
        } catch (Exception ignored) {}
        if (responseXml != null && !responseXml.trim().isEmpty()) {
            String sanitized = sanitizeErrorText(responseXml);
            if (sanitized != null && !sanitized.trim().isEmpty()) {
                return sanitized;
            }
            String compact = responseXml.replaceAll("\\s+", " ").trim();
            if (compact.length() > 300) {
                compact = compact.substring(0, 300) + "...";
            }
            return compact;
        }
        return "Erro desconhecido";
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

    private String sanitizeErrorText(String raw) {
        if (raw == null) {
            return "";
        }
        String text = raw.replaceAll("<[^>]+>", " ").replaceAll("\\s+", " ").trim();
        if (text.length() > 600) {
            return text.substring(0, 600) + "...";
        }
        return text;
    }
}
