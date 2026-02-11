package br.com.bellube.fastchannel.service.strategy;

import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.service.OrderXmlBuilder;
import br.com.bellube.fastchannel.service.auth.SankhyaAuthManager;
import br.com.bellube.fastchannel.service.auth.SankhyaAuthManager.AuthContext;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Estrategia FALLBACK 2 (ultimo recurso): Chamada HTTP com autenticacao completa.
 * Usa login/logout para obter JSESSIONID antes de chamar servico.
 */
public class HttpServiceStrategy implements OrderCreationStrategy {

    private static final Logger log = Logger.getLogger(HttpServiceStrategy.class.getName());

    private static final String SERVICE_PATH = "/mge/service.sbr";

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
        // HTTP sempre disponivel (exceto se config invalida)
        return true;
    }

    @Override
    public BigDecimal createOrder(OrderDTO order, BigDecimal codParc,
                                  BigDecimal codTipVenda, BigDecimal codVend,
                                  BigDecimal codNat, BigDecimal codCenCus) throws Exception {

        log.info("[HTTP] Criando pedido " + order.getOrderId() + " via HTTP com autenticacao");

        AuthContext authContext = null;
        try {
            // 1. LOGIN - Obter JSESSIONID
            authContext = authManager.login();

            // 2. Construir XML
            String requestXml = xmlBuilder.buildIncluirNotaXml(order, codParc, codTipVenda, codVend, codNat, codCenCus);

            log.fine("[HTTP] XML: " + requestXml);

            // 3. Chamar servico com JSESSIONID
            String responseXml = invokeService(authContext, requestXml);

            log.fine("[HTTP] Resposta: " + responseXml);

            // 4. Extrair NUNOTA
            BigDecimal nuNota = parseNuNotaFromResponse(responseXml);
            if (nuNota == null) {
                throw new Exception("NUNOTA nao encontrado na resposta");
            }

            log.info("[HTTP] Pedido " + order.getOrderId() + " criado como NUNOTA " + nuNota);
            return nuNota;

        } catch (Exception e) {
            log.log(Level.SEVERE, "[HTTP] Erro ao criar pedido", e);
            throw new Exception("Falha no HTTP: " + e.getMessage(), e);

        } finally {
            // 5. LOGOUT - Sempre fazer logout
            if (authContext != null) {
                authManager.logout(authContext);
            }
        }
    }

    /**
     * Invoca servico CACSP.incluirNota via HTTP com autenticacao.
     */
    private String invokeService(AuthContext authContext, String requestXml) throws Exception {
        String fullUrl = authContext.getServerUrl() + SERVICE_PATH +
                       "?serviceName=CACSP.incluirNota" +
                       "&mgeSession=" + authContext.getJsessionId();

        HttpURLConnection conn = null;
        try {
            URL url = new URL(fullUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/xml; charset=UTF-8");
            conn.setRequestProperty("Cookie", "JSESSIONID=" + authContext.getJsessionId());
            conn.setRequestProperty("JSESSIONID", authContext.getJsessionId());
            conn.setDoOutput(true);
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(60000);

            // Enviar XML
            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = requestXml.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                throw new Exception("HTTP error code: " + responseCode);
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
                String base64Msg = matcher.group(1);
                // Decodificar Base64
                byte[] decoded = java.util.Base64.getDecoder().decode(base64Msg);
                return new String(decoded, StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao extrair mensagem de erro", e);
        }
        return "Erro desconhecido";
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
