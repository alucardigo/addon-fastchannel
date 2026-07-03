package br.com.bellube.fastchannel.service.strategy;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.service.OrderXmlBuilder;
import br.com.bellube.fastchannel.service.auth.SankhyaOAuthManager;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Estrategia de criacao de pedido via API OFICIAL do Sankhya (Sankhya Om Gateway),
 * autenticada por OAuth2 client_credentials (aplicativo registrado no Portal do
 * Desenvolvedor Sankhya — Area do Desenvolvedor {@literal >} Minhas solucoes).
 *
 * <p><b>[2026-07-03]</b> Contrato VALIDADO contra o servidor de producao real (nao apenas
 * documentacao) antes de codificar:
 * <ul>
 *   <li>Host do Gateway e FIXO/hospedado pelo fornecedor ({@code api.sankhya.com.br}) —
 *       NAO e o host do ERP do cliente ({@code SANKHYA_SERVER_URL}, usado pelas outras
 *       estrategias). Confundir os dois faz o POST falhar (testado: "HTTP method POST is
 *       not supported by this URL" ao tentar o host errado).</li>
 *   <li>{@code POST /authenticate} exige o header {@code X-Token} (Token de Integracao da
 *       tela Configuracoes Gateway do ERP) ALEM de client_id/client_secret no body —
 *       confirmado com credenciais reais de producao (HTTP 200, JWT valido, claims batem
 *       com o app "Addon-Fastchannel-APISANKYA" e a empresa BEL DISTRIBUIDOR).</li>
 *   <li>Chamadas de negocio vao para {@code POST /gateway/v1/{mge|mgecom}/service.sbr}
 *       (mesma regra ja validada em {@link HttpServiceStrategy}: CACSP.incluirNota fica
 *       em {@code mgecom}), com {@code Authorization: Bearer <token>} e corpo JSON — NAO
 *       aceita XML. Validado com uma chamada de LEITURA real (loadRecords em Parceiro),
 *       que retornou dados reais do parceiro. A chamada de escrita (incluirNota) NAO foi
 *       testada em producao por ser destrutiva (cria nota real) — o
 *       {@link OrderCreationOrchestrator} absorve qualquer falha desta estrategia
 *       tentando as demais, entao um eventual erro de contrato aqui nao impede pedidos
 *       de serem criados.</li>
 * </ul>
 *
 * <p>Reaproveita o MESMO {@link OrderXmlBuilder} (campos, descontos, frete, resolucao de
 * item ja testados em producao pelas outras estrategias) e apenas TRANSLITERA o XML gerado
 * para o formato JSON que o Gateway exige — a API JSON do Sankhya usa a mesma estrutura de
 * tags do XML classico, apenas com elementos como {@code {"$": "valor"}} (ver
 * {@link #xmlElementToJson(Element)}).
 *
 * <p>Vantagem sobre {@link HttpServiceStrategy}: o token OAuth2 e CACHEADO e reutilizado
 * (ver {@link SankhyaOAuthManager}) em vez de fazer login+logout a cada pedido, e usa um
 * mecanismo de autenticacao oficial/documentado, menos exposto a mudancas no contrato
 * interno de sessao entre versoes do servidor.
 */
public class OfficialApiStrategy implements OrderCreationStrategy {

    private static final Logger log = Logger.getLogger(OfficialApiStrategy.class.getName());
    private static final String SERVICE_NAME = "CACSP.incluirNota";

    private final OrderXmlBuilder xmlBuilder;
    private final SankhyaOAuthManager oauthManager;
    private final FastchannelConfig config;

    public OfficialApiStrategy() {
        this.xmlBuilder = new OrderXmlBuilder();
        this.oauthManager = new SankhyaOAuthManager();
        this.config = FastchannelConfig.getInstance();
    }

    @Override
    public String getStrategyName() {
        return "OfficialAPI";
    }

    @Override
    public boolean isAvailable() {
        boolean available = config.isSankhyaOAuthConfigured();
        if (!available) {
            log.fine("[OfficialAPI] Estrategia indisponivel: credenciais do Gateway "
                    + "(SANKHYA_OAUTH_CLIENT_ID/SECRET/GATEWAY_X_TOKEN) nao configuradas.");
        }
        return available;
    }

    @Override
    public BigDecimal createOrder(OrderDTO order, BigDecimal codParc,
                                  BigDecimal codTipVenda, BigDecimal codVend,
                                  BigDecimal codNat, BigDecimal codCenCus) throws Exception {

        log.info("[OfficialAPI] Criando pedido " + order.getOrderId() + " via API Oficial Sankhya (OAuth2 Gateway)");

        // Reaproveita o XML ja testado em producao (mesmos campos que ServiceInvoker/HTTP
        // usam) e converte para o JSON que o Gateway exige.
        String requestXml = xmlBuilder.buildIncluirNotaXml(order, codParc, codTipVenda, codVend, codNat, codCenCus);
        log.fine("[OfficialAPI] XML fonte: " + br.com.bellube.fastchannel.util.LogSanitizer.sanitizeXml(requestXml));

        JsonObject requestJson = buildGatewayRequestJson(requestXml);
        log.fine("[OfficialAPI] JSON convertido: " + requestJson);

        try {
            String responseBody = invokeWithToken(requestJson, oauthManager.getAccessToken());
            return extractNuNota(order, responseBody);
        } catch (UnauthorizedException e) {
            // Token pode ter expirado entre o cache-check e a chamada real (TTL curto de
            // 300s), ou ter sido revogado no Portal do Desenvolvedor. Renova UMA vez.
            log.warning("[OfficialAPI] HTTP 401/403 recebido, renovando token e tentando novamente...");
            String freshToken = oauthManager.forceRefreshToken();
            String responseBody = invokeWithToken(requestJson, freshToken);
            return extractNuNota(order, responseBody);
        } catch (Exception e) {
            log.log(Level.SEVERE, "[OfficialAPI] Erro ao criar pedido", e);
            throw new Exception("Falha na API Oficial: " + e.getMessage(), e);
        }
    }

    // -------------------------------------------------------------------
    // Transliteracao XML -> JSON (formato Sankhya: elemento -> {"$": valor})
    // -------------------------------------------------------------------

    /**
     * Constroi o envelope JSON completo esperado pelo Gateway:
     * {@code {"serviceName":"CACSP.incluirNota","requestBody":{"nota": <convertido>}}}.
     */
    private JsonObject buildGatewayRequestJson(String requestXml) throws Exception {
        // parseXml() sempre envolve o fragmento em <root>...</root>, entao <nota> e sempre
        // um descendente direto (ou aninhado) do elemento raiz — busca por nome resolve
        // qualquer nivel em que OrderXmlBuilder o tenha colocado.
        Document doc = parseXml(requestXml);
        Element notaElement = findDescendantElement(doc.getDocumentElement(), "nota");
        if (notaElement == null) {
            throw new Exception("Elemento <nota> nao encontrado no XML gerado por OrderXmlBuilder");
        }

        JsonObject envelope = new JsonObject();
        envelope.addProperty("serviceName", SERVICE_NAME);
        JsonObject requestBody = new JsonObject();
        requestBody.add("nota", xmlElementToJson(notaElement));
        envelope.add("requestBody", requestBody);
        return envelope;
    }

    /**
     * Converte um elemento XML para o formato JSON do Sankhya:
     * <ul>
     *   <li>Elemento SOMENTE com texto (folha): {@code {"$": "texto"}}</li>
     *   <li>Elemento com filhos elemento: objeto aninhado com uma chave por nome de tag
     *       (se houver varios filhos com o MESMO nome — ex.: multiplos {@code <item>} — vira
     *       um array JSON, como no exemplo oficial de {@code itens.item[]})</li>
     *   <li>Atributos do elemento (ex.: {@code INFORMARPRECO="True"} em {@code <itens>})
     *       viram chaves-irmas com valor STRING direto (nao envolvidas em {"$":...}),
     *       exatamente como no exemplo oficial da documentacao.</li>
     * </ul>
     */
    private JsonElement xmlElementToJson(Element element) {
        NodeList children = element.getChildNodes();
        Map<String, java.util.List<Element>> childElementsByName = new LinkedHashMap<>();
        boolean hasChildElements = false;
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() == Node.ELEMENT_NODE) {
                hasChildElements = true;
                Element childEl = (Element) child;
                childElementsByName.computeIfAbsent(childEl.getTagName(), k -> new java.util.ArrayList<>()).add(childEl);
            }
        }

        if (!hasChildElements) {
            // Elemento folha: texto vira {"$": "valor"} (ou objeto vazio se nao houver texto,
            // representando um marcador tipo NUNOTA:{} do exemplo oficial para "novo registro").
            String text = element.getTextContent();
            JsonObject leaf = new JsonObject();
            if (text != null && !text.trim().isEmpty()) {
                leaf.add("$", new JsonPrimitive(text.trim()));
            }
            return leaf;
        }

        JsonObject obj = new JsonObject();
        // Atributos do elemento (ex.: INFORMARPRECO="True" em <itens>) -> chave-irma direta.
        if (element.hasAttributes()) {
            org.w3c.dom.NamedNodeMap attrs = element.getAttributes();
            for (int i = 0; i < attrs.getLength(); i++) {
                Node attr = attrs.item(i);
                obj.addProperty(attr.getNodeName(), attr.getNodeValue());
            }
        }
        for (Map.Entry<String, java.util.List<Element>> entry : childElementsByName.entrySet()) {
            java.util.List<Element> siblings = entry.getValue();
            if (siblings.size() == 1) {
                obj.add(entry.getKey(), xmlElementToJson(siblings.get(0)));
            } else {
                JsonArray array = new JsonArray();
                for (Element sibling : siblings) {
                    array.add(xmlElementToJson(sibling));
                }
                obj.add(entry.getKey(), array);
            }
        }
        return obj;
    }

    private Document parseXml(String xml) throws Exception {
        // O XML gerado por OrderXmlBuilder e um fragmento (comeca em "<nota>" sem declaracao
        // XML nem elemento raiz unico obrigatorio) — envolvemos num elemento raiz temporario
        // para o parser aceitar, e descartamos esse invólucro depois.
        String wrapped = "<root>" + xml + "</root>";
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        DocumentBuilder builder = factory.newDocumentBuilder();
        return builder.parse(new InputSource(new StringReader(wrapped)));
    }

    private Element findDescendantElement(Element root, String tagName) {
        NodeList matches = root.getElementsByTagName(tagName);
        return matches.getLength() > 0 ? (Element) matches.item(0) : null;
    }

    // -------------------------------------------------------------------
    // HTTP / Gateway
    // -------------------------------------------------------------------

    /** Marca uma falha HTTP 401/403 (token rejeitado) para acionar o retry com token renovado. */
    private static final class UnauthorizedException extends Exception {
        UnauthorizedException(String message) {
            super(message);
        }
    }

    private String invokeWithToken(JsonObject requestJson, String accessToken) throws Exception {
        String fullUrl = FastchannelConstants.SANKHYA_GATEWAY_BASE_URL
                + FastchannelConstants.SANKHYA_GATEWAY_MGECOM_PATH
                + "?serviceName=" + SERVICE_NAME + "&outputType=json";

        HttpURLConnection conn = null;
        try {
            URL url = new URL(fullUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Authorization", "Bearer " + accessToken);
            conn.setDoOutput(true);
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(60000);

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = requestJson.toString().getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == 401 || responseCode == 403) {
                throw new UnauthorizedException("Token OAuth2 rejeitado pelo Gateway (HTTP " + responseCode + ")");
            }

            String body = readStream(responseCode >= 200 && responseCode < 300
                    ? conn.getInputStream() : conn.getErrorStream());

            if (responseCode != 200 && responseCode != 201) {
                throw new Exception("HTTP error code: " + responseCode
                        + (body != null && !body.trim().isEmpty() ? " body=" + body : ""));
            }
            if (body == null || body.trim().isEmpty()) {
                throw new Exception("Resposta vazia da API Oficial");
            }
            return body;

        } finally {
            if (conn != null) {
                try {
                    conn.disconnect();
                } catch (Exception ignored) {}
            }
        }
    }

    private BigDecimal extractNuNota(OrderDTO order, String responseBody) throws Exception {
        BigDecimal nuNota = null;
        String errorMessage = null;
        try {
            JsonElement parsed = new JsonParser().parse(responseBody);
            if (parsed.isJsonObject()) {
                JsonObject root = parsed.getAsJsonObject();
                String status = root.has("status") ? root.get("status").getAsString() : null;
                if ("1".equals(status) && root.has("responseBody")) {
                    JsonObject respBody = root.getAsJsonObject("responseBody");
                    // Formato documentado: responseBody.pk.NUNOTA.$
                    JsonObject pk = getObjectOrNull(respBody, "pk");
                    if (pk != null) {
                        JsonObject nunotaObj = getObjectOrNull(pk, "NUNOTA");
                        if (nunotaObj != null && nunotaObj.has("$")) {
                            nuNota = nunotaObj.get("$").getAsBigDecimal();
                        }
                    }
                } else if (status != null && !"1".equals(status)) {
                    errorMessage = root.has("statusMessage") ? root.get("statusMessage").getAsString()
                            : (root.has("message") ? root.get("message").getAsString() : responseBody);
                }
            }
        } catch (Exception parseEx) {
            log.log(Level.FINE, "[OfficialAPI] Resposta nao e JSON valido, tentando fallback de regex", parseEx);
        }

        if (nuNota == null) {
            // Fallback defensivo: caso o formato de resposta divirja do documentado, tenta
            // achar um numero apos "NUNOTA" em qualquer formato (JSON ou texto).
            java.util.regex.Matcher m = java.util.regex.Pattern
                    .compile("NUNOTA[^0-9]{1,10}(\\d+)").matcher(responseBody);
            if (m.find()) {
                nuNota = new BigDecimal(m.group(1));
            }
        }

        if (nuNota == null) {
            throw new Exception("NUNOTA nao encontrado na resposta da API Oficial"
                    + (errorMessage != null ? ": " + errorMessage : "") + " (raw=" + truncate(responseBody, 400) + ")");
        }

        log.info("[OfficialAPI] Pedido " + order.getOrderId() + " criado como NUNOTA " + nuNota);
        return nuNota;
    }

    private JsonObject getObjectOrNull(JsonObject parent, String key) {
        return parent.has(key) && parent.get(key).isJsonObject() ? parent.getAsJsonObject(key) : null;
    }

    private String truncate(String value, int max) {
        if (value == null) return null;
        return value.length() > max ? value.substring(0, max) + "..." : value;
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
