package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.service.nativeapi.SankhyaNativeServiceCaller;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Invocador de servicos Sankhya com fallback HTTP.
 */
public class SankhyaServiceInvoker {

    private static final Logger log = Logger.getLogger(SankhyaServiceInvoker.class.getName());
    private static final String SERVICE_URL_PATH = "/mge/service.sbr";
    private final SankhyaNativeServiceCaller nativeCaller = new SankhyaNativeServiceCaller();
    private final FastchannelConfig config = FastchannelConfig.getInstance();

    /**
     * Invoca servico nativo do Sankhya usando ServiceInvoker com fallback HTTP.
     *
     * @param serviceName Nome do servico (ex: CACSP.incluirNota)
     * @param requestXml XML de requisicao
     * @return XML de resposta
     * @throws Exception se ambas as tentativas falharem
     */
    public String invokeService(String serviceName, String requestXml) throws Exception {
        try {
            return invokeViaNativeBridge(serviceName, requestXml);
        } catch (Exception e) {
            log.log(Level.WARNING, "Invocacao nativa falhou. Tentando fallback HTTP.", e);
            return invokeViaHttp(serviceName, requestXml);
        }
    }

    /**
     * Invoca servico por bridge nativo (ServiceInvoker legado ou ServiceCaller modelcore).
     */
    private String invokeViaNativeBridge(String serviceName, String requestXml) throws Exception {
        log.info("Invocando servico via bridge nativo: " + serviceName);
        String response = nativeCaller.invoke(serviceName, requestXml, config.getSankhyaUser(), config.getSankhyaPassword());
        if (response == null || response.trim().isEmpty()) {
            throw new Exception("Resposta vazia da invocacao nativa");
        }
        return response;
    }

    /**
     * Invoca servico via HTTP (fallback).
     */
    private String invokeViaHttp(String serviceName, String requestXml) throws Exception {
        log.info("Invocando servico via HTTP: " + serviceName);
        List<String> errors = new ArrayList<>();

        for (String baseUrl : getServerBaseUrlCandidates()) {
            String fullUrl = baseUrl + SERVICE_URL_PATH + "?serviceName=" + serviceName;
            HttpURLConnection conn = null;
            try {
                URL url = new URL(fullUrl);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("Content-Type", "application/xml; charset=UTF-8");
                conn.setDoOutput(true);
                conn.setConnectTimeout(30000);
                conn.setReadTimeout(60000);

                try (OutputStream os = conn.getOutputStream()) {
                    byte[] input = requestXml.getBytes(StandardCharsets.UTF_8);
                    os.write(input, 0, input.length);
                }

                int responseCode = conn.getResponseCode();
                if (responseCode != 200) {
                    throw new Exception("HTTP error code: " + responseCode);
                }

                String response = readStream(conn.getInputStream());
                if (response == null || response.trim().isEmpty()) {
                    throw new Exception("Resposta vazia do servico HTTP");
                }

                return response;

            } catch (Exception e) {
                String detail = baseUrl + " -> " + e.getMessage();
                errors.add(detail);
                log.log(Level.WARNING, "Falha ao invocar servico HTTP em " + baseUrl, e);
            } finally {
                if (conn != null) {
                    try {
                        conn.disconnect();
                    } catch (Exception ignored) {}
                }
            }
        }

        throw new Exception("Falha ao invocar " + serviceName + " via HTTP em todos os endpoints: " + String.join(" | ", errors));
    }

    private String readStream(java.io.InputStream stream) throws Exception {
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

    /**
     * Obtem URL base do servidor a partir do contexto.
     */
    private List<String> getServerBaseUrlCandidates() {
        Set<String> candidates = new LinkedHashSet<>();

        String serverUrl = System.getProperty("sankhya.server.url");
        if (serverUrl != null && !serverUrl.isEmpty()) {
            candidates.add(serverUrl.trim());
        }

        String envUrl = System.getenv("FASTCHANNEL_SANKHYA_URL");
        if (envUrl != null && !envUrl.isEmpty()) {
            candidates.add(envUrl.trim());
        }

        candidates.add("http://127.0.0.1:8080");
        candidates.add("http://localhost:8080");
        candidates.add("http://127.0.0.1:8180");

        return new ArrayList<>(candidates);
    }
}
