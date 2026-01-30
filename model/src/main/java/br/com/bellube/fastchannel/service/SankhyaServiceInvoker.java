package br.com.bellube.fastchannel.service;

import br.com.sankhya.extensions.actionbutton.utils.ServiceInvoker;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Invocador de servicos Sankhya com fallback HTTP.
 */
public class SankhyaServiceInvoker {

    private static final Logger log = Logger.getLogger(SankhyaServiceInvoker.class.getName());

    private static final String SERVICE_URL_PATH = "/mge/service.sbr";

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
            return invokeViaServiceInvoker(serviceName, requestXml);
        } catch (Exception e) {
            log.log(Level.WARNING, "ServiceInvoker falhou. Tentando fallback HTTP.", e);
            return invokeViaHttp(serviceName, requestXml);
        }
    }

    /**
     * Invoca servico usando ServiceInvoker interno.
     */
    private String invokeViaServiceInvoker(String serviceName, String requestXml) throws Exception {
        log.info("Invocando servico via ServiceInvoker: " + serviceName);

        try {
            ServiceInvoker invoker = new ServiceInvoker(serviceName, requestXml);
            String response = invoker.invoke();

            if (response == null || response.trim().isEmpty()) {
                throw new Exception("Resposta vazia do ServiceInvoker");
            }

            return response;
        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao invocar via ServiceInvoker", e);
            throw e;
        }
    }

    /**
     * Invoca servico via HTTP (fallback).
     */
    private String invokeViaHttp(String serviceName, String requestXml) throws Exception {
        log.info("Invocando servico via HTTP: " + serviceName);

        String baseUrl = getServerBaseUrl();
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
            byte[] responseBytes = conn.getInputStream().readAllBytes();
            String response = new String(responseBytes, StandardCharsets.UTF_8);

            if (response == null || response.trim().isEmpty()) {
                throw new Exception("Resposta vazia do servico HTTP");
            }

            return response;

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao invocar via HTTP", e);
            throw e;
        } finally {
            if (conn != null) {
                try {
                    conn.disconnect();
                } catch (Exception ignored) {}
            }
        }
    }

    /**
     * Obtem URL base do servidor a partir do contexto.
     */
    private String getServerBaseUrl() {
        // Tentativa 1: Propriedade do sistema
        String serverUrl = System.getProperty("sankhya.server.url");
        if (serverUrl != null && !serverUrl.isEmpty()) {
            return serverUrl;
        }

        // Tentativa 2: URL local padrao
        serverUrl = "http://localhost:8080";
        log.warning("URL do servidor nao configurada. Usando padrao: " + serverUrl);
        return serverUrl;
    }
}
