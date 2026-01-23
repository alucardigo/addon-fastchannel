package br.com.bellube.fastchannel.http;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.dto.OrderInvoiceDTO;
import br.com.bellube.fastchannel.dto.OrderStatusDTO;
import br.com.bellube.fastchannel.dto.OrderTrackingDTO;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Cliente especializado para Order Management API do Fastchannel.
 *
 * Operações:
 * - Listar pedidos (com filtros e paginação)
 * - Atualizar status de pedido
 * - Enviar nota fiscal
 * - Enviar rastreamento
 * - Marcar como sincronizado
 */
public class FastchannelOrdersClient {

    private static final Logger log = Logger.getLogger(FastchannelOrdersClient.class.getName());
    private static final Gson gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd'T'HH:mm:ss")
            .create();

    private final FastchannelHttpClient httpClient;
    private final FastchannelConfig config;

    public FastchannelOrdersClient() {
        this.httpClient = new FastchannelHttpClient();
        this.config = FastchannelConfig.getInstance();
    }

    public FastchannelOrdersClient(FastchannelHttpClient httpClient) {
        this.httpClient = httpClient;
        this.config = FastchannelConfig.getInstance();
    }

    /**
     * Lista pedidos pendentes desde a última sincronização.
     *
     * @param lastSync timestamp da última sincronização (null = todos)
     * @param page número da página (1-based)
     * @param pageSize tamanho da página
     * @return lista de pedidos
     */
    public List<OrderDTO> listOrders(Timestamp lastSync, int page, int pageSize) throws Exception {
        StringBuilder endpoint = new StringBuilder(FastchannelConstants.ENDPOINT_ORDERS);
        endpoint.append("?page=").append(page);
        endpoint.append("&pageSize=").append(pageSize);

        // Filtro por reseller se configurado
        String resellerId = config.getResellerId();
        if (resellerId != null && !resellerId.isEmpty()) {
            endpoint.append("&resellerId=").append(resellerId);
        }

        // Filtro por data
        if (lastSync != null) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            endpoint.append("&createdAfter=").append(sdf.format(lastSync));
        }

        log.info("Buscando pedidos: " + endpoint);

        FastchannelHttpClient.HttpResult result = httpClient.getOrders(endpoint.toString());

        if (!result.isSuccess()) {
            log.warning("Erro ao listar pedidos: HTTP " + result.getStatusCode() + " - " + result.getBody());
            throw new Exception("Erro ao listar pedidos: " + result.getErrorMessage());
        }

        Type listType = new TypeToken<ArrayList<OrderDTO>>(){}.getType();
        List<OrderDTO> orders = gson.fromJson(result.getBody(), listType);

        log.info("Retornados " + (orders != null ? orders.size() : 0) + " pedidos.");
        return orders != null ? orders : new ArrayList<>();
    }

    /**
     * Obtém detalhes de um pedido específico.
     *
     * @param orderId ID do pedido no Fastchannel
     * @return dados do pedido
     */
    public OrderDTO getOrder(String orderId) throws Exception {
        String endpoint = FastchannelConstants.ENDPOINT_ORDERS + "/" + orderId;

        FastchannelHttpClient.HttpResult result = httpClient.getOrders(endpoint);

        if (!result.isSuccess()) {
            log.warning("Erro ao obter pedido " + orderId + ": HTTP " + result.getStatusCode());
            throw new Exception("Erro ao obter pedido: " + result.getErrorMessage());
        }

        return gson.fromJson(result.getBody(), OrderDTO.class);
    }

    /**
     * Atualiza status de um pedido no Fastchannel.
     *
     * @param orderId ID do pedido
     * @param status novo status (ex: 300 = Faturado)
     * @param message mensagem opcional
     */
    public void updateOrderStatus(String orderId, int status, String message) throws Exception {
        String endpoint = String.format(FastchannelConstants.ENDPOINT_ORDER_STATUS, orderId);

        OrderStatusDTO statusDto = new OrderStatusDTO();
        statusDto.setStatus(status);
        statusDto.setMessage(message);

        String json = gson.toJson(statusDto);
        log.info("Atualizando status do pedido " + orderId + " para " + status);

        FastchannelHttpClient.HttpResult result = httpClient.putOrders(endpoint, json);

        if (!result.isSuccess()) {
            log.warning("Erro ao atualizar status: HTTP " + result.getStatusCode() + " - " + result.getBody());
            throw new Exception("Erro ao atualizar status: " + result.getErrorMessage());
        }

        log.info("Status do pedido " + orderId + " atualizado com sucesso.");
    }

    /**
     * Envia nota fiscal para o pedido.
     *
     * @param orderId ID do pedido
     * @param invoice dados da nota fiscal
     */
    public void sendInvoice(String orderId, OrderInvoiceDTO invoice) throws Exception {
        String endpoint = String.format(FastchannelConstants.ENDPOINT_ORDER_INVOICES, orderId);

        String json = gson.toJson(invoice);
        log.info("Enviando NF para pedido " + orderId + ": " + invoice.getInvoiceNumber());

        FastchannelHttpClient.HttpResult result = httpClient.postOrders(endpoint, json);

        if (!result.isSuccess()) {
            log.warning("Erro ao enviar NF: HTTP " + result.getStatusCode() + " - " + result.getBody());
            throw new Exception("Erro ao enviar NF: " + result.getErrorMessage());
        }

        log.info("NF enviada com sucesso para pedido " + orderId);
    }

    /**
     * Envia informações de rastreamento.
     *
     * @param orderId ID do pedido
     * @param tracking dados de rastreamento
     */
    public void sendTracking(String orderId, OrderTrackingDTO tracking) throws Exception {
        String endpoint = String.format(FastchannelConstants.ENDPOINT_ORDER_TRACKING, orderId);

        String json = gson.toJson(tracking);
        log.info("Enviando rastreamento para pedido " + orderId + ": " + tracking.getTrackingCode());

        FastchannelHttpClient.HttpResult result = httpClient.postOrders(endpoint, json);

        if (!result.isSuccess()) {
            log.warning("Erro ao enviar tracking: HTTP " + result.getStatusCode() + " - " + result.getBody());
            throw new Exception("Erro ao enviar tracking: " + result.getErrorMessage());
        }

        log.info("Rastreamento enviado com sucesso para pedido " + orderId);
    }

    /**
     * Marca pedido como sincronizado no Fastchannel.
     *
     * @param orderId ID do pedido
     * @param externalId ID externo (NUNOTA do Sankhya)
     */
    public void markAsSynced(String orderId, String externalId) throws Exception {
        String endpoint = String.format(FastchannelConstants.ENDPOINT_ORDER_SYNC, orderId);

        String json = String.format("{\"externalId\":\"%s\",\"synced\":true}", externalId);

        FastchannelHttpClient.HttpResult result = httpClient.putOrders(endpoint, json);

        if (!result.isSuccess()) {
            log.warning("Erro ao marcar como sincronizado: HTTP " + result.getStatusCode());
            throw new Exception("Erro ao marcar como sincronizado: " + result.getErrorMessage());
        }

        log.info("Pedido " + orderId + " marcado como sincronizado. ExternalId: " + externalId);
    }

    /**
     * Notifica que o pedido foi negado/cancelado.
     *
     * @param orderId ID do pedido
     * @param reason motivo do cancelamento
     */
    public void denyOrder(String orderId, String reason) throws Exception {
        updateOrderStatus(orderId, FastchannelConstants.STATUS_DENIED, reason);
    }

    /**
     * Notifica que o pedido foi aprovado.
     *
     * @param orderId ID do pedido
     */
    public void approveOrder(String orderId) throws Exception {
        updateOrderStatus(orderId, FastchannelConstants.STATUS_APPROVED, "Pedido aprovado");
    }

    /**
     * Notifica que o pedido foi entregue.
     *
     * @param orderId ID do pedido
     */
    public void markAsDelivered(String orderId) throws Exception {
        updateOrderStatus(orderId, FastchannelConstants.STATUS_DELIVERED, "Pedido entregue");
    }
}
