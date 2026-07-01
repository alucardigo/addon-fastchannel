package br.com.bellube.fastchannel.http;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.dto.OrderAddressDTO;
import br.com.bellube.fastchannel.dto.OrderCustomerDTO;
import br.com.bellube.fastchannel.dto.OrderInvoiceDTO;
import br.com.bellube.fastchannel.dto.OrderItemDTO;
import br.com.bellube.fastchannel.dto.OrderStatusDTO;
import br.com.bellube.fastchannel.dto.OrderTrackingDTO;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.google.gson.annotations.SerializedName;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import java.lang.reflect.Type;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilderFactory;

/**
 * Cliente especializado para Order Management API do Fastchannel.
 *
 * Operacoes:
 * - Listar pedidos (com filtros e paginacao)
 * - Atualizar status de pedido
 * - Enviar nota fiscal
 * - Enviar rastreamento
 * - Marcar como sincronizado
 */
public class FastchannelOrdersClient {

    private static final Logger log = Logger.getLogger(FastchannelOrdersClient.class.getName());
    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter(Timestamp.class, (JsonDeserializer<Timestamp>) (json, typeOfT, context) ->
                    parseTimestamp(json))
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
     * Lista pedidos pendentes desde a ultima sincronizacao.
     *
     * @param lastSync timestamp da ultima sincronizacao (null = todos)
     * @param page numero da pagina (1-based)
     * @param pageSize tamanho da pagina
     * @return lista de pedidos
     */
    public List<OrderDTO> listOrders(Timestamp lastSync, int page, int pageSize) throws Exception {
        OrderListResult result = listOrdersWithMeta(lastSync, page, pageSize, Boolean.FALSE);
        return result != null && result.getOrders() != null ? result.getOrders() : new ArrayList<>();
    }

    // Safety overlap: subtrai 72h do lastSync antes de enviar CreatedAfter.
    // Isso garante que pedidos que falharam na importacao anterior nao fiquem
    // perdidos quando o cursor (lastOrderSync) avanca apos o ciclo.
    // O dedup (isOrderAlreadyImported) evita reprocessamento dos ja importados.
    // 72h cobre: erros transientes, retries, finais de semana e downtime operacional.
    private static final long SAFETY_OVERLAP_MS = 72L * 60 * 60 * 1000;

    public OrderListResult listOrdersWithMeta(Timestamp lastSync, int page, int pageSize, Boolean isSynched) throws Exception {
        StringBuilder endpoint = new StringBuilder(FastchannelConstants.ENDPOINT_ORDERS);
        endpoint.append("?PageNumber=").append(page);
        endpoint.append("&PageSize=").append(pageSize);

        // Filtro por reseller se configurado
        String resellerId = config.getResellerId();
        if (resellerId != null && !resellerId.isEmpty()) {
            endpoint.append("&ResellerIds=").append(resellerId);
        }

        // Filtro por sincronizacao: so usar IsSynched=false se o addon
        // realmente marca pedidos como synced (SYNC_STATUS_ENABLED=S).
        // Caso contrario, nao filtrar por IsSynched para evitar retornar
        // todos os pedidos historicos como "pendentes".
        if (isSynched != null && config.isSyncStatusEnabled()) {
            endpoint.append("&IsSynched=").append(isSynched ? "true" : "false");
        }

        // Controle temporal: CreatedAfter limita o escopo de pedidos retornados.
        // IMPORTANTE: NAO enviar IgnoreCreationDate=true junto com CreatedAfter!
        // O parametro IgnoreCreationDate=true faz a API ignorar TODOS os filtros
        // de data (inclusive CreatedAfter), retornando pedidos historicos.
        // O legado (pedidos.js linhas 940-965) tambem NAO envia IgnoreCreationDate
        // quando usa CreatedAfter.
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        if (lastSync != null) {
            // Aplica safety overlap: recua 48h para capturar pedidos que falharam
            // na importacao anterior. O dedup no OrderService impede reprocessamento.
            Timestamp safeSync = new Timestamp(lastSync.getTime() - SAFETY_OVERLAP_MS);
            endpoint.append("&CreatedAfter=").append(sdf.format(safeSync));
            log.info("lastSync=" + sdf.format(lastSync) + " com overlap 48h => CreatedAfter=" + sdf.format(safeSync));
        } else {
            // Fallback: 72h para cobrir finais de semana prolongados
            Timestamp defaultSync = new Timestamp(System.currentTimeMillis() - (72L * 60 * 60 * 1000));
            endpoint.append("&CreatedAfter=").append(sdf.format(defaultSync));
            log.info("lastSync nulo - usando fallback de 72h atras: " + sdf.format(defaultSync));
        }

        log.info("Buscando pedidos: " + endpoint);

        FastchannelHttpClient.HttpResult result = httpClient.getOrders(endpoint.toString());

        if (!result.isSuccess()) {
            log.warning("Erro ao listar pedidos: HTTP " + result.getStatusCode() + " - " + result.getBody());
            throw new Exception("Erro ao listar pedidos: " + result.getErrorMessage());
        }

        String body = sanitizeBody(result.getBody());
        List<OrderDTO> orders = new ArrayList<>();
        Integer totalRecords = null;
        Integer totalPages = null;
        if (body.startsWith("<")) {
            OrderListResult xmlResult = parseOrdersXml(body);
            if (xmlResult != null) {
                orders = xmlResult.getOrders();
                totalRecords = xmlResult.getTotalRecords();
                totalPages = xmlResult.getTotalPages();
            }
        } else if (body.startsWith("[")) {
            Type listType = new TypeToken<ArrayList<OrderDTO>>(){}.getType();
            orders = gson.fromJson(body, listType);
        } else {
            OrderListResponse response = gson.fromJson(body, OrderListResponse.class);
            if (response != null) {
                if (response.payload != null) {
                    orders = response.payload;
                }
                totalRecords = response.totalRecords;
                totalPages = response.totalPages;
            }
        }

        log.info("Retornados " + (orders != null ? orders.size() : 0) + " pedidos.");
        return new OrderListResult(orders != null ? orders : new ArrayList<>(), totalRecords, totalPages);
    }

    /**
     * Obtem detalhes de um pedido especifico.
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

        return parseOrderPayload(result.getBody());
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
     * Envia informacoes de rastreamento.
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

        String json = String.format("{\"IsSynched\":true,\"ExternalId\":\"%s\"}", externalId);

        FastchannelHttpClient.HttpResult result = httpClient.putOrders(endpoint, json);

        if (!result.isSuccess()) {
            log.warning("Erro ao marcar como sincronizado: HTTP " + result.getStatusCode());
            throw new Exception("Erro ao marcar como sincronizado: " + result.getErrorMessage());
        }

        log.info("Pedido " + orderId + " marcado como sincronizado. ExternalId: " + externalId);
    }

    /**
     * [FCAdminSP.markAsUnsynced] Operacao inversa de markAsSynced: devolve o pedido ao
     * pool de pendentes da Fastchannel zerando IsSynched e ExternalId.
     *
     * Uso tipico: cenario de coexistencia parallel addon+legado em producao. O addon
     * importa para uma TOP de teste isolada; ao final, desmarca o pedido para o legado
     * consumir de verdade na TOP real.
     *
     * @param orderId ID do pedido no Fastchannel
     */
    public void markAsUnsynced(String orderId) throws Exception {
        String endpoint = String.format(FastchannelConstants.ENDPOINT_ORDER_SYNC, orderId);
        // ExternalId esvaziado: removemos o vinculo ao NUNOTA anterior
        String json = "{\"IsSynched\":false,\"ExternalId\":\"\"}";

        FastchannelHttpClient.HttpResult result = httpClient.putOrders(endpoint, json);

        if (!result.isSuccess()) {
            log.warning("Erro ao marcar como NAO sincronizado: HTTP " + result.getStatusCode());
            throw new Exception("Erro ao marcar como nao sincronizado: " + result.getErrorMessage());
        }

        log.info("Pedido " + orderId + " marcado como NAO sincronizado (devolvido ao pool FC).");
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

    /**
     * Consulta tracking de um pedido.
     *
     * @param orderId ID do pedido
     * @return JSON com dados de rastreamento
     */
    public String getOrderTracking(String orderId) throws Exception {
        String endpoint = String.format(FastchannelConstants.ENDPOINT_ORDER_TRACKING, orderId);
        FastchannelHttpClient.HttpResult result = httpClient.getOrders(endpoint);
        if (!result.isSuccess()) {
            throw new Exception("Erro ao consultar tracking: " + result.getErrorMessage());
        }
        return result.getBody();
    }

    /**
     * Consulta status de sincronizacao de um pedido.
     *
     * @param orderId ID do pedido
     * @return JSON com status de sync
     */
    public String getOrderSyncStatus(String orderId) throws Exception {
        String endpoint = String.format(FastchannelConstants.ENDPOINT_ORDER_SYNC, orderId);
        FastchannelHttpClient.HttpResult result = httpClient.getOrders(endpoint);
        if (!result.isSuccess()) {
            throw new Exception("Erro ao consultar sync status: " + result.getErrorMessage());
        }
        return result.getBody();
    }

    /**
     * Lista notas fiscais de um pedido.
     *
     * @param orderId ID do pedido
     * @return JSON com lista de invoices
     */
    public String listOrderInvoices(String orderId) throws Exception {
        String endpoint = String.format(FastchannelConstants.ENDPOINT_ORDER_INVOICES, orderId);
        FastchannelHttpClient.HttpResult result = httpClient.getOrders(endpoint);
        if (!result.isSuccess()) {
            throw new Exception("Erro ao listar invoices: " + result.getErrorMessage());
        }
        return result.getBody();
    }

    private static OrderDTO parseOrderPayload(String body) {
        if (body == null || body.trim().isEmpty()) {
            return null;
        }

        String trimmed = sanitizeBody(body);
        if (trimmed.startsWith("<")) {
            return parseOrderXml(trimmed);
        }
        if (trimmed.startsWith("{")) {
            try {
                JsonObject obj = new com.google.gson.JsonParser().parse(trimmed).getAsJsonObject();
                if (obj.has("Payload")) {
                    JsonElement payload = obj.get("Payload");
                    if (payload != null) {
                        if (payload.isJsonObject()) {
                            OrderDTO parsed = parseOrderFromJsonObject(payload.getAsJsonObject());
                            if (parsed != null) {
                                return parsed;
                            }
                        } else if (payload.isJsonArray() && payload.getAsJsonArray().size() > 0
                                && payload.getAsJsonArray().get(0).isJsonObject()) {
                            OrderDTO parsed = parseOrderFromJsonObject(payload.getAsJsonArray().get(0).getAsJsonObject());
                            if (parsed != null) {
                                return parsed;
                            }
                        }
                    }
                }
                OrderDTO parsed = parseOrderFromJsonObject(obj);
                if (parsed != null) {
                    return parsed;
                }
            } catch (Exception ignored) {
            }
        }

        return gson.fromJson(trimmed, OrderDTO.class);
    }

    private static OrderDTO parseOrderFromJsonObject(JsonObject json) {
        if (json == null) {
            return null;
        }

        OrderDTO parsed = gson.fromJson(json, OrderDTO.class);
        if (parsed == null) {
            parsed = new OrderDTO();
        }

        if (isBlank(parsed.getOrderId())) {
            parsed.setOrderId(getJsonString(json, "OrderId", "orderId", "Id", "id"));
        }
        if (isBlank(parsed.getExternalOrderId())) {
            parsed.setExternalOrderId(getJsonString(json, "OrderCode", "orderCode", "ExternalOrderId", "externalOrderId"));
        }
        if (isBlank(parsed.getResellerId())) {
            parsed.setResellerId(getJsonString(json, "ResellerId", "resellerId"));
        }
        if (isBlank(parsed.getStorageId())) {
            parsed.setStorageId(getJsonString(json, "StorageId", "storageId"));
        }

        OrderCustomerDTO customer = parsed.getCustomer();
        if (customer == null) {
            customer = new OrderCustomerDTO();
        }

        JsonObject customerObj = getJsonObject(json, "Customer", "customer", "CustomerData", "customerData", "Buyer", "buyer");
        if (customerObj != null) {
            if (isBlank(customer.getCustomerId())) {
                customer.setCustomerId(getJsonString(customerObj, "CustomerId", "customerId", "Id", "id"));
            }
            if (isBlank(customer.getName())) {
                customer.setName(getJsonString(customerObj, "FullName", "fullName", "Name", "name", "CorporateName", "corporateName"));
            }
            if (isBlank(customer.getEmail())) {
                customer.setEmail(getJsonString(customerObj, "EmailAddress", "emailAddress", "Email", "email"));
            }
            if (isBlank(customer.getCpfCnpj())) {
                customer.setCpfCnpj(firstNonEmptyMany(
                        getJsonString(customerObj, "CompanyFederalRegistry", "companyFederalRegistry"),
                        getJsonString(customerObj, "CustomerFederalRegistry", "customerFederalRegistry"),
                        getJsonString(customerObj, "CpfCnpj", "cpfCnpj"),
                        getJsonString(customerObj, "TaxVat", "taxVat"),
                        getJsonString(customerObj, "Document", "document")));
            }
            if (isBlank(customer.getPersonType())) {
                customer.setPersonType(getJsonString(customerObj, "CustomerTypeFlag", "customerTypeFlag",
                        "CustomerTypeName", "customerTypeName", "PersonType", "personType"));
            }
        }

        if (isBlank(customer.getName())) {
            customer.setName(firstNonEmptyMany(
                    getJsonString(json, "CustomerName", "customerName", "BuyerName", "buyerName", "FullName", "fullName"),
                    parsed.getShippingAddress() != null ? parsed.getShippingAddress().getRecipientName() : null,
                    parsed.getBillingAddress() != null ? parsed.getBillingAddress().getRecipientName() : null));
        }
        if (isBlank(customer.getCpfCnpj())) {
            customer.setCpfCnpj(firstNonEmptyMany(
                    getJsonString(json, "CustomerFederalRegistry", "customerFederalRegistry", "CompanyFederalRegistry", "companyFederalRegistry",
                            "CpfCnpj", "cpfCnpj", "Document", "document", "TaxVat", "taxVat"),
                    getNestedJsonString(json, "ShippingData", "shippingData", "CustomerFederalRegistry", "customerFederalRegistry",
                            "CompanyFederalRegistry", "companyFederalRegistry", "CpfCnpj", "cpfCnpj", "Document", "document"),
                    getNestedJsonString(json, "BillingData", "billingData", "CustomerFederalRegistry", "customerFederalRegistry",
                            "CompanyFederalRegistry", "companyFederalRegistry", "CpfCnpj", "cpfCnpj", "Document", "document")));
        }

        parsed.setCustomer(customer);

        // [FIX 2026-04-27] Endpoint /orders/{id} retorna address em Customer.Addresses[]
        // (cada item com OrderAddressTypeId: 1=billing, 2=shipping). Sem este parser
        // o JSON parser deixava billing/shipping null e createParceiro falhava com
        // "CODCID nao resolvido" ao tentar criar cliente novo (caso pedido 4726
        // Alegra Tur Ubá/MG/IBGE 3169901). O parser XML ja foi corrigido na mesma
        // sessao mas a API FC responde JSON nativo - este branch eh o que executa.
        if (customerObj != null && parsed.getBillingAddress() == null && parsed.getShippingAddress() == null) {
            JsonElement addressesEl = customerObj.has("Addresses") ? customerObj.get("Addresses") : null;
            if (addressesEl != null && addressesEl.isJsonArray()) {
                OrderAddressDTO billingFromCustomer = null;
                OrderAddressDTO shippingFromCustomer = null;
                for (JsonElement addrItem : addressesEl.getAsJsonArray()) {
                    if (addrItem == null || !addrItem.isJsonObject()) continue;
                    JsonObject addrObj = addrItem.getAsJsonObject();
                    OrderAddressDTO addr = parseAddressFromJson(addrObj);
                    String typeId = getJsonString(addrObj, "OrderAddressTypeId", "orderAddressTypeId");
                    if ("1".equals(typeId)) {
                        billingFromCustomer = addr;
                    } else if ("2".equals(typeId)) {
                        shippingFromCustomer = addr;
                    } else if (billingFromCustomer == null) {
                        billingFromCustomer = addr; // fallback: 1o address vira billing
                    }
                }
                if (parsed.getBillingAddress() == null && billingFromCustomer != null) {
                    parsed.setBillingAddress(billingFromCustomer);
                }
                if (parsed.getShippingAddress() == null && shippingFromCustomer != null) {
                    parsed.setShippingAddress(shippingFromCustomer);
                }
                if (parsed.getShippingAddress() == null && parsed.getBillingAddress() != null) {
                    parsed.setShippingAddress(parsed.getBillingAddress());
                }
            }
        }

        return parsed;
    }

    /**
     * Parser de endereco vindo de Customer.Addresses[] em JSON.
     * Tenta extrair primeiro do sub-objeto "Address" (StreetName/CityName/CityId/StateId)
     * e cai em fallback no envelope (DsAddress/DsCity/IdCity/IdState).
     */
    private static OrderAddressDTO parseAddressFromJson(JsonObject addrObj) {
        OrderAddressDTO dto = new OrderAddressDTO();
        JsonObject nested = getJsonObject(addrObj, "Address", "address");
        // Tenta nested primeiro, depois envelope
        dto.setStreet(firstNonEmptyMany(
                nested != null ? getJsonString(nested, "StreetName", "streetName") : null,
                getJsonString(addrObj, "DsAddress", "dsAddress")));
        dto.setNumber(firstNonEmptyMany(
                nested != null ? getJsonString(nested, "StreetNumber", "streetNumber") : null,
                getJsonString(addrObj, "DsNumber", "dsNumber")));
        dto.setComplement(firstNonEmptyMany(
                nested != null ? getJsonString(nested, "Complement", "complement") : null,
                getJsonString(addrObj, "DsComplement", "dsComplement")));
        dto.setNeighborhood(firstNonEmptyMany(
                nested != null ? getJsonString(nested, "Neighborhood", "neighborhood") : null,
                getJsonString(addrObj, "DsDistrict", "dsDistrict")));
        dto.setCity(firstNonEmptyMany(
                nested != null ? getJsonString(nested, "CityName", "cityName") : null,
                getJsonString(addrObj, "DsCity", "dsCity")));
        dto.setState(firstNonEmptyMany(
                nested != null ? getJsonString(nested, "StateId", "stateId") : null,
                getJsonString(addrObj, "IdState", "idState")));
        dto.setZipCode(firstNonEmptyMany(
                nested != null ? getJsonString(nested, "ZipCode", "zipCode") : null,
                getJsonString(addrObj, "NuZip", "nuZip")));
        dto.setCityIbgeCode(firstNonEmptyMany(
                nested != null ? getJsonString(nested, "CityId", "cityId") : null,
                getJsonString(addrObj, "IdCity", "idCity")));
        dto.setRecipientName(firstNonEmptyMany(
                getJsonString(addrObj, "DeliveryTo", "deliveryTo"),
                getJsonString(addrObj, "RecipientName", "recipientName")));
        dto.setRecipientPhone(firstNonEmptyMany(
                getJsonString(addrObj, "NuPhone", "nuPhone"),
                getJsonString(addrObj, "NuMobilePhone", "nuMobilePhone")));
        return dto;
    }

    private static JsonObject getJsonObject(JsonObject source, String... keys) {
        if (source == null || keys == null) return null;
        for (String key : keys) {
            if (key == null || !source.has(key)) continue;
            JsonElement el = source.get(key);
            if (el != null && el.isJsonObject()) {
                return el.getAsJsonObject();
            }
        }
        return null;
    }

    private static String getNestedJsonString(JsonObject source, String keyA, String keyB, String... nestedKeys) {
        JsonObject nested = getJsonObject(source, keyA, keyB);
        return getJsonString(nested, nestedKeys);
    }

    private static String getJsonString(JsonObject source, String... keys) {
        if (source == null || keys == null) return null;
        for (String key : keys) {
            if (key == null || !source.has(key)) continue;
            JsonElement el = source.get(key);
            if (el == null || el.isJsonNull()) continue;
            try {
                String value = el.getAsString();
                if (!isBlank(value)) {
                    return value;
                }
            } catch (Exception ignored) {
            }
        }
        return null;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static String firstNonEmptyMany(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (!isBlank(value)) {
                return value;
            }
        }
        return null;
    }

    private static Timestamp parseTimestamp(JsonElement json) {
        if (json == null || json.isJsonNull()) {
            return null;
        }

        String raw = json.getAsString();
        if (raw == null || raw.isEmpty()) {
            return null;
        }

        raw = raw.trim();
        if (raw.startsWith("/Date(") && raw.endsWith(")/")) {
            int start = raw.indexOf('(');
            int end = raw.indexOf(')');
            if (start >= 0 && end > start) {
                String millis = raw.substring(start + 1, end);
                try {
                    long value = Long.parseLong(millis);
                    return new Timestamp(value);
                } catch (NumberFormatException ignored) {
                }
            }
        }

        try {
            long value = Long.parseLong(raw);
            return new Timestamp(value);
        } catch (NumberFormatException ignored) {
        }

        try {
            return Timestamp.valueOf(raw.replace("T", " ").replace("Z", ""));
        } catch (Exception ignored) {
        }

        try {
            return Timestamp.from(OffsetDateTime.parse(raw).toInstant());
        } catch (Exception ignored) {
        }

        try {
            return Timestamp.from(Instant.parse(raw));
        } catch (Exception ignored) {
        }

        try {
            return Timestamp.valueOf(LocalDateTime.parse(raw.replace("Z", "")));
        } catch (Exception ignored) {
        }

        return null;
    }

    private static String sanitizeBody(String body) {
        if (body == null) return "";
        String trimmed = body.trim();
        if (!trimmed.isEmpty() && trimmed.charAt(0) == '\uFEFF') {
            trimmed = trimmed.substring(1).trim();
        }
        return trimmed;
    }

    private static OrderListResult parseOrdersXml(String xml) {
        try {
            Document doc = parseXml(xml);
            if (doc == null) return null;

            List<OrderDTO> orders = new ArrayList<>();
            NodeList summaries = doc.getElementsByTagName("OrderSummary");
            for (int i = 0; i < summaries.getLength(); i++) {
                Node node = summaries.item(i);
                if (!(node instanceof Element)) continue;
                Element el = (Element) node;
                OrderDTO order = new OrderDTO();
                order.setOrderId(getChildText(el, "OrderId"));
                order.setExternalOrderId(getChildText(el, "OrderCode"));
                order.setResellerId(getChildText(el, "ResellerId"));
                order.setStorageId(getChildText(el, "StorageId"));

                String statusId = getChildText(el, "CurrentStatusId");
                if (statusId != null && !statusId.trim().isEmpty()) {
                    try {
                        order.setStatus(Integer.parseInt(statusId.trim()));
                    } catch (NumberFormatException ignored) {
                    }
                }
                order.setStatusDescription(getChildText(el, "CurrentStatusDescription"));
                order.setCreatedAt(parseTimestamp(getChildText(el, "CreatedAt")));
                orders.add(order);
            }

            Integer totalRecords = tryParseInt(firstTagText(doc, "TotalRecords"));
            Integer totalPages = tryParseInt(firstTagText(doc, "TotalPages"));
            return new OrderListResult(orders, totalRecords, totalPages);
        } catch (Exception e) {
            log.log(Level.WARNING, "Falha ao parsear XML da listagem de pedidos", e);
            return new OrderListResult(new ArrayList<>(), null, null);
        }
    }

    private static OrderDTO parseOrderXml(String xml) {
        try {
            Document doc = parseXml(xml);
            if (doc == null) return null;

            Element payload = firstElement(doc, "Payload");
            if (payload == null) return null;

            OrderDTO order = new OrderDTO();
            order.setOrderId(firstTagText(payload, "OrderId"));
            order.setExternalOrderId(firstTagText(payload, "OrderCode"));
            order.setResellerId(firstTagText(payload, "ResellerId"));
            order.setStorageId(firstTagText(payload, "StorageId"));
            order.setStatus(tryParseInt(firstTagText(payload, "OrderStatusId"), 0));
            order.setStatusDescription(firstTagText(payload, "OrderStatusDescription"));
            order.setCreatedAt(parseTimestamp(firstTagText(payload, "CreatedAt")));
            order.setSubtotalProducts(tryParseMoney(firstTagText(payload, "SubtotalProducts")));
            order.setShippingCost(tryParseMoney(firstTagText(payload, "ShippingCost")));
            order.setShippingDiscount(tryParseMoney(firstTagText(payload, "ShippingDiscount")));
            order.setShippingDiscountAmount(tryParseMoney(firstTagText(payload, "ShippingDiscountAmount")));
            order.setProductDiscount(tryParseMoney(firstTagText(payload, "ProductDiscount")));
            order.setProductDiscountCoupon(tryParseMoney(firstTagText(payload, "ProductDiscountCoupon")));
            order.setProductDiscountManual(tryParseMoney(firstTagText(payload, "ProductDiscountManual")));
            order.setProductDiscountPayment(tryParseMoney(firstTagText(payload, "ProductDiscountPayment")));
            order.setProductDiscountAssociation(tryParseMoney(firstTagText(payload, "ProductDiscountAssociation")));
            order.setTotalOrderValue(tryParseMoney(firstTagText(payload, "TotalOrderValue")));

            OrderCustomerDTO customer = new OrderCustomerDTO();
            Element customerEl = firstElement(payload, "Customer");
            if (customerEl != null) {
                customer.setCustomerId(firstTagText(customerEl, "CustomerId"));
                customer.setName(firstTagText(customerEl, "FullName"));
                customer.setEmail(firstTagText(customerEl, "EmailAddress"));
                customer.setCpfCnpj(firstNonEmpty(
                        firstTagText(customerEl, "CompanyFederalRegistry"),
                        firstTagText(customerEl, "CustomerFederalRegistry")));
                String customerType = firstTagText(customerEl, "CustomerTypeFlag");
                if (customerType == null || customerType.trim().isEmpty()) {
                    customerType = firstTagText(customerEl, "CustomerTypeName");
                }
                customer.setPersonType(customerType);
                customer.setCompanyName(firstTagText(customerEl, "FullName"));
            }
            order.setCustomer(customer);

            Element shippingEl = firstElement(payload, "ShippingData");
            if (shippingEl != null) {
                order.setShippingAddress(parseAddress(shippingEl));
            }
            Element billingEl = firstElement(payload, "BillingData");
            if (billingEl != null) {
                order.setBillingAddress(parseAddress(billingEl));
            }

            // [FIX 2026-04-27] Endpoint GET /orders/{id} retorna enderecos em
            // Customer.Addresses[] (cada item tem OrderAddressTypeId: 1=billing, 2=shipping)
            // - NAO em ShippingData/BillingData top-level. Sem este parse, customer.address
            // chegava null em createParceiro -> "CODCID nao resolvido" para todo cliente novo.
            // Caso reportado: pedido 4726 cliente Alegra Tur (Uba/MG/IBGE 3169901).
            if (customerEl != null) {
                Element addressesEl = firstElement(customerEl, "Addresses");
                if (addressesEl != null) {
                    NodeList addressNodes = addressesEl.getChildNodes();
                    OrderAddressDTO billingFromCustomer = null;
                    OrderAddressDTO shippingFromCustomer = null;
                    for (int i = 0; i < addressNodes.getLength(); i++) {
                        Node n = addressNodes.item(i);
                        if (!(n instanceof Element)) continue;
                        Element addrEl = (Element) n;
                        String typeId = firstTagText(addrEl, "OrderAddressTypeId");
                        // Tenta primeiro o sub-elemento Address (estrutura aninhada)
                        Element nestedAddress = firstElement(addrEl, "Address");
                        OrderAddressDTO parsed = parseAddress(nestedAddress != null ? nestedAddress : addrEl);
                        // Se IBGE/cidade nao vieram do nested, retentar no envelope
                        if (parsed.getCityIbgeCode() == null) {
                            String envIbge = firstNonEmpty(firstTagText(addrEl, "CityId"), firstTagText(addrEl, "IdCity"));
                            if (envIbge != null) parsed.setCityIbgeCode(envIbge);
                        }
                        if (parsed.getCity() == null) {
                            String envCity = firstNonEmpty(firstTagText(addrEl, "CityName"), firstTagText(addrEl, "DsCity"));
                            if (envCity != null) parsed.setCity(envCity);
                        }
                        if (parsed.getState() == null) {
                            String envState = firstNonEmpty(firstTagText(addrEl, "StateId"), firstTagText(addrEl, "IdState"));
                            if (envState != null) parsed.setState(envState);
                        }
                        if ("1".equals(typeId)) {
                            billingFromCustomer = parsed;
                        } else if ("2".equals(typeId)) {
                            shippingFromCustomer = parsed;
                        } else if (billingFromCustomer == null) {
                            billingFromCustomer = parsed; // fallback: 1o endereco eh billing
                        }
                    }
                    if (order.getBillingAddress() == null && billingFromCustomer != null) {
                        order.setBillingAddress(billingFromCustomer);
                    }
                    if (order.getShippingAddress() == null && shippingFromCustomer != null) {
                        order.setShippingAddress(shippingFromCustomer);
                    }
                    // Se ainda sem shipping mas tem billing, reaproveita
                    if (order.getShippingAddress() == null && order.getBillingAddress() != null) {
                        order.setShippingAddress(order.getBillingAddress());
                    }
                }
            }

            List<OrderItemDTO> items = new ArrayList<>();
            Element itemsEl = firstElement(payload, "Items");
            if (itemsEl != null) {
                NodeList itemNodes = itemsEl.getElementsByTagName("OrderItem");
                for (int i = 0; i < itemNodes.getLength(); i++) {
                    Node node = itemNodes.item(i);
                    if (!(node instanceof Element)) continue;
                    Element itemEl = (Element) node;
                    OrderItemDTO item = new OrderItemDTO();
                    item.setSku(firstTagText(itemEl, "ProductId"));
                    item.setProductName(firstTagText(itemEl, "ProductName"));
                    item.setQuantity(tryParseMoney(firstTagText(itemEl, "Quantity")));
                    item.setUnitPrice(tryParseMoney(firstTagText(itemEl, "SalePrice")));
                    item.setListPrice(tryParseMoney(firstTagText(itemEl, "ListPrice")));
                    item.setTotalPrice(tryParseMoney(firstTagText(itemEl, "TotalProductCost")));
                    item.setAssociationDiscount(tryParseMoney(firstTagText(itemEl, "AssociationDiscount")));
                    item.setManualDiscount(tryParseMoney(firstTagText(itemEl, "ManualDiscount")));
                    item.setCatalogDiscount(tryParseMoney(firstTagText(itemEl, "CatalogDiscount")));
                    item.setCouponDiscount(tryParseMoney(firstTagText(itemEl, "CouponDiscount")));
                    item.setPaymentDiscount(tryParseMoney(firstTagText(itemEl, "PaymentDiscount")));
                    items.add(item);
                }
            }
            order.setItems(items);

            return order;
        } catch (Exception e) {
            log.log(Level.WARNING, "Falha ao parsear XML de detalhe do pedido", e);
            return null;
        }
    }

    private static OrderAddressDTO parseAddress(Element addressRoot) {
        OrderAddressDTO dto = new OrderAddressDTO();
        dto.setStreet(firstNonEmpty(firstTagText(addressRoot, "StreetName"), firstTagText(addressRoot, "DsAddress")));
        dto.setNumber(firstNonEmpty(firstTagText(addressRoot, "StreetNumber"), firstTagText(addressRoot, "DsNumber")));
        dto.setComplement(firstNonEmpty(firstTagText(addressRoot, "Complement"), firstTagText(addressRoot, "DsComplement")));
        dto.setNeighborhood(firstNonEmpty(firstTagText(addressRoot, "Neighborhood"), firstTagText(addressRoot, "DsDistrict")));
        dto.setCity(firstNonEmpty(firstTagText(addressRoot, "CityName"), firstTagText(addressRoot, "DsCity")));
        dto.setState(firstNonEmpty(firstTagText(addressRoot, "StateId"), firstTagText(addressRoot, "IdState")));
        // CityId/IdCity = codigo IBGE do municipio (7 digitos). Match exato em TSICID.CODMUNFIS.
        dto.setCityIbgeCode(firstNonEmpty(firstTagText(addressRoot, "CityId"), firstTagText(addressRoot, "IdCity")));
        dto.setZipCode(firstNonEmpty(firstTagText(addressRoot, "ZipCode"), firstTagText(addressRoot, "NuZip")));
        dto.setRecipientName(firstNonEmpty(firstTagText(addressRoot, "DeliveryTo"), firstTagText(addressRoot, "RecipientName")));
        dto.setRecipientPhone(firstNonEmpty(firstTagText(addressRoot, "NuPhone"), firstTagText(addressRoot, "NuMobilePhone")));
        return dto;
    }

    private static Document parseXml(String xml) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(false);
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        Document doc = factory.newDocumentBuilder().parse(new InputSource(new StringReader(xml)));
        doc.getDocumentElement().normalize();
        return doc;
    }

    private static Element firstElement(Document doc, String tag) {
        NodeList list = doc.getElementsByTagName(tag);
        if (list == null || list.getLength() == 0) return null;
        Node node = list.item(0);
        return (node instanceof Element) ? (Element) node : null;
    }

    private static Element firstElement(Element root, String tag) {
        NodeList list = root.getElementsByTagName(tag);
        if (list == null || list.getLength() == 0) return null;
        Node node = list.item(0);
        return (node instanceof Element) ? (Element) node : null;
    }

    private static String firstTagText(Document doc, String tag) {
        Element el = firstElement(doc, tag);
        return el != null ? el.getTextContent() : null;
    }

    private static String firstTagText(Element root, String tag) {
        Element el = firstElement(root, tag);
        return el != null ? el.getTextContent() : null;
    }

    private static String getChildText(Element root, String tag) {
        NodeList children = root.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child instanceof Element && tag.equals(((Element) child).getTagName())) {
                return child.getTextContent();
            }
        }
        return null;
    }

    private static Integer tryParseInt(String value) {
        if (value == null || value.trim().isEmpty()) return null;
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static int tryParseInt(String value, int fallback) {
        Integer parsed = tryParseInt(value);
        return parsed != null ? parsed : fallback;
    }

    private static BigDecimal tryParseMoney(String value) {
        if (value == null || value.trim().isEmpty()) return null;
        try {
            return new BigDecimal(value.trim());
        } catch (Exception e) {
            return null;
        }
    }

    private static Timestamp parseTimestamp(String value) {
        if (value == null || value.trim().isEmpty()) return null;
        String raw = value.trim();

        try {
            return Timestamp.from(OffsetDateTime.parse(raw).toInstant());
        } catch (Exception ignored) {
        }

        try {
            return Timestamp.from(Instant.parse(raw));
        } catch (Exception ignored) {
        }

        try {
            return Timestamp.valueOf(raw.replace("T", " ").replace("Z", ""));
        } catch (Exception ignored) {
        }

        try {
            return Timestamp.valueOf(LocalDateTime.parse(raw.replace("Z", "")));
        } catch (Exception ignored) {
        }

        return null;
    }

    private static String firstNonEmpty(String a, String b) {
        if (a != null && !a.trim().isEmpty()) return a;
        return b;
    }

    private static class OrderListResponse {
        @SerializedName("Payload")
        private List<OrderDTO> payload;
        @SerializedName("TotalRecords")
        private Integer totalRecords;
        @SerializedName("TotalPages")
        private Integer totalPages;
    }

    public static class OrderListResult {
        private final List<OrderDTO> orders;
        private final Integer totalRecords;
        private final Integer totalPages;

        public OrderListResult(List<OrderDTO> orders, Integer totalRecords, Integer totalPages) {
            this.orders = orders;
            this.totalRecords = totalRecords;
            this.totalPages = totalPages;
        }

        public List<OrderDTO> getOrders() {
            return orders;
        }

        public Integer getTotalRecords() {
            return totalRecords;
        }

        public Integer getTotalPages() {
            return totalPages;
        }
    }
}
