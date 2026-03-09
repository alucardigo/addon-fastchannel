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
     * Lista pedidos pendentes desde a última sincronização.
     *
     * @param lastSync timestamp da última sincronização (null = todos)
     * @param page número da página (1-based)
     * @param pageSize tamanho da página
     * @return lista de pedidos
     */
    public List<OrderDTO> listOrders(Timestamp lastSync, int page, int pageSize) throws Exception {
        OrderListResult result = listOrdersWithMeta(lastSync, page, pageSize, Boolean.FALSE);
        return result != null && result.getOrders() != null ? result.getOrders() : new ArrayList<>();
    }

    public OrderListResult listOrdersWithMeta(Timestamp lastSync, int page, int pageSize, Boolean isSynched) throws Exception {
        StringBuilder endpoint = new StringBuilder(FastchannelConstants.ENDPOINT_ORDERS);
        endpoint.append("?PageNumber=").append(page);
        endpoint.append("&PageSize=").append(pageSize);

        // Filtro por reseller se configurado
        String resellerId = config.getResellerId();
        if (resellerId != null && !resellerId.isEmpty()) {
            endpoint.append("&ResellerIds=").append(resellerId);
        }

        if (isSynched != null) {
            endpoint.append("&IsSynched=").append(isSynched ? "true" : "false");
        }

        // Filtro por data
        if (lastSync != null) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            endpoint.append("&CreatedAfter=").append(sdf.format(lastSync));
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
        return parsed;
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
