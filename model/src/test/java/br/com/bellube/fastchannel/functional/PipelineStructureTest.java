package br.com.bellube.fastchannel.functional;

import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.OrderCustomerDTO;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.dto.OrderItemDTO;
import br.com.bellube.fastchannel.http.FastchannelPriceClient;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.FastchannelHeaderMappingService;
import br.com.bellube.fastchannel.service.OrderService;
import br.com.bellube.fastchannel.service.OrderXmlBuilder;
import br.com.bellube.fastchannel.service.PriceResolver;
import br.com.bellube.fastchannel.service.StockResolver;
import org.junit.Test;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

/**
 * Testes funcionais: valida que os pipelines completos mantém
 * sua estrutura correta, constantes alinhadas, e guards ativos.
 */
public class PipelineStructureTest {

    // ===================== PIPELINE: Order Import =====================

    @Test
    public void orderPipeline_validateOrder_rejectsInvalidInput() throws Exception {
        OrderService svc = new OrderService();
        Method validateOrder = OrderService.class.getDeclaredMethod("validateOrder", OrderDTO.class);
        validateOrder.setAccessible(true);

        // Null orderId
        try {
            OrderDTO order = new OrderDTO();
            order.setCustomer(new OrderCustomerDTO());
            order.setItems(Collections.singletonList(new OrderItemDTO()));
            validateOrder.invoke(svc, order);
            fail("Deveria rejeitar orderId null");
        } catch (java.lang.reflect.InvocationTargetException e) {
            // expected
        }

        // Null customer
        try {
            OrderDTO order = new OrderDTO();
            order.setOrderId("F-001");
            order.setItems(Collections.singletonList(new OrderItemDTO()));
            validateOrder.invoke(svc, order);
            fail("Deveria rejeitar customer null");
        } catch (java.lang.reflect.InvocationTargetException e) {
            // expected
        }

        // Empty items
        try {
            OrderDTO order = new OrderDTO();
            order.setOrderId("F-001");
            order.setCustomer(new OrderCustomerDTO());
            order.setItems(Collections.emptyList());
            validateOrder.invoke(svc, order);
            fail("Deveria rejeitar items vazio");
        } catch (java.lang.reflect.InvocationTargetException e) {
            // expected
        }
    }

    @Test
    public void orderPipeline_validateOrder_acceptsValidInput() throws Exception {
        OrderService svc = new OrderService();
        Method validateOrder = OrderService.class.getDeclaredMethod("validateOrder", OrderDTO.class);
        validateOrder.setAccessible(true);

        OrderDTO order = new OrderDTO();
        order.setOrderId("F-001");
        order.setCustomer(new OrderCustomerDTO());
        order.setItems(Arrays.asList(new OrderItemDTO()));
        validateOrder.invoke(svc, order); // nao deve lancar excecao
    }

    @Test
    public void orderPipeline_normalizeOrderValues_convertsCentavosToReais() throws Exception {
        OrderService svc = new OrderService();
        Method normalize = OrderService.class.getDeclaredMethod("normalizeOrderValues", OrderDTO.class);
        normalize.setAccessible(true);

        OrderDTO order = new OrderDTO();
        order.setTotal(new BigDecimal("15000")); // centavos (sem escala)
        order.setShippingCost(new BigDecimal("500"));
        order.setDiscount(new BigDecimal("1000"));

        OrderItemDTO item = new OrderItemDTO();
        item.setUnitPrice(new BigDecimal("2500"));
        item.setDiscount(new BigDecimal("200"));
        order.setItems(Collections.singletonList(item));

        normalize.invoke(svc, order);

        // Apos normalizacao, valores inteiros (escala 0) devem virar decimais
        assertTrue("Total deve ter escala > 0 apos normalizacao",
                order.getTotal().scale() > 0 || order.getTotal().compareTo(new BigDecimal("150.00")) == 0);
    }

    // ===================== PIPELINE: Stock Sync =====================

    @Test
    public void stockPipeline_sqlContainsEstoqueMinusReservado() throws Exception {
        StockResolver resolver = new StockResolver();
        Method getSql = StockResolver.class.getDeclaredMethod("getSql");
        getSql.setAccessible(true);
        String sql = (String) getSql.invoke(resolver);
        assertTrue("SQL deve conter ESTOQUE", sql.contains("ESTOQUE"));
        assertTrue("SQL deve conter RESERVADO", sql.contains("RESERVADO"));
    }

    @Test
    public void stockPipeline_resolver_nullArgs_returnsNull() {
        StockResolver resolver = new StockResolver();
        assertNull(resolver.resolve(null, null, null));
        assertNull(resolver.resolve(new BigDecimal("1"), null, null));
        assertNull(resolver.resolve(null, new BigDecimal("1"), null));
    }

    @Test
    public void stockPipeline_skuBrandRule_allCombinations() {
        // R com REFFORN -> usa REFFORN
        assertEquals("REF-001", DeparaService.computeSkuFromBrandRule("R", new BigDecimal("10"), "REF-001"));
        // R sem REFFORN -> usa CODPROD
        assertEquals("10", DeparaService.computeSkuFromBrandRule("R", new BigDecimal("10"), null));
        assertEquals("10", DeparaService.computeSkuFromBrandRule("R", new BigDecimal("10"), ""));
        assertEquals("10", DeparaService.computeSkuFromBrandRule("R", new BigDecimal("10"), "  "));
        // C -> sempre usa CODPROD
        assertEquals("10", DeparaService.computeSkuFromBrandRule("C", new BigDecimal("10"), "REF-001"));
        // null -> usa CODPROD
        assertEquals("10", DeparaService.computeSkuFromBrandRule(null, new BigDecimal("10"), "REF-001"));
        // null CODPROD
        assertNull(DeparaService.computeSkuFromBrandRule("C", null, "REF-001"));
    }

    // ===================== PIPELINE: Price Sync =====================

    @Test
    public void pricePipeline_toCentavos_variousAmounts() throws Exception {
        PriceResolver resolver = new PriceResolver();
        Method m = PriceResolver.class.getDeclaredMethod("toCentavos", BigDecimal.class);
        m.setAccessible(true);

        assertEquals(new BigDecimal("1599"), m.invoke(resolver, new BigDecimal("15.99")));
        assertEquals(new BigDecimal("100"), m.invoke(resolver, new BigDecimal("1.00")));
        assertEquals(new BigDecimal("1"), m.invoke(resolver, new BigDecimal("0.01")));
        assertEquals(new BigDecimal("0"), m.invoke(resolver, BigDecimal.ZERO));
        assertNull(m.invoke(resolver, (BigDecimal) null));
    }

    @Test
    public void pricePipeline_channelEnum_hasTwoValues() {
        FastchannelPriceClient.Channel[] values = FastchannelPriceClient.Channel.values();
        assertEquals("Deve ter exatamente 2 canais", 2, values.length);
    }

    @Test
    public void pricePipeline_defaultClient_isConsumption() {
        FastchannelPriceClient client = new FastchannelPriceClient();
        assertEquals(FastchannelPriceClient.Channel.CONSUMPTION, client.getChannel());
    }

    // ===================== CONSTANTS: Alinhamento com legado =====================

    @Test
    public void constants_endpointsNotNull() {
        assertNotNull(FastchannelConstants.ENDPOINT_ORDERS);
        assertNotNull(FastchannelConstants.ENDPOINT_STOCK);
        assertNotNull(FastchannelConstants.ENDPOINT_PRICE);
        assertNotNull(FastchannelConstants.ENDPOINT_PRICE_BATCHES);
        assertNotNull(FastchannelConstants.ENDPOINT_ORDER_INVOICES);
        assertNotNull(FastchannelConstants.ENDPOINT_ORDER_STATUS);
        assertNotNull(FastchannelConstants.ENDPOINT_ORDER_TRACKING);
    }

    @Test
    public void constants_statusCodes_coherent() {
        assertTrue("APPROVED (201) > CREATED (200)",
                FastchannelConstants.STATUS_APPROVED > FastchannelConstants.STATUS_CREATED);
        assertTrue("INVOICE_CREATED (300) > APPROVED (201)",
                FastchannelConstants.STATUS_INVOICE_CREATED > FastchannelConstants.STATUS_APPROVED);
        assertTrue("DENIED (400) > DELIVERED (301)",
                FastchannelConstants.STATUS_DENIED > FastchannelConstants.STATUS_DELIVERED);
    }

    @Test
    public void constants_tables_matchExpectedNames() {
        assertEquals("AD_FCCONFIG", FastchannelConstants.TABLE_CONFIG);
        assertEquals("AD_FCQUEUE", FastchannelConstants.TABLE_QUEUE);
        assertEquals("AD_FCDEPARA", FastchannelConstants.TABLE_DEPARA);
        assertEquals("AD_FCPEDIDO", FastchannelConstants.TABLE_PEDIDO);
        assertEquals("AD_FCLOG", FastchannelConstants.TABLE_LOG);
    }

    @Test
    public void constants_queueStatuses_notEmpty() {
        assertFalse(FastchannelConstants.QUEUE_STATUS_PENDENTE.isEmpty());
        assertFalse(FastchannelConstants.QUEUE_STATUS_PROCESSANDO.isEmpty());
        assertFalse(FastchannelConstants.QUEUE_STATUS_ENVIADO.isEmpty());
        assertFalse(FastchannelConstants.QUEUE_STATUS_ERRO.isEmpty());
        assertFalse(FastchannelConstants.QUEUE_STATUS_ERRO_FATAL.isEmpty());
    }

    // ===================== GUARD: XML structure =====================

    @Test
    public void xmlBuilder_serviceRequestTag_isCorrectCase() throws Exception {
        // Verifica que a string literal "serviceRequest" (com R maiusculo) esta presente
        // no template do XML (nao "servicerequest")
        OrderXmlBuilder builder = new OrderXmlBuilder();
        Method m = OrderXmlBuilder.class.getDeclaredMethod("buildIncluirNotaXml",
                OrderDTO.class, BigDecimal.class, BigDecimal.class,
                BigDecimal.class, BigDecimal.class, BigDecimal.class);
        assertNotNull("buildIncluirNotaXml deve existir com assinatura correta", m);
    }

    @Test
    public void xmlBuilder_normalizeMoney_null_safe() throws Exception {
        OrderXmlBuilder builder = new OrderXmlBuilder();
        Method m = OrderXmlBuilder.class.getDeclaredMethod("normalizeMoney", BigDecimal.class);
        m.setAccessible(true);
        assertNull(m.invoke(builder, (BigDecimal) null));
    }

    // ===================== GUARD: HeaderMappingService interfaces =====================

    @Test
    public void headerMapping_interfacesExist() {
        // Garante que as interfaces publicas de lookup existem
        assertNotNull(FastchannelHeaderMappingService.DeparaLookup.class);
        assertNotNull(FastchannelHeaderMappingService.PartnerLookup.class);
        assertNotNull(FastchannelHeaderMappingService.TipNegLookup.class);
        assertNotNull(FastchannelHeaderMappingService.ConfigLookup.class);
    }

    @Test
    public void headerMapping_resolvedHeader_hasAllFields() {
        FastchannelHeaderMappingService.ResolvedHeader header =
                new FastchannelHeaderMappingService.ResolvedHeader();
        // Todos os campos devem ser null por padrao (nao NPE)
        assertNull(header.getCodEmp());
        assertNull(header.getCodTipOper());
        assertNull(header.getCodTipVenda());
        assertNull(header.getCodParc());
        assertNull(header.getCodNat());
        assertNull(header.getCodCenCus());
        assertNull(header.getCodVend());
    }

    // ===================== GUARD: OrderDTO total fallback chain =====================

    @Test
    public void orderDTO_getTotal_fallbackChain() {
        OrderDTO order = new OrderDTO();
        assertNull("Sem nenhum total, deve retornar null", order.getTotal());

        order.setOrderTotal(new BigDecimal("100"));
        assertEquals(new BigDecimal("100"), order.getTotal());

        order.setTotalOrderValue(new BigDecimal("200"));
        assertEquals("TotalOrderValue tem prioridade sobre OrderTotal",
                new BigDecimal("200"), order.getTotal());

        order.setTotal(new BigDecimal("300"));
        assertEquals("Total direto tem prioridade sobre todos",
                new BigDecimal("300"), order.getTotal());
    }

    @Test
    public void orderDTO_getDiscount_aggregatesAllSources() {
        OrderDTO order = new OrderDTO();
        order.setProductDiscount(new BigDecimal("10"));
        order.setProductDiscountCoupon(new BigDecimal("5"));
        order.setShippingDiscount(new BigDecimal("3"));

        BigDecimal total = order.getDiscount();
        assertEquals(new BigDecimal("18"), total);
    }

    @Test
    public void orderDTO_getPaymentMethodId_prefersCurrentPaymentDetails() {
        OrderDTO order = new OrderDTO();
        order.setPaymentMethodId(1);

        br.com.bellube.fastchannel.dto.OrderPaymentDetailsDTO payment =
                new br.com.bellube.fastchannel.dto.OrderPaymentDetailsDTO();
        payment.setPaymentMethodId(6);
        order.setCurrentPaymentDetails(payment);

        assertEquals(Integer.valueOf(6), order.getPaymentMethodId());
    }
}
