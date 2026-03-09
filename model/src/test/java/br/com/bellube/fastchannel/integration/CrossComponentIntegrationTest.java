package br.com.bellube.fastchannel.integration;

import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.dto.OrderCustomerDTO;
import br.com.bellube.fastchannel.dto.OrderPaymentDetailsDTO;
import br.com.bellube.fastchannel.http.FastchannelPriceClient;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.FastchannelHeaderMappingService;
import br.com.bellube.fastchannel.service.OrderXmlBuilder;
import br.com.bellube.fastchannel.service.PriceResolver;
import br.com.bellube.fastchannel.service.strategy.HttpServiceStrategy;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.math.BigDecimal;

import static org.junit.Assert.*;

/**
 * Testes de integracao: verifica interacao entre componentes
 * sem dependencia de banco ou APIs externas.
 */
public class CrossComponentIntegrationTest {

    // ===================== OrderXmlBuilder: XML escape + normalize =====================

    @Test
    public void xmlEscape_worksWithAllSpecialChars() throws Exception {
        OrderXmlBuilder builder = new OrderXmlBuilder();
        Method m = OrderXmlBuilder.class.getDeclaredMethod("xmlEscape", String.class);
        m.setAccessible(true);

        String result = (String) m.invoke(builder, "R&D <Grupo> \"Fast\" 'channel'");
        assertTrue(result.contains("&amp;"));
        assertTrue(result.contains("&lt;"));
        assertTrue(result.contains("&gt;"));
        assertTrue(result.contains("&quot;"));
        assertTrue(result.contains("&apos;"));
        assertFalse("Nao deve ter & solto (fora de entities)",
                result.contains("& ") || result.contains("&<")); // & sempre escapado
    }

    @Test
    public void xmlEscape_nullReturnsEmpty() throws Exception {
        OrderXmlBuilder builder = new OrderXmlBuilder();
        Method m = OrderXmlBuilder.class.getDeclaredMethod("xmlEscape", String.class);
        m.setAccessible(true);
        assertEquals("", m.invoke(builder, (String) null));
    }

    @Test
    public void xmlEscape_emptyString() throws Exception {
        OrderXmlBuilder builder = new OrderXmlBuilder();
        Method m = OrderXmlBuilder.class.getDeclaredMethod("xmlEscape", String.class);
        m.setAccessible(true);
        assertEquals("", m.invoke(builder, ""));
    }

    @Test
    public void isBlank_worksInXmlContext() throws Exception {
        OrderXmlBuilder builder = new OrderXmlBuilder();
        Method m = OrderXmlBuilder.class.getDeclaredMethod("isBlank", String.class);
        m.setAccessible(true);
        assertTrue((boolean) m.invoke(builder, (String) null));
        assertTrue((boolean) m.invoke(builder, ""));
        assertTrue((boolean) m.invoke(builder, "   "));
        assertFalse((boolean) m.invoke(builder, "CTRL-001"));
    }

    // ===================== normalizeMoney: centavos/reais boundary =====================

    @Test
    public void normalizeMoney_edgeCases() throws Exception {
        OrderXmlBuilder builder = new OrderXmlBuilder();
        Method m = OrderXmlBuilder.class.getDeclaredMethod("normalizeMoney", BigDecimal.class);
        m.setAccessible(true);

        // Zero inteiro (sem escala) -> 0.00
        BigDecimal zeroInt = (BigDecimal) m.invoke(builder, BigDecimal.ZERO);
        assertEquals(new BigDecimal("0.00"), zeroInt);

        // Zero com escala = ja decimal
        BigDecimal zeroDecimal = (BigDecimal) m.invoke(builder, new BigDecimal("0.00"));
        assertEquals(new BigDecimal("0.00"), zeroDecimal);

        // Valor pequeno inteiro
        BigDecimal small = (BigDecimal) m.invoke(builder, new BigDecimal("1"));
        assertEquals(new BigDecimal("0.01"), small);

        // Valor grande inteiro
        BigDecimal large = (BigDecimal) m.invoke(builder, new BigDecimal("999999"));
        assertEquals(new BigDecimal("9999.99"), large);
    }

    // ===================== PriceResolver.toCentavos + normalizeMoney roundtrip =====================

    @Test
    public void priceResolver_toCentavos_roundTripWithNormalize() throws Exception {
        // Simula: Sankhya envia 15.99 -> PriceResolver converte para centavos 1599
        // -> Fastchannel retorna 1599 -> OrderXmlBuilder normaliza para 15.99
        PriceResolver resolver = new PriceResolver();
        Method toCentavos = PriceResolver.class.getDeclaredMethod("toCentavos", BigDecimal.class);
        toCentavos.setAccessible(true);
        BigDecimal centavos = (BigDecimal) toCentavos.invoke(resolver, new BigDecimal("15.99"));
        assertEquals(new BigDecimal("1599"), centavos);

        // Agora normalizar de volta
        OrderXmlBuilder builder = new OrderXmlBuilder();
        Method normalize = OrderXmlBuilder.class.getDeclaredMethod("normalizeMoney", BigDecimal.class);
        normalize.setAccessible(true);
        BigDecimal reais = (BigDecimal) normalize.invoke(builder, centavos);
        assertEquals(new BigDecimal("15.99"), reais);
    }

    // ===================== HttpServiceStrategy: parseNuNotaFromResponse =====================

    @Test
    public void httpStrategy_parseNuNota_standardXml() throws Exception {
        HttpServiceStrategy strategy = new HttpServiceStrategy();
        Method m = HttpServiceStrategy.class.getDeclaredMethod("parseNuNotaFromResponse", String.class);
        m.setAccessible(true);
        String xml = "<serviceResponse><responseBody><NUNOTA>67890</NUNOTA></responseBody></serviceResponse>";
        BigDecimal result = (BigDecimal) m.invoke(strategy, xml);
        assertEquals(new BigDecimal("67890"), result);
    }

    @Test
    public void httpStrategy_parseNuNota_notFound_returnsNull() throws Exception {
        HttpServiceStrategy strategy = new HttpServiceStrategy();
        Method m = HttpServiceStrategy.class.getDeclaredMethod("parseNuNotaFromResponse", String.class);
        m.setAccessible(true);
        assertNull(m.invoke(strategy, "<response><status>1</status></response>"));
    }

    @Test
    public void httpStrategy_extractErrorMessage() throws Exception {
        HttpServiceStrategy strategy = new HttpServiceStrategy();
        Method m = HttpServiceStrategy.class.getDeclaredMethod("extractErrorMessage", String.class);
        m.setAccessible(true);
        String xml = "<serviceResponse status=\"0\"><statusMessage>Erro de validacao</statusMessage></serviceResponse>";
        String msg = (String) m.invoke(strategy, xml);
        assertNotNull(msg);
        assertTrue(msg.contains("Erro de validacao"));
    }

    // ===================== SKU brand rule + PriceResolver integration =====================

    @Test
    public void skuBrandRule_thenPriceResolverConverts() throws Exception {
        // Regra de marca retorna SKU correto
        String sku = DeparaService.computeSkuFromBrandRule("R", new BigDecimal("500"), "SHELL-5W30");
        assertEquals("SHELL-5W30", sku);

        // PriceResolver converte preço
        PriceResolver resolver = new PriceResolver();
        Method toCentavos = PriceResolver.class.getDeclaredMethod("toCentavos", BigDecimal.class);
        toCentavos.setAccessible(true);
        BigDecimal centavos = (BigDecimal) toCentavos.invoke(resolver, new BigDecimal("49.90"));
        assertEquals(new BigDecimal("4990"), centavos);
    }

    // ===================== HeaderMappingService: TipNeg fallback to config =====================

    @Test
    public void headerMapping_tipNeg_fallsBackToConfig_whenNoPaymentMethod() throws Exception {
        OrderDTO order = new OrderDTO();
        order.setOrderId("INT-001");
        order.setCustomer(new OrderCustomerDTO());
        order.setPaymentMethodId(null);

        FastchannelHeaderMappingService.ConfigLookup config = createConfig(
                BigDecimal.ONE, new BigDecimal("403"), new BigDecimal("99"),
                null, null, new BigDecimal("167"));

        FastchannelHeaderMappingService.TipNegLookup tipNeg = paymentMethodId -> null;

        FastchannelHeaderMappingService service = createHeaderMappingService(
                (tipo, codExterno) -> null,
                new FastchannelHeaderMappingService.PartnerLookup() {
                    public BigDecimal findCodParcByDocument(String doc) { return null; }
                    public BigDecimal findCodVendByParc(BigDecimal codParc) { return null; }
                },
                tipNeg,
                config);

        Method m = FastchannelHeaderMappingService.class.getDeclaredMethod("resolveTipNeg", OrderDTO.class);
        m.setAccessible(true);
        BigDecimal result = (BigDecimal) m.invoke(service, order);
        assertEquals(new BigDecimal("99"), result);
    }

    @Test
    public void headerMapping_tipNeg_throwsWhenPaymentMethodNotMapped() throws Exception {
        OrderDTO order = new OrderDTO();
        order.setOrderId("INT-002");
        order.setCustomer(new OrderCustomerDTO());

        OrderPaymentDetailsDTO payment = new OrderPaymentDetailsDTO();
        payment.setPaymentMethodId(999);
        order.setCurrentPaymentDetails(payment);

        FastchannelHeaderMappingService.ConfigLookup config = createConfig(
                BigDecimal.ONE, new BigDecimal("403"), new BigDecimal("99"),
                null, null, new BigDecimal("167"));

        // TipNeg lookup retorna null -> deve falhar
        FastchannelHeaderMappingService.TipNegLookup tipNeg = paymentMethodId -> null;

        FastchannelHeaderMappingService service = createHeaderMappingService(
                (tipo, codExterno) -> null,
                new FastchannelHeaderMappingService.PartnerLookup() {
                    public BigDecimal findCodParcByDocument(String doc) { return null; }
                    public BigDecimal findCodVendByParc(BigDecimal codParc) { return null; }
                },
                tipNeg,
                config);

        Method m = FastchannelHeaderMappingService.class.getDeclaredMethod("resolveTipNeg", OrderDTO.class);
        m.setAccessible(true);
        try {
            m.invoke(service, order);
            fail("Deveria ter lancado excecao para PaymentMethodId sem mapeamento");
        } catch (java.lang.reflect.InvocationTargetException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
            assertTrue(e.getCause().getMessage().contains("CODTIPVENDA"));
        }
    }

    // ===================== PriceClient channel routing =====================

    @Test
    public void priceClient_channelRouting_differentInstances() {
        FastchannelPriceClient dist = new FastchannelPriceClient(FastchannelPriceClient.Channel.DISTRIBUTION);
        FastchannelPriceClient cons = new FastchannelPriceClient(FastchannelPriceClient.Channel.CONSUMPTION);

        assertEquals(FastchannelPriceClient.Channel.DISTRIBUTION, dist.getChannel());
        assertEquals(FastchannelPriceClient.Channel.CONSUMPTION, cons.getChannel());
        assertNotEquals(dist.getChannel(), cons.getChannel());
    }

    // ===================== Helpers =====================

    private FastchannelHeaderMappingService createHeaderMappingService(
            FastchannelHeaderMappingService.DeparaLookup deparaLookup,
            FastchannelHeaderMappingService.PartnerLookup partnerLookup,
            FastchannelHeaderMappingService.TipNegLookup tipNegLookup,
            FastchannelHeaderMappingService.ConfigLookup config) throws Exception {
        Constructor<FastchannelHeaderMappingService> ctor =
                FastchannelHeaderMappingService.class.getDeclaredConstructor(
                        FastchannelHeaderMappingService.DeparaLookup.class,
                        FastchannelHeaderMappingService.PartnerLookup.class,
                        FastchannelHeaderMappingService.TipNegLookup.class,
                        FastchannelHeaderMappingService.ConfigLookup.class);
        ctor.setAccessible(true);
        return ctor.newInstance(deparaLookup, partnerLookup, tipNegLookup, config);
    }

    private FastchannelHeaderMappingService.ConfigLookup createConfig(
            BigDecimal codEmp, BigDecimal codTipOper, BigDecimal tipNeg,
            BigDecimal codNat, BigDecimal codCenCus, BigDecimal codVend) {
        return new FastchannelHeaderMappingService.ConfigLookup() {
            public BigDecimal getCodemp() { return codEmp; }
            public BigDecimal getCodTipOper() { return codTipOper; }
            public BigDecimal getTipNeg() { return tipNeg; }
            public BigDecimal getCodNat() { return codNat; }
            public BigDecimal getCodCenCus() { return codCenCus; }
            public BigDecimal getCodVendPadrao() { return codVend; }
        };
    }
}
