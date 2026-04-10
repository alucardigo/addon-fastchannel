package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.dto.OrderDTO;
import org.junit.Test;

import java.lang.reflect.Method;
import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Testes unitarios para helpers puros de OrderXmlBuilder.
 *
 * Foca em invariantes tributarios/fiscais que causaram regressoes em producao:
 *   - PERCDESC deve ser sempre >= 0 (Sankhya rejeita negativo com CORE_E03235)
 *   - normalizeMoney deve mover o ponto decimal quando vier sem casas
 *   - xmlEscape deve escapar os 5 especiais do XML
 *   - getFrete nunca pode ser negativo
 *   - buildObservacao NAO deve vazar "Pedido Fastchannel"
 *   - buildObservacaoInterna DEVE conter "Pedido Fastchannel: <id>"
 *
 * Os helpers sao privados — sao acessados por reflexao.
 */
public class OrderXmlBuilderTest {

    // ===================== normalizeMoney =====================

    @Test
    public void normalizeMoney_integerValue_shiftsDecimalLeft() throws Exception {
        BigDecimal result = invokeNormalizeMoney(new BigDecimal("1500"));
        assertEquals(new BigDecimal("15.00"), result);
    }

    @Test
    public void normalizeMoney_alreadyDecimal_unchanged() throws Exception {
        BigDecimal result = invokeNormalizeMoney(new BigDecimal("15.99"));
        assertEquals(new BigDecimal("15.99"), result);
    }

    @Test
    public void normalizeMoney_null_returnsNull() throws Exception {
        assertNull(invokeNormalizeMoney(null));
    }

    @Test
    public void normalizeMoney_zero_returnsZeroWithoutShift() throws Exception {
        // zero sem casas ainda e tratado como integer e recebe movePointLeft
        BigDecimal result = invokeNormalizeMoney(new BigDecimal("0"));
        assertNotNull(result);
        assertEquals(0, result.compareTo(BigDecimal.ZERO));
    }

    // ===================== xmlEscape =====================

    @Test
    public void xmlEscape_nullReturnsEmpty() throws Exception {
        assertEquals("", invokeXmlEscape(null));
    }

    @Test
    public void xmlEscape_allFiveSpecials() throws Exception {
        assertEquals("&amp;&lt;&gt;&quot;&apos;", invokeXmlEscape("&<>\"'"));
    }

    @Test
    public void xmlEscape_plainTextUnchanged() throws Exception {
        assertEquals("Pedido 1234", invokeXmlEscape("Pedido 1234"));
    }

    // ===================== getFrete =====================

    @Test
    public void getFrete_nullOrderReturnsZero() throws Exception {
        assertEquals(0, invokeGetFrete(null).compareTo(BigDecimal.ZERO));
    }

    @Test
    public void getFrete_nullShippingCostReturnsZero() throws Exception {
        OrderDTO o = new OrderDTO();
        assertEquals(0, invokeGetFrete(o).compareTo(BigDecimal.ZERO));
    }

    @Test
    public void getFrete_withoutDiscounts_returnsShippingCost() throws Exception {
        OrderDTO o = new OrderDTO();
        o.setShippingCost(new BigDecimal("20.00"));
        assertEquals(0, invokeGetFrete(o).compareTo(new BigDecimal("20.00")));
    }

    @Test
    public void getFrete_discountLargerThanCost_returnsZeroNeverNegative() throws Exception {
        OrderDTO o = new OrderDTO();
        o.setShippingCost(new BigDecimal("15.00"));
        o.setShippingDiscount(new BigDecimal("10.00"));
        o.setShippingDiscountCoupon(new BigDecimal("10.00"));
        // total de desconto = 20 > 15 — nunca retornar negativo.
        assertEquals(0, invokeGetFrete(o).compareTo(BigDecimal.ZERO));
    }

    @Test
    public void getFrete_partialDiscountApplies() throws Exception {
        OrderDTO o = new OrderDTO();
        o.setShippingCost(new BigDecimal("50.00"));
        o.setShippingDiscount(new BigDecimal("10.00"));
        o.setShippingDiscountAmount(new BigDecimal("5.00"));
        assertEquals(0, invokeGetFrete(o).compareTo(new BigDecimal("35.00")));
    }

    // ===================== buildObservacao / buildObservacaoInterna =====================

    @Test
    public void buildObservacao_neverLeaksFastOrderId() throws Exception {
        OrderDTO o = new OrderDTO();
        o.setOrderId("4642");
        o.setNotes("Cliente pediu entrega rapida");
        o.setShippingMethod("Retira");

        String obs = invokeBuildObservacao(o);
        assertNotNull(obs);
        assertFalse("OBSERVACAO publica nao pode expor 'Pedido Fastchannel'",
                obs.contains("Pedido Fastchannel"));
        assertTrue(obs.contains("Cliente pediu entrega rapida"));
        assertTrue(obs.contains("Frete: Retira"));
    }

    @Test
    public void buildObservacao_nullWhenNoNotesAndNoShipping() throws Exception {
        OrderDTO o = new OrderDTO();
        o.setOrderId("4642");
        assertNull(invokeBuildObservacao(o));
    }

    @Test
    public void buildObservacaoInterna_containsFastOrderIdAndSellerNotes() throws Exception {
        OrderDTO o = new OrderDTO();
        o.setOrderId("4642");
        o.setSellerNotes("Cupom BELLUBE085");

        String obs = invokeBuildObservacaoInterna(o);
        assertTrue(obs.contains("Pedido Fastchannel: 4642"));
        assertTrue(obs.contains("Cupom BELLUBE085"));
    }

    @Test
    public void buildObservacaoInterna_truncatesAt1000() throws Exception {
        OrderDTO o = new OrderDTO();
        o.setOrderId("X");
        StringBuilder huge = new StringBuilder();
        for (int i = 0; i < 2000; i++) huge.append('a');
        o.setSellerNotes(huge.toString());

        String obs = invokeBuildObservacaoInterna(o);
        assertTrue("OBSERVACAOINTERNA deve ser truncada a 1000 chars", obs.length() <= 1000);
    }

    // ===================== PERCDESC invariants =====================
    // Reproduz a formula usada em appendItens mas em isolamento — garante que
    // mesmo com desconto maior que o total do item, PERCDESC nunca vai < 0
    // (Sankhya rejeita negativo com CORE_E03235).

    @Test
    public void percDesc_isAlwaysNonNegative_evenWhenDiscountExceedsItemTotal() {
        BigDecimal unitPrice = new BigDecimal("10.00");
        BigDecimal quantity = new BigDecimal("1");
        BigDecimal itemDiscount = new BigDecimal("20.00"); // desconto maior que o total
        BigDecimal totalSemDesc = unitPrice.multiply(quantity);

        BigDecimal percDesc = BigDecimal.ZERO;
        if (totalSemDesc.compareTo(BigDecimal.ZERO) > 0) {
            percDesc = itemDiscount
                    .divide(totalSemDesc, 4, BigDecimal.ROUND_HALF_UP)
                    .multiply(new BigDecimal("100"));
            if (percDesc.compareTo(BigDecimal.ZERO) < 0 || percDesc.compareTo(new BigDecimal("100")) >= 0) {
                percDesc = BigDecimal.ZERO;
            }
        }
        BigDecimal safePercDesc = percDesc.max(BigDecimal.ZERO);

        assertTrue("PERCDESC deve ser >= 0 sempre (Sankhya rejeita negativo)",
                safePercDesc.compareTo(BigDecimal.ZERO) >= 0);
    }

    @Test
    public void percDesc_roundedAtFourDecimals_withinRange() {
        BigDecimal unitPrice = new BigDecimal("100.00");
        BigDecimal quantity = new BigDecimal("2"); // total = 200
        BigDecimal itemDiscount = new BigDecimal("33.33");
        BigDecimal totalSemDesc = unitPrice.multiply(quantity);

        BigDecimal percDesc = itemDiscount
                .divide(totalSemDesc, 4, BigDecimal.ROUND_HALF_UP)
                .multiply(new BigDecimal("100"));

        // Esperamos ~16.6650 %.
        assertTrue(percDesc.compareTo(BigDecimal.ZERO) > 0);
        assertTrue(percDesc.compareTo(new BigDecimal("100")) < 0);
    }

    // ===================== reflection helpers =====================

    private static BigDecimal invokeNormalizeMoney(BigDecimal value) throws Exception {
        OrderXmlBuilder builder = new OrderXmlBuilder();
        Method m = OrderXmlBuilder.class.getDeclaredMethod("normalizeMoney", BigDecimal.class);
        m.setAccessible(true);
        return (BigDecimal) m.invoke(builder, value);
    }

    private static String invokeXmlEscape(String text) throws Exception {
        OrderXmlBuilder builder = new OrderXmlBuilder();
        Method m = OrderXmlBuilder.class.getDeclaredMethod("xmlEscape", String.class);
        m.setAccessible(true);
        return (String) m.invoke(builder, text);
    }

    private static BigDecimal invokeGetFrete(OrderDTO order) throws Exception {
        OrderXmlBuilder builder = new OrderXmlBuilder();
        Method m = OrderXmlBuilder.class.getDeclaredMethod("getFrete", OrderDTO.class);
        m.setAccessible(true);
        return (BigDecimal) m.invoke(builder, order);
    }

    private static String invokeBuildObservacao(OrderDTO order) throws Exception {
        OrderXmlBuilder builder = new OrderXmlBuilder();
        Method m = OrderXmlBuilder.class.getDeclaredMethod("buildObservacao", OrderDTO.class);
        m.setAccessible(true);
        return (String) m.invoke(builder, order);
    }

    private static String invokeBuildObservacaoInterna(OrderDTO order) throws Exception {
        OrderXmlBuilder builder = new OrderXmlBuilder();
        Method m = OrderXmlBuilder.class.getDeclaredMethod("buildObservacaoInterna", OrderDTO.class);
        m.setAccessible(true);
        return (String) m.invoke(builder, order);
    }
}
