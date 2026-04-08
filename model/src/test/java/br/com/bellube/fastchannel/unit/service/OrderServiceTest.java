package br.com.bellube.fastchannel.unit.service;

import br.com.bellube.fastchannel.dto.OrderCustomerDTO;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.dto.OrderItemDTO;
import br.com.bellube.fastchannel.service.OrderService;
import org.junit.Test;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

/**
 * Testes unitarios para OrderService.
 * Foca em logica pura que nao depende de JAPE/BD.
 */
public class OrderServiceTest {

    // ===================== CPF Validation =====================

    @Test
    public void validCpf_returnsTrue() throws Exception {
        assertTrue(invokeIsValidCpfCnpj("52998224725"));
    }

    @Test
    public void invalidCpf_allSameDigits_returnsFalse() throws Exception {
        assertFalse(invokeIsValidCpfCnpj("11111111111"));
    }

    @Test
    public void invalidCpf_wrongCheckDigit_returnsFalse() throws Exception {
        assertFalse(invokeIsValidCpfCnpj("52998224726"));
    }

    @Test
    public void nullCpf_returnsFalse() throws Exception {
        assertFalse(invokeIsValidCpfCnpj(null));
    }

    @Test
    public void emptyCpf_returnsFalse() throws Exception {
        assertFalse(invokeIsValidCpfCnpj(""));
    }

    @Test
    public void cpfWithMask_sanitizesAndValidates() throws Exception {
        // sanitizeDigits strips non-digits: "529.982.247-25xxx" -> "52998224725" (11 digits, CPF valido)
        assertTrue(invokeIsValidCpfCnpj("529.982.247-25"));
    }

    @Test
    public void cpfWithExtraDigits_returnsFalse() throws Exception {
        // 12 digits nao e CPF nem CNPJ
        assertFalse(invokeIsValidCpfCnpj("123456789012"));
    }

    // ===================== CNPJ Validation =====================

    @Test
    public void validCnpj_returnsTrue() throws Exception {
        assertTrue(invokeIsValidCpfCnpj("11222333000181"));
    }

    @Test
    public void invalidCnpj_allSameDigits_returnsFalse() throws Exception {
        assertFalse(invokeIsValidCpfCnpj("11111111111111"));
    }

    @Test
    public void invalidCnpj_wrongCheckDigit_returnsFalse() throws Exception {
        assertFalse(invokeIsValidCpfCnpj("11222333000182"));
    }

    // ===================== Money Normalization =====================

    @Test
    public void normalizeMoney_integerValue_movesDecimalLeft() throws Exception {
        BigDecimal input = new BigDecimal("1500");
        BigDecimal result = invokeNormalizeMoney(input);
        assertEquals(new BigDecimal("15.00"), result);
    }

    @Test
    public void normalizeMoney_alreadyDecimal_unchanged() throws Exception {
        BigDecimal input = new BigDecimal("15.99");
        BigDecimal result = invokeNormalizeMoney(input);
        assertEquals(new BigDecimal("15.99"), result);
    }

    @Test
    public void normalizeMoney_null_returnsNull() throws Exception {
        assertNull(invokeNormalizeMoney(null));
    }

    // ===================== Validate Order =====================

    @Test(expected = Exception.class)
    public void validateOrder_nullOrderId_throws() throws Exception {
        OrderDTO order = new OrderDTO();
        order.setOrderId(null);
        order.setCustomer(new OrderCustomerDTO());
        order.setItems(Collections.singletonList(new OrderItemDTO()));
        invokeValidateOrder(order);
    }

    @Test(expected = Exception.class)
    public void validateOrder_emptyOrderId_throws() throws Exception {
        OrderDTO order = new OrderDTO();
        order.setOrderId("");
        order.setCustomer(new OrderCustomerDTO());
        order.setItems(Collections.singletonList(new OrderItemDTO()));
        invokeValidateOrder(order);
    }

    @Test(expected = Exception.class)
    public void validateOrder_nullCustomer_throws() throws Exception {
        OrderDTO order = new OrderDTO();
        order.setOrderId("ORD-001");
        order.setCustomer(null);
        order.setItems(Collections.singletonList(new OrderItemDTO()));
        invokeValidateOrder(order);
    }

    @Test(expected = Exception.class)
    public void validateOrder_emptyItems_throws() throws Exception {
        OrderDTO order = new OrderDTO();
        order.setOrderId("ORD-001");
        order.setCustomer(new OrderCustomerDTO());
        order.setItems(Collections.emptyList());
        invokeValidateOrder(order);
    }

    @Test(expected = Exception.class)
    public void validateOrder_nullItems_throws() throws Exception {
        OrderDTO order = new OrderDTO();
        order.setOrderId("ORD-001");
        order.setCustomer(new OrderCustomerDTO());
        order.setItems(null);
        invokeValidateOrder(order);
    }

    @Test
    public void validateOrder_validOrder_noException() throws Exception {
        OrderDTO order = new OrderDTO();
        order.setOrderId("ORD-001");
        order.setCustomer(new OrderCustomerDTO());
        order.setItems(Arrays.asList(new OrderItemDTO()));
        invokeValidateOrder(order); // should not throw
    }

    // ===================== Truncate =====================

    @Test
    public void truncate_shorterThanMax_unchanged() throws Exception {
        assertEquals("abc", invokeTruncate("abc", 10));
    }

    @Test
    public void truncate_longerThanMax_trimmed() throws Exception {
        assertEquals("abc", invokeTruncate("abcdef", 3));
    }

    @Test
    public void truncate_null_returnsNull() throws Exception {
        assertNull(invokeTruncate(null, 10));
    }

    // ===================== isBlank =====================

    @Test
    public void isBlank_null_returnsTrue() throws Exception {
        assertTrue(invokeIsBlank(null));
    }

    @Test
    public void isBlank_empty_returnsTrue() throws Exception {
        assertTrue(invokeIsBlank(""));
    }

    @Test
    public void isBlank_spaces_returnsTrue() throws Exception {
        assertTrue(invokeIsBlank("   "));
    }

    @Test
    public void isBlank_value_returnsFalse() throws Exception {
        assertFalse(invokeIsBlank("abc"));
    }

    // ===================== Fallback CPF generation =====================

    @Test
    public void buildFallbackCpf_generatesValid11Digits() throws Exception {
        OrderCustomerDTO customer = new OrderCustomerDTO();
        customer.setEmail("test@example.com");
        String cpf = invokeBuildFallbackCpf(customer);
        assertNotNull(cpf);
        assertEquals(11, cpf.length());
        assertTrue(cpf.matches("\\d{11}"));
    }

    @Test
    public void buildFallbackCpf_differentEmails_differentCpfs() throws Exception {
        OrderCustomerDTO c1 = new OrderCustomerDTO();
        c1.setEmail("a@example.com");
        OrderCustomerDTO c2 = new OrderCustomerDTO();
        c2.setEmail("b@example.com");
        String cpf1 = invokeBuildFallbackCpf(c1);
        String cpf2 = invokeBuildFallbackCpf(c2);
        assertNotEquals(cpf1, cpf2);
    }

    @Test
    public void buildFallbackCpf_nullCustomer_generatesDefault() throws Exception {
        String cpf = invokeBuildFallbackCpf(null);
        assertNotNull(cpf);
        assertEquals(11, cpf.length());
    }

    // ===================== Helpers (reflection) =====================

    private boolean invokeIsValidCpfCnpj(String value) throws Exception {
        OrderService svc = new OrderService();
        Method m = OrderService.class.getDeclaredMethod("isValidCpfCnpj", String.class);
        m.setAccessible(true);
        return (boolean) m.invoke(svc, value);
    }

    private BigDecimal invokeNormalizeMoney(BigDecimal value) throws Exception {
        OrderService svc = new OrderService();
        Method m = OrderService.class.getDeclaredMethod("normalizeMoney", BigDecimal.class);
        m.setAccessible(true);
        return (BigDecimal) m.invoke(svc, value);
    }

    private void invokeValidateOrder(OrderDTO order) throws Exception {
        OrderService svc = new OrderService();
        Method m = OrderService.class.getDeclaredMethod("validateOrder", OrderDTO.class);
        m.setAccessible(true);
        try {
            m.invoke(svc, order);
        } catch (java.lang.reflect.InvocationTargetException e) {
            throw (Exception) e.getCause();
        }
    }

    private String invokeTruncate(String str, int max) throws Exception {
        OrderService svc = new OrderService();
        Method m = OrderService.class.getDeclaredMethod("truncate", String.class, int.class);
        m.setAccessible(true);
        return (String) m.invoke(svc, str, max);
    }

    private boolean invokeIsBlank(String value) throws Exception {
        OrderService svc = new OrderService();
        Method m = OrderService.class.getDeclaredMethod("isBlank", String.class);
        m.setAccessible(true);
        return (boolean) m.invoke(svc, value);
    }

    private String invokeBuildFallbackCpf(OrderCustomerDTO customer) throws Exception {
        OrderService svc = new OrderService();
        Method m = OrderService.class.getDeclaredMethod("buildFallbackCpf", OrderCustomerDTO.class);
        m.setAccessible(true);
        return (String) m.invoke(svc, customer);
    }
}
