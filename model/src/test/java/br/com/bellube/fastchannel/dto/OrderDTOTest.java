package br.com.bellube.fastchannel.dto;

import com.google.gson.Gson;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OrderDTOTest {

    @Test
    public void parsesPaymentMethodIdFromCurrentPaymentDetails() {
        String json = "{" +
                "\"OrderId\":\"123\"," +
                "\"CurrentPaymentDetails\":{" +
                "\"PaymentMethodId\":6," +
                "\"PaymentMethodName\":\"VISA\"" +
                "}" +
                "}";

        OrderDTO order = new Gson().fromJson(json, OrderDTO.class);
        assertNotNull(order);
        assertEquals(Integer.valueOf(6), order.getPaymentMethodId());
    }
}
