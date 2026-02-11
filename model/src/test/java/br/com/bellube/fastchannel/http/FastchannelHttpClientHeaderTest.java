package br.com.bellube.fastchannel.http;

import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;

public class FastchannelHttpClientHeaderTest {

    @Test
    public void exposesSubscriptionKeyHeaderName() throws Exception {
        Method method;
        try {
            method = FastchannelHttpClient.class.getDeclaredMethod("getSubscriptionHeaderName");
        } catch (NoSuchMethodException e) {
            throw new AssertionError("Expected getSubscriptionHeaderName() to exist", e);
        }
        method.setAccessible(true);
        Object value = method.invoke(null);
        assertEquals("Subscription-Key", value);
    }
}
