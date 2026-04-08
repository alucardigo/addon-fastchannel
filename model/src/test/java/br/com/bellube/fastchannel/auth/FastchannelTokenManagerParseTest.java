package br.com.bellube.fastchannel.auth;

import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class FastchannelTokenManagerParseTest {

    @Test
    public void parsesAccessTokenFromStandardJson() throws Exception {
        String body = "{\"token_type\":\"Bearer\",\"expires_in\":3599,\"ext_expires_in\":3599,\"access_token\":\"abc.def\"}";
        Object tokenData = invokeParse(body);
        assertNotNull(tokenData);
        String token = readField(tokenData, "accessToken");
        int expiresIn = readIntField(tokenData, "expiresIn");
        assertEquals("abc.def", token);
        assertEquals(3599, expiresIn);
    }

    @Test
    public void parsesAccessTokenWhenKeyHasBom() throws Exception {
        String body = "{\"\\uFEFFaccess_token\":\"bom.token\",\"expires_in\":1200}";
        Object tokenData = invokeParse(body);
        assertNotNull(tokenData);
        String token = readField(tokenData, "accessToken");
        int expiresIn = readIntField(tokenData, "expiresIn");
        assertEquals("bom.token", token);
        assertEquals(1200, expiresIn);
    }

    private Object invokeParse(String body) throws Exception {
        Method method;
        try {
            method = FastchannelTokenManager.class.getDeclaredMethod("parseTokenResponse", String.class);
        } catch (NoSuchMethodException e) {
            throw new AssertionError("Expected parseTokenResponse(String) to exist", e);
        }
        method.setAccessible(true);
        return method.invoke(null, body);
    }

    private String readField(Object tokenData, String field) throws Exception {
        java.lang.reflect.Field f = tokenData.getClass().getDeclaredField(field);
        f.setAccessible(true);
        Object value = f.get(tokenData);
        return value != null ? value.toString() : null;
    }

    private int readIntField(Object tokenData, String field) throws Exception {
        java.lang.reflect.Field f = tokenData.getClass().getDeclaredField(field);
        f.setAccessible(true);
        Object value = f.get(tokenData);
        return value == null ? 0 : ((Number) value).intValue();
    }
}
