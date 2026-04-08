package br.com.bellube.fastchannel.service;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class StockResolverTest {

    @Test
    public void usesEstoqueMinusReservado() {
        StockResolver resolver = new StockResolver();
        String sql = resolver.getSql();
        assertTrue(sql.contains("ESTOQUE"));
        assertTrue(sql.contains("RESERVADO"));
    }

    @Test
    public void returnsNullWhenMissingKeys() {
        StockResolver resolver = new StockResolver();
        assertTrue(resolver.resolve(null, null, null) == null);
    }
}
