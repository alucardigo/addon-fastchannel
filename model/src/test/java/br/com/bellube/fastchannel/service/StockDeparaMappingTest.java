package br.com.bellube.fastchannel.service;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StockDeparaMappingTest {

    @Test
    public void resolvesStockMappingsByType() {
        assertEquals("STOCK_STORAGE", DeparaService.TIPO_STOCK_STORAGE);
        assertEquals("STOCK_RESELLER", DeparaService.TIPO_STOCK_RESELLER);
    }
}
