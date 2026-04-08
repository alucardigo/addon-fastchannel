package br.com.bellube.fastchannel.service;

import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SkuResolverTest {

    @Test
    public void resolvesSkuByBrandRule() {
        String skuRef = DeparaService.computeSkuFromBrandRule("R", new BigDecimal("123"), "REF001");
        assertEquals("REF001", skuRef);

        String skuCodProd = DeparaService.computeSkuFromBrandRule("C", new BigDecimal("123"), "REF001");
        assertEquals("123", skuCodProd);
    }

    @Test
    public void returnsNullWhenCodProdMissing() {
        String sku = DeparaService.computeSkuFromBrandRule("C", null, "REF001");
        assertNull(sku);
    }
}
