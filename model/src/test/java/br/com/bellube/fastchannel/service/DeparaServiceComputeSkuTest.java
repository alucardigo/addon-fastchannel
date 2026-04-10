package br.com.bellube.fastchannel.service;

import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Testes unitarios para DeparaService.computeSkuFromBrandRule.
 *
 * Reproduz a regra oficial (memoria reference_sku_rule_legacy.md):
 *   TGFMAR.AD_FAST='S' + AD_FASTREF:
 *     - R => usar REFFORN
 *     - C => usar CODPROD (padrao)
 *
 * A funcao e publica/static, portanto 100% unit-testavel sem mock.
 */
public class DeparaServiceComputeSkuTest {

    @Test
    public void adFastRef_R_returnsRefforn() {
        String sku = DeparaService.computeSkuFromBrandRule("R", new BigDecimal("123"), "ABC-001");
        assertEquals("ABC-001", sku);
    }

    @Test
    public void adFastRef_R_caseInsensitive() {
        String sku = DeparaService.computeSkuFromBrandRule("r", new BigDecimal("123"), "ABC-001");
        assertEquals("ABC-001", sku);
    }

    @Test
    public void adFastRef_R_withBlankRefforn_fallsBackToCodprod() {
        String sku = DeparaService.computeSkuFromBrandRule("R", new BigDecimal("555"), "   ");
        assertEquals("555", sku);
    }

    @Test
    public void adFastRef_R_withNullRefforn_fallsBackToCodprod() {
        String sku = DeparaService.computeSkuFromBrandRule("R", new BigDecimal("555"), null);
        assertEquals("555", sku);
    }

    @Test
    public void adFastRef_C_returnsCodprod() {
        String sku = DeparaService.computeSkuFromBrandRule("C", new BigDecimal("9999"), "ABC");
        assertEquals("9999", sku);
    }

    @Test
    public void adFastRef_null_returnsCodprod() {
        String sku = DeparaService.computeSkuFromBrandRule(null, new BigDecimal("1"), "REF");
        assertEquals("1", sku);
    }

    @Test
    public void adFastRef_C_withNullCodprod_returnsNull() {
        assertNull(DeparaService.computeSkuFromBrandRule("C", null, "REF"));
    }

    @Test
    public void adFastRef_R_withNullCodprodAndRefforn_returnsRefforn() {
        assertEquals("REF-01", DeparaService.computeSkuFromBrandRule("R", null, "REF-01"));
    }

    @Test
    public void adFastRef_unrecognized_treatedAsCodprod() {
        String sku = DeparaService.computeSkuFromBrandRule("X", new BigDecimal("42"), "REF");
        assertEquals("42", sku);
    }

    @Test
    public void adFastRef_R_trimsRefforn() {
        String sku = DeparaService.computeSkuFromBrandRule("R", new BigDecimal("1"), "  ABC  ");
        assertEquals("ABC", sku);
    }

    @Test
    public void adFastRef_R_emptyRefforn_fallsBackToCodprod() {
        String sku = DeparaService.computeSkuFromBrandRule("R", new BigDecimal("77"), "");
        assertEquals("77", sku);
    }
}
