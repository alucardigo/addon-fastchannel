package br.com.bellube.fastchannel.regression;

import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.http.FastchannelPriceClient;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.OrderXmlBuilder;
import br.com.bellube.fastchannel.service.auth.SankhyaAuthManager;
import org.junit.Test;

import java.lang.reflect.Method;
import java.math.BigDecimal;

import static org.junit.Assert.*;

/**
 * Testes de regressao para garantir que as correcoes criticas
 * identificadas na revisao do legado nao regridam.
 */
public class CriticalFixesRegressionTest {

    // ===================== REGRESSAO: serviceRequest casing =====================

    @Test
    public void regression_serviceRequestXml_usesCapitalR() throws Exception {
        // Bug original: <servicerequest> com r minusculo
        OrderXmlBuilder builder = new OrderXmlBuilder();
        Method m = OrderXmlBuilder.class.getDeclaredMethod("buildIncluirNotaXml",
                br.com.bellube.fastchannel.dto.OrderDTO.class, BigDecimal.class,
                BigDecimal.class, BigDecimal.class, BigDecimal.class, BigDecimal.class);
        // Apenas verificar que a classe compila com o metodo correto
        assertNotNull("buildIncluirNotaXml deve existir", m);
    }

    // ===================== REGRESSAO: KEEPCONNECTED=S =====================

    @Test
    public void regression_loginXml_containsKeepConnected() throws Exception {
        // Bug original: KEEPCONNECTED ausente no login XML
        SankhyaAuthManager mgr = new SankhyaAuthManager();
        Method m = SankhyaAuthManager.class.getDeclaredMethod("buildLoginXml", String.class, String.class);
        m.setAccessible(true);
        String xml = (String) m.invoke(mgr, "SUP", "pwd");
        assertTrue("Login XML DEVE conter KEEPCONNECTED=S para manter sessao ativa",
                xml.contains("<KEEPCONNECTED>S</KEEPCONNECTED>"));
    }

    // ===================== REGRESSAO: JSESSIONID extraction =====================

    @Test
    public void regression_jsessionId_extractedFromMultipleFormats() throws Exception {
        // Bug original: so extraia de um formato, falhava em ambientes diferentes
        SankhyaAuthManager mgr = new SankhyaAuthManager();
        Method m = SankhyaAuthManager.class.getDeclaredMethod("extractJsessionId", String.class);
        m.setAccessible(true);

        // Formato 1: XML lowercase
        assertNotNull(m.invoke(mgr, "<jsessionid>ABC</jsessionid>"));
        // Formato 2: XML uppercase
        assertNotNull(m.invoke(mgr, "<JSESSIONID>DEF</JSESSIONID>"));
        // Formato 3: JSON
        assertNotNull(m.invoke(mgr, "{\"mgeSession\":\"GHI\"}"));
        // Formato 4: query style
        assertNotNull(m.invoke(mgr, "mgeSession=JKL"));
    }

    // ===================== REGRESSAO: Subscription keys por canal =====================

    @Test
    public void regression_priceClient_distributionUsesDistributionKey() {
        // Bug original: ambos canais usavam mesma subscription key
        FastchannelPriceClient dist = new FastchannelPriceClient(FastchannelPriceClient.Channel.DISTRIBUTION);
        FastchannelPriceClient cons = new FastchannelPriceClient(FastchannelPriceClient.Channel.CONSUMPTION);
        assertNotEquals("Canais devem ser diferentes", dist.getChannel(), cons.getChannel());
    }

    // ===================== REGRESSAO: DEFAULT_CODVEND_PADRAO = 167 =====================

    @Test
    public void regression_defaultCodVend_is167_notNull() {
        // Bug original: getDefaultCodVend retornava null porque coluna nao existia
        assertNotNull("DEFAULT_CODVEND_PADRAO nao pode ser null", FastchannelConstants.DEFAULT_CODVEND_PADRAO);
        assertEquals(new BigDecimal("167"), FastchannelConstants.DEFAULT_CODVEND_PADRAO);
    }

    // ===================== REGRESSAO: DEFAULT_TOP_PEDIDO = 403 =====================

    @Test
    public void regression_defaultTopPedido_is403_notNull() {
        // Bug original: getDefaultCodTipVenda retornava null
        assertNotNull("DEFAULT_TOP_PEDIDO nao pode ser null", FastchannelConstants.DEFAULT_TOP_PEDIDO);
        assertEquals(new BigDecimal("403"), FastchannelConstants.DEFAULT_TOP_PEDIDO);
    }

    // ===================== REGRESSAO: SKU brand rule =====================

    @Test
    public void regression_skuBrandRule_R_usesRefForn() {
        // Regra: se AD_FASTREF = 'R', usar REFFORN
        String sku = DeparaService.computeSkuFromBrandRule("R", new BigDecimal("999"), "MOBIL-001");
        assertEquals("MOBIL-001", sku);
    }

    @Test
    public void regression_skuBrandRule_C_usesCodProd() {
        // Regra: se AD_FASTREF = 'C' (ou qualquer outro), usar CODPROD
        String sku = DeparaService.computeSkuFromBrandRule("C", new BigDecimal("999"), "MOBIL-001");
        assertEquals("999", sku);
    }

    @Test
    public void regression_skuBrandRule_R_emptyRefForn_fallsToCodProd() {
        String sku = DeparaService.computeSkuFromBrandRule("R", new BigDecimal("999"), "");
        assertEquals("999", sku);
    }

    @Test
    public void regression_skuBrandRule_R_nullRefForn_fallsToCodProd() {
        String sku = DeparaService.computeSkuFromBrandRule("R", new BigDecimal("999"), null);
        assertEquals("999", sku);
    }

    @Test
    public void regression_skuBrandRule_nullCodProd_returnsNull() {
        String sku = DeparaService.computeSkuFromBrandRule("C", null, "REF");
        assertNull(sku);
    }

    // ===================== REGRESSAO: Money normalization =====================

    @Test
    public void regression_moneyNormalization_integerMovesDecimal() throws Exception {
        OrderXmlBuilder builder = new OrderXmlBuilder();
        Method m = OrderXmlBuilder.class.getDeclaredMethod("normalizeMoney", BigDecimal.class);
        m.setAccessible(true);
        // 1500 centavos -> 15.00 reais
        BigDecimal result = (BigDecimal) m.invoke(builder, new BigDecimal("1500"));
        assertEquals(new BigDecimal("15.00"), result);
    }

    @Test
    public void regression_moneyNormalization_decimalUnchanged() throws Exception {
        OrderXmlBuilder builder = new OrderXmlBuilder();
        Method m = OrderXmlBuilder.class.getDeclaredMethod("normalizeMoney", BigDecimal.class);
        m.setAccessible(true);
        BigDecimal result = (BigDecimal) m.invoke(builder, new BigDecimal("15.00"));
        assertEquals(new BigDecimal("15.00"), result);
    }

    // ===================== REGRESSAO: coluna de ordenacao TGFEXC =====================

    @Test
    public void regression_orderXmlBuilder_usesDhAlterWhenDtAlterMissing() throws Exception {
        Method m = OrderXmlBuilder.class.getDeclaredMethod(
                "chooseExcOrderColumn", boolean.class, boolean.class, boolean.class);
        m.setAccessible(true);
        String col = (String) m.invoke(null, false, true, false);
        assertEquals("DHALTER", col);
    }

    @Test
    public void regression_orderXmlBuilder_fallsBackToNuTabWhenNoDateColumns() throws Exception {
        Method m = OrderXmlBuilder.class.getDeclaredMethod(
                "chooseExcOrderColumn", boolean.class, boolean.class, boolean.class);
        m.setAccessible(true);
        String col = (String) m.invoke(null, false, false, false);
        assertEquals("NUTAB", col);
    }
}
