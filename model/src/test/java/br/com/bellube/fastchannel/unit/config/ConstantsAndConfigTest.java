package br.com.bellube.fastchannel.unit.config;

import br.com.bellube.fastchannel.config.FastchannelConstants;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.*;

/**
 * Testes unitarios para FastchannelConstants.
 * Garante que valores do legado estao configurados corretamente.
 */
public class ConstantsAndConfigTest {

    @Test
    public void defaultTopPedido_is403() {
        assertEquals(new BigDecimal("403"), FastchannelConstants.DEFAULT_TOP_PEDIDO);
    }

    @Test
    public void defaultCodVendPadrao_is167() {
        assertEquals(new BigDecimal("167"), FastchannelConstants.DEFAULT_CODVEND_PADRAO);
    }

    @Test
    public void defaultBatchSize_is50() {
        assertEquals(50, FastchannelConstants.DEFAULT_BATCH_SIZE);
    }

    @Test
    public void defaultMaxRetries_is3() {
        assertEquals(3, FastchannelConstants.DEFAULT_MAX_RETRIES);
    }

    @Test
    public void defaultTimeout_is30() {
        assertEquals(30, FastchannelConstants.DEFAULT_TIMEOUT_SECONDS);
    }

    @Test
    public void orderApiBase_isHttps() {
        assertTrue(FastchannelConstants.ORDER_API_BASE.startsWith("https://"));
    }

    @Test
    public void stockApiBase_isHttps() {
        assertTrue(FastchannelConstants.STOCK_API_BASE.startsWith("https://"));
    }

    @Test
    public void priceApiBase_isHttps() {
        assertTrue(FastchannelConstants.PRICE_API_BASE.startsWith("https://"));
    }

    @Test
    public void statusApproved_is201() {
        assertEquals(201, FastchannelConstants.STATUS_APPROVED);
    }

    @Test
    public void endpointPrice_hasPlaceholder() {
        assertTrue(FastchannelConstants.ENDPOINT_PRICE.contains("%s"));
    }

    @Test
    public void endpointStock_hasPlaceholder() {
        assertTrue(FastchannelConstants.ENDPOINT_STOCK.contains("%s"));
    }

    @Test
    public void endpointOrderInvoices_hasPlaceholder() {
        assertTrue(FastchannelConstants.ENDPOINT_ORDER_INVOICES.contains("%s"));
    }
}
