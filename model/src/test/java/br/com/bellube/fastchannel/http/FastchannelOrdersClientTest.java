package br.com.bellube.fastchannel.http;

import br.com.bellube.fastchannel.dto.OrderDTO;
import com.google.gson.Gson;
import org.junit.Test;

import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FastchannelOrdersClientTest {

    @Test
    public void listOrdersWithMeta_mustSendCreatedAfterWithSafetyOverlap() throws Exception {
        RecordingHttpClient httpClient = new RecordingHttpClient();
        FastchannelOrdersClient client = new FastchannelOrdersClient(httpClient);

        // lastSync = 2026-03-10 09:51:00
        // Com safety overlap de 72h, CreatedAfter deve ser 2026-03-07T09:51:00
        client.listOrdersWithMeta(Timestamp.valueOf("2026-03-10 09:51:00"), 2, 50, Boolean.FALSE);

        assertNotNull(httpClient.lastEndpoint);
        // CreatedAfter DEVE conter o timestamp com overlap de 72h aplicado
        assertTrue("Deve conter CreatedAfter com overlap 72h (2026-03-07)",
                httpClient.lastEndpoint.contains("CreatedAfter=2026-03-07T09:51:00"));
        // IgnoreCreationDate=true NAO deve ser enviado junto com CreatedAfter!
        assertFalse("NAO deve conter IgnoreCreationDate=true (anula CreatedAfter)",
                httpClient.lastEndpoint.contains("IgnoreCreationDate=true"));
        assertTrue(httpClient.lastEndpoint.contains("PageNumber=2"));
        assertTrue(httpClient.lastEndpoint.contains("PageSize=50"));
    }

    @Test
    public void listOrdersWithMeta_mustFallbackTo72hWhenLastSyncIsNull() throws Exception {
        RecordingHttpClient httpClient = new RecordingHttpClient();
        FastchannelOrdersClient client = new FastchannelOrdersClient(httpClient);

        client.listOrdersWithMeta(null, 1, 20, Boolean.FALSE);

        assertNotNull(httpClient.lastEndpoint);
        // CreatedAfter DEVE estar presente mesmo sem lastSync (fallback 72h)
        assertTrue("Deve conter CreatedAfter mesmo quando lastSync e nulo (fallback 72h)",
                httpClient.lastEndpoint.contains("CreatedAfter="));
        // IgnoreCreationDate=true NAO deve ser enviado
        assertFalse("NAO deve conter IgnoreCreationDate=true (anula CreatedAfter)",
                httpClient.lastEndpoint.contains("IgnoreCreationDate=true"));
    }

    @Test
    public void orderDto_mustDeserializeCurrentStatusTypeId() {
        String json = "{"
                + "\"OrderId\":\"4490\","
                + "\"CurrentStatusId\":300,"
                + "\"CurrentStatusTypeId\":2"
                + "}";

        OrderDTO dto = new Gson().fromJson(json, OrderDTO.class);

        assertEquals("4490", dto.getOrderId());
        assertEquals(300, dto.getStatus());
        assertEquals(Integer.valueOf(2), dto.getCurrentStatusTypeId());
    }

    private static final class RecordingHttpClient extends FastchannelHttpClient {
        private String lastEndpoint;

        @Override
        public HttpResult getOrders(String endpoint) {
            this.lastEndpoint = endpoint;
            return new HttpResult(200, "{\"Payload\":[],\"TotalRecords\":0,\"TotalPages\":0}");
        }
    }
}
