package br.com.bellube.fastchannel.http;

import br.com.bellube.fastchannel.dto.PriceBatchItemDTO;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FastchannelPriceClientTest {

    @Test
    public void updatePriceBatches_mustReconcileRemoteAndCreateOnlyMissingOrChanged() throws Exception {
        RecordingPriceHttpClient httpClient = new RecordingPriceHttpClient();
        httpClient.getResponseBody = "{\"ProductPriceBatch\":["
                + "{\"BatchId\":\"old-1\",\"PriceTableId\":10,\"MinimumBatchSize\":1,\"MaximumBatchSize\":5,\"UnitaryPriceForBatch\":100,\"BatchDisabled\":false},"
                + "{\"BatchId\":\"old-2\",\"PriceTableId\":10,\"MinimumBatchSize\":6,\"MaximumBatchSize\":10,\"UnitaryPriceForBatch\":220,\"BatchDisabled\":false}"
                + "]}";

        FastchannelPriceClient client = new FastchannelPriceClient(httpClient, FastchannelPriceClient.Channel.CONSUMPTION);

        List<PriceBatchItemDTO> desired = new ArrayList<>();
        desired.add(batch("1", "5", "100"));
        desired.add(batch("6", "10", "250"));
        desired.add(batch("11", "20", "300"));

        client.updatePriceBatches("SKU-1", new BigDecimal("10"), desired);

        assertEquals(1, httpClient.deleteEndpoints.stream().filter(e -> e.endsWith("/old-2")).count());
        assertEquals(2, httpClient.postEndpoints.size());
        assertTrue(httpClient.postBodies.stream().anyMatch(body -> body.contains("\"MinimumBatchSize\":6") && body.contains("\"UnitaryPriceForBatch\":250")));
        assertTrue(httpClient.postBodies.stream().anyMatch(body -> body.contains("\"MinimumBatchSize\":11") && body.contains("\"UnitaryPriceForBatch\":300")));
    }

    @Test
    public void listPriceBatches_mustParseWrappedPayload() throws Exception {
        RecordingPriceHttpClient httpClient = new RecordingPriceHttpClient();
        httpClient.getResponseBody = "{\"ProductPriceBatch\":[{\"BatchId\":\"abc\",\"PriceTableId\":10,\"MinimumBatchSize\":3,\"MaximumBatchSize\":7,\"UnitaryPriceForBatch\":150,\"BatchDisabled\":false}]}";

        FastchannelPriceClient client = new FastchannelPriceClient(httpClient, FastchannelPriceClient.Channel.CONSUMPTION);
        List<PriceBatchItemDTO> batches = client.listPriceBatches("SKU-2");

        assertEquals(1, batches.size());
        assertEquals("abc", batches.get(0).getBatchId());
        assertEquals(new BigDecimal("10"), batches.get(0).getPriceTableId());
    }

    private PriceBatchItemDTO batch(String min, String max, String price) {
        PriceBatchItemDTO dto = new PriceBatchItemDTO();
        dto.setMinimumBatchSize(new BigDecimal(min));
        dto.setMaximumBatchSize(new BigDecimal(max));
        dto.setUnitaryPriceForBatch(new BigDecimal(price));
        dto.setBatchDisabled(Boolean.FALSE);
        return dto;
    }

    private static final class RecordingPriceHttpClient extends FastchannelHttpClient {
        private final List<String> deleteEndpoints = new ArrayList<>();
        private final List<String> postEndpoints = new ArrayList<>();
        private final List<String> postBodies = new ArrayList<>();
        private String getResponseBody = "{\"ProductPriceBatch\":[]}";

        @Override
        public HttpResult getPrice(String endpoint, String subscriptionKey) {
            return new HttpResult(200, getResponseBody);
        }

        @Override
        public HttpResult postPrice(String endpoint, String jsonBody, String subscriptionKey) {
            postEndpoints.add(endpoint);
            postBodies.add(jsonBody);
            return new HttpResult(200, "{}");
        }

        @Override
        public HttpResult deletePrice(String endpoint, String subscriptionKey) {
            deleteEndpoints.add(endpoint);
            return new HttpResult(200, "{}");
        }
    }
}
