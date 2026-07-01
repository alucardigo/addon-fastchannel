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

    // ===================== REGRESSION 2026-04-14: BatchDisabled null == false =====================

    /**
     * Regressao: FC pode nao retornar BatchDisabled explicitamente para batches ativos.
     * null deve ser tratado como false para evitar re-criacao desnecessaria de batches.
     * Se null != false, batchs ativos seriam deletados e recriados a cada sync.
     */
    @Test
    public void updatePriceBatches_nullBatchDisabled_treatedAsFalse_noUnnecessaryRecreation() throws Exception {
        RecordingPriceHttpClient httpClient = new RecordingPriceHttpClient();
        // FC retorna sem campo BatchDisabled (null apos Gson parse)
        httpClient.getResponseBody = "{\"ProductPriceBatch\":["
                + "{\"BatchId\":\"b1\",\"PriceTableId\":10,\"MinimumBatchSize\":1,\"MaximumBatchSize\":5,\"UnitaryPriceForBatch\":100}"
                + "]}";

        FastchannelPriceClient client = new FastchannelPriceClient(httpClient, FastchannelPriceClient.Channel.CONSUMPTION);

        // Desejado: mesmo batch, BatchDisabled=false explicitamente
        List<PriceBatchItemDTO> desired = new ArrayList<>();
        PriceBatchItemDTO d = new PriceBatchItemDTO();
        d.setMinimumBatchSize(new BigDecimal("1"));
        d.setMaximumBatchSize(new BigDecimal("5"));
        d.setUnitaryPriceForBatch(new BigDecimal("100"));
        d.setBatchDisabled(false);
        desired.add(d);

        client.updatePriceBatches("SKU-3", new BigDecimal("10"), desired);

        // Nenhum delete (batch eh equivalente)
        assertEquals("null BatchDisabled deve ser equivalente a false — nao deve deletar", 0, httpClient.deleteEndpoints.size());
        // Nenhum post (batch ja existe e eh equivalente)
        assertEquals("null BatchDisabled deve ser equivalente a false — nao deve recriar", 0, httpClient.postEndpoints.size());
    }

    /**
     * Regressao: se o batch no FC tem BatchDisabled=true (desativado) e o desejado tem
     * BatchDisabled=false (ativo), devem ser considerados DIFERENTES — o antigo deve ser
     * deletado e o novo criado como ativo.
     */
    @Test
    public void updatePriceBatches_disabledInFc_activatedInDesired_deletesAndRecreates() throws Exception {
        RecordingPriceHttpClient httpClient = new RecordingPriceHttpClient();
        httpClient.getResponseBody = "{\"ProductPriceBatch\":["
                + "{\"BatchId\":\"disabled-1\",\"PriceTableId\":10,\"MinimumBatchSize\":1,\"MaximumBatchSize\":5,\"UnitaryPriceForBatch\":100,\"BatchDisabled\":true}"
                + "]}";

        FastchannelPriceClient client = new FastchannelPriceClient(httpClient, FastchannelPriceClient.Channel.CONSUMPTION);

        List<PriceBatchItemDTO> desired = new ArrayList<>();
        PriceBatchItemDTO d = new PriceBatchItemDTO();
        d.setMinimumBatchSize(new BigDecimal("1"));
        d.setMaximumBatchSize(new BigDecimal("5"));
        d.setUnitaryPriceForBatch(new BigDecimal("100"));
        d.setBatchDisabled(false); // quero ativar
        desired.add(d);

        client.updatePriceBatches("SKU-4", new BigDecimal("10"), desired);

        // Deve deletar o batch desativado
        assertEquals("Batch desativado no FC deve ser deletado", 1, httpClient.deleteEndpoints.size());
        assertTrue(httpClient.deleteEndpoints.get(0).contains("disabled-1"));
        // Deve criar o batch ativo
        assertEquals("Deve criar batch ativo no lugar", 1, httpClient.postEndpoints.size());
        assertTrue("Novo batch deve ter BatchDisabled:false",
                httpClient.postBodies.get(0).contains("\"BatchDisabled\":false"));
    }

    /**
     * Regressao 2026-04-14: quando todos os batches sao expirados (lista desired vazia),
     * os batches existentes no FC devem ser DELETADOS — nao criados como desativados.
     */
    @Test
    public void updatePriceBatches_emptyDesired_deletesExistingFcBatches() throws Exception {
        RecordingPriceHttpClient httpClient = new RecordingPriceHttpClient();
        httpClient.getResponseBody = "{\"ProductPriceBatch\":["
                + "{\"BatchId\":\"old-A\",\"PriceTableId\":10,\"MinimumBatchSize\":1,\"MaximumBatchSize\":5,\"UnitaryPriceForBatch\":100,\"BatchDisabled\":false},"
                + "{\"BatchId\":\"old-B\",\"PriceTableId\":10,\"MinimumBatchSize\":6,\"MaximumBatchSize\":99999,\"UnitaryPriceForBatch\":80,\"BatchDisabled\":false}"
                + "]}";

        FastchannelPriceClient client = new FastchannelPriceClient(httpClient, FastchannelPriceClient.Channel.CONSUMPTION);

        // Lista vazia = promocao expirou em Sankhya — PriceBatchResolver filtrou tudo
        List<PriceBatchItemDTO> desired = new ArrayList<>();

        client.updatePriceBatches("SKU-5", new BigDecimal("10"), desired);

        assertEquals("Todos os batches FC devem ser deletados quando promocao expirou", 2, httpClient.deleteEndpoints.size());
        assertEquals("Nenhum batch novo deve ser criado", 0, httpClient.postEndpoints.size());
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
