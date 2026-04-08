package br.com.bellube.fastchannel.regression;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Regressao: garante que a importacao nao marque pedido como
 * confirmado/faturado no momento da inclusao.
 */
public class OrderStateRegressionTest {

    @Test
    public void orderService_mustKeepCabecalhoPendingAndNotFaturado() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/OrderService.java");

        assertTrue("OrderService deve forcar STATUSNOTA='P' no cabecalho",
                src.contains("updateVO = updateVO.set(\"STATUSNOTA\", \"P\")"));
        assertTrue("OrderService deve forcar PENDENTE='S' no cabecalho",
                src.contains("updateVO = updateVO.set(\"PENDENTE\", \"S\")"));
        assertTrue("OrderService deve limpar DTFATUR no cabecalho",
                src.contains("updateVO = updateVO.set(\"DTFATUR\", null)"));
    }

    @Test
    public void orderService_mustKeepItensNotEntregueAndPending() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/OrderService.java");

        assertTrue("OrderService deve forcar QTDENTREGUE=0 no item",
                src.contains("updateVO = updateVO.set(\"QTDENTREGUE\", BigDecimal.ZERO)"));
        assertTrue("OrderService deve forcar STATUSNOTA='P' no item",
                src.contains("updateVO = updateVO.set(\"STATUSNOTA\", \"P\")"));
    }

    @Test
    public void internalApiStrategy_mustCreateCabecalhoPending() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java");

        assertTrue("InternalApiStrategy deve criar cabecalho com STATUSNOTA='P'",
                src.contains("cabBuilder = cabBuilder.set(\"STATUSNOTA\", \"P\")"));
        assertTrue("InternalApiStrategy deve criar cabecalho com PENDENTE='S'",
                src.contains("cabBuilder = cabBuilder.set(\"PENDENTE\", \"S\")"));
    }

    @Test
    public void internalApiStrategy_mustCreateItensNotEntregueAndPending() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java");

        assertTrue("InternalApiStrategy deve criar item com QTDENTREGUE=0",
                src.contains("itemBuilder = itemBuilder.set(\"QTDENTREGUE\", BigDecimal.ZERO)"));
        assertTrue("InternalApiStrategy deve criar item com STATUSNOTA='P'",
                src.contains("itemBuilder = itemBuilder.set(\"STATUSNOTA\", \"P\")"));
    }

    @Test
    public void mustNotReintroduceConfirmedOrFaturadoDefaults() throws Exception {
        String orderService = readMainSource("br/com/bellube/fastchannel/service/OrderService.java");
        String internalApi = readMainSource("br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java");

        assertFalse("Nao pode voltar STATUSNOTA='L' no fluxo de importacao",
                orderService.contains("set(\"STATUSNOTA\", \"L\")") || internalApi.contains("set(\"STATUSNOTA\", \"L\")"));
        assertFalse("Nao pode voltar PENDENTE='N' no fluxo de importacao",
                orderService.contains("set(\"PENDENTE\", \"N\")") || internalApi.contains("set(\"PENDENTE\", \"N\")"));
        assertFalse("Nao pode voltar QTDENTREGUE com quantidade negociada",
                internalApi.contains("set(\"QTDENTREGUE\", quantity)") || internalApi.contains("set(\"QTDENTREGUE\", qtdNeg)"));
    }

    @Test
    public void internalApiStrategy_mustUsePartnerPreferredSeller() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java");

        assertTrue("CODVEND deve priorizar TGFPAR.CODVEND",
                src.contains("BigDecimal codVendParceiro = resolveCodVendByParc(codParc);"));
        assertTrue("CODVEND deve ser sobrescrito pelo vendedor do parceiro quando existir",
                src.contains("if (!isNullOrZero(codVendParceiro)) {"));
    }

    @Test
    public void internalApiStrategy_mustNotFallbackToRandomNutabWhenPreferredExists() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java");

        assertTrue("Nao deve cair no fallback geral de NUTAB quando ha NUTAB preferencial",
                src.contains("if (isNullOrZero(preferredNuTab) && (isNullOrZero(data.nuTab) || data.precoBase == null))"));
        assertTrue("NUTAB deve ser normalizado para versao ativa mais recente",
                src.contains("data.nuTab = normalizeNuTabToLatestActive(data.nuTab);"));
    }

    @Test
    public void internalApiStrategy_mustNotUsePartialPkForTipoVendaVersionada() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java");

        assertFalse("TipoVenda versionada nao pode ser resolvida por findByPK(codTipVenda) isolado",
                src.contains("return tpvDAO.findByPK(codTipVenda);"));
        assertTrue("TipoVenda deve ser resolvida buscando por CODTIPVENDA e escolhendo a versao vigente",
                src.contains("Collection<DynamicVO> vendas = tpvDAO.find(\"this.CODTIPVENDA = ?\", codTipVenda);"));
    }

    @Test
    public void internalApiStrategy_mustFailFastWhenHeaderCompositeKeysAreIncomplete() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java");

        assertTrue("Cabecalho deve falhar quando CODPARC nao for resolvido",
                src.contains("throw new Exception(\"CODPARC nao resolvido para o pedido"));
        assertTrue("Cabecalho deve falhar quando DHTIPOPER nao for resolvido",
                src.contains("throw new Exception(\"DHTIPOPER nao resolvido para CODTIPOPER"));
        assertTrue("Cabecalho deve falhar quando DHTIPVENDA nao for resolvido",
                src.contains("throw new Exception(\"DHTIPVENDA nao resolvido para CODTIPVENDA"));
    }

    @Test
    public void importFlow_mustNotUseSqlFunctionsInsideJapeFindPredicates() throws Exception {
        String orderService = readMainSource("br/com/bellube/fastchannel/service/OrderService.java");
        String internalApi = readMainSource("br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java");

        assertFalse("OrderService nao deve usar UPPER(...) dentro de Jape find para cidade/parceiro",
                orderService.contains(".find(\"UPPER("));
        assertFalse("InternalApiStrategy nao deve usar UPPER(...) dentro de Jape find para usuario",
                internalApi.contains(".find(\"UPPER("));
    }

    @Test
    public void orderService_mustUseClaimBasedIdempotencyWithoutAppLock() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/OrderService.java");

        assertTrue("Importacao deve reclamar o pedido via claimOrderImport(order)",
                src.contains("claim = claimOrderImport(order);"));
        assertTrue("Importacao deve usar STATUS_IMPORT='PROCESSANDO' como posse tecnica",
                src.contains("STATUS_IMPORT_PROCESSANDO"));
        assertFalse("Fluxo nao pode voltar a usar sp_getapplock",
                src.contains("sp_getapplock"));
        assertFalse("Fluxo nao pode voltar a usar sp_releaseapplock",
                src.contains("sp_releaseapplock"));
    }

    @Test
    public void orderService_mustUpsertOrderMappingWithoutCheckThenInsertRace() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/OrderService.java");

        assertFalse("Nao pode voltar ao SELECT 1 previo em AD_FCPEDIDO antes do insert/update",
                src.contains("SELECT 1 FROM AD_FCPEDIDO WHERE ORDER_ID = :orderId"));
        assertTrue("Deve existir bind centralizado dos parametros de mapeamento",
                src.contains("bindOrderMappingParameters("));
        assertTrue("Deve tratar colisao de UK_FCPEDIDO_ORDERID como fluxo idempotente",
                src.contains("isOrderMappingUniqueViolation"));
    }

    @Test
    public void orderStatusSyncJob_mustTolerateSchemaWithoutNunotaFatura() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/job/OrderStatusSyncJob.java");

        assertTrue("Job deve validar presenca fisica da coluna NUNOTA_FATURA",
                src.contains("DbColumnSupport.hasColumn(conn, \"AD_FCPEDIDO\", \"NUNOTA_FATURA\")"));
        assertTrue("Job deve montar SELECT dinamico quando a coluna nao existir",
                src.contains("CAST(NULL AS VARCHAR(50)) AS NUNOTA_FATURA"));
    }

    @Test
    public void orderService_mustResolvePartnerFallbackWithoutSessionlessJapeRead() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/OrderService.java");

        assertTrue("Fallback de parceiro deve depender apenas da configuracao explicita",
                src.contains("BigDecimal configured = getDefaultCodParc();"));
        assertTrue("Sem configuracao explicita nao deve escolher parceiro arbitrario",
                src.contains("Nenhum parceiro arbitrario sera escolhido"));
        assertFalse("Fallback de parceiro nao pode voltar a usar SELECT arbitrario em TGFPAR",
                src.contains("SELECT TOP 1 CODPARC FROM TGFPAR WHERE CLIENTE = 'S' ORDER BY CODPARC"));
        assertFalse("Fallback de parceiro nao pode voltar a usar parceiroDAO.find fora de sessao",
                src.contains("Collection<DynamicVO> parceiros = parceiroDAO.find(\"this.CLIENTE = ?\", \"S\")"));
    }

    @Test
    public void orderService_mustSkipOrdersWithIneligibleStatusType() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/OrderService.java");

        assertTrue("Importacao deve ignorar pedidos com CurrentStatusTypeId=2, como no legado",
                src.contains("order.getCurrentStatusTypeId() != null && order.getCurrentStatusTypeId().intValue() == 2"));
        assertTrue("Skip deve ser logado para diagnostico operacional",
                src.contains("ignorado por CurrentStatusTypeId=2"));
    }

    @Test
    public void fastchannelOrdersClient_mustNotSendIgnoreCreationDateWithCreatedAfter() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/http/FastchannelOrdersClient.java");

        // IgnoreCreationDate=true NAO deve ser enviado junto com CreatedAfter!
        // Esse parametro faz a API Fastchannel ignorar TODOS os filtros de data,
        // incluindo CreatedAfter, o que causa a regressao dos 2836 pedidos historicos.
        // O legado (pedidos.js linhas 940-965) tambem NAO envia IgnoreCreationDate
        // quando usa CreatedAfter para filtrar pedidos.
        assertFalse("Listagem de pedidos NAO deve enviar IgnoreCreationDate=true (anula CreatedAfter, causa regressao pedidos historicos)",
                src.contains("endpoint.append(\"&IgnoreCreationDate=true\")"));
        assertTrue("Listagem de pedidos DEVE enviar CreatedAfter com safety overlap (safeSync)",
                src.contains("endpoint.append(\"&CreatedAfter=\").append(sdf.format(safeSync))"));
        assertTrue("Listagem de pedidos DEVE ter safety overlap de 48h para nao perder pedidos com erro",
                src.contains("SAFETY_OVERLAP_MS"));
        assertTrue("Listagem de pedidos DEVE ter fallback de 72h quando lastSync for nulo",
                src.contains("72L * 60 * 60 * 1000"));
    }

    @Test
    public void dashboard_mustUseLastOrderSyncNotNull() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/web/FCDashboardService.java");

        // O dashboard DEVE usar cfg.getLastOrderSync() para limitar o escopo temporal.
        // Passar null como lastSync faz o fallback de 72h, mas se combinado com
        // IgnoreCreationDate=true (bug corrigido), retorna TODOS os pedidos historicos.
        // Usar o cursor do config garante que apenas pedidos recentes sejam contados.
        assertTrue("Dashboard DEVE usar lastOrderSync da config, nao null",
                src.contains("cfg.getLastOrderSync()"));
        assertFalse("Dashboard NAO deve passar null literal para listOrdersWithMeta (causa 2836+ pedidos historicos)",
                src.contains("listOrdersWithMeta(null,"));
    }

    @Test
    public void fastchannelPriceClient_mustReconcileRemoteBatchesLikeLegacy() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/http/FastchannelPriceClient.java");

        assertTrue("Preco escalonado deve listar batches remotos antes de reconciliar",
                src.contains("List<PriceBatchItemDTO> currentBatches = listPriceBatches(sku);"));
        assertTrue("Preco escalonado deve remover batches remotos divergentes",
                src.contains("deletePriceBatch(sku, currentBatch.getBatchId())"));
        assertTrue("Preco escalonado deve recriar apenas batches ausentes ou alterados",
                src.contains("containsEquivalentBatch(currentBatches, desiredBatch)"));
    }

    @Test
    public void stockFullSyncJob_mustUseMappedCodEmpAndCodLocalInsteadOfGlobalDefaults() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/job/StockFullSyncJob.java");

        assertTrue("Full sync de estoque deve iterar por CODEMP/CODLOCAL reais do estoque",
                src.contains("SELECT DISTINCT E.CODPROD, E.CODEMP, E.CODLOCAL, P.ATIVO"));
        assertTrue("Full sync de estoque deve resolver estoque por linha, nao por config global",
                src.contains("resolver.resolve(codProd, codEmp, codLocal)"));
        assertTrue("Full sync de estoque deve enviar StorageId e ResellerId mapeados",
                src.contains("stockClient.updateStock(sku, effectiveQty, storageId, resellerId)"));
        assertFalse("Full sync de estoque nao pode voltar a usar somente CODEMP/CODLOCAL globais",
                src.contains("resolver.resolve(codProd, config.getCodemp(), config.getCodLocal())"));
    }

    @Test
    public void orderService_mustRequireCodCidBeforeNativePartnerInsert() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/OrderService.java");

        assertTrue("Criacao de parceiro deve falhar cedo se CODCID nao for resolvido",
                src.contains("throw new Exception(\"CODCID nao resolvido para criacao nativa do parceiro Fastchannel.\")"));
        assertTrue("Criacao de parceiro deve alimentar CODCID no TGFPAR antes do save",
                src.contains(".set(\"CODCID\", codCid)"));
    }

    @Test
    public void fcAdminService_mustRetryStaleProcessingOrders() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/web/FCAdminService.java");

        assertTrue("Retry manual deve considerar PROCESSANDO vencido",
                src.contains("UPPER(COALESCE(STATUS_IMPORT, '')) = 'PROCESSANDO'"));
        assertTrue("Retry manual deve usar cutoff temporal parametrizado",
                src.contains("DH_IMPORTACAO < ?"));
    }

    @Test
    public void fcAdminService_mustNotReportBlindSuccessWhenRetriesFail() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/web/FCAdminService.java");
        int methodStart = src.indexOf("public Map<String, Object> importarPedidos(");
        int methodEnd = src.indexOf("public Map<String, Object> processarFila(", methodStart);
        String importarPedidosBody = methodEnd > methodStart
                ? src.substring(methodStart, methodEnd)
                : src.substring(methodStart);

        assertTrue("Resultado administrativo deve derivar success a partir de falhas do retry",
                importarPedidosBody.contains("boolean success = retrySummary.failed == 0;"));
        assertTrue("Resposta administrativa deve expor contagem de falhas no retry",
                importarPedidosBody.contains("result.put(\"retryFailedCount\", retrySummary.failed);"));
        assertFalse("Nao pode voltar a setar success=true de forma fixa na importacao manual",
                importarPedidosBody.contains("result.put(\"success\", true);"));
    }

    private String readMainSource(String relativeMainJavaPath) throws IOException {
        Path fromRepoRoot = Paths.get("model", "src", "main", "java")
                .resolve(relativeMainJavaPath);
        if (Files.exists(fromRepoRoot)) {
            return new String(Files.readAllBytes(fromRepoRoot), StandardCharsets.UTF_8);
        }

        Path fromModelRoot = Paths.get("src", "main", "java")
                .resolve(relativeMainJavaPath);
        if (Files.exists(fromModelRoot)) {
            return new String(Files.readAllBytes(fromModelRoot), StandardCharsets.UTF_8);
        }

        throw new IOException("Arquivo fonte nao encontrado: " + relativeMainJavaPath);
    }
}
