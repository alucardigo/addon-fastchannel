package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.dto.PriceBatchItemDTO;
import br.com.bellube.fastchannel.dto.PriceDTO;
import br.com.bellube.fastchannel.http.FastchannelPriceClient;

import br.com.bellube.fastchannel.util.DBUtil;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Orquestra sincronizacao de preco para os canais de distribuicao/consumo.
 */
public class PriceService {

    private static final Logger log = Logger.getLogger(PriceService.class.getName());

    private final DeparaService deparaService;
    private final PriceResolver priceResolver;
    private final PriceTableResolver priceTableResolver;
    private final FastchannelPriceClient distributionClient;
    private final FastchannelPriceClient consumptionClient;

    public PriceService() {
        this(new DeparaServiceProvider(), new PriceResolver(), new PriceTableResolver(),
                new FastchannelPriceClient(FastchannelPriceClient.Channel.DISTRIBUTION),
                new FastchannelPriceClient(FastchannelPriceClient.Channel.CONSUMPTION));
    }

    PriceService(DeparaServiceProvider deparaProvider,
                 PriceResolver priceResolver,
                 PriceTableResolver priceTableResolver,
                 FastchannelPriceClient distributionClient,
                 FastchannelPriceClient consumptionClient) {
        this.deparaService = deparaProvider.get();
        this.priceResolver = priceResolver;
        this.priceTableResolver = priceTableResolver;
        this.distributionClient = distributionClient;
        this.consumptionClient = consumptionClient;
    }

    /**
     * Cache do mapa FC table → NUTAB para evitar reabrir conexoes JAPE em cada chamada.
     * Populado na primeira invocacao de syncPrice e reutilizado nas seguintes.
     * Thread-safe porque syncEmLote roda em thread unico (protegido por mutex).
     */
    private volatile Map<String, BigDecimal> cachedFcTableToNuTab = null;

    /**
     * [TABLE-VALIDATION] Conjunto de tabelas FC que ja foram confirmadas como invalidas
     * pela API nesta sessao JVM (HTTP 400 "tabela nao valida").
     *
     * Uma vez que uma tabela e classificada como invalida:
     * 1. E removida de cachedFcTableToNuTab → proximos produtos nao tentam
     * 2. E registrada aqui para log SEVERE unico (nao repetir spam por SKU)
     * 3. Persiste ate restart do addon (intencional — tabela invalida nao magicamente fica valida)
     *
     * O admin deve corrigir o De-Para TABELA_PRECO removendo o ID invalido.
     */
    private final java.util.Set<BigDecimal> knownInvalidFcTables =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());

    public void syncPrice(BigDecimal codProd, String sku) throws Exception {
        if (codProd == null || sku == null || sku.trim().isEmpty()) {
            return;
        }

        FastchannelPriceClient.Channel channel = determineChannel(codProd, sku);
        // [POOL-FIX] Cachear resolveTableToNuTabMap para nao abrir conexoes em cada item.
        // Sem esse cache, 1510 items × 3+ conn por resolveTableToNuTabMap = 4530+ conexoes abertas.
        Map<String, BigDecimal> fcTableToNuTab = cachedFcTableToNuTab;
        if (fcTableToNuTab == null) {
            fcTableToNuTab = priceTableResolver.resolveTableToNuTabMap();
            cachedFcTableToNuTab = fcTableToNuTab;
        }

        if (fcTableToNuTab.isEmpty()) {
            List<BigDecimal> tables = priceTableResolver.resolveEligibleTables();
            for (BigDecimal nuTab : tables) {
                BigDecimal tableId = resolvePriceTableId(nuTab);
                if (tableId != null) {
                    fcTableToNuTab.put(tableId.toPlainString(), nuTab);
                }
            }
        }

        if (fcTableToNuTab.isEmpty()) {
            log.warning("Nenhuma tabela de preco Fast elegivel encontrada para sincronizar SKU " + sku);
            return;
        }

        FastchannelPriceClient client = clientFor(channel);
        PriceBatchResolver batchResolver = new PriceBatchResolver();
        boolean atLeastOneTableSynced = false;
        List<String> invalidatedNow = new ArrayList<>(); // coleta para remover apos o loop

        for (Map.Entry<String, BigDecimal> entry : fcTableToNuTab.entrySet()) {
            BigDecimal tableId = new BigDecimal(entry.getKey());
            BigDecimal nuTab = entry.getValue();

            // [TABLE-VALIDATION] Pular tabelas ja conhecidas como invalidas nesta sessao JVM.
            // Evita spam de HTTP 400 contra a API FC e economiza tempo em lotes grandes.
            if (knownInvalidFcTables.contains(tableId)) {
                log.fine("[TABLE-VALIDATION] Tabela FC " + tableId + " ja validada como invalida — ignorando SKU=" + sku);
                continue;
            }

            try {
                syncPriceTable(client, batchResolver, codProd, sku, nuTab, tableId, channel);
                atLeastOneTableSynced = true;
            } catch (Exception e) {
                // [TABLE-ISOLATION-FIX] Se uma tabela FC for invalida (HTTP 400 "tabela de precos nao valida"),
                // nao abortar todo o sync do produto — isolar por tabela e evictar do cache.
                // INCIDENTE 2026-04-14: PriceTableId=62 devolvia HTTP 400 para TODOS os produtos,
                // causando 100% de falhas (0 sucesso, 1508 erros). Esse guard isola o erro por tabela.
                if (isPriceTableInvalidError(e)) {
                    // Primeira vez: SEVERE + marcar para evictar do cache
                    if (knownInvalidFcTables.add(tableId)) {
                        log.severe("[TABLE-VALIDATION] ACAO NECESSARIA: Tabela FC ID=" + tableId
                                + " retornou HTTP 400 'tabela de precos invalida'. "
                                + "Remova este ID do De-Para TABELA_PRECO (AD_FCDEPARA) "
                                + "ou crie a tabela na API FC. "
                                + "A tabela foi REMOVIDA do cache e nao sera tentada novamente ate restart do addon.");
                    }
                    invalidatedNow.add(entry.getKey());
                } else {
                    throw e;
                }
            }
        }

        // Evictar tabelas invalidas do cache compartilhado (afeta proximos SKUs desta sessao)
        if (!invalidatedNow.isEmpty() && cachedFcTableToNuTab != null) {
            for (String key : invalidatedNow) {
                cachedFcTableToNuTab.remove(key);
                log.warning("[TABLE-VALIDATION] Tabela FC ID=" + key
                        + " removida do cache. Proximos SKUs nao tentarao esta tabela.");
            }
        }

        if (!atLeastOneTableSynced) {
            throw new Exception("Nenhuma tabela FC valida encontrada para sincronizar SKU=" + sku
                    + ". Verifique o De-Para de TABELA_PRECO — todas as tabelas configuradas sao invalidas na API FC.");
        }
    }

    /**
     * Detecta erros de tabela de preco invalida na API FC (HTTP 400 + mensagem especifica).
     * Usado para isolar falhas de configuracao por tabela sem abortar o sync do produto.
     */
    private boolean isPriceTableInvalidError(Exception e) {
        if (e == null || e.getMessage() == null) return false;
        String msg = e.getMessage();
        // HTTP 400 com body indicando tabela invalida
        return msg.contains("status=400") && (
                msg.contains("tabela de pre")      // "tabela de preços não é válido"
                || msg.contains("PriceTable")      // campo PriceTableId em stack trace
                || msg.contains("BadRequest")      // codigo generico da API FC
        );
    }

    private String truncateMsg(String msg) {
        if (msg == null) return "";
        return msg.length() > 300 ? msg.substring(0, 300) + "..." : msg;
    }

    public void syncPriceBatch(List<BigDecimal> codProds) throws Exception {
        if (codProds == null || codProds.isEmpty()) {
            return;
        }

        // [PERF 2026-04-24] Paralelismo controlado:
        // Antes: sequencial 1510 produtos x 6 tabelas x ~2-3 HTTP/item = ~2.5h total
        //        com chance de interrupção por timeout agregado (conexao cai, thread morre)
        // Agora: thread pool fixo de 4 workers com limite de rate interno
        //        (FastchannelHttpClient.waitForRateLimit) segura throttling da API.
        //        Cada produto e uma unidade isolada - falha em um nao derruba os outros.
        int total = codProds.size();
        int parallelism = 4;
        String prop = System.getProperty("fc.sync.parallelism");
        if (prop != null) {
            try { parallelism = Math.max(1, Math.min(16, Integer.parseInt(prop.trim()))); } catch (Exception ignore) {}
        }
        log.info("[syncPriceBatch] iniciando sync paralelo: " + total + " produtos, " + parallelism + " workers");
        long t0 = System.currentTimeMillis();

        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(parallelism, r -> {
            Thread t = new Thread(r);
            t.setName("fc-sync-price-" + t.getId());
            t.setDaemon(true);
            return t;
        });
        java.util.concurrent.atomic.AtomicInteger done = new java.util.concurrent.atomic.AtomicInteger(0);
        java.util.concurrent.atomic.AtomicInteger failed = new java.util.concurrent.atomic.AtomicInteger(0);
        java.util.concurrent.atomic.AtomicInteger skipped = new java.util.concurrent.atomic.AtomicInteger(0);
        java.util.List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>(total);

        // [SYNC-OPT 2026-06-01] Prefetch dos SKUs que EXISTEM na FC. Produtos ausentes serao
        // pulados (so gerariam HTTP 404 "SKU nao existe" — ~75% do catalogo AD_FAST nao esta na FC).
        // Fail-open: se getFcExistingSkus() devolver null (feature off/prefetch falhou), nada e pulado.
        final java.util.Set<String> fcExistingSkus = getFcExistingSkus();

        try {
            for (BigDecimal codProd : codProds) {
                if (codProd == null) continue;
                futures.add(pool.submit(() -> {
                    String sku = null;
                    try {
                        sku = deparaService.getSkuForStock(codProd);
                        if (shouldSkipNonexistentSku(fcExistingSkus, sku)) {
                            skipped.incrementAndGet();
                            return;
                        }
                        syncPrice(codProd, sku);
                    } catch (Exception e) {
                        failed.incrementAndGet();
                        log.log(Level.WARNING, "Falha ao sincronizar preco SKU=" + sku + " CODPROD=" + codProd, e);
                    } finally {
                        int d = done.incrementAndGet();
                        if (d % 100 == 0 || d == total) {
                            long elapsed = (System.currentTimeMillis() - t0) / 1000;
                            log.info("[syncPriceBatch] progresso: " + d + "/" + total
                                    + " (falhas=" + failed.get() + " pulados=" + skipped.get() + ") tempo=" + elapsed + "s");
                        }
                    }
                }));
            }

            // Aguarda todas as futures completarem (erros individuais ja foram logados)
            for (java.util.concurrent.Future<?> f : futures) {
                try { f.get(); } catch (java.util.concurrent.ExecutionException | InterruptedException ignore) {}
            }
        } finally {
            pool.shutdown();
            try {
                if (!pool.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                }
            } catch (InterruptedException ie) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        long elapsedSec = (System.currentTimeMillis() - t0) / 1000;
        int processados = total - skipped.get();
        log.info("[syncPriceBatch] concluido: " + total + " produtos em " + elapsedSec
                + "s (" + (processados - failed.get()) + " ok, " + failed.get() + " falhas, "
                + skipped.get() + " pulados por nao existir na FC)");
    }

    /**
     * [SYNC-OPT 2026-06-01] Cache curto do conjunto de SKUs que existem na FC.
     * Reaproveitado entre o full sync (a cada 6h) e o "Sincronizar Todos" manual.
     */
    private static volatile java.util.Set<String> FC_EXISTING_SKUS = null;
    private static volatile long FC_EXISTING_SKUS_TS = 0L;
    private static final long FC_EXISTING_SKUS_TTL_MS = 5 * 60_000L;

    /**
     * Conjunto de SKUs existentes na FC, com cache de 5 min. Usado para PULAR produtos
     * inexistentes em full/large sync (que so gerariam HTTP 404).
     *
     * <p><b>Fail-open por design:</b> retorna {@code null} (=nao pular nada) quando:
     * <ul>
     *   <li>a feature esta desligada via {@code -Dfc.sync.skipNonexistent=false};</li>
     *   <li>nao ha tabelas FC resolviveis; ou</li>
     *   <li>o prefetch falhou/retornou vazio (hiccup transitorio da API).</li>
     * </ul>
     * Assim nunca zeramos o sync por engano — no pior caso, voltamos ao comportamento legado.</p>
     */
    public java.util.Set<String> getFcExistingSkus() {
        if (!"true".equalsIgnoreCase(System.getProperty("fc.sync.skipNonexistent", "true"))) {
            return null; // feature desligada
        }
        long now = System.currentTimeMillis();
        java.util.Set<String> cached = FC_EXISTING_SKUS;
        if (cached != null && (now - FC_EXISTING_SKUS_TS) < FC_EXISTING_SKUS_TTL_MS) {
            return cached;
        }
        try {
            java.util.List<BigDecimal> fcTableIds = resolveFcTableIdsForPrefetch();
            if (fcTableIds.isEmpty()) {
                return null; // sem tabelas => fail-open
            }
            java.util.Set<String> skus = consumptionClient.listExistingSkus(fcTableIds);
            if (skus == null || skus.isEmpty()) {
                log.warning("[SYNC-OPT] prefetch de SKUs FC vazio — fail-open (nenhum produto sera pulado).");
                return null;
            }
            FC_EXISTING_SKUS = skus;
            FC_EXISTING_SKUS_TS = now;
            log.info("[SYNC-OPT] prefetch FC concluido: " + skus.size() + " SKU(s) existentes (cache 5min).");
            return skus;
        } catch (Exception e) {
            log.log(Level.WARNING, "[SYNC-OPT] falha no prefetch de SKUs FC — fail-open.", e);
            return null;
        }
    }

    /** Resolve os PriceTableId (FC) a partir das tabelas elegiveis (NUTAB) para o prefetch. */
    private java.util.List<BigDecimal> resolveFcTableIdsForPrefetch() {
        java.util.LinkedHashSet<BigDecimal> ids = new java.util.LinkedHashSet<>();
        try {
            for (BigDecimal nuTab : priceTableResolver.resolveEligibleTables()) {
                BigDecimal fcId = resolvePriceTableId(nuTab);
                if (fcId != null) {
                    ids.add(fcId);
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "[SYNC-OPT] erro resolvendo FC tableIds para prefetch", e);
        }
        return new java.util.ArrayList<>(ids);
    }

    /** True se devemos pular o SKU por nao existir na FC (so quando temos um set valido). */
    public boolean shouldSkipNonexistentSku(java.util.Set<String> fcExistingSkus, String sku) {
        return fcExistingSkus != null && sku != null && !fcExistingSkus.contains(sku.trim());
    }

    private void syncPriceTable(FastchannelPriceClient client,
                                PriceBatchResolver batchResolver,
                                BigDecimal codProd,
                                String sku,
                                BigDecimal nuTab,
                                BigDecimal tableId,
                                FastchannelPriceClient.Channel channel) throws Exception {
        boolean productExists = productExistsInTable(codProd, nuTab);
        PriceResolver.PriceResult result = productExists ? priceResolver.resolve(codProd, nuTab) : null;
        boolean hasPositivePrice = result != null
                && result.getPriceCentavos() != null
                && result.getPriceCentavos().compareTo(BigDecimal.ZERO) > 0;

        // [ESCALONADO-OVERRIDE 2026-05-26] Resolver os batches ANTES do PUT de preco. Se houver uma
        // faixa escalonada cobrindo a 1a unidade (Min<=1<=Max), o preco dessa faixa e o preco efetivo
        // de 1 unidade e DEVE sobrepor o SalePrice (Preco de Venda). O escalonado e uma campanha com
        // data-fim e tem prioridade; sem isto a listagem/carrinho mostram o SalePrice cheio enquanto o
        // detalhe mostra o escalonado — incoerente. Mantemos o ListPrice (De) como referencia riscada.
        List<PriceBatchItemDTO> batches = Collections.emptyList();
        if (hasPositivePrice) {
            try {
                batches = batchResolver.resolve(codProd, nuTab, tableId);
            } catch (Exception batchResolveEx) {
                log.log(Level.WARNING, "Erro ao resolver batches SKU " + sku + " fcTable=" + tableId, batchResolveEx);
            }
        }

        PriceDTO dto = new PriceDTO();
        dto.setSku(sku);
        dto.setPriceTableId(tableId);
        if (hasPositivePrice) {
            BigDecimal salePrice = result.getPriceCentavos();
            BigDecimal listPrice = result.getListPriceCentavos() != null
                    ? result.getListPriceCentavos()
                    : result.getPriceCentavos();

            BigDecimal firstUnitBatchPrice = findFirstUnitBatchPrice(batches);
            if (firstUnitBatchPrice != null && firstUnitBatchPrice.compareTo(BigDecimal.ZERO) > 0
                    && firstUnitBatchPrice.compareTo(salePrice) != 0) {
                log.info("[ESCALONADO-OVERRIDE] SKU=" + sku + " fcTable=" + tableId
                        + " SalePrice " + salePrice + " -> " + firstUnitBatchPrice
                        + " (faixa escalonada a partir de 1 un. sobrepoe o Preco de Venda)");
                salePrice = firstUnitBatchPrice;
            }
            dto.setPrice(salePrice);
            dto.setListPrice(listPrice);
        } else {
            dto.setPrice(BigDecimal.ZERO);
            dto.setListPrice(BigDecimal.ZERO);
        }

        client.updatePrice(dto);
        log.info("PUT preco: codProd=" + codProd + " sku=" + sku + " nuTab=" + nuTab
                + " fcTable=" + tableId + " sale=" + dto.getPrice()
                + " list=" + dto.getListPrice() + " channel=" + channel
                + " existsInTable=" + productExists + " mirroredZero=" + (!hasPositivePrice));

        // [VERIFY-SKIP] So verificar apos PUT se FINE logging estiver habilitado.
        // Em producao (INFO/WARNING), pular o GET extra economiza ~12.000 chamadas por full-sync.
        if (log.isLoggable(Level.FINE)) {
            verifySyncedPrice(client, sku, tableId, dto.getPrice());
        }

        client.updatePriceBatches(sku, tableId, batches);
        log.info("Batches reconciliados: codProd=" + codProd + " fcTable=" + tableId + " faixas=" + batches.size());
    }

    /**
     * Retorna o preco (centavos) da faixa escalonada que cobre a 1a unidade (Min&lt;=1&lt;=Max),
     * ou null se nao houver. Se houver mais de uma, retorna a de MENOR preco (mais vantajosa).
     * Usado para sobrepor o SalePrice quando o escalonado vale "a partir de 1 unidade".
     */
    private BigDecimal findFirstUnitBatchPrice(List<PriceBatchItemDTO> batches) {
        if (batches == null || batches.isEmpty()) {
            return null;
        }
        BigDecimal one = BigDecimal.ONE;
        BigDecimal best = null;
        for (PriceBatchItemDTO b : batches) {
            if (b == null) {
                continue;
            }
            BigDecimal price = b.getUnitaryPriceForBatch();
            if (price == null || price.compareTo(BigDecimal.ZERO) <= 0) {
                continue;
            }
            BigDecimal min = b.getMinimumBatchSize();
            BigDecimal max = b.getMaximumBatchSize();
            boolean coversFirstUnit = (min == null || min.compareTo(one) <= 0)
                    && (max == null || max.compareTo(one) >= 0);
            if (coversFirstUnit && (best == null || price.compareTo(best) < 0)) {
                best = price;
            }
        }
        return best;
    }

    private void verifySyncedPrice(FastchannelPriceClient client, String sku, BigDecimal tableId, BigDecimal expectedPrice) {
        try {
            PriceDTO verify = client.getPrice(sku, tableId);
            if (verify != null && verify.getPrice() != null) {
                if (verify.getPrice().compareTo(expectedPrice) != 0) {
                    log.warning("VERIFY MISMATCH: PUT sale=" + expectedPrice
                            + " mas GET retornou=" + verify.getPrice()
                            + " SKU=" + sku + " fcTable=" + tableId);
                } else {
                    log.info("VERIFY OK: SKU=" + sku + " fcTable=" + tableId
                            + " preco=" + verify.getPrice());
                }
            } else {
                log.warning("VERIFY FALHOU: GET nao retornou preco SKU=" + sku
                        + " fcTable=" + tableId);
            }
        } catch (Exception verifyEx) {
            log.log(Level.WARNING, "VERIFY ERRO SKU=" + sku + " fcTable=" + tableId, verifyEx);
        }
    }

    FastchannelPriceClient.Channel determineChannel(BigDecimal codProd, String sku) {
        String mapped = deparaService.getCodigoExterno(DeparaService.TIPO_TABELA_PRECO, codProd);
        if (mapped != null && mapped.toUpperCase().contains("DIST")) {
            return FastchannelPriceClient.Channel.DISTRIBUTION;
        }
        if (sku != null && sku.toUpperCase().startsWith("D-")) {
            return FastchannelPriceClient.Channel.DISTRIBUTION;
        }
        return FastchannelPriceClient.Channel.CONSUMPTION;
    }

    private boolean productExistsInTable(BigDecimal codProd, BigDecimal nuTab) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement("SELECT TOP 1 1 FROM TGFEXC WHERE CODPROD = ? AND NUTAB = ?");
            stmt.setBigDecimal(1, codProd);
            stmt.setBigDecimal(2, nuTab);
            rs = stmt.executeQuery();
            return rs.next();
        } catch (Exception e) {
            log.log(Level.FINE, "Erro ao verificar TGFEXC codProd=" + codProd + " nuTab=" + nuTab, e);
            return false;
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
    }

    private BigDecimal resolvePriceTableId(BigDecimal nuTab) {
        if (nuTab == null) return null;
        // Tentativa 1: de-para direto por NUTAB
        String mapped = deparaService.getCodigoExterno(DeparaService.TIPO_TABELA_PRECO, nuTab);
        if (mapped != null && !mapped.trim().isEmpty()) {
            try { return new BigDecimal(mapped.trim()); } catch (NumberFormatException e) { /* fall through */ }
        }
        // Tentativa 2: resolver CODTAB e buscar de-para por qualquer NUTAB da mesma CODTAB
        // Necessario porque resolveEligibleTables pode retornar o NUTAB mais recente (ex: 4410),
        // mas o de-para foi configurado com o NUTAB original (ex: 3060), ambos da mesma CODTAB.
        BigDecimal codTab = resolveCodTabFromNuTab(nuTab);
        if (codTab != null) {
            mapped = deparaService.getCodigoExterno(DeparaService.TIPO_TABELA_PRECO, codTab);
            if (mapped != null && !mapped.trim().isEmpty()) {
                try { return new BigDecimal(mapped.trim()); } catch (NumberFormatException e) { /* fall through */ }
            }
            // Tentativa 3: buscar de-para de qualquer NUTAB irmao da mesma CODTAB
            String fromSibling = findPriceTableIdByCodTabFamily(codTab);
            if (fromSibling != null) {
                try { return new BigDecimal(fromSibling); } catch (NumberFormatException e) { /* fall through */ }
            }
        }
        return null;
    }

    private BigDecimal resolveCodTabFromNuTab(BigDecimal nuTab) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement("SELECT CODTAB FROM TGFTAB WHERE NUTAB = ?");
            stmt.setBigDecimal(1, nuTab);
            rs = stmt.executeQuery();
            if (rs.next()) return rs.getBigDecimal("CODTAB");
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver CODTAB para NUTAB " + nuTab, e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return null;
    }

    private String findPriceTableIdByCodTabFamily(BigDecimal codTab) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                "SELECT TOP 1 D.COD_EXTERNO FROM AD_FCDEPARA D " +
                "INNER JOIN TGFTAB T ON T.NUTAB = CAST(D.COD_SANKHYA AS INT) " +
                "WHERE D.TIPO_ENTIDADE = 'TABELA_PRECO' AND T.CODTAB = ? " +
                "ORDER BY T.DTVIGOR DESC");
            stmt.setBigDecimal(1, codTab);
            rs = stmt.executeQuery();
            if (rs.next()) return rs.getString("COD_EXTERNO");
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao buscar priceTableId por CODTAB family " + codTab, e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return null;
    }

    private FastchannelPriceClient clientFor(FastchannelPriceClient.Channel channel) {
        return channel == FastchannelPriceClient.Channel.DISTRIBUTION ? distributionClient : consumptionClient;
    }

    static final class DeparaServiceProvider {
        DeparaService get() {
            return DeparaService.getInstance();
        }
    }
}
