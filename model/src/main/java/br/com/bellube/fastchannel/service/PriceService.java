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

    public void syncPrice(BigDecimal codProd, String sku) throws Exception {
        if (codProd == null || sku == null || sku.trim().isEmpty()) {
            return;
        }

        FastchannelPriceClient.Channel channel = determineChannel(codProd, sku);
        Map<String, BigDecimal> fcTableToNuTab = priceTableResolver.resolveTableToNuTabMap();

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
        for (Map.Entry<String, BigDecimal> entry : fcTableToNuTab.entrySet()) {
            BigDecimal tableId = new BigDecimal(entry.getKey());
            BigDecimal nuTab = entry.getValue();
            syncPriceTable(client, batchResolver, codProd, sku, nuTab, tableId, channel);
        }
    }

    public void syncPriceBatch(List<BigDecimal> codProds) throws Exception {
        if (codProds == null || codProds.isEmpty()) {
            return;
        }

        for (BigDecimal codProd : codProds) {
            if (codProd == null) {
                continue;
            }
            String sku = deparaService.getSkuForStock(codProd);
            try {
                syncPrice(codProd, sku);
            } catch (Exception e) {
                log.warning("Falha ao sincronizar preco SKU=" + sku + " CODPROD=" + codProd + ": " + e.getMessage());
            }
        }
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

        PriceDTO dto = new PriceDTO();
        dto.setSku(sku);
        dto.setPriceTableId(tableId);
        if (hasPositivePrice) {
            dto.setPrice(result.getPriceCentavos());
            dto.setListPrice(result.getListPriceCentavos() != null
                    ? result.getListPriceCentavos()
                    : result.getPriceCentavos());
        } else {
            dto.setPrice(BigDecimal.ZERO);
            dto.setListPrice(BigDecimal.ZERO);
        }

        client.updatePrice(dto);
        log.info("PUT preco: codProd=" + codProd + " sku=" + sku + " nuTab=" + nuTab
                + " fcTable=" + tableId + " sale=" + dto.getPrice()
                + " list=" + dto.getListPrice() + " channel=" + channel
                + " existsInTable=" + productExists + " mirroredZero=" + (!hasPositivePrice));

        verifySyncedPrice(client, sku, tableId, dto.getPrice());

        List<PriceBatchItemDTO> batches = Collections.emptyList();
        if (hasPositivePrice) {
            try {
                batches = batchResolver.resolve(codProd, nuTab, tableId);
            } catch (Exception batchResolveEx) {
                log.log(Level.WARNING, "Erro ao resolver batches SKU " + sku + " fcTable=" + tableId, batchResolveEx);
            }
        }
        client.updatePriceBatches(sku, tableId, batches);
        log.info("Batches reconciliados: codProd=" + codProd + " fcTable=" + tableId + " faixas=" + batches.size());
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
            log.warning("VERIFY ERRO: " + verifyEx.getMessage()
                    + " SKU=" + sku + " fcTable=" + tableId);
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
