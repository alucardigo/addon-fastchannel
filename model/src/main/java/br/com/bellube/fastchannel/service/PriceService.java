package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.dto.PriceBatchItemDTO;
import br.com.bellube.fastchannel.dto.PriceDTO;
import br.com.bellube.fastchannel.http.FastchannelPriceClient;

import br.com.bellube.fastchannel.util.DBUtil;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
        List<BigDecimal> tables = priceTableResolver.resolveEligibleTables();
        if (tables.isEmpty()) {
            tables = Collections.singletonList(BigDecimal.ZERO);
        }

        for (BigDecimal nuTab : tables) {
            PriceResolver.PriceResult result = priceResolver.resolve(codProd, nuTab);
            if (result == null || result.getPriceCentavos() == null
                    || result.getPriceCentavos().compareTo(BigDecimal.ZERO) <= 0) {
                continue;
            }
            PriceDTO dto = new PriceDTO();
            dto.setSku(sku);
            dto.setPrice(result.getPriceCentavos());
            dto.setListPrice(result.getListPriceCentavos() != null
                    ? result.getListPriceCentavos()
                    : result.getPriceCentavos());
            BigDecimal tableId = resolvePriceTableId(nuTab);
            if (tableId == null) continue; // Skip tabelas sem mapeamento
            dto.setPriceTableId(tableId);
            FastchannelPriceClient client = clientFor(channel);
            client.updatePrice(dto);
            log.fine("Preco sincronizado: codProd=" + codProd + " sku=" + sku + " nuTab=" + nuTab + " channel=" + channel);

            // Sync batch/scaled pricing (precos escalonados por quantidade)
            try {
                PriceBatchResolver batchResolver = new PriceBatchResolver();
                List<PriceBatchItemDTO> batches = batchResolver.resolve(codProd, nuTab, tableId);
                if (!batches.isEmpty()) {
                    client.updatePriceBatches(sku, tableId, batches);
                    log.fine("Batches sincronizados: codProd=" + codProd + " sku=" + sku + " batches=" + batches.size());
                }
            } catch (Exception batchEx) {
                log.log(Level.WARNING, "Erro ao sincronizar batches para SKU " + sku + ": " + batchEx.getMessage(), batchEx);
            }
        }
    }

    public void syncPriceBatch(List<BigDecimal> codProds) throws Exception {
        if (codProds == null || codProds.isEmpty()) {
            return;
        }

        List<PriceDTO> dist = new ArrayList<>();
        List<PriceDTO> cons = new ArrayList<>();
        List<BigDecimal> tables = priceTableResolver.resolveEligibleTables();
        if (tables.isEmpty()) {
            tables = Collections.singletonList(BigDecimal.ZERO);
        }

        for (BigDecimal codProd : codProds) {
            if (codProd == null) continue;
            String sku = deparaService.getSkuForStock(codProd);
            if (sku == null || sku.trim().isEmpty()) continue;
            FastchannelPriceClient.Channel channel = determineChannel(codProd, sku);
            for (BigDecimal nuTab : tables) {
                PriceResolver.PriceResult result = priceResolver.resolve(codProd, nuTab);
                if (result == null || result.getPriceCentavos() == null
                        || result.getPriceCentavos().compareTo(BigDecimal.ZERO) <= 0) continue;
                PriceDTO dto = new PriceDTO();
                dto.setSku(sku);
                dto.setPrice(result.getPriceCentavos());
                dto.setListPrice(result.getListPriceCentavos() != null
                        ? result.getListPriceCentavos()
                        : result.getPriceCentavos());
                BigDecimal tableId = resolvePriceTableId(nuTab);
                if (tableId == null) continue; // Skip produtos sem tabela mapeada
                dto.setPriceTableId(tableId);
                if (channel == FastchannelPriceClient.Channel.DISTRIBUTION) {
                    dist.add(dto);
                } else {
                    cons.add(dto);
                }
            }
        }

        // Batch endpoint /prices/{resellerId}/batches requer RESELLER_ID configurado.
        // Como o endpoint correto e por SKU (/prices/{sku}), iterar individualmente.
        for (PriceDTO dto : dist) {
            try {
                distributionClient.updatePrice(dto);
            } catch (Exception e) {
                log.warning("Falha ao sincronizar preco dist SKU=" + dto.getSku() + ": " + e.getMessage());
            }
        }
        for (PriceDTO dto : cons) {
            try {
                consumptionClient.updatePrice(dto);
            } catch (Exception e) {
                log.warning("Falha ao sincronizar preco cons SKU=" + dto.getSku() + ": " + e.getMessage());
            }
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

