package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.dto.PriceDTO;
import br.com.bellube.fastchannel.http.FastchannelPriceClient;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
            if (result == null || result.getPriceCentavos() == null) {
                continue;
            }
            PriceDTO dto = new PriceDTO();
            dto.setSku(sku);
            dto.setPrice(result.getPriceCentavos());
            dto.setListPrice(result.getListPriceCentavos() != null
                    ? result.getListPriceCentavos()
                    : result.getPriceCentavos());
            dto.setPriceTableId(resolvePriceTableId(nuTab));
            clientFor(channel).updatePrice(dto);
            log.fine("Preco sincronizado: codProd=" + codProd + " sku=" + sku + " nuTab=" + nuTab + " channel=" + channel);
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
                if (result == null || result.getPriceCentavos() == null) continue;
                PriceDTO dto = new PriceDTO();
                dto.setSku(sku);
                dto.setPrice(result.getPriceCentavos());
                dto.setListPrice(result.getListPriceCentavos() != null
                        ? result.getListPriceCentavos()
                        : result.getPriceCentavos());
                dto.setPriceTableId(resolvePriceTableId(nuTab));
                if (channel == FastchannelPriceClient.Channel.DISTRIBUTION) {
                    dist.add(dto);
                } else {
                    cons.add(dto);
                }
            }
        }

        if (!dist.isEmpty()) {
            distributionClient.updatePricesBatch(null, dist);
        }
        if (!cons.isEmpty()) {
            consumptionClient.updatePricesBatch(null, cons);
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
        String mapped = deparaService.getCodigoExterno(DeparaService.TIPO_TABELA_PRECO, nuTab);
        if (mapped == null || mapped.trim().isEmpty()) {
            return null;
        }
        try {
            return new BigDecimal(mapped.trim());
        } catch (NumberFormatException e) {
            return null;
        }
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

