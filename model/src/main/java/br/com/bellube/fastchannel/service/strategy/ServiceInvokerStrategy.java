package br.com.bellube.fastchannel.service.strategy;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.service.OrderXmlBuilder;
import br.com.bellube.fastchannel.service.nativeapi.SankhyaNativeServiceCaller;

import java.math.BigDecimal;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Estrategia FALLBACK 1: usa invocacao nativa de servicos Sankhya
 * (ServiceInvoker legado ou modelcore ServiceCaller oficial).
 */
public class ServiceInvokerStrategy implements OrderCreationStrategy {

    private static final Logger log = Logger.getLogger(ServiceInvokerStrategy.class.getName());
    private static final String SERVICE_NAME = "CACSP.incluirNota";

    private final OrderXmlBuilder xmlBuilder;
    private final SankhyaNativeServiceCaller nativeCaller;
    private final FastchannelConfig config;

    public ServiceInvokerStrategy() {
        this.xmlBuilder = new OrderXmlBuilder();
        this.nativeCaller = new SankhyaNativeServiceCaller();
        this.config = FastchannelConfig.getInstance();
    }

    @Override
    public String getStrategyName() {
        return "ServiceInvoker";
    }

    @Override
    public boolean isAvailable() {
        return nativeCaller.isAvailable();
    }

    @Override
    public BigDecimal createOrder(OrderDTO order, BigDecimal codParc,
                                  BigDecimal codTipVenda, BigDecimal codVend,
                                  BigDecimal codNat, BigDecimal codCenCus) throws Exception {

        log.info("[ServiceInvoker] Criando pedido " + order.getOrderId() + " via bridge nativo Sankhya");

        try {
            String requestXml = xmlBuilder.buildIncluirNotaXml(order, codParc, codTipVenda, codVend, codNat, codCenCus);
            String responseXml = nativeCaller.invoke(SERVICE_NAME, requestXml, config.getSankhyaUser(), config.getSankhyaPassword());

            if (responseXml == null || responseXml.trim().isEmpty()) {
                throw new Exception("Resposta vazia ao invocar " + SERVICE_NAME);
            }

            BigDecimal nuNota = parseNuNotaFromResponse(responseXml);
            if (nuNota == null) {
                throw new Exception("NUNOTA nao encontrado na resposta do servico");
            }

            log.info("[ServiceInvoker] Pedido " + order.getOrderId() + " criado como NUNOTA " + nuNota);
            return nuNota;

        } catch (Exception e) {
            log.log(Level.SEVERE, "[ServiceInvoker] Erro ao criar pedido", e);
            throw new Exception("Falha no ServiceInvoker: " + e.getMessage(), e);
        }
    }

    private BigDecimal parseNuNotaFromResponse(String responseXml) {
        try {
            java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("<NUNOTA>(\\d+)</NUNOTA>");
            java.util.regex.Matcher matcher = pattern.matcher(responseXml);
            if (matcher.find()) {
                return new BigDecimal(matcher.group(1));
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao parsear NUNOTA", e);
        }
        return null;
    }
}

