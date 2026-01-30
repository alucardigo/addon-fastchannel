package br.com.bellube.fastchannel.service.strategy;

import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.service.OrderXmlBuilder;
import br.com.sankhya.extensions.actionbutton.utils.ServiceInvoker;

import java.math.BigDecimal;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Estrategia FALLBACK 1: Usa ServiceInvoker do Sankhya.
 * Funciona bem em contextos com sessao ativa (eventos, acoes).
 */
public class ServiceInvokerStrategy implements OrderCreationStrategy {

    private static final Logger log = Logger.getLogger(ServiceInvokerStrategy.class.getName());

    private final OrderXmlBuilder xmlBuilder;

    public ServiceInvokerStrategy() {
        this.xmlBuilder = new OrderXmlBuilder();
    }

    @Override
    public String getStrategyName() {
        return "ServiceInvoker";
    }

    @Override
    public boolean isAvailable() {
        try {
            // Verificar se ServiceInvoker esta disponivel
            Class.forName("br.com.sankhya.extensions.actionbutton.utils.ServiceInvoker");
            return true;
        } catch (ClassNotFoundException e) {
            log.log(Level.WARNING, "ServiceInvoker nao disponivel", e);
            return false;
        }
    }

    @Override
    public BigDecimal createOrder(OrderDTO order, BigDecimal codParc,
                                  BigDecimal codTipVenda, BigDecimal codVend,
                                  BigDecimal codNat, BigDecimal codCenCus) throws Exception {

        log.info("[ServiceInvoker] Criando pedido " + order.getOrderId() + " usando ServiceInvoker");

        try {
            // Construir XML
            String requestXml = xmlBuilder.buildIncluirNotaXml(order, codParc, codTipVenda, codVend, codNat, codCenCus);

            log.fine("[ServiceInvoker] XML: " + requestXml);

            // Invocar servico
            ServiceInvoker invoker = new ServiceInvoker("CACSP.incluirNota", requestXml);
            String responseXml = invoker.invoke();

            log.fine("[ServiceInvoker] Resposta: " + responseXml);

            if (responseXml == null || responseXml.trim().isEmpty()) {
                throw new Exception("Resposta vazia do ServiceInvoker");
            }

            // Extrair NUNOTA
            BigDecimal nuNota = parseNuNotaFromResponse(responseXml);
            if (nuNota == null) {
                throw new Exception("NUNOTA nao encontrado na resposta");
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
            // Formato: <NUNOTA>12345</NUNOTA>
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
