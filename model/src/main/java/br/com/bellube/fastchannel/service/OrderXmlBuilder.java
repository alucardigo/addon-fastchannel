package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.dto.OrderItemDTO;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;

/**
 * Constroi XML de pedido para o servico CACSP.incluirNota.
 */
public class OrderXmlBuilder {

    private static final Logger log = Logger.getLogger(OrderXmlBuilder.class.getName());

    private final FastchannelConfig config;
    private final DeparaService deparaService;

    public OrderXmlBuilder() {
        this.config = FastchannelConfig.getInstance();
        this.deparaService = DeparaService.getInstance();
    }

    /**
     * Constroi XML de pedido para CACSP.incluirNota.
     *
     * @param order Pedido do Fastchannel
     * @param codParc Codigo do parceiro
     * @param codTipVenda Codigo do tipo de venda
     * @param codVend Codigo do vendedor
     * @param codNat Codigo da natureza de operacao
     * @param codCenCus Codigo do centro de custo
     * @return XML formatado
     */
    public String buildIncluirNotaXml(OrderDTO order, BigDecimal codParc,
                                      BigDecimal codTipVenda, BigDecimal codVend,
                                      BigDecimal codNat, BigDecimal codCenCus) throws Exception {

        StringBuilder xml = new StringBuilder();
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.append("<servicerequest serviceName=\"CACSP.incluirNota\">\n");
        xml.append("  <requestBody>\n");
        xml.append("    <nota>\n");

        // Cabecalho
        appendCabecalho(xml, order, codParc, codTipVenda, codVend, codNat, codCenCus);

        // Itens
        appendItens(xml, order);

        xml.append("    </nota>\n");
        xml.append("  </requestBody>\n");
        xml.append("</servicerequest>");

        return xml.toString();
    }

    private void appendCabecalho(StringBuilder xml, OrderDTO order, BigDecimal codParc,
                                  BigDecimal codTipVenda, BigDecimal codVend,
                                  BigDecimal codNat, BigDecimal codCenCus) {

        BigDecimal codEmp = config.getCodemp();
        BigDecimal codTipOper = config.getCodTipOper();
        BigDecimal codLocal = config.getCodLocal();

        String dtNeg = new SimpleDateFormat("dd/MM/yyyy").format(new Date());

        xml.append("      <cabecalho>\n");
        xml.append("        <CODEMP>").append(codEmp).append("</CODEMP>\n");
        xml.append("        <CODPARC>").append(codParc).append("</CODPARC>\n");
        xml.append("        <CODTIPOPER>").append(codTipOper).append("</CODTIPOPER>\n");
        xml.append("        <DTNEG>").append(dtNeg).append("</DTNEG>\n");
        xml.append("        <TIPMOV>P</TIPMOV>\n");

        if (codTipVenda != null) {
            xml.append("        <CODTIPVENDA>").append(codTipVenda).append("</CODTIPVENDA>\n");
        }
        if (codVend != null) {
            xml.append("        <CODVEND>").append(codVend).append("</CODVEND>\n");
        }
        if (codNat != null) {
            xml.append("        <CODNAT>").append(codNat).append("</CODNAT>\n");
        }
        if (codCenCus != null) {
            xml.append("        <CODCENCUS>").append(codCenCus).append("</CODCENCUS>\n");
        }
        if (codLocal != null) {
            xml.append("        <CODLOCAL>").append(codLocal).append("</CODLOCAL>\n");
        }

        // Valores
        if (order.getTotal() != null) {
            xml.append("        <VLRNOTA>").append(order.getTotal()).append("</VLRNOTA>\n");
        }
        if (order.getDiscount() != null && order.getDiscount().compareTo(BigDecimal.ZERO) > 0) {
            xml.append("        <VLRDESC>").append(order.getDiscount()).append("</VLRDESC>\n");
        }
        if (order.getShippingCost() != null && order.getShippingCost().compareTo(BigDecimal.ZERO) > 0) {
            xml.append("        <VLRFRETE>").append(order.getShippingCost()).append("</VLRFRETE>\n");
        }

        // Campos customizados
        xml.append("        <AD_NUMFAST>").append(xmlEscape(order.getOrderId())).append("</AD_NUMFAST>\n");

        // Observacao
        String obs = buildObservacao(order);
        if (obs != null && !obs.isEmpty()) {
            xml.append("        <OBSERVACAO>").append(xmlEscape(obs)).append("</OBSERVACAO>\n");
        }

        xml.append("      </cabecalho>\n");
    }

    private void appendItens(StringBuilder xml, OrderDTO order) throws Exception {
        xml.append("      <itens>\n");

        int sequencia = 1;
        for (OrderItemDTO item : order.getItems()) {
            // Buscar CODPROD pelo SKU - CRITICAL: nao criar produto se nao existir
            BigDecimal codProd = deparaService.getCodProdBySkuOrEan(item.getSku());
            if (codProd == null) {
                throw new Exception("Produto nao encontrado para SKU: " + item.getSku() +
                                    ". Pedido " + order.getOrderId() + " nao pode ser importado.");
            }

            // Buscar CODVOL do produto
            String codVol = getVolumePadrao(codProd);

            xml.append("        <item>\n");
            xml.append("          <SEQUENCIA>").append(sequencia).append("</SEQUENCIA>\n");
            xml.append("          <CODPROD>").append(codProd).append("</CODPROD>\n");
            xml.append("          <CODVOL>").append(xmlEscape(codVol)).append("</CODVOL>\n");
            xml.append("          <QTDNEG>").append(item.getQuantity()).append("</QTDNEG>\n");
            xml.append("          <VLRUNIT>").append(item.getUnitPrice()).append("</VLRUNIT>\n");

            if (item.getDiscount() != null && item.getDiscount().compareTo(BigDecimal.ZERO) > 0) {
                // Calcular percentual de desconto
                BigDecimal totalSemDesc = item.getUnitPrice().multiply(item.getQuantity());
                if (totalSemDesc.compareTo(BigDecimal.ZERO) > 0) {
                    BigDecimal percDesc = item.getDiscount()
                            .divide(totalSemDesc, 4, BigDecimal.ROUND_HALF_UP)
                            .multiply(new BigDecimal("100"));
                    xml.append("          <PERCDESC>").append(percDesc).append("</PERCDESC>\n");
                }
            }

            // Origem do produto (campo customizado)
            xml.append("          <ORIGPROD>F</ORIGPROD>\n"); // F = Fastchannel

            xml.append("        </item>\n");
            sequencia++;
        }

        xml.append("      </itens>\n");
    }

    private String buildObservacao(OrderDTO order) {
        StringBuilder obs = new StringBuilder();
        obs.append("Pedido Fastchannel: ").append(order.getOrderId());

        if (order.getNotes() != null && !order.getNotes().isEmpty()) {
            obs.append(" | ").append(order.getNotes());
        }

        if (order.getShippingMethod() != null) {
            obs.append(" | Frete: ").append(order.getShippingMethod());
        }

        String result = obs.toString();
        return result.length() > 500 ? result.substring(0, 500) : result;
    }

    private String getVolumePadrao(BigDecimal codProd) {
        // Implementacao simplificada - retorna UN como padrao
        // TODO: Buscar CODVOL real do produto se necessario
        return "UN";
    }

    private String xmlEscape(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace("\"", "&quot;")
                   .replace("'", "&apos;");
    }
}
