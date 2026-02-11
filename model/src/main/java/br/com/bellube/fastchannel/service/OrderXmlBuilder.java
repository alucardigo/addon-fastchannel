package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.dto.OrderItemDTO;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
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
        xml.append("<serviceRequest serviceName=\"CACSP.incluirNota\">\n"); // R maiusculo!
        xml.append("  <requestBody>\n");
        xml.append("    <nota>\n");

        // Cabecalho
        appendCabecalho(xml, order, codParc, codTipVenda, codVend, codNat, codCenCus);

        // Itens
        appendItens(xml, order);

        xml.append("    </nota>\n");
        xml.append("  </requestBody>\n");
        xml.append("</serviceRequest>");

        return xml.toString();
    }

    private void appendCabecalho(StringBuilder xml, OrderDTO order, BigDecimal codParc,
                                  BigDecimal codTipVenda, BigDecimal codVend,
                                  BigDecimal codNat, BigDecimal codCenCus) {

        BigDecimal codEmp = order.getCodEmp();
        BigDecimal codTipOper = order.getCodTipOper();
        BigDecimal codLocal = config.getCodLocal();

        String dtNeg = new SimpleDateFormat("dd/MM/yyyy").format(new Date());

        xml.append("      <cabecalho>\n");
        if (codEmp == null || codTipOper == null) {
            throw new IllegalStateException("CODEMP/CODTIPOPER nao resolvidos via de-para");
        }
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
            xml.append("        <VLRNOTA>").append(normalizeMoney(order.getTotal())).append("</VLRNOTA>\n");
        }
        if (order.getDiscount() != null && order.getDiscount().compareTo(BigDecimal.ZERO) > 0) {
            xml.append("        <VLRDESC>").append(normalizeMoney(order.getDiscount())).append("</VLRDESC>\n");
        }
        BigDecimal frete = order.getShippingCost();
        if (frete != null && frete.compareTo(BigDecimal.ZERO) > 0) {
            xml.append("        <VLRFRETE>").append(normalizeMoney(frete)).append("</VLRFRETE>\n");
        }

        // Campos customizados
        xml.append("        <AD_NUMFAST>").append(xmlEscape(order.getOrderId())).append("</AD_NUMFAST>\n");
        if (order.getOrderId() != null) {
            xml.append("        <AD_FASTCHANNEL_ID>").append(xmlEscape(order.getOrderId())).append("</AD_FASTCHANNEL_ID>\n");
        }
        xml.append("        <AD_MCAPORTAL>P</AD_MCAPORTAL>\n");
        xml.append("        <CIF_FOB>C</CIF_FOB>\n");

        // Observacao
        String obs = buildObservacao(order);
        if (obs != null && !obs.isEmpty()) {
            xml.append("        <OBSERVACAO>").append(xmlEscape(obs)).append("</OBSERVACAO>\n");
        }

        xml.append("      </cabecalho>\n");
    }

    private void appendItens(StringBuilder xml, OrderDTO order) throws Exception {
        xml.append("      <itens INFORMARPRECO=\"True\">\n"); // Adicionar atributo INFORMARPRECO!

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
            String origProd = getOrigProd(codProd);

            xml.append("        <item>\n");
            xml.append("          <SEQUENCIA>").append(sequencia).append("</SEQUENCIA>\n");
            xml.append("          <CODPROD>").append(codProd).append("</CODPROD>\n");
            xml.append("          <CODVOL>").append(xmlEscape(codVol)).append("</CODVOL>\n");
            xml.append("          <QTDNEG>").append(item.getQuantity()).append("</QTDNEG>\n");
            if (item.getUnitPrice() != null) {
                xml.append("          <VLRUNIT>").append(normalizeMoney(item.getUnitPrice())).append("</VLRUNIT>\n");
            }

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

            // Origem do produto (Sankhya)
            if (origProd == null || origProd.isEmpty()) {
                origProd = "F";
            }
            xml.append("          <ORIGPROD>").append(xmlEscape(origProd)).append("</ORIGPROD>\n");
            xml.append("          <ORIGPRODPAD>").append(xmlEscape(origProd)).append("</ORIGPRODPAD>\n");

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
        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT CODVOL FROM TGFPRO WHERE CODPROD = :codProd");
            sql.setNamedParameter("codProd", codProd);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getString("CODVOL");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar CODVOL", e);
        } finally {
            closeQuietly(rs);
        }
        return "UN";
    }

    private String getOrigProd(BigDecimal codProd) {
        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT ORIGPROD FROM TGFPRO WHERE CODPROD = :codProd");
            sql.setNamedParameter("codProd", codProd);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getString("ORIGPROD");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar ORIGPROD", e);
        } finally {
            closeQuietly(rs);
        }
        return null;
    }

    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (Exception ignored) {}
        }
    }

    private String xmlEscape(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace("\"", "&quot;")
                   .replace("'", "&apos;");
    }

    private BigDecimal normalizeMoney(BigDecimal value) {
        if (value == null) return null;
        if (value.scale() <= 0) {
            return value.movePointLeft(2);
        }
        return value;
    }
}


