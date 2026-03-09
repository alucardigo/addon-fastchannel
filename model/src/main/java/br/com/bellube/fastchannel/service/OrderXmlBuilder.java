package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.dto.OrderItemDTO;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Constroi XML de pedido para o servico CACSP.incluirNota.
 */
public class OrderXmlBuilder {

    private static final Logger log = Logger.getLogger(OrderXmlBuilder.class.getName());
    private static final Map<String, Boolean> CAB_FIELD_SUPPORT = new ConcurrentHashMap<>();
    private static final Map<String, Boolean> ITEM_FIELD_SUPPORT = new ConcurrentHashMap<>();
    private static final Map<String, Boolean> EXC_FIELD_SUPPORT = new ConcurrentHashMap<>();

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
        xml.append("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n");
        xml.append("<serviceRequest serviceName=\"CACSP.incluirNota\">\n"); // R maiusculo!
        xml.append("  <requestBody>\n");
        xml.append("    <nota>\n");

        // Cabecalho
        appendCabecalho(xml, order, codParc, codTipVenda, codVend, codNat, codCenCus);

        // Itens
        appendItens(xml, order, codVend, codTipVenda);

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
        if (codTipOper == null) {
            codTipOper = FastchannelConstants.DEFAULT_TOP_PEDIDO;
        }
        if (codVend == null || codVend.compareTo(BigDecimal.ZERO) <= 0) {
            codVend = resolveCodVendByParc(codParc);
        }
        if (codVend == null || codVend.compareTo(BigDecimal.ZERO) <= 0) {
            codVend = FastchannelConstants.DEFAULT_CODVEND_PADRAO;
        }
        BigDecimal codLocal = order.getCodLocal();
        if (codLocal == null) {
            codLocal = config.getCodLocal();
        }

        String dtNeg = new SimpleDateFormat("dd/MM/yyyy").format(new Date());

        xml.append("      <cabecalho>\n");
        if (codEmp == null || codTipOper == null) {
            throw new IllegalStateException("CODEMP/CODTIPOPER nao resolvidos via de-para");
        }
        xml.append("        <NUNOTA/>\n");
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
            if (supportsCabField("AD_CODVENDEXEC")) {
                xml.append("        <AD_CODVENDEXEC>281</AD_CODVENDEXEC>\n");
            }
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
        if (order.getDiscount() != null && order.getDiscount().compareTo(BigDecimal.ZERO) > 0 && supportsCabField("VLRDESC")) {
            xml.append("        <VLRDESC>").append(normalizeMoney(order.getDiscount())).append("</VLRDESC>\n");
        }
        // Sempre enviar VLRFRETE (mesmo quando zero) - compatibilidade com legado
        BigDecimal frete = order.getShippingCost() != null ? order.getShippingCost() : BigDecimal.ZERO;
        xml.append("        <VLRFRETE>").append(normalizeMoney(frete)).append("</VLRFRETE>\n");

        // Campos customizados
        if (supportsCabField("AD_NUMFAST")) {
            xml.append("        <AD_NUMFAST>").append(xmlEscape(order.getOrderId())).append("</AD_NUMFAST>\n");
        }
        if (order.getOrderId() != null && supportsCabField("AD_FASTCHANNEL_ID")) {
            xml.append("        <AD_FASTCHANNEL_ID>").append(xmlEscape(order.getOrderId())).append("</AD_FASTCHANNEL_ID>\n");
        }
        if (supportsCabField("AD_MCAPORTAL")) {
            xml.append("        <AD_MCAPORTAL>P</AD_MCAPORTAL>\n");
        }
        xml.append("        <CIF_FOB>C</CIF_FOB>\n");

        // Observacao publica sem numero da Fast
        String obs = buildObservacao(order);
        if (obs != null && !obs.isEmpty()) {
            xml.append("        <OBSERVACAO>").append(xmlEscape(obs)).append("</OBSERVACAO>\n");
        }
        String obsInterna = buildObservacaoInterna(order);
        if (obsInterna != null && !obsInterna.isEmpty() && supportsCabField("OBSERVACAOINTERNA")) {
            xml.append("        <OBSERVACAOINTERNA>").append(xmlEscape(obsInterna)).append("</OBSERVACAOINTERNA>\n");
        }

        xml.append("      </cabecalho>\n");
    }

    private void appendItens(StringBuilder xml, OrderDTO order, BigDecimal codVend, BigDecimal codTipVenda) throws Exception {
        xml.append("      <itens INFORMARPRECO=\"True\">\n"); // Adicionar atributo INFORMARPRECO!

        int sequencia = 1;
        for (OrderItemDTO item : order.getItems()) {
            // Buscar CODPROD pelo SKU - CRITICAL: nao criar produto se nao existir
            BigDecimal codProd = deparaService.resolveCodProdForOrderItem(item);
            if (codProd == null) {
                throw new Exception("Produto nao encontrado para SKU: " + item.getSku() +
                                    ". Pedido " + order.getOrderId() + " nao pode ser importado.");
            }

            // Buscar CODVOL do produto
            String codVol = resolveCodVol(item, codProd);
            String origProd = getOrigProd(codProd);
            ItemPricingData pricingData = resolveItemPricingData(codProd, order.getCodEmp(), codVend, codTipVenda);
            BigDecimal itemNuTab = resolveItemNuTab(pricingData);
            BigDecimal quantity = sanitizeQuantity(item, order);
            BigDecimal unitPrice = sanitizeUnitPrice(item, quantity, order);

            xml.append("        <item>\n");
            xml.append("          <NUNOTA/>\n");
            xml.append("          <SEQUENCIA>").append(sequencia).append("</SEQUENCIA>\n");
            xml.append("          <CODPROD>").append(codProd).append("</CODPROD>\n");
            xml.append("          <CODVOL>").append(xmlEscape(codVol)).append("</CODVOL>\n");
            xml.append("          <QTDNEG>").append(quantity).append("</QTDNEG>\n");
            xml.append("          <VLRUNIT>").append(normalizeMoney(unitPrice)).append("</VLRUNIT>\n");
            if (itemNuTab != null && supportsItemField("NUTAB")) {
                xml.append("          <NUTAB>").append(itemNuTab).append("</NUTAB>\n");
            }
            if (pricingData != null) {
                if (pricingData.precoBase != null && supportsItemField("PRECOBASE")) {
                    xml.append("          <PRECOBASE>").append(normalizeMoney(pricingData.precoBase)).append("</PRECOBASE>\n");
                }
                if (pricingData.custo != null && supportsItemField("CUSTO")) {
                    xml.append("          <CUSTO>").append(normalizeMoney(pricingData.custo)).append("</CUSTO>\n");
                }
                if (pricingData.custo != null && supportsItemField("VLRCUS")) {
                    xml.append("          <VLRCUS>").append(normalizeMoney(pricingData.custo)).append("</VLRCUS>\n");
                }
                if (pricingData.usoProd != null && supportsItemField("USOPROD")) {
                    xml.append("          <USOPROD>").append(xmlEscape(pricingData.usoProd)).append("</USOPROD>\n");
                }
            }
            if (supportsItemField("ATUALESTTERC")) {
                xml.append("          <ATUALESTTERC>N</ATUALESTTERC>\n");
            }
            if (supportsItemField("ATUALESTOQUE")) {
                xml.append("          <ATUALESTOQUE>1</ATUALESTOQUE>\n");
            }
            if (supportsItemField("RESERVA")) {
                xml.append("          <RESERVA>S</RESERVA>\n");
            }
            if (supportsItemField("TERCEIRO")) {
                xml.append("          <TERCEIRO>N</TERCEIRO>\n");
            } else if (supportsItemField("TERCEIROS")) {
                xml.append("          <TERCEIROS>N</TERCEIROS>\n");
            }
            if (!isBlank(item.getGradeControlId())) {
                xml.append("          <CONTROLE>").append(xmlEscape(item.getGradeControlId().trim())).append("</CONTROLE>\n");
            }

            BigDecimal percDesc = BigDecimal.ZERO;
            if (item.getDiscount() != null && item.getDiscount().compareTo(BigDecimal.ZERO) > 0) {
                BigDecimal totalSemDesc = unitPrice.multiply(quantity);
                if (totalSemDesc.compareTo(BigDecimal.ZERO) > 0) {
                    percDesc = item.getDiscount()
                            .divide(totalSemDesc, 4, BigDecimal.ROUND_HALF_UP)
                            .multiply(new BigDecimal("100"));
                    if (percDesc.compareTo(BigDecimal.ZERO) < 0 || percDesc.compareTo(new BigDecimal("100")) >= 0) {
                        percDesc = BigDecimal.ZERO;
                    }
                }
            }
            if (supportsItemField("PERCDESC")) {
                xml.append("          <PERCDESC>").append(percDesc).append("</PERCDESC>\n");
            }

            // Origem do produto (Sankhya)
            if (origProd == null || origProd.isEmpty()) {
                origProd = "0";
            }
            xml.append("          <ORIGPROD>").append(xmlEscape(origProd)).append("</ORIGPROD>\n");
            xml.append("          <ORIGPRODPAD>").append(xmlEscape(origProd)).append("</ORIGPRODPAD>\n");

            xml.append("        </item>\n");
            sequencia++;
        }

        xml.append("      </itens>\n");
    }

    private BigDecimal resolveItemNuTab(ItemPricingData pricingData) {
        if (pricingData != null && pricingData.nuTab != null) {
            return pricingData.nuTab;
        }
        if (config.getNuTab() != null) {
            return config.getNuTab();
        }
        return null;
    }

    private String resolveCodVol(OrderItemDTO item, BigDecimal codProd) {
        if (item != null && !isBlank(item.getVolumeId())) {
            return item.getVolumeId().trim();
        }
        return getVolumePadrao(codProd);
    }

    private BigDecimal sanitizeQuantity(OrderItemDTO item, OrderDTO order) throws Exception {
        BigDecimal quantity = item != null ? item.getQuantity() : null;
        if (quantity == null || quantity.compareTo(BigDecimal.ZERO) <= 0) {
            throw new Exception("Quantidade invalida para SKU " + (item != null ? item.getSku() : null)
                    + " no pedido " + (order != null ? order.getOrderId() : null));
        }
        return quantity;
    }

    private BigDecimal sanitizeUnitPrice(OrderItemDTO item, BigDecimal quantity, OrderDTO order) {
        BigDecimal unitPrice = item != null ? item.getUnitPrice() : null;
        if (unitPrice != null && unitPrice.compareTo(BigDecimal.ZERO) > 0) {
            return unitPrice;
        }

        BigDecimal totalPrice = item != null ? item.getTotalPrice() : null;
        if (totalPrice != null && totalPrice.compareTo(BigDecimal.ZERO) > 0
                && quantity != null && quantity.compareTo(BigDecimal.ZERO) > 0) {
            BigDecimal calculated = totalPrice.divide(quantity, 6, BigDecimal.ROUND_HALF_UP);
            if (calculated.compareTo(BigDecimal.ZERO) > 0) {
                log.warning("[OrderXmlBuilder] VLRUNIT ajustado por TotalProductCost para SKU "
                        + item.getSku() + " no pedido " + order.getOrderId() + ": " + calculated);
                return calculated;
            }
        }

        log.warning("[OrderXmlBuilder] VLRUNIT ausente/zerado para SKU "
                + (item != null ? item.getSku() : null)
                + " no pedido " + (order != null ? order.getOrderId() : null)
                + ". Aplicando fallback 0.000001 para evitar rejeicao do CACSP.");
        return new BigDecimal("0.000001");
    }

    private String buildObservacao(OrderDTO order) {
        StringBuilder obs = new StringBuilder();
        if (order.getNotes() != null && !order.getNotes().isEmpty()) {
            obs.append(order.getNotes());
        }

        if (order.getShippingMethod() != null) {
            if (obs.length() > 0) {
                obs.append(" | ");
            }
            obs.append("Frete: ").append(order.getShippingMethod());
        }

        String result = obs.toString();
        return result.length() > 500 ? result.substring(0, 500) : result;
    }

    private String buildObservacaoInterna(OrderDTO order) {
        StringBuilder obs = new StringBuilder();
        obs.append("Pedido Fastchannel: ").append(order.getOrderId());
        if (order.getSellerNotes() != null && !order.getSellerNotes().trim().isEmpty()) {
            obs.append(" | ").append(order.getSellerNotes().trim());
        }
        String result = obs.toString();
        return result.length() > 1000 ? result.substring(0, 1000) : result;
    }

    private String getVolumePadrao(BigDecimal codProd) {
        if (codProd == null) {
            return "UN";
        }
        try {
            JapeWrapper produtoDAO = JapeFactory.dao("Produto");
            DynamicVO produtoVO = produtoDAO.findByPK(codProd);
            if (produtoVO != null) {
                String codVol = trimToNull(produtoVO.asString("CODVOL"));
                if (codVol != null) {
                    return codVol;
                }
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar CODVOL", e);
        }
        return "UN";
    }

    private String getOrigProd(BigDecimal codProd) {
        if (codProd == null) {
            return null;
        }
        try {
            JapeWrapper produtoDAO = JapeFactory.dao("Produto");
            DynamicVO produtoVO = produtoDAO.findByPK(codProd);
            if (produtoVO != null) {
                return trimToNull(produtoVO.asString("ORIGPROD"));
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar ORIGPROD", e);
        }
        return null;
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
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

    private boolean supportsCabField(String field) {
        if (field == null || field.trim().isEmpty()) return false;
        String normalizedField = field.trim().toUpperCase();
        Boolean cached = CAB_FIELD_SUPPORT.get(normalizedField);
        if (cached != null) return cached;

        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT COUNT(*) AS CNT ");
            sql.appendSql("FROM INFORMATION_SCHEMA.COLUMNS ");
            sql.appendSql("WHERE TABLE_NAME = 'TGFCAB' AND COLUMN_NAME = :columnName");
            sql.setNamedParameter("columnName", normalizedField);
            rs = sql.executeQuery();
            boolean supported = rs.next();
            if (supported) {
                supported = rs.getInt("CNT") > 0;
            }
            CAB_FIELD_SUPPORT.put(normalizedField, supported);
            return supported;
        } catch (Exception e) {
            log.log(Level.WARNING, "Nao foi possivel validar campo TGFCAB." + normalizedField, e);
            CAB_FIELD_SUPPORT.put(normalizedField, false);
            return false;
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
    }

    private boolean supportsItemField(String field) {
        if (field == null || field.trim().isEmpty()) return false;
        String normalizedField = field.trim().toUpperCase();
        Boolean cached = ITEM_FIELD_SUPPORT.get(normalizedField);
        if (cached != null) return cached;

        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT COUNT(*) AS CNT ");
            sql.appendSql("FROM INFORMATION_SCHEMA.COLUMNS ");
            sql.appendSql("WHERE TABLE_NAME = 'TGFITE' AND COLUMN_NAME = :columnName");
            sql.setNamedParameter("columnName", normalizedField);
            rs = sql.executeQuery();
            boolean supported = rs.next() && rs.getInt("CNT") > 0;
            ITEM_FIELD_SUPPORT.put(normalizedField, supported);
            return supported;
        } catch (Exception e) {
            log.log(Level.WARNING, "Nao foi possivel validar campo TGFITE." + normalizedField, e);
            ITEM_FIELD_SUPPORT.put(normalizedField, false);
            return false;
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
    }

    private boolean supportsExcField(String field) {
        if (field == null || field.trim().isEmpty()) return false;
        String normalizedField = field.trim().toUpperCase();
        Boolean cached = EXC_FIELD_SUPPORT.get(normalizedField);
        if (cached != null) return cached;

        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT COUNT(*) AS CNT ");
            sql.appendSql("FROM INFORMATION_SCHEMA.COLUMNS ");
            sql.appendSql("WHERE TABLE_NAME = 'TGFEXC' AND COLUMN_NAME = :columnName");
            sql.setNamedParameter("columnName", normalizedField);
            rs = sql.executeQuery();
            boolean supported = rs.next() && rs.getInt("CNT") > 0;
            EXC_FIELD_SUPPORT.put(normalizedField, supported);
            return supported;
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel validar campo TGFEXC." + normalizedField, e);
            EXC_FIELD_SUPPORT.put(normalizedField, false);
            return false;
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
    }

    private String resolveExcOrderColumn() {
        return chooseExcOrderColumn(
                supportsExcField("DTALTER"),
                supportsExcField("DHALTER"),
                supportsExcField("DTVIGOR"));
    }

    static String chooseExcOrderColumn(boolean hasDtAlter, boolean hasDhAlter, boolean hasDtVigor) {
        if (hasDtAlter) return "DTALTER";
        if (hasDhAlter) return "DHALTER";
        if (hasDtVigor) return "DTVIGOR";
        return "NUTAB";
    }

    private ItemPricingData resolveItemPricingData(BigDecimal codProd, BigDecimal codEmp,
                                                   BigDecimal codVend, BigDecimal codTipVenda) {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            ItemPricingData data = new ItemPricingData();
            BigDecimal preferredNuTab = resolvePreferredNuTab(codVend, codTipVenda);
            if (isNullOrZero(preferredNuTab)) {
                preferredNuTab = normalizeNuTabToLatestActive(config.getNuTab());
            }

            if (supportsItemField("NUTAB") || supportsItemField("PRECOBASE")) {
                ItemPricingData nativeExc = resolveExcPricingNative(codProd, codEmp, preferredNuTab);
                if (nativeExc != null) {
                    data.nuTab = nativeExc.nuTab;
                    data.precoBase = nativeExc.precoBase;
                } else {
                    NativeSql sqlExc = new NativeSql(jdbc);
                    sqlExc.appendSql("SELECT TOP 1 NUTAB, VLRVENDA ");
                    sqlExc.appendSql("FROM TGFEXC ");
                    sqlExc.appendSql("WHERE CODPROD = :codProd ");
                    if (!isNullOrZero(preferredNuTab)) {
                        sqlExc.appendSql("AND NUTAB = :nuTab ");
                        sqlExc.setNamedParameter("nuTab", preferredNuTab);
                    }
                    if (!isNullOrZero(codEmp) && supportsExcField("CODEMP")) {
                        sqlExc.appendSql("AND CODEMP = :codEmp ");
                        sqlExc.setNamedParameter("codEmp", codEmp);
                    }
                    sqlExc.appendSql("ORDER BY " + resolveExcOrderColumn() + " DESC");
                    sqlExc.setNamedParameter("codProd", codProd);
                    rs = sqlExc.executeQuery();
                    if (rs.next()) {
                        data.nuTab = rs.getBigDecimal("NUTAB");
                        data.precoBase = rs.getBigDecimal("VLRVENDA");
                    }
                    closeQuietly(rs);
                }
            }

            try {
                JapeWrapper produtoDAO = JapeFactory.dao("Produto");
                DynamicVO produtoVO = produtoDAO.findByPK(codProd);
                if (produtoVO != null) {
                    data.usoProd = trimToNull(produtoVO.asString("USOPROD"));
                }
            } catch (Exception nativeErr) {
                NativeSql sqlProd = new NativeSql(jdbc);
                sqlProd.appendSql("SELECT USOPROD FROM TGFPRO WHERE CODPROD = :codProd");
                sqlProd.setNamedParameter("codProd", codProd);
                rs = sqlProd.executeQuery();
                if (rs.next()) {
                    data.usoProd = trimToNull(rs.getString("USOPROD"));
                }
            }
            data.custo = resolveCurrentCost(codProd, codEmp);
            return data;
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver dados de preco/custo de item para CODPROD " + codProd, e);
            return null;
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
    }

    private ItemPricingData resolveExcPricingNative(BigDecimal codProd, BigDecimal codEmp, BigDecimal preferredNuTab) {
        if (codProd == null) {
            return null;
        }
        String[] daoCandidates = {"ExcecaoTabelaPreco", "ExcecaoPreco", "TGFEXC"};
        for (String daoName : daoCandidates) {
            try {
                JapeWrapper excDAO = JapeFactory.dao(daoName);
                Collection<DynamicVO> rows = excDAO.find("this.CODPROD = ?", codProd);
                if (rows == null || rows.isEmpty()) {
                    continue;
                }
                DynamicVO selected = null;
                BigDecimal bestNutab = null;
                for (DynamicVO row : rows) {
                    if (row == null) {
                        continue;
                    }
                    BigDecimal nutab = safeAsBigDecimal(row, "NUTAB");
                    if (!isNullOrZero(preferredNuTab) && (nutab == null || nutab.compareTo(preferredNuTab) != 0)) {
                        continue;
                    }
                    if (!isNullOrZero(codEmp)) {
                        BigDecimal rowCodEmp = safeAsBigDecimal(row, "CODEMP");
                        if (rowCodEmp != null && rowCodEmp.compareTo(codEmp) != 0) {
                            continue;
                        }
                    }
                    if (selected == null || (nutab != null && (bestNutab == null || nutab.compareTo(bestNutab) > 0))) {
                        selected = row;
                        bestNutab = nutab;
                    }
                }
                if (selected != null) {
                    ItemPricingData result = new ItemPricingData();
                    result.nuTab = safeAsBigDecimal(selected, "NUTAB");
                    result.precoBase = safeAsBigDecimal(selected, "VLRVENDA");
                    return result;
                }
            } catch (Exception e) {
                log.log(Level.FINE, "Falha ao resolver TGFEXC via Jape (" + daoName + ")", e);
            }
        }
        return null;
    }

    private BigDecimal resolvePreferredNuTab(BigDecimal codVend, BigDecimal codTipVenda) {
        if (isNullOrZero(codVend)) {
            return null;
        }
        BigDecimal nativeNuTab = resolvePreferredNuTabNative(codVend, codTipVenda);
        if (!isNullOrZero(nativeNuTab)) {
            return nativeNuTab;
        }
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 T.NUTAB ");
            sql.appendSql("FROM TGFNPV N ");
            sql.appendSql("INNER JOIN TGFTAB T ON T.CODTAB = N.CODTAB ");
            sql.appendSql("WHERE N.CODVEND = :codVend ");
            if (!isNullOrZero(codTipVenda) && hasTableColumn("TGFNPV", "CODTIPVENDA")) {
                sql.appendSql("AND N.CODTIPVENDA = :codTipVenda ");
                sql.setNamedParameter("codTipVenda", codTipVenda);
            }
            if (hasTableColumn("TGFTAB", "DTVIGOR")) {
                sql.appendSql("AND (T.DTVIGOR IS NULL OR T.DTVIGOR <= GETDATE()) ");
            }
            if (hasTableColumn("TGFTAB", "INATIVO")) {
                sql.appendSql("AND (T.INATIVO IS NULL OR T.INATIVO = 'N') ");
            }
            sql.appendSql("ORDER BY T.DTVIGOR DESC, T.NUTAB DESC");
            sql.setNamedParameter("codVend", codVend);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("NUTAB");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver NUTAB por TGFNPV para CODVEND " + codVend, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal resolvePreferredNuTabNative(BigDecimal codVend, BigDecimal codTipVenda) {
        String[] daoCandidates = {"NegociacaoVendedor", "TGFNPV"};
        for (String daoName : daoCandidates) {
            try {
                JapeWrapper npvDAO = JapeFactory.dao(daoName);
                Collection<DynamicVO> rows = npvDAO.find("this.CODVEND = ?", codVend);
                if (rows == null || rows.isEmpty()) {
                    continue;
                }
                BigDecimal selectedNutab = null;
                Timestamp selectedDtVigor = null;
                for (DynamicVO row : rows) {
                    if (row == null) {
                        continue;
                    }
                    if (!isNullOrZero(codTipVenda)) {
                        BigDecimal rowTipVenda = safeAsBigDecimal(row, "CODTIPVENDA");
                        if (rowTipVenda != null && rowTipVenda.compareTo(codTipVenda) != 0) {
                            continue;
                        }
                    }
                    BigDecimal codTab = safeAsBigDecimal(row, "CODTAB");
                    if (isNullOrZero(codTab)) {
                        continue;
                    }
                    DynamicVO tabVO = resolveLatestActiveTabByCodTabNative(codTab);
                    if (tabVO == null) {
                        continue;
                    }
                    BigDecimal rowNutab = safeAsBigDecimal(tabVO, "NUTAB");
                    Timestamp rowDtVigor = safeAsTimestamp(tabVO, "DTVIGOR");
                    if (isNullOrZero(rowNutab)) {
                        continue;
                    }
                    if (selectedNutab == null
                            || (rowDtVigor != null && (selectedDtVigor == null || rowDtVigor.after(selectedDtVigor)))
                            || (rowDtVigor != null && selectedDtVigor != null && rowDtVigor.equals(selectedDtVigor)
                            && rowNutab.compareTo(selectedNutab) > 0)) {
                        selectedNutab = rowNutab;
                        selectedDtVigor = rowDtVigor;
                    }
                }
                if (!isNullOrZero(selectedNutab)) {
                    return selectedNutab;
                }
            } catch (Exception e) {
                log.log(Level.FINE, "Falha ao resolver NUTAB via Jape (" + daoName + ")", e);
            }
        }
        return null;
    }

    private DynamicVO resolveLatestActiveTabByCodTabNative(BigDecimal codTab) {
        if (isNullOrZero(codTab)) {
            return null;
        }
        String[] daoCandidates = {"TabelaPreco", "TGFTAB"};
        for (String daoName : daoCandidates) {
            try {
                JapeWrapper tabDAO = JapeFactory.dao(daoName);
                Collection<DynamicVO> rows = tabDAO.find("this.CODTAB = ?", codTab);
                if (rows == null || rows.isEmpty()) {
                    continue;
                }
                DynamicVO selected = null;
                Timestamp bestDtVigor = null;
                BigDecimal bestNutab = null;
                Timestamp now = new Timestamp(System.currentTimeMillis());
                for (DynamicVO row : rows) {
                    if (row == null) {
                        continue;
                    }
                    String inativo = trimToNull(safeAsString(row, "INATIVO"));
                    if ("S".equalsIgnoreCase(inativo)) {
                        continue;
                    }
                    Timestamp dtVigor = safeAsTimestamp(row, "DTVIGOR");
                    if (dtVigor != null && dtVigor.after(now)) {
                        continue;
                    }
                    BigDecimal nuTab = safeAsBigDecimal(row, "NUTAB");
                    if (isNullOrZero(nuTab)) {
                        continue;
                    }
                    if (selected == null
                            || (dtVigor != null && (bestDtVigor == null || dtVigor.after(bestDtVigor)))
                            || (dtVigor != null && bestDtVigor != null && dtVigor.equals(bestDtVigor) && nuTab.compareTo(bestNutab) > 0)
                            || (dtVigor == null && bestDtVigor == null && (bestNutab == null || nuTab.compareTo(bestNutab) > 0))) {
                        selected = row;
                        bestDtVigor = dtVigor;
                        bestNutab = nuTab;
                    }
                }
                if (selected != null) {
                    return selected;
                }
            } catch (Exception e) {
                log.log(Level.FINE, "Falha ao resolver TGFTAB via Jape (" + daoName + ")", e);
            }
        }
        return null;
    }

    private BigDecimal normalizeNuTabToLatestActive(BigDecimal nuTab) {
        if (isNullOrZero(nuTab)) {
            return null;
        }
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 T2.NUTAB ");
            sql.appendSql("FROM TGFTAB T1 ");
            sql.appendSql("INNER JOIN TGFTAB T2 ON T2.CODTAB = T1.CODTAB ");
            sql.appendSql("WHERE T1.NUTAB = :nuTab ");
            if (hasTableColumn("TGFTAB", "DTVIGOR")) {
                sql.appendSql("AND (T2.DTVIGOR IS NULL OR T2.DTVIGOR <= GETDATE()) ");
            }
            if (hasTableColumn("TGFTAB", "INATIVO")) {
                sql.appendSql("AND (T2.INATIVO IS NULL OR T2.INATIVO = 'N') ");
            }
            sql.appendSql("ORDER BY T2.DTVIGOR DESC, T2.NUTAB DESC");
            sql.setNamedParameter("nuTab", nuTab);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("NUTAB");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel normalizar NUTAB " + nuTab, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return nuTab;
    }

    private BigDecimal resolveCurrentCost(BigDecimal codProd, BigDecimal codEmp) {
        if (isNullOrZero(codProd)) {
            return null;
        }
        BigDecimal nativeCost = resolveCurrentCostNative(codProd, codEmp);
        if (!isNullOrZero(nativeCost)) {
            return nativeCost;
        }
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT ISNULL(MAX(CUSREP),0) AS CUSREP ");
            sql.appendSql("FROM TGFCUS ");
            sql.appendSql("WHERE CODPROD = :codProd ");
            sql.appendSql("AND DTATUAL = (SELECT MAX(DTATUAL) FROM TGFCUS CN ");
            sql.appendSql("               WHERE CN.CODPROD = :codProd ");
            sql.appendSql("               AND CN.DTATUAL <= GETDATE() ");
            if (!isNullOrZero(codEmp) && hasTableColumn("TGFCUS", "CODEMP")) {
                sql.appendSql("               AND CN.CODEMP = :codEmp) ");
                sql.appendSql("AND CODEMP = :codEmp ");
                sql.setNamedParameter("codEmp", codEmp);
            } else {
                sql.appendSql(")");
            }
            sql.setNamedParameter("codProd", codProd);
            rs = sql.executeQuery();
            if (rs.next()) {
                BigDecimal cus = rs.getBigDecimal("CUSREP");
                if (!isNullOrZero(cus)) {
                    return cus;
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver custo atual em TGFCUS para CODPROD " + codProd, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal resolveCurrentCostNative(BigDecimal codProd, BigDecimal codEmp) {
        String[] daoCandidates = {"Custo", "TGFCUS"};
        for (String daoName : daoCandidates) {
            try {
                JapeWrapper cusDAO = JapeFactory.dao(daoName);
                Collection<DynamicVO> rows = cusDAO.find("this.CODPROD = ?", codProd);
                if (rows == null || rows.isEmpty()) {
                    continue;
                }
                BigDecimal bestCus = null;
                Timestamp bestDt = null;
                Timestamp now = new Timestamp(System.currentTimeMillis());
                for (DynamicVO row : rows) {
                    if (row == null) {
                        continue;
                    }
                    if (!isNullOrZero(codEmp)) {
                        BigDecimal rowEmp = safeAsBigDecimal(row, "CODEMP");
                        if (rowEmp != null && rowEmp.compareTo(codEmp) != 0) {
                            continue;
                        }
                    }
                    Timestamp dtAtual = safeAsTimestamp(row, "DTATUAL");
                    if (dtAtual != null && dtAtual.after(now)) {
                        continue;
                    }
                    BigDecimal cusRep = safeAsBigDecimal(row, "CUSREP");
                    if (isNullOrZero(cusRep)) {
                        continue;
                    }
                    if (bestCus == null
                            || (dtAtual != null && (bestDt == null || dtAtual.after(bestDt)))
                            || (dtAtual != null && bestDt != null && dtAtual.equals(bestDt) && cusRep.compareTo(bestCus) > 0)) {
                        bestCus = cusRep;
                        bestDt = dtAtual;
                    }
                }
                if (!isNullOrZero(bestCus)) {
                    return bestCus;
                }
            } catch (Exception e) {
                log.log(Level.FINE, "Falha ao resolver TGFCUS via Jape (" + daoName + ")", e);
            }
        }
        return null;
    }

    private BigDecimal resolveCodVendByParc(BigDecimal codParc) {
        if (isNullOrZero(codParc)) {
            return null;
        }
        try {
            JapeWrapper parceiroDAO = JapeFactory.dao("Parceiro");
            DynamicVO parceiroVO = parceiroDAO.findByPK(codParc);
            if (parceiroVO != null) {
                BigDecimal codVend = safeAsBigDecimal(parceiroVO, "CODVEND");
                if (!isNullOrZero(codVend)) {
                    return codVend;
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver CODVEND do parceiro " + codParc, e);
        }
        return null;
    }

    private static final class ItemPricingData {
        private BigDecimal nuTab;
        private BigDecimal precoBase;
        private BigDecimal custo;
        private String usoProd;
    }

    private String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private BigDecimal safeAsBigDecimal(DynamicVO vo, String field) {
        if (vo == null || field == null) {
            return null;
        }
        try {
            return vo.asBigDecimal(field);
        } catch (Exception ignored) {
            return null;
        }
    }

    private Timestamp safeAsTimestamp(DynamicVO vo, String field) {
        if (vo == null || field == null) {
            return null;
        }
        try {
            return vo.asTimestamp(field);
        } catch (Exception ignored) {
            return null;
        }
    }

    private String safeAsString(DynamicVO vo, String field) {
        if (vo == null || field == null) {
            return null;
        }
        try {
            return vo.asString(field);
        } catch (Exception ignored) {
            return null;
        }
    }

    private boolean isNullOrZero(BigDecimal value) {
        return value == null || value.compareTo(BigDecimal.ZERO) <= 0;
    }

    private boolean hasTableColumn(String tableName, String columnName) {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT COUNT(*) AS CNT ");
            sql.appendSql("FROM INFORMATION_SCHEMA.COLUMNS ");
            sql.appendSql("WHERE TABLE_NAME = :tableName AND COLUMN_NAME = :columnName");
            sql.setNamedParameter("tableName", tableName);
            sql.setNamedParameter("columnName", columnName);
            rs = sql.executeQuery();
            return rs.next() && rs.getInt("CNT") > 0;
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel validar coluna " + tableName + "." + columnName, e);
            return false;
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
    }

    private JdbcWrapper openJdbc() throws Exception {
        JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
        jdbc.openSession();
        return jdbc;
    }

    private void closeJdbc(JdbcWrapper jdbc) {
        if (jdbc != null) {
            try {
                jdbc.closeSession();
            } catch (Exception e) {
                log.log(Level.FINE, "Erro ao fechar session do JdbcWrapper", e);
            }
        }
    }
}
