package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.dto.OrderCustomerDTO;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.util.DBUtil;
import br.com.bellube.fastchannel.util.DbColumnSupport;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Resolve campos de cabecalho do pedido via de-para Fastchannel -> Sankhya.
 */
public class FastchannelHeaderMappingService {

    private static final Logger log = Logger.getLogger(FastchannelHeaderMappingService.class.getName());

    public static final String TIPO_EMPRESA = "EMPRESA";
    public static final String TIPO_TOP_PEDIDO = "TOP_PEDIDO";
    public static final String TIPO_TIPNEG = "TIPNEG";
    public static final String TIPO_PARCEIRO = "PARCEIRO";
    public static final String TIPO_CODNAT = "CODNAT";
    public static final String TIPO_CODCENCUS = "CODCENCUS";
    public static final String TIPO_CODVEND = "CODVEND";

    public interface DeparaLookup {
        BigDecimal getCodigoSankhya(String tipo, String codExterno);
    }

    public interface PartnerLookup {
        BigDecimal findCodParcByDocument(String document);
        BigDecimal findCodVendByParc(BigDecimal codParc);
    }

    public interface TipNegLookup {
        BigDecimal resolveByPaymentMethod(Integer paymentMethodId);
    }

    public interface ConfigLookup {
        BigDecimal getCodemp();
        BigDecimal getCodTipOper();
        BigDecimal getTipNeg();
        BigDecimal getCodNat();
        BigDecimal getCodCenCus();
        BigDecimal getCodVendPadrao();
    }

    public static class ResolvedHeader {
        private BigDecimal codEmp;
        private BigDecimal codTipOper;
        private BigDecimal codTipVenda;
        private BigDecimal codParc;
        private BigDecimal codNat;
        private BigDecimal codCenCus;
        private BigDecimal codVend;

        public BigDecimal getCodEmp() {
            return codEmp;
        }

        public BigDecimal getCodTipOper() {
            return codTipOper;
        }

        public BigDecimal getCodTipVenda() {
            return codTipVenda;
        }

        public BigDecimal getCodParc() {
            return codParc;
        }

        public BigDecimal getCodNat() {
            return codNat;
        }

        public BigDecimal getCodCenCus() {
            return codCenCus;
        }

        public BigDecimal getCodVend() {
            return codVend;
        }
    }

    private final DeparaLookup deparaLookup;
    private final PartnerLookup partnerLookup;
    private final TipNegLookup tipNegLookup;
    private final ConfigLookup config;
    private final Map<String, ResolvedHeader> cache = new ConcurrentHashMap<>();

    public FastchannelHeaderMappingService() {
        this(new DefaultDeparaLookup(), new DefaultPartnerLookup(), new DefaultTipNegLookup(), new DefaultConfigLookup());
    }

    FastchannelHeaderMappingService(DeparaLookup deparaLookup, PartnerLookup partnerLookup,
                                    TipNegLookup tipNegLookup, ConfigLookup config) {
        this.deparaLookup = deparaLookup;
        this.partnerLookup = partnerLookup;
        this.tipNegLookup = tipNegLookup;
        this.config = config;
    }

    public ResolvedHeader resolve(OrderDTO order) {
        if (order == null) {
            throw new IllegalStateException("Pedido nao informado para de-para");
        }

        String storageId = normalize(order.getStorageId());
        String resellerId = normalize(order.getResellerId());
        String customerDoc = normalizeCustomerDocument(order.getCustomer());

        String cacheKey = (storageId != null ? storageId : "-") + "|" +
                (resellerId != null ? resellerId : "-") + "|" +
                (customerDoc != null ? customerDoc : "-");
        ResolvedHeader cached = cache.get(cacheKey);
        if (cached != null) {
            return cached;
        }

        List<String> keys = buildKeyCandidates(storageId, resellerId);

        ResolvedHeader resolved = new ResolvedHeader();
        resolved.codEmp = firstNonNull(resolveByKeys(TIPO_EMPRESA, keys), config.getCodemp());
        resolved.codTipOper = config.getCodTipOper();
        resolved.codTipVenda = resolveTipNeg(order);
        resolved.codNat = firstNonNull(resolveByKeys(TIPO_CODNAT, keys), config.getCodNat());
        resolved.codCenCus = firstNonNull(resolveByKeys(TIPO_CODCENCUS, keys), config.getCodCenCus());
        resolved.codVend = firstNonNull(resolveByKeys(TIPO_CODVEND, keys), config.getCodVendPadrao());
        resolved.codParc = resolveByKeys(TIPO_PARCEIRO, keys);

        resolved.codEmp = require(resolved.codEmp, "CODEMP", storageId, resellerId);
        resolved.codTipOper = require(resolved.codTipOper, "CODTIPOPER", storageId, resellerId);
        resolved.codTipVenda = require(resolved.codTipVenda, "CODTIPVENDA", storageId, resellerId);

        if (resolved.codParc == null && customerDoc != null) {
            resolved.codParc = deparaLookup.getCodigoSankhya(TIPO_PARCEIRO, customerDoc);
        }
        if (resolved.codParc == null && customerDoc != null) {
            resolved.codParc = partnerLookup.findCodParcByDocument(customerDoc);
        }

        if (resolved.codVend == null && resolved.codParc != null) {
            resolved.codVend = partnerLookup.findCodVendByParc(resolved.codParc);
        }

        cache.put(cacheKey, resolved);
        return resolved;
    }

    private BigDecimal resolveByKeys(String tipo, List<String> keys) {
        if (keys.isEmpty()) return null;
        for (String key : keys) {
            BigDecimal value = deparaLookup.getCodigoSankhya(tipo, key);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private BigDecimal resolveTipNeg(OrderDTO order) {
        Integer paymentMethodId = order != null ? order.getPaymentMethodId() : null;
        if (paymentMethodId != null) {
            try {
                BigDecimal resolved = tipNegLookup.resolveByPaymentMethod(paymentMethodId);
                if (resolved != null) {
                    return resolved;
                }
            } catch (Exception e) {
                log.log(Level.WARNING, "Erro ao resolver TIPNEG por PaymentMethodId=" + paymentMethodId, e);
            }
        }
        return config.getTipNeg();
    }

    private BigDecimal require(BigDecimal value, String label, String storageId, String resellerId) {
        if (value == null) {
            throw new IllegalStateException(label + " nao resolvido para StorageId=" + storageId + " ResellerId=" + resellerId);
        }
        return value;
    }

    private BigDecimal firstNonNull(BigDecimal primary, BigDecimal fallback) {
        return primary != null ? primary : fallback;
    }

    private List<String> buildKeyCandidates(String storageId, String resellerId) {
        List<String> keys = new ArrayList<>();
        if (storageId != null && resellerId != null) {
            keys.add(buildCompositeKey(storageId, resellerId));
            keys.add(storageId + "|" + resellerId);
        }
        if (storageId != null) {
            keys.add("S:" + storageId);
            keys.add(storageId);
        }
        if (resellerId != null) {
            keys.add("R:" + resellerId);
            keys.add(resellerId);
        }
        return keys;
    }

    private String buildCompositeKey(String storageId, String resellerId) {
        return "S:" + storageId + "|R:" + resellerId;
    }

    private String normalize(String value) {
        if (value == null) return null;
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private String normalizeCustomerDocument(OrderCustomerDTO customer) {
        if (customer == null) return null;
        String doc = customer.getCleanCpfCnpj();
        if (doc == null || doc.isEmpty()) {
            doc = customer.getCpfCnpj();
        }
        if (doc == null) return null;
        return doc.replaceAll("[^0-9]", "");
    }

    private static class DefaultDeparaLookup implements DeparaLookup {
        private final DeparaService deparaService = DeparaService.getInstance();

        @Override
        public BigDecimal getCodigoSankhya(String tipo, String codExterno) {
            if (codExterno == null || codExterno.isEmpty()) return null;
            try {
                return deparaService.getCodigoSankhya(tipo, codExterno);
            } catch (Exception e) {
                log.log(Level.WARNING, "Erro ao resolver de-para para tipo " + tipo, e);
                return null;
            }
        }
    }

    private static class DefaultTipNegLookup implements TipNegLookup {
        @Override
        public BigDecimal resolveByPaymentMethod(Integer paymentMethodId) {
            if (paymentMethodId == null) {
                return null;
            }

            Connection conn = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;

            try {
                conn = DBUtil.getConnection();
                if (!DbColumnSupport.hasColumn(conn, "TGFTPV", "AD_IDFAST")) {
                    return null;
                }

                stmt = conn.prepareStatement(
                        "SELECT TOP 1 CODTIPVENDA FROM TGFTPV WHERE AD_IDFAST = ? ORDER BY DHALTER DESC");
                stmt.setInt(1, paymentMethodId);
                rs = stmt.executeQuery();

                if (rs.next()) {
                    return rs.getBigDecimal("CODTIPVENDA");
                }
            } catch (Exception e) {
                log.log(Level.WARNING, "Erro ao buscar TIPNEG por PaymentMethodId", e);
            } finally {
                DBUtil.closeAll(rs, stmt, conn);
            }

            return null;
        }
    }

    private static class DefaultConfigLookup implements ConfigLookup {
        private final FastchannelConfig config = FastchannelConfig.getInstance();

        @Override
        public BigDecimal getCodemp() {
            return config.getCodemp();
        }

        @Override
        public BigDecimal getCodTipOper() {
            return config.getCodTipOper();
        }

        @Override
        public BigDecimal getTipNeg() {
            return config.getTipNeg();
        }

        @Override
        public BigDecimal getCodNat() {
            return config.getCodNat();
        }

        @Override
        public BigDecimal getCodCenCus() {
            return config.getCodCenCus();
        }

        @Override
        public BigDecimal getCodVendPadrao() {
            return config.getCodVendPadrao();
        }
    }

    private static class DefaultPartnerLookup implements PartnerLookup {
        @Override
        public BigDecimal findCodParcByDocument(String document) {
            if (document == null || document.isEmpty()) return null;
            return PartnerLookupUtil.findCodParcByDocument(document);
        }

        @Override
        public BigDecimal findCodVendByParc(BigDecimal codParc) {
            if (codParc == null) return null;
            return PartnerLookupUtil.findCodVendByParc(codParc);
        }
    }
}
