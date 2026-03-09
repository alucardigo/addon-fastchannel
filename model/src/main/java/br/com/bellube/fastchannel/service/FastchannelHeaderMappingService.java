package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.OrderCustomerDTO;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
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
    private volatile BigDecimal cachedFallbackTop;
    private volatile BigDecimal cachedFallbackCodEmp;

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
        if (resolved.codEmp == null) {
            resolved.codEmp = getFallbackCodEmp();
        }
        resolved.codTipOper = firstNonNull(config.getCodTipOper(), getFallbackTop());
        resolved.codTipVenda = resolveTipNeg(order);
        resolved.codNat = firstNonNull(resolveByKeys(TIPO_CODNAT, keys), config.getCodNat());
        resolved.codCenCus = firstNonNull(resolveByKeys(TIPO_CODCENCUS, keys), config.getCodCenCus());
        resolved.codVend = resolveByKeys(TIPO_CODVEND, keys);
        resolved.codParc = resolveByKeys(TIPO_PARCEIRO, keys);

        resolved.codEmp = require(resolved.codEmp, "CODEMP", storageId, resellerId);
        resolved.codTipOper = require(resolved.codTipOper, "CODTIPOPER", storageId, resellerId);
        resolved.codTipVenda = require(resolved.codTipVenda, "CODTIPVENDA", storageId, resellerId);
        resolved.codTipOper = ensureTopExists(resolved.codTipOper);
        resolved.codTipOper = preferTop403(resolved.codTipOper);

        if (resolved.codParc == null && customerDoc != null) {
            resolved.codParc = deparaLookup.getCodigoSankhya(TIPO_PARCEIRO, customerDoc);
        }
        if (resolved.codParc == null && customerDoc != null) {
            resolved.codParc = partnerLookup.findCodParcByDocument(customerDoc);
        }

        if (resolved.codVend == null && resolved.codParc != null) {
            resolved.codVend = partnerLookup.findCodVendByParc(resolved.codParc);
        }
        if (resolved.codVend == null) {
            resolved.codVend = config.getCodVendPadrao();
        }
        if (resolved.codVend == null) {
            resolved.codVend = new BigDecimal("281");
        }

        cache.put(cacheKey, resolved);
        return resolved;
    }

    private BigDecimal ensureTopExists(BigDecimal codTipOper) {
        if (codTipOper == null) {
            return null;
        }
        if (existsTopForPedido(codTipOper)) {
            return codTipOper;
        }

        BigDecimal fallback = findFallbackTop();
        if (fallback != null) {
            log.warning("TOP " + codTipOper + " nao valida para pedido (TIPMOV='P'). Usando fallback " + fallback + ".");
            return fallback;
        }

        return codTipOper;
    }

    private BigDecimal preferTop403(BigDecimal currentTop) {
        BigDecimal preferred = FastchannelConstants.DEFAULT_TOP_PEDIDO;
        if (existsTopForPedido(preferred)) {
            return preferred;
        }
        return currentTop;
    }

    private boolean existsTopForPedido(BigDecimal codTipOper) {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 CODTIPOPER FROM TGFTOP ");
            sql.appendSql("WHERE CODTIPOPER = :codTipOper ");
            sql.appendSql("AND TIPMOV = 'P' ");
            sql.setNamedParameter("codTipOper", codTipOper);
            rs = sql.executeQuery();
            return rs.next();
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao validar existencia de TOP " + codTipOper, e);
            return false;
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
    }

    private BigDecimal findFallbackTop() {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 CODTIPOPER FROM TGFTOP ");
            sql.appendSql("WHERE TIPMOV = 'P' ");
            sql.appendSql("ORDER BY DHALTER DESC, CODTIPOPER DESC");
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODTIPOPER");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar TOP fallback em TGFTOP", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal getFallbackTop() {
        if (cachedFallbackTop != null) {
            return cachedFallbackTop;
        }
        BigDecimal resolved = findFallbackTop();
        if (resolved != null) {
            cachedFallbackTop = resolved;
        }
        return resolved;
    }

    private BigDecimal getFallbackCodEmp() {
        if (cachedFallbackCodEmp != null) {
            return cachedFallbackCodEmp;
        }
        BigDecimal resolved = findFallbackCodEmp();
        if (resolved != null) {
            cachedFallbackCodEmp = resolved;
        }
        return resolved;
    }

    private BigDecimal findFallbackCodEmp() {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 CODEMP FROM TSIEMP ORDER BY CODEMP");
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODEMP");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar CODEMP fallback em TSIEMP", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
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
                throw new IllegalStateException("CODTIPVENDA nao resolvido para PaymentMethodId="
                        + paymentMethodId + ". Configure TGFTPV.AD_IDFAST.");
            } catch (Exception e) {
                log.log(Level.WARNING, "Erro ao resolver TIPNEG por PaymentMethodId=" + paymentMethodId, e);
                if (e instanceof IllegalStateException) {
                    throw (IllegalStateException) e;
                }
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

            JdbcWrapper jdbc = null;
            ResultSet rs = null;

            try {
                jdbc = openJdbc();
                if (!hasColumn(jdbc, "TGFTPV", "AD_IDFAST")) {
                    return null;
                }

                NativeSql sql = new NativeSql(jdbc);
                sql.appendSql("SELECT TOP 1 CODTIPVENDA FROM TGFTPV ");
                sql.appendSql("WHERE AD_IDFAST = :paymentMethodId ");
                if (hasColumn(jdbc, "TGFTPV", "ATIVO")) {
                    sql.appendSql("AND COALESCE(ATIVO, 'S') = 'S' ");
                }
                if (hasColumn(jdbc, "TGFTPV", "DHALTER")) {
                    sql.appendSql("AND DHALTER <= CURRENT_TIMESTAMP ");
                    sql.appendSql("ORDER BY DHALTER DESC");
                } else {
                    sql.appendSql("ORDER BY CODTIPVENDA DESC");
                }
                sql.setNamedParameter("paymentMethodId", paymentMethodId);
                rs = sql.executeQuery();

                if (rs.next()) {
                    return rs.getBigDecimal("CODTIPVENDA");
                }
            } catch (Exception e) {
                log.log(Level.WARNING, "Erro ao buscar TIPNEG por PaymentMethodId", e);
            } finally {
                closeQuietly(rs);
                closeJdbc(jdbc);
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

    private static JdbcWrapper openJdbc() throws Exception {
        JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
        jdbc.openSession();
        return jdbc;
    }

    private static void closeJdbc(JdbcWrapper jdbc) {
        if (jdbc != null) {
            try {
                jdbc.closeSession();
            } catch (Exception e) {
                log.log(Level.FINE, "Erro ao fechar session do JdbcWrapper", e);
            }
        }
    }

    private static void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception ignored) {}
        }
    }

    private static boolean hasColumn(JdbcWrapper jdbc, String tableName, String columnName) {
        ResultSet rs = null;
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT COUNT(*) AS CNT ");
            sql.appendSql("FROM INFORMATION_SCHEMA.COLUMNS ");
            sql.appendSql("WHERE TABLE_NAME = :tableName AND COLUMN_NAME = :columnName");
            sql.setNamedParameter("tableName", tableName);
            sql.setNamedParameter("columnName", columnName);
            rs = sql.executeQuery();
            return rs.next() && rs.getInt("CNT") > 0;
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao validar coluna " + tableName + "." + columnName, e);
            return false;
        } finally {
            closeQuietly(rs);
        }
    }
}
