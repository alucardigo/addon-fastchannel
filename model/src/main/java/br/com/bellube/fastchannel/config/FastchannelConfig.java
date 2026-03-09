package br.com.bellube.fastchannel.config;

import br.com.bellube.fastchannel.installation.FastchannelAutoProvisioning;
import br.com.bellube.fastchannel.util.DBUtil;
import br.com.bellube.fastchannel.util.DbColumnSupport;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Gerenciador de configuracao para integracao Fastchannel.
 */
public class FastchannelConfig {

    private static final Logger log = Logger.getLogger(FastchannelConfig.class.getName());

    private static FastchannelConfig instance;

    private String clientId;
    private String clientSecret;
    private String scope;
    private String subscriptionKey;
    private String subscriptionKeyDistribution;
    private String subscriptionKeyConsumption;
    private String baseUrl;
    private String authUrl;
    private BigDecimal codTipOper;
    private BigDecimal tipNeg;
    private BigDecimal codLocal;
    private BigDecimal nuTab;
    private BigDecimal codemp;
    private BigDecimal codNat;
    private BigDecimal codCenCus;
    private BigDecimal codVendPadrao;
    private BigDecimal codParcPadrao;
    private String priceTableTipos;
    private String priceTableIds;
    private String resellerId;
    private String storageId;
    private boolean ativo;
    private int batchSize;
    private int maxRequestsPerMinute;
    private Timestamp lastOrderSync;
    private Timestamp lastProductSync;
    private Timestamp lastStockSync;
    private Timestamp lastPriceSync;
    private boolean syncStatusEnabled;
    private String sankhyaServerUrl;
    private String sankhyaUser;
    private String sankhyaPassword;
    private Integer uiSourceDefault;
    private boolean uiEnableSource2;
    private boolean uiEnableSource3;
    private boolean disableDuplicateCheckFromConfig;

    private long lastLoadTime;
    private static final long CACHE_TTL_MS = 300_000;

    private FastchannelConfig() {
        // Nao carrega na inicializacao para evitar erros de timing
        // Carrega lazy no primeiro acesso
    }

    public static synchronized FastchannelConfig getInstance() {
        if (instance == null) {
            instance = new FastchannelConfig();
        }
        FastchannelAutoProvisioning.ensureStarted(null, null);
        return instance;
    }

    public synchronized void reload() {
        loadConfiguration();
    }

    private void checkCacheValidity() {
        // Carrega na primeira vez
        if (lastLoadTime == 0) {
            loadConfiguration();
            return;
        }
        // Recarrega se cache expirou
        if (System.currentTimeMillis() - lastLoadTime > CACHE_TTL_MS) {
            loadConfiguration();
        }
    }

    private void loadConfiguration() {
        try {
            if (!loadFromConfigTable()) {
                log.warning("Configuracao Fastchannel nao encontrada. Integracao desativada.");
                setDefaults();
            }
            lastLoadTime = System.currentTimeMillis();
            log.info("Configuracao Fastchannel carregada com sucesso.");
        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao carregar configuracao Fastchannel", e);
            setDefaults();
        }
    }

    private boolean loadFromConfigTable() {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                "SELECT * FROM AD_FCCONFIG ORDER BY CODCONFIG DESC"
            );
            rs = stmt.executeQuery();

            if (rs.next()) {
                applyFromResultSet(rs);
                return true;
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Tabela AD_FCCONFIG nao encontrada ou vazia.", e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return false;
    }

    private void applyFromResultSet(ResultSet rs) throws Exception {
        this.clientId = rs.getString("CLIENT_ID");
        this.clientSecret = rs.getString("CLIENT_SECRET");
        this.scope = rs.getString("SCOPE");
        this.subscriptionKey = rs.getString("SUBSCRIPTION_KEY");
        this.subscriptionKeyDistribution = rs.getString("SUBSCRIPTION_KEY_DISTRIBUTION");
        this.subscriptionKeyConsumption = rs.getString("SUBSCRIPTION_KEY_CONSUMPTION");
        this.baseUrl = rs.getString("BASE_URL");
        this.authUrl = rs.getString("AUTH_URL");
        this.codTipOper = rs.getBigDecimal("CODTIPOPER");
        this.tipNeg = rs.getBigDecimal("TIPNEG");
        this.codLocal = rs.getBigDecimal("CODLOCAL");
        this.nuTab = rs.getBigDecimal("NUTAB");
        this.codemp = rs.getBigDecimal("CODEMP");
        if (br.com.bellube.fastchannel.util.DbColumnSupport.hasColumn(rs, "CODNAT")) {
            this.codNat = rs.getBigDecimal("CODNAT");
        } else {
            this.codNat = null;
        }
        if (br.com.bellube.fastchannel.util.DbColumnSupport.hasColumn(rs, "CODCENCUS")) {
            this.codCenCus = rs.getBigDecimal("CODCENCUS");
        } else {
            this.codCenCus = null;
        }
        if (br.com.bellube.fastchannel.util.DbColumnSupport.hasColumn(rs, "CODVEND_PADRAO")) {
            this.codVendPadrao = rs.getBigDecimal("CODVEND_PADRAO");
        } else {
            this.codVendPadrao = null;
        }
        if (br.com.bellube.fastchannel.util.DbColumnSupport.hasColumn(rs, "CODPARC_PADRAO")) {
            this.codParcPadrao = rs.getBigDecimal("CODPARC_PADRAO");
        } else {
            this.codParcPadrao = null;
        }
        if (br.com.bellube.fastchannel.util.DbColumnSupport.hasColumn(rs, "PRICE_TABLE_TIPOS")) {
            this.priceTableTipos = rs.getString("PRICE_TABLE_TIPOS");
        } else {
            this.priceTableTipos = null;
        }
        if (br.com.bellube.fastchannel.util.DbColumnSupport.hasColumn(rs, "PRICE_TABLE_IDS")) {
            this.priceTableIds = rs.getString("PRICE_TABLE_IDS");
        } else {
            this.priceTableIds = null;
        }
        this.ativo = "S".equals(rs.getString("ATIVO"));
        this.batchSize = rs.getInt("BATCH_SIZE");
        this.maxRequestsPerMinute = rs.getInt("MAX_REQUESTS_MIN");
        this.lastOrderSync = rs.getTimestamp("LAST_ORDER_SYNC");
        this.lastProductSync = rs.getTimestamp("LAST_PRODUCT_SYNC");
        this.lastStockSync = rs.getTimestamp("LAST_STOCK_SYNC");
        this.lastPriceSync = rs.getTimestamp("LAST_PRICE_SYNC");
        this.resellerId = rs.getString("RESELLER_ID");
        this.storageId = rs.getString("STORAGE_ID");
        this.syncStatusEnabled = "S".equals(rs.getString("SYNC_STATUS_ENABLED"));
        this.sankhyaServerUrl = rs.getString("SANKHYA_SERVER_URL");
        this.sankhyaUser = rs.getString("SANKHYA_USER");
        this.sankhyaPassword = rs.getString("SANKHYA_PASSWORD");

        if (DbColumnSupport.hasColumn(rs, "UI_SOURCE_DEFAULT")) {
            Object raw = rs.getObject("UI_SOURCE_DEFAULT");
            if (raw instanceof Number) {
                this.uiSourceDefault = ((Number) raw).intValue();
            } else if (raw != null) {
                try {
                    this.uiSourceDefault = Integer.parseInt(raw.toString());
                } catch (NumberFormatException e) {
                    this.uiSourceDefault = null;
                }
            } else {
                this.uiSourceDefault = null;
            }
            this.uiEnableSource2 = "S".equals(rs.getString("UI_ENABLE_SOURCE_2"));
            this.uiEnableSource3 = "S".equals(rs.getString("UI_ENABLE_SOURCE_3"));
        } else {
            this.uiSourceDefault = null;
            this.uiEnableSource2 = false;
            this.uiEnableSource3 = false;
        }

        if (DbColumnSupport.hasColumn(rs, "DISABLE_DUPLICATE_CHECK")) {
            this.disableDuplicateCheckFromConfig = "S".equalsIgnoreCase(rs.getString("DISABLE_DUPLICATE_CHECK"));
        } else {
            this.disableDuplicateCheckFromConfig = false;
        }

        if (this.subscriptionKeyDistribution == null || this.subscriptionKeyDistribution.isEmpty()) {
            this.subscriptionKeyDistribution = this.subscriptionKey;
        }
        if (this.subscriptionKeyConsumption == null || this.subscriptionKeyConsumption.isEmpty()) {
            this.subscriptionKeyConsumption = this.subscriptionKey;
        }
        if (this.batchSize <= 0) this.batchSize = FastchannelConstants.DEFAULT_BATCH_SIZE;
        if (this.maxRequestsPerMinute <= 0) this.maxRequestsPerMinute = FastchannelConstants.DEFAULT_RATE_LIMIT_PER_MINUTE;
    }

    private void setDefaults() {
        this.authUrl = FastchannelConstants.AUTH_URL;
        this.baseUrl = FastchannelConstants.ORDER_API_BASE;
        this.subscriptionKey = null;
        this.subscriptionKeyDistribution = null;
        this.subscriptionKeyConsumption = null;
        this.ativo = false;
        this.syncStatusEnabled = true;
        this.sankhyaServerUrl = null;
        this.sankhyaUser = null;
        this.sankhyaPassword = null;
        this.codNat = null;
        this.codCenCus = null;
        this.codVendPadrao = null;
        this.codParcPadrao = null;
        this.tipNeg = null;
        this.priceTableTipos = null;
        this.priceTableIds = null;
        this.uiSourceDefault = null;
        this.uiEnableSource2 = false;
        this.uiEnableSource3 = false;
        this.disableDuplicateCheckFromConfig = false;
        this.batchSize = FastchannelConstants.DEFAULT_BATCH_SIZE;
        this.maxRequestsPerMinute = FastchannelConstants.DEFAULT_RATE_LIMIT_PER_MINUTE;
    }

    public String getClientId() {
        checkCacheValidity();
        return clientId;
    }

    public String getClientSecret() {
        checkCacheValidity();
        return clientSecret;
    }

    public String getScope() {
        checkCacheValidity();
        return scope;
    }

    public String getSubscriptionKey() {
        return getSubscriptionKeyDistribution();
    }

    public String getSubscriptionKeyDistribution() {
        checkCacheValidity();
        if (subscriptionKeyDistribution != null && !subscriptionKeyDistribution.isEmpty()) {
            return subscriptionKeyDistribution;
        }
        return subscriptionKey;
    }

    public String getSubscriptionKeyConsumption() {
        checkCacheValidity();
        if (subscriptionKeyConsumption != null && !subscriptionKeyConsumption.isEmpty()) {
            return subscriptionKeyConsumption;
        }
        return subscriptionKey;
    }

    public String getBaseUrl() {
        checkCacheValidity();
        return baseUrl != null ? baseUrl : FastchannelConstants.ORDER_API_BASE;
    }

    public String getAuthUrl() {
        checkCacheValidity();
        return authUrl != null ? authUrl : FastchannelConstants.AUTH_URL;
    }

    public BigDecimal getCodTipOper() {
        checkCacheValidity();
        return codTipOper;
    }

    public BigDecimal getTipNeg() {
        checkCacheValidity();
        return tipNeg;
    }

    public BigDecimal getCodNat() {
        checkCacheValidity();
        return codNat;
    }

    public BigDecimal getCodCenCus() {
        checkCacheValidity();
        return codCenCus;
    }

    public BigDecimal getCodVendPadrao() {
        checkCacheValidity();
        return codVendPadrao;
    }

    public BigDecimal getCodParcPadrao() {
        checkCacheValidity();
        return codParcPadrao;
    }

    public BigDecimal getCodLocal() {
        checkCacheValidity();
        return codLocal;
    }

    public BigDecimal getNuTab() {
        checkCacheValidity();
        return nuTab;
    }

    public BigDecimal getCodemp() {
        checkCacheValidity();
        return codemp;
    }

    public String getPriceTableTipos() {
        checkCacheValidity();
        return priceTableTipos;
    }

    public String getPriceTableIds() {
        checkCacheValidity();
        return priceTableIds;
    }

    public String getResellerId() {
        checkCacheValidity();
        return resellerId;
    }

    public String getStorageId() {
        checkCacheValidity();
        return storageId;
    }

    public boolean isAtivo() {
        checkCacheValidity();
        return ativo;
    }

    public int getBatchSize() {
        checkCacheValidity();
        return batchSize > 0 ? batchSize : FastchannelConstants.DEFAULT_BATCH_SIZE;
    }

    public int getMaxRequestsPerMinute() {
        checkCacheValidity();
        return maxRequestsPerMinute > 0 ? maxRequestsPerMinute : FastchannelConstants.DEFAULT_RATE_LIMIT_PER_MINUTE;
    }

    public Timestamp getLastOrderSync() {
        checkCacheValidity();
        return lastOrderSync;
    }

    public Timestamp getLastProductSync() {
        checkCacheValidity();
        return lastProductSync;
    }

    public Timestamp getLastStockSync() {
        checkCacheValidity();
        return lastStockSync;
    }

    public Timestamp getLastPriceSync() {
        checkCacheValidity();
        return lastPriceSync;
    }

    public boolean isSyncStatusEnabled() {
        checkCacheValidity();
        return syncStatusEnabled;
    }

    public String getSankhyaServerUrl() {
        checkCacheValidity();
        if (sankhyaServerUrl != null && !sankhyaServerUrl.isEmpty()) {
            return sankhyaServerUrl;
        }
        // Fallback: tentar propriedade do sistema
        String systemUrl = System.getProperty("sankhya.server.url");
        if (systemUrl != null && !systemUrl.isEmpty()) {
            return systemUrl;
        }
        // Ultimo fallback: variavel de ambiente
        return System.getenv("SANKHYA_SERVER_URL");
    }

    public String getSankhyaUser() {
        checkCacheValidity();
        return sankhyaUser;
    }

    public String getSankhyaPassword() {
        checkCacheValidity();
        return sankhyaPassword;
    }

    public Integer getUiSourceDefault() {
        checkCacheValidity();
        return uiSourceDefault;
    }

    public boolean isUiEnableSource2() {
        checkCacheValidity();
        return uiEnableSource2;
    }

    public boolean isUiEnableSource3() {
        checkCacheValidity();
        return uiEnableSource3;
    }

    public SourceConfig getSourceConfig() {
        checkCacheValidity();
        return SourceConfig.from(uiSourceDefault, uiEnableSource2, uiEnableSource3);
    }

    public boolean isDuplicateCheckEnabled() {
        String disable = System.getProperty("fastchannel.disableDuplicateCheck");
        if (disable == null || disable.trim().isEmpty()) {
            disable = System.getenv("FASTCHANNEL_DISABLE_DUPLICATE_CHECK");
        }
        if (disable != null && !disable.trim().isEmpty()) {
            return !Boolean.parseBoolean(disable);
        }
        checkCacheValidity();
        return !disableDuplicateCheckFromConfig;
    }

    public void updateLastOrderSync(Timestamp timestamp) {
        this.lastOrderSync = timestamp;
        persistLastSync("LAST_ORDER_SYNC", timestamp);
    }

    public void updateLastProductSync(Timestamp timestamp) {
        this.lastProductSync = timestamp;
        persistLastSync("LAST_PRODUCT_SYNC", timestamp);
    }

    public void updateLastStockSync(Timestamp timestamp) {
        this.lastStockSync = timestamp;
        persistLastSync("LAST_STOCK_SYNC", timestamp);
    }

    public void updateLastPriceSync(Timestamp timestamp) {
        this.lastPriceSync = timestamp;
        persistLastSync("LAST_PRICE_SYNC", timestamp);
    }

    private void persistLastSync(String field, Timestamp value) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement("UPDATE AD_FCCONFIG SET " + field + " = ?, DH_ALTERACAO = CURRENT_TIMESTAMP WHERE CODCONFIG = (SELECT MAX(CODCONFIG) FROM AD_FCCONFIG)");
            stmt.setTimestamp(1, value);
            stmt.executeUpdate();
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao persistir " + field, e);
        } finally {
            DBUtil.closeAll(null, stmt, conn);
        }
    }
}
