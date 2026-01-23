package br.com.bellube.fastchannel.config;

import br.com.bellube.fastchannel.util.DBUtil;

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
    private BigDecimal codLocal;
    private BigDecimal nuTab;
    private BigDecimal codemp;
    private String resellerId;
    private String storageId;
    private boolean ativo;
    private int batchSize;
    private int maxRequestsPerMinute;
    private Timestamp lastOrderSync;
    private Timestamp lastProductSync;
    private Timestamp lastStockSync;
    private Timestamp lastPriceSync;

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
            stmt = conn.prepareStatement("SELECT * FROM AD_FCCONFIG WHERE ATIVO = 'S' ORDER BY CODCONFIG");
            rs = stmt.executeQuery();

            if (rs.next()) {
                this.clientId = rs.getString("CLIENT_ID");
                this.clientSecret = rs.getString("CLIENT_SECRET");
                this.scope = rs.getString("SCOPE");
                this.subscriptionKey = rs.getString("SUBSCRIPTION_KEY");
                this.subscriptionKeyDistribution = rs.getString("SUBSCRIPTION_KEY_DISTRIBUTION");
                this.subscriptionKeyConsumption = rs.getString("SUBSCRIPTION_KEY_CONSUMPTION");
                this.baseUrl = rs.getString("BASE_URL");
                this.authUrl = rs.getString("AUTH_URL");
                this.codTipOper = rs.getBigDecimal("CODTIPOPER");
                this.codLocal = rs.getBigDecimal("CODLOCAL");
                this.nuTab = rs.getBigDecimal("NUTAB");
                this.codemp = rs.getBigDecimal("CODEMP");
                this.ativo = "S".equals(rs.getString("ATIVO"));
                this.batchSize = rs.getInt("BATCH_SIZE");
                this.maxRequestsPerMinute = rs.getInt("MAX_REQUESTS_MIN");
                this.lastOrderSync = rs.getTimestamp("LAST_ORDER_SYNC");
                this.lastProductSync = rs.getTimestamp("LAST_PRODUCT_SYNC");
                this.lastStockSync = rs.getTimestamp("LAST_STOCK_SYNC");
                this.lastPriceSync = rs.getTimestamp("LAST_PRICE_SYNC");
                this.resellerId = rs.getString("RESELLER_ID");
                this.storageId = rs.getString("STORAGE_ID");

                if (this.subscriptionKeyDistribution == null || this.subscriptionKeyDistribution.isEmpty()) {
                    this.subscriptionKeyDistribution = this.subscriptionKey;
                }
                if (this.subscriptionKeyConsumption == null || this.subscriptionKeyConsumption.isEmpty()) {
                    this.subscriptionKeyConsumption = this.subscriptionKey;
                }
                if (this.batchSize <= 0) this.batchSize = FastchannelConstants.DEFAULT_BATCH_SIZE;
                if (this.maxRequestsPerMinute <= 0) this.maxRequestsPerMinute = FastchannelConstants.DEFAULT_RATE_LIMIT_PER_MINUTE;

                return true;
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Tabela AD_FCCONFIG nao encontrada ou vazia.", e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return false;
    }

    private void setDefaults() {
        this.authUrl = FastchannelConstants.AUTH_URL;
        this.baseUrl = FastchannelConstants.ORDER_API_BASE;
        this.subscriptionKey = null;
        this.subscriptionKeyDistribution = null;
        this.subscriptionKeyConsumption = null;
        this.ativo = false;
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
            stmt = conn.prepareStatement("UPDATE AD_FCCONFIG SET " + field + " = ?, DH_ALTERACAO = CURRENT_TIMESTAMP WHERE ATIVO = 'S'");
            stmt.setTimestamp(1, value);
            stmt.executeUpdate();
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao persistir " + field, e);
        } finally {
            DBUtil.closeAll(null, stmt, conn);
        }
    }
}
