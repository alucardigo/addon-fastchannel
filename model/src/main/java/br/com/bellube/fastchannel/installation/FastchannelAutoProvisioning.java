package br.com.bellube.fastchannel.installation;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.job.OrderImportJob;
import br.com.bellube.fastchannel.job.OrderStatusSyncJob;
import br.com.bellube.fastchannel.job.OutboxProcessorJob;
import br.com.bellube.fastchannel.job.PriceFullSyncJob;
import br.com.bellube.fastchannel.job.StockFullSyncJob;
import br.com.bellube.fastchannel.util.DBUtil;
import br.com.sankhya.jape.vo.DynamicVO;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Auto provisionamento da automacao da integracao Fastchannel.
 *
 * Fluxo:
 * 1) Tenta iniciar jobs nativos do Sankhya (AcaoAgendada) para o modulo do add-on.
 * 2) Caso nao encontre agendamentos nativos, ativa fallback interno idempotente.
 */
public final class FastchannelAutoProvisioning {

    private static final Logger log = Logger.getLogger(FastchannelAutoProvisioning.class.getName());
    private static final String SCHEDULED_ACTIONS_UTILS_FQN = "br.com.sankhya.acaoagendada.ScheduledActionsUtils";
    private static final AtomicBoolean INTERNAL_STARTED = new AtomicBoolean(false);
    private static final Map<String, AtomicBoolean> RUN_GUARD = new ConcurrentHashMap<>();
    private static volatile ScheduledExecutorService internalScheduler;

    private FastchannelAutoProvisioning() {
    }

    public static synchronized void ensureStarted(String appKey, BigDecimal explicitCodModulo) {
        boolean nativeStarted = false;
        try {
            nativeStarted = tryStartNativeScheduledActions(appKey, explicitCodModulo);
        } catch (Exception e) {
            log.log(Level.WARNING, "Falha ao iniciar scheduler nativo. Ativando fallback interno.", e);
        }

        if (!nativeStarted) {
            startInternalFallback();
        } else {
            log.info("AutoProvisionamento: scheduler nativo ativo.");
        }
    }

    public static synchronized void stopAll(String appKey, BigDecimal explicitCodModulo) {
        stopInternalFallback();
        try {
            stopNativeScheduledActions(appKey, explicitCodModulo);
        } catch (Exception e) {
            log.log(Level.WARNING, "Falha ao interromper scheduler nativo no uninstall.", e);
        }
    }

    private static boolean tryStartNativeScheduledActions(String appKey, BigDecimal explicitCodModulo) {
        BigDecimal codModulo = explicitCodModulo != null ? explicitCodModulo : resolveCodModuloByAppKey(appKey);
        if (codModulo == null) {
            log.info("AutoProvisionamento: CODMODULO nao identificado para appKey. Fallback interno sera usado.");
            return false;
        }

        Collection<?> actions = invokeModuleHasActions(codModulo, "S");
        if (actions == null || actions.isEmpty()) {
            log.info("AutoProvisionamento: modulo sem Acoes Agendadas ativas (CODMODULO=" + codModulo + ").");
            return false;
        }

        int started = 0;
        for (Object obj : actions) {
            BigDecimal nuaag = extractNuAag(obj);
            if (nuaag == null) {
                continue;
            }
            try {
                invokeAction("refreshJob", nuaag);
                invokeAction("startJob", nuaag);
                started++;
            } catch (Exception e) {
                log.log(Level.WARNING, "Falha ao iniciar AcaoAgendada NUAAG=" + nuaag, e);
            }
        }

        log.info("AutoProvisionamento: " + started + " acao(oes) agendada(s) nativa(s) iniciada(s).");
        return started > 0;
    }

    private static void stopNativeScheduledActions(String appKey, BigDecimal explicitCodModulo) {
        BigDecimal codModulo = explicitCodModulo != null ? explicitCodModulo : resolveCodModuloByAppKey(appKey);
        if (codModulo == null) {
            return;
        }

        Collection<?> actions = invokeModuleHasActions(codModulo, null);
        if (actions == null || actions.isEmpty()) {
            return;
        }

        for (Object obj : actions) {
            BigDecimal nuaag = extractNuAag(obj);
            if (nuaag == null) {
                continue;
            }
            try {
                invokeAction("stopJob", nuaag);
            } catch (Exception e) {
                log.log(Level.FINE, "Falha ao parar AcaoAgendada NUAAG=" + nuaag, e);
            }
        }
    }

    private static BigDecimal extractNuAag(Object obj) {
        try {
            if (obj instanceof DynamicVO) {
                return ((DynamicVO) obj).asBigDecimal("NUAAG");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel extrair NUAAG da acao agendada.", e);
        }
        return null;
    }

    private static synchronized void startInternalFallback() {
        if (INTERNAL_STARTED.get()) {
            return;
        }

        ThreadFactory factory = runnable -> {
            Thread t = Executors.defaultThreadFactory().newThread(runnable);
            t.setName("fastchannel-auto-" + t.getId());
            t.setDaemon(true);
            return t;
        };
        internalScheduler = Executors.newScheduledThreadPool(5, factory);

        schedule("order-import", readPositiveLong("fc.auto.order.import.minutes", 5), TimeUnit.MINUTES,
                () -> new OrderImportJob().executeScheduler());
        schedule("outbox", readPositiveLong("fc.auto.outbox.minutes", 1), TimeUnit.MINUTES,
                () -> new OutboxProcessorJob().executeScheduler());
        schedule("status-sync", readPositiveLong("fc.auto.status.minutes", 3), TimeUnit.MINUTES,
                () -> new OrderStatusSyncJob().executeScheduler());
        schedule("price-full", readPositiveLong("fc.auto.price.hours", 6), TimeUnit.HOURS,
                () -> new PriceFullSyncJob().executeScheduler());
        schedule("stock-full", readPositiveLong("fc.auto.stock.hours", 6), TimeUnit.HOURS,
                () -> new StockFullSyncJob().executeScheduler());

        INTERNAL_STARTED.set(true);
        log.info("AutoProvisionamento: fallback interno ativado.");
    }

    private static synchronized void stopInternalFallback() {
        ScheduledExecutorService scheduler = internalScheduler;
        internalScheduler = null;
        INTERNAL_STARTED.set(false);
        RUN_GUARD.clear();
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    private static void schedule(String name, long period, TimeUnit unit, ThrowingRunnable task) {
        AtomicBoolean running = RUN_GUARD.computeIfAbsent(name, key -> new AtomicBoolean(false));
        long initialDelay = Math.max(20L, Math.min(120L, unit.toSeconds(period)));

        internalScheduler.scheduleWithFixedDelay(() -> {
            if (!running.compareAndSet(false, true)) {
                log.fine("AutoProvisionamento[" + name + "]: execucao anterior ainda em andamento; skip.");
                return;
            }
            try {
                FastchannelConfig cfg = FastchannelConfig.getInstance();
                if (!cfg.isAtivo()) {
                    return;
                }
                task.run();
            } catch (Exception e) {
                log.log(Level.WARNING, "AutoProvisionamento[" + name + "] falhou.", e);
            } finally {
                running.set(false);
            }
        }, initialDelay, period, unit);
    }

    private static long readPositiveLong(String key, long fallback) {
        String value = System.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            value = System.getenv(key.toUpperCase(Locale.ROOT).replace('.', '_'));
        }
        if (value == null || value.trim().isEmpty()) {
            return fallback;
        }
        try {
            long parsed = Long.parseLong(value.trim());
            return parsed > 0 ? parsed : fallback;
        } catch (Exception e) {
            return fallback;
        }
    }

    @SuppressWarnings("unchecked")
    private static Collection<?> invokeModuleHasActions(BigDecimal codModulo, String ativo) {
        try {
            Class<?> clazz = Class.forName(SCHEDULED_ACTIONS_UTILS_FQN);
            return (Collection<?>) clazz
                    .getMethod("moduleHasActions", BigDecimal.class, String.class)
                    .invoke(null, codModulo, ativo);
        } catch (ClassNotFoundException e) {
            log.info("AutoProvisionamento: ScheduledActionsUtils indisponivel neste runtime.");
            return null;
        } catch (Exception e) {
            log.log(Level.WARNING, "Falha ao consultar moduloHasActions no scheduler nativo.", e);
            return null;
        }
    }

    private static void invokeAction(String method, BigDecimal nuaag) throws Exception {
        Class<?> clazz = Class.forName(SCHEDULED_ACTIONS_UTILS_FQN);
        clazz.getMethod(method, BigDecimal.class).invoke(null, nuaag);
    }

    private static BigDecimal resolveCodModuloByAppKey(String appKey) {
        if (appKey == null || appKey.trim().isEmpty()) {
            return null;
        }
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();

            stmt = conn.prepareStatement(
                    "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                            "WHERE UPPER(COLUMN_NAME) IN ('APPKEY','APP_KEY')"
            );
            rs = stmt.executeQuery();

            while (rs.next()) {
                String table = rs.getString("TABLE_NAME");
                String appKeyColumn = rs.getString("COLUMN_NAME");

                DBUtil.closeAll(null, stmt, null);
                stmt = conn.prepareStatement(
                        "SELECT TOP 1 * FROM INFORMATION_SCHEMA.COLUMNS " +
                                "WHERE TABLE_NAME = ? AND UPPER(COLUMN_NAME) IN ('CODMODULO','COD_MODULO')"
                );
                stmt.setString(1, table);
                ResultSet rsColumns = stmt.executeQuery();
                String codModuloColumn = null;
                if (rsColumns.next()) {
                    codModuloColumn = rsColumns.getString("COLUMN_NAME");
                }
                rsColumns.close();

                if (codModuloColumn == null) {
                    continue;
                }

                DBUtil.closeAll(null, stmt, null);
                stmt = conn.prepareStatement(
                        "SELECT TOP 1 " + codModuloColumn + " AS CODMODULO FROM " + table +
                                " WHERE " + appKeyColumn + " = ?"
                );
                stmt.setString(1, appKey);
                ResultSet rsMod = stmt.executeQuery();
                try {
                    if (rsMod.next()) {
                        return rsMod.getBigDecimal("CODMODULO");
                    }
                } finally {
                    rsMod.close();
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver CODMODULO por APPKEY.", e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return null;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
