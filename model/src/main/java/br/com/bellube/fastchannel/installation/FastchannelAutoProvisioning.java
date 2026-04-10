package br.com.bellube.fastchannel.installation;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.job.OrderImportJob;
import br.com.bellube.fastchannel.job.OrderStatusSyncJob;
import br.com.bellube.fastchannel.job.OutboxProcessorJob;
import br.com.bellube.fastchannel.job.PriceFullSyncJob;
import br.com.bellube.fastchannel.job.StockFullSyncJob;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.util.DBUtil;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.vo.DynamicVO;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.ToLongFunction;
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
    private static final Map<String, Long> LAST_ACTUAL_RUN_MS = new ConcurrentHashMap<>();
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

        // [CRIT-PURGE] One-shot: processar entradas pendentes em AD_FCDUPPURGE
        // usando a sessao JDBC do WildFly (inherit corretas SET options do DataSource).
        // Essas entradas vieram da sessao sqlcmd que falhava em trigger SSPMB
        // por mismatch de SET ANSI_NULLS/QUOTED_IDENTIFIER com indexed views.
        try {
            runOneShotPurgeAdNumFastDups();
        } catch (Throwable t) {
            log.log(Level.WARNING, "One-shot purge AD_NUMFAST dups falhou (nao-fatal)", t);
        }
    }

    /**
     * [CRIT-PURGE] Purga duplicatas AD_NUMFAST remanescentes bloqueadas pela trigger
     * TRG_INC_UPD_DLT_TGFFIN_SSPMB (Sankhya bug com SET options OFF em indexed views).
     *
     * Le NUNOTAs de AD_FCDUPPURGE onde TGFCAB.AD_NUMFAST ainda nao foi nullado e
     * aplica UPDATE via JDBC do DataSource pool - que herda as SET options corretas
     * do WildFly (opcoes default do MGEDS).
     *
     * Fail-open: qualquer erro apenas loga warning, nao bloqueia startup.
     */
    private static void runOneShotPurgeAdNumFastDups() {
        java.sql.Connection conn = null;
        java.sql.PreparedStatement selectPs = null;
        java.sql.PreparedStatement updatePs = null;
        java.sql.ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            selectPs = conn.prepareStatement(
                "SELECT p.NUNOTA FROM AD_FCDUPPURGE p " +
                "INNER JOIN TGFCAB c ON c.NUNOTA = p.NUNOTA " +
                "WHERE c.AD_NUMFAST IS NOT NULL"
            );
            rs = selectPs.executeQuery();
            java.util.List<java.math.BigDecimal> pending = new java.util.ArrayList<>();
            while (rs.next()) {
                pending.add(rs.getBigDecimal(1));
            }
            DBUtil.closeAll(rs, selectPs, null);
            rs = null; selectPs = null;

            if (pending.isEmpty()) {
                log.fine("AutoProvisionamento: nenhuma entrada AD_FCDUPPURGE pendente.");
                return;
            }

            log.info("AutoProvisionamento: [CRIT-PURGE] " + pending.size()
                + " entrada(s) AD_FCDUPPURGE pendentes. Executando UPDATE via JDBC pool.");

            updatePs = conn.prepareStatement(
                "UPDATE TGFCAB SET AD_NUMFAST = NULL WHERE NUNOTA = ? AND AD_NUMFAST IS NOT NULL");

            int success = 0;
            int failed = 0;
            for (java.math.BigDecimal nunota : pending) {
                try {
                    updatePs.setBigDecimal(1, nunota);
                    updatePs.executeUpdate();
                    success++;
                } catch (Exception e) {
                    failed++;
                    if (failed < 5) {
                        log.log(Level.WARNING, "[CRIT-PURGE] UPDATE falhou para NUNOTA=" + nunota, e);
                    }
                }
            }

            log.info("AutoProvisionamento: [CRIT-PURGE] concluido. success=" + success + " failed=" + failed);
        } catch (Exception e) {
            log.log(Level.WARNING, "[CRIT-PURGE] Erro geral no one-shot purge", e);
        } finally {
            DBUtil.closeAll(rs, selectPs, null);
            DBUtil.closeAll(null, updatePs, conn);
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

        // Sincronizacao preventiva de De-Para de produtos: roda no startup e a cada 24h
        schedule("depara-sync", readPositiveLong("fc.auto.depara.hours", 24), TimeUnit.HOURS,
                () -> DeparaService.getInstance().syncProductDeparaFromRefforn());
        scheduleDynamic("order-import", 30,
                cfg -> cfg.getIntervalOrders() * 60_000L,
                () -> new OrderImportJob().executeScheduler());
        scheduleDynamic("outbox", 60,
                cfg -> cfg.getIntervalQueue() * 60_000L,
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
        LAST_ACTUAL_RUN_MS.clear();
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    private static void scheduleDynamic(String name, int initialDelaySecs,
            ToLongFunction<FastchannelConfig> intervalMsSupplier, ThrowingRunnable task) {
        AtomicBoolean running = RUN_GUARD.computeIfAbsent(name, key -> new AtomicBoolean(false));
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
                long intervalMs = intervalMsSupplier.applyAsLong(cfg);
                Long lastRun = LAST_ACTUAL_RUN_MS.get(name);
                if (lastRun != null && System.currentTimeMillis() - lastRun < intervalMs) {
                    log.fine("AutoProvisionamento[" + name + "]: intervalo configurado ainda nao atingido; skip.");
                    return;
                }
                LAST_ACTUAL_RUN_MS.put(name, System.currentTimeMillis());
                runInJapeSession(name, task);
            } catch (Exception e) {
                log.log(Level.WARNING, "AutoProvisionamento[" + name + "] falhou.", e);
            } finally {
                running.set(false);
            }
        }, initialDelaySecs, 60, TimeUnit.SECONDS);
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
                runInJapeSession(name, task);
            } catch (Exception e) {
                log.log(Level.WARNING, "AutoProvisionamento[" + name + "] falhou.", e);
            } finally {
                running.set(false);
            }
        }, initialDelay, period, unit);
    }

    /**
     * [CRIT-4] Executa uma task do scheduler dentro de um contexto JapeSession.
     *
     * Por que eh necessario:
     *  1) O ScheduledExecutorService (daemon thread) nao tem contexto EJB,
     *     entao EntityFacadeFactory.getCoreFacade() lanca "Erro ao inicializar
     *     datasource para provider mge-core" porque o DataSourceDescriptor e
     *     resolvido via JNDI atrelado ao ThreadLocal do JAPE.
     *  2) Sem JapeSession.open() aberta, AD_FCMAP (upsertOrderMapping) nao e gravado,
     *     fazendo o Dashboard mentir ("Importados Hoje: 0") e pedidos mostrarem "--"
     *     em Cliente/Data/ValorTotal (relatorio QA 2026-04-09).
     *  3) Sem contexto de usuario, CACSP.incluirNota grava as notas com CODUSU=0
     *     (anonimo) e o AD_NUMFAST pode nao persistir (relatorio homolog 2026-04-09).
     *
     * Estrategia: tentar abrir JapeSession; se falhar (classloader ainda quebrado),
     * cai para execucao crua para NAO bloquear importacao. Fail-open por design.
     */
    private static void runInJapeSession(String name, ThrowingRunnable task) throws Exception {
        JapeSession.SessionHandle hnd = null;
        try {
            try {
                hnd = JapeSession.open();
            } catch (Throwable openT) {
                log.log(Level.FINE, "AutoProvisionamento[" + name
                    + "]: JapeSession.open() falhou, rodando sem session ("
                    + openT.getClass().getSimpleName() + ")", openT);
            }
            task.run();
        } finally {
            if (hnd != null) {
                try { JapeSession.close(hnd); } catch (Throwable ignored) {}
            }
        }
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
