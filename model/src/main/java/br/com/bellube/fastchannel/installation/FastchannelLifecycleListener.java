package br.com.bellube.fastchannel.installation;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ServletContextListener que garante shutdown limpo do scheduler interno
 * quando o WAR (addon-fastchannel) é undeployed, e tambem faz uma varredura
 * de threads zumbis de classloaders anteriores no boot.
 *
 * <p><b>Problema resolvido</b> ([CRIT 2026-04-27]):
 * Antes deste listener, cada redeploy do EAR ({@code Reinstalar} no Place,
 * ou troca de versao via Area Dev) deixava as threads do
 * {@code ScheduledExecutorService} antigas RODANDO em paralelo com as novas.
 * Resultado: o classloader antigo nao era liberado, o pool antigo continuava
 * disparando jobs com o codigo VELHO (linhas antigas no stack-trace), e a
 * mesma operacao rodava 2x (ou Nx) por ciclo. Confirmado pelos logs:
 * stack-trace pos-redeploy 12:53 mostrava AMBAS as linhas {@code OrderService.java:1840}
 * (v8 antiga) e {@code OrderService.java:1847} (v10 nova) na mesma janela.
 *
 * <p>Agora, no {@code contextDestroyed}, chamamos {@link FastchannelAutoProvisioning#stopAll}
 * para que {@code internalScheduler.shutdownNow()} seja invocado e as daemon threads
 * sejam interrompidas antes do classloader ir embora.
 *
 * <p><b>[FIX 2026-04-28]</b> Adicionado tambem varredura no {@code contextInitialized}:
 * percorre TODAS as threads JVM-wide e interrompe qualquer thread cujo nome
 * comece com {@code fastchannel-auto-} mas nao seja do classloader atual.
 * Isso quebra a cascata de classloaders v8 -> v10 -> v11 que se acumulavam quando
 * stopAll falhava no contextDestroyed (por exemplo se {@code awaitTermination(15s)}
 * estourava sem os I/O bloqueados terminarem).
 */
public class FastchannelLifecycleListener implements ServletContextListener {
    private static final Logger log = Logger.getLogger(FastchannelLifecycleListener.class.getName());

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        log.info("[FastchannelLifecycleListener] contextInitialized - aguardando ensureStarted via boot do modulo Sankhya.");
        try {
            interruptZombieFastchannelThreads();
        } catch (Throwable t) {
            log.log(Level.WARNING, "[FastchannelLifecycleListener] Erro ao varrer threads zumbis", t);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        try {
            log.info("[FastchannelLifecycleListener] contextDestroyed - solicitando shutdown do scheduler interno.");
            // appkey e codModulo nao sao necessarios para o caminho interno (so afetam stopNativeScheduledActions)
            FastchannelAutoProvisioning.stopAll(null, null);
            log.info("[FastchannelLifecycleListener] Scheduler interno desligado com sucesso.");
        } catch (Throwable t) {
            log.log(Level.WARNING, "[FastchannelLifecycleListener] Erro ao desligar scheduler interno", t);
        }
    }

    /**
     * Varre todas as threads da JVM e INTERROMPE qualquer "fastchannel-auto-*"
     * que pertenca a um classloader DIFERENTE do nosso. Essas threads sao
     * remanescentes de redeploys anteriores (v8/v10/v11) que nao foram limpas
     * pelo seu proprio contextDestroyed (timeout no awaitTermination, por exemplo).
     *
     * <p>Estrategia segura: interrupt() apenas. Nao usamos Thread.stop() (deprecated +
     * inseguro). interrupt() libera I/O bloqueado em waits/sleeps. Threads que
     * persistirem apos isso devem ser eliminadas pelo GC do classloader.
     */
    private void interruptZombieFastchannelThreads() {
        ClassLoader currentCl = FastchannelLifecycleListener.class.getClassLoader();
        ThreadGroup root = Thread.currentThread().getThreadGroup();
        while (root.getParent() != null) {
            root = root.getParent();
        }
        Thread[] threads = new Thread[root.activeCount() * 2 + 32];
        int count = root.enumerate(threads, true);

        int interrupted = 0;
        int matched = 0;
        for (int i = 0; i < count; i++) {
            Thread t = threads[i];
            if (t == null) continue;
            String name = t.getName();
            if (name == null || !name.startsWith("fastchannel-auto-")) continue;

            matched++;
            ClassLoader threadCl = null;
            try {
                threadCl = t.getContextClassLoader();
            } catch (Throwable ignored) {}

            if (threadCl != null && threadCl != currentCl) {
                try {
                    log.warning("[FastchannelLifecycleListener] Interrompendo thread zumbi de classloader anterior: name="
                            + name + " state=" + t.getState() + " cl=" + threadCl);
                    t.interrupt();
                    interrupted++;
                } catch (Throwable interruptT) {
                    log.log(Level.FINE, "[FastchannelLifecycleListener] interrupt falhou em "
                            + name, interruptT);
                }
            }
        }

        if (matched == 0) {
            log.info("[FastchannelLifecycleListener] Varredura zumbis: nenhuma thread fastchannel-auto-* encontrada.");
        } else {
            log.info("[FastchannelLifecycleListener] Varredura zumbis: encontradas="
                    + matched + " interrompidas=" + interrupted
                    + " (do classloader atual=" + (matched - interrupted) + ").");
        }
    }
}
