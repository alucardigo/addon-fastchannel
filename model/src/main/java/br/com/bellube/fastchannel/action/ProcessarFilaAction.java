package br.com.bellube.fastchannel.action;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.job.OutboxProcessorJob;
import br.com.bellube.fastchannel.service.LogService;
import br.com.bellube.fastchannel.service.QueueService;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Ação para forçar processamento da fila de sincronização.
 * Processa todos os itens pendentes imediatamente.
 */
public class ProcessarFilaAction implements AcaoRotinaJava {

    private static final Logger log = Logger.getLogger(ProcessarFilaAction.class.getName());

    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        StringBuilder resultado = new StringBuilder();

        try {
            resultado.append("=== Processamento Manual da Fila ===\n\n");

            // Verificar se integração está ativa
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                resultado.append("[ERRO] Integração não está ativa!\n");
                resultado.append("Ative a integração na configuração antes de processar a fila.");
                contexto.setMensagemRetorno(resultado.toString());
                return;
            }

            // Verificar quantidade de itens na fila
            QueueService queueService = QueueService.getInstance();
            int pendingCount = queueService.countPending();
            int errorCount = queueService.countErrors();

            resultado.append("Status atual da fila:\n");
            resultado.append("  - Itens pendentes: ").append(pendingCount).append("\n");
            resultado.append("  - Itens com erro: ").append(errorCount).append("\n\n");

            if (pendingCount == 0 && errorCount == 0) {
                resultado.append("[INFO] Não há itens para processar na fila.\n");
                resultado.append("A fila está vazia.");
                contexto.setMensagemRetorno(resultado.toString());
                return;
            }

            resultado.append("Iniciando processamento...\n\n");

            // Log de início
            LogService.getInstance().info(LogService.OP_QUEUE_PROCESS,
                    "Processamento manual iniciado",
                    "Pendentes: " + pendingCount + ", Erros: " + errorCount);

            long startTime = System.currentTimeMillis();

            // Executar processamento
            OutboxProcessorJob processorJob = new OutboxProcessorJob();
            processorJob.executeScheduler();

            long endTime = System.currentTimeMillis();
            long duration = (endTime - startTime) / 1000;

            // Verificar status após processamento
            int newPendingCount = queueService.countPending();
            int newErrorCount = queueService.countErrors();
            int processed = (pendingCount + errorCount) - (newPendingCount + newErrorCount);

            resultado.append("[SUCESSO] Processamento concluído!\n\n");
            resultado.append("Tempo de execução: ").append(duration).append(" segundos\n");
            resultado.append("Itens processados: ").append(processed).append("\n\n");
            resultado.append("Status após processamento:\n");
            resultado.append("  - Itens pendentes: ").append(newPendingCount).append("\n");
            resultado.append("  - Itens com erro: ").append(newErrorCount).append("\n");

            if (newErrorCount > 0) {
                resultado.append("\n[AVISO] Ainda existem itens com erro na fila.\n");
                resultado.append("Verifique a tela de Fila de Sincronização para detalhes.");
            }

            // Log de sucesso
            LogService.getInstance().info(LogService.OP_QUEUE_PROCESS,
                    "Processamento manual concluído",
                    "Processados: " + processed + ", Duração: " + duration + "s");

        } catch (Exception e) {
            resultado.append("\n[ERRO] Falha no processamento: ").append(e.getMessage()).append("\n");
            resultado.append("\nVerifique a tela de Logs para mais detalhes do erro.");

            log.log(Level.SEVERE, "Erro no processamento manual da fila", e);
            LogService.getInstance().error(LogService.OP_QUEUE_PROCESS,
                    "Falha no processamento manual", e);
        }

        contexto.setMensagemRetorno(resultado.toString());
    }
}
