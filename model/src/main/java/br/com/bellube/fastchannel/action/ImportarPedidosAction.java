package br.com.bellube.fastchannel.action;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.job.OrderImportJob;
import br.com.bellube.fastchannel.service.LogService;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Ação para forçar importação de pedidos do Fastchannel.
 * Executa o job de importação imediatamente.
 */
public class ImportarPedidosAction implements AcaoRotinaJava {

    private static final Logger log = Logger.getLogger(ImportarPedidosAction.class.getName());

    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        StringBuilder resultado = new StringBuilder();

        try {
            resultado.append("=== Importação Manual de Pedidos ===\n\n");

            // Verificar se integração está ativa
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                resultado.append("[ERRO] Integração não está ativa!\n");
                resultado.append("Ative a integração na configuração antes de importar pedidos.");
                contexto.setMensagemRetorno(resultado.toString());
                return;
            }

            resultado.append("Iniciando importação de pedidos...\n\n");

            // Criar e executar o job de importação
            OrderImportJob importJob = new OrderImportJob();

            // Log de início
            LogService.getInstance().info(LogService.OP_ORDER_IMPORT,
                    "Importação manual iniciada", "Ação do usuário");

            long startTime = System.currentTimeMillis();

            // Executar importação
            importJob.executeScheduler();

            long endTime = System.currentTimeMillis();
            long duration = (endTime - startTime) / 1000;

            resultado.append("[SUCESSO] Importação concluída!\n\n");
            resultado.append("Tempo de execução: ").append(duration).append(" segundos\n");
            resultado.append("\nVerifique a tela de Logs para detalhes dos pedidos importados.\n");
            resultado.append("Verifique a tela de Pedidos Fastchannel para ver os pedidos criados.");

            // Log de sucesso
            LogService.getInstance().info(LogService.OP_ORDER_IMPORT,
                    "Importação manual concluída", "Duração: " + duration + "s");

        } catch (Exception e) {
            resultado.append("\n[ERRO] Falha na importação: ").append(e.getMessage()).append("\n");
            resultado.append("\nVerifique a tela de Logs para mais detalhes do erro.");

            log.log(Level.SEVERE, "Erro na importação manual de pedidos", e);
            LogService.getInstance().error(LogService.OP_ORDER_IMPORT,
                    "Falha na importação manual", e);
        }

        contexto.setMensagemRetorno(resultado.toString());
    }
}
