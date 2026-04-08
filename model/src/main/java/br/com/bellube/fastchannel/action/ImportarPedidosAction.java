package br.com.bellube.fastchannel.action;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.job.OrderImportJob;
import br.com.bellube.fastchannel.service.LogService;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Acao para forcar importacao de pedidos do Fastchannel.
 * Executa o job de importacao imediatamente.
 */
public class ImportarPedidosAction implements AcaoRotinaJava {

    private static final Logger log = Logger.getLogger(ImportarPedidosAction.class.getName());

    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        StringBuilder resultado = new StringBuilder();

        try {
            resultado.append("=== Importacao Manual de Pedidos ===\n\n");

            // Verificar se integracao esta ativa
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                resultado.append("[ERRO] Integracao nao esta ativa!\n");
                resultado.append("Ative a integracao na configuracao antes de importar pedidos.");
                contexto.setMensagemRetorno(resultado.toString());
                return;
            }

            resultado.append("Iniciando importacao de pedidos...\n\n");

            // Criar e executar o job de importacao
            OrderImportJob importJob = new OrderImportJob();

            // Log de inicio
            LogService.getInstance().info(LogService.OP_ORDER_IMPORT,
                    "Importacao manual iniciada", "Acao do usuario");

            long startTime = System.currentTimeMillis();

            // Executar importacao
            importJob.executeScheduler();

            long endTime = System.currentTimeMillis();
            long duration = (endTime - startTime) / 1000;

            resultado.append("[SUCESSO] Importacao concluida!\n\n");
            resultado.append("Tempo de execucao: ").append(duration).append(" segundos\n");
            resultado.append("\nVerifique a tela de Logs para detalhes dos pedidos importados.\n");
            resultado.append("Verifique a tela de Pedidos Fastchannel para ver os pedidos criados.");

            // Log de sucesso
            LogService.getInstance().info(LogService.OP_ORDER_IMPORT,
                    "Importacao manual concluida", "Duracao: " + duration + "s");

        } catch (Exception e) {
            resultado.append("\n[ERRO] Falha na importacao: ").append(e.getMessage()).append("\n");
            resultado.append("\nVerifique a tela de Logs para mais detalhes do erro.");

            log.log(Level.SEVERE, "Erro na importacao manual de pedidos", e);
            LogService.getInstance().error(LogService.OP_ORDER_IMPORT,
                    "Falha na importacao manual", e);
        }

        contexto.setMensagemRetorno(resultado.toString());
    }
}
