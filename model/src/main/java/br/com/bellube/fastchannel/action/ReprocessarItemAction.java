package br.com.bellube.fastchannel.action;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.service.LogService;
import br.com.bellube.fastchannel.service.QueueService;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;

import java.math.BigDecimal;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Ação para reprocessar item específico da fila.
 * Pode ser usado para itens com erro ou pendentes.
 */
public class ReprocessarItemAction implements AcaoRotinaJava {

    private static final Logger log = Logger.getLogger(ReprocessarItemAction.class.getName());

    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        StringBuilder resultado = new StringBuilder();

        try {
            // Verificar se integração está ativa
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                resultado.append("[ERRO] Integração não está ativa!");
                contexto.setMensagemRetorno(resultado.toString());
                return;
            }

            // Obter registros selecionados
            Registro[] registros = contexto.getLinhas();

            if (registros == null || registros.length == 0) {
                resultado.append("[ERRO] Nenhum item selecionado para reprocessar.");
                contexto.setMensagemRetorno(resultado.toString());
                return;
            }

            resultado.append("=== Reprocessamento de Itens ===\n\n");
            resultado.append("Itens selecionados: ").append(registros.length).append("\n\n");

            QueueService queueService = QueueService.getInstance();
            int sucesso = 0;
            int falha = 0;

            for (Registro registro : registros) {
                BigDecimal idQueue = (BigDecimal) registro.getCampo("IDQUEUE");
                String entityType = (String) registro.getCampo("ENTITY_TYPE");
                String entityKey = (String) registro.getCampo("ENTITY_KEY");

                try {
                    // Resetar status para PENDING e zerar tentativas
                    queueService.resetForRetry(idQueue);

                    resultado.append("[OK] Item ").append(idQueue)
                            .append(" (").append(entityType).append(": ").append(entityKey)
                            .append(") marcado para reprocessamento\n");
                    sucesso++;

                    LogService.getInstance().info(LogService.OP_QUEUE_PROCESS,
                            "Item marcado para reprocessamento: " + idQueue,
                            entityType + ": " + entityKey);

                } catch (Exception e) {
                    resultado.append("[ERRO] Item ").append(idQueue)
                            .append(": ").append(e.getMessage()).append("\n");
                    falha++;

                    log.log(Level.WARNING, "Erro ao marcar item para reprocessamento: " + idQueue, e);
                }
            }

            resultado.append("\n=== Resultado ===\n");
            resultado.append("Sucesso: ").append(sucesso).append("\n");
            resultado.append("Falha: ").append(falha).append("\n");

            if (sucesso > 0) {
                resultado.append("\nOs itens foram marcados para reprocessamento.\n");
                resultado.append("Execute 'Processar Fila Agora' na tela de Configuração\n");
                resultado.append("ou aguarde o processamento automático.");
            }

        } catch (Exception e) {
            resultado.append("\n[ERRO] Falha no reprocessamento: ").append(e.getMessage());
            log.log(Level.SEVERE, "Erro no reprocessamento de itens", e);
        }

        contexto.setMensagemRetorno(resultado.toString());
    }
}
