package br.com.bellube.fastchannel.action;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.service.strategy.OrderCreationOrchestrator;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;

/**
 * Action para testar disponibilidade das estrategias de criacao de pedidos.
 * Util para diagnostico e troubleshooting.
 */
public class TestarEstrategiasAction implements AcaoRotinaJava {

    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        StringBuilder resultado = new StringBuilder();

        try {
            resultado.append("=== Teste de Estrategias de Criacao de Pedidos ===\n\n");

            // Testar disponibilidade
            OrderCreationOrchestrator orchestrator = new OrderCreationOrchestrator();
            String testResult = orchestrator.testStrategies();
            resultado.append(testResult).append("\n\n");

            // Mostrar configuracao
            resultado.append("=== Configuracao Atual ===\n");
            FastchannelConfig config = FastchannelConfig.getInstance();

            String serverUrl = config.getSankhyaServerUrl();
            String user = config.getSankhyaUser();
            String password = config.getSankhyaPassword();

            resultado.append("SANKHYA_SERVER_URL: ")
                    .append(serverUrl != null ? serverUrl : "[NAO CONFIGURADO]")
                    .append("\n");

            resultado.append("SANKHYA_USER: ")
                    .append(user != null ? user : "[NAO CONFIGURADO]")
                    .append("\n");

            resultado.append("SANKHYA_PASSWORD: ")
                    .append(password != null ? "****** (configurado)" : "[NAO CONFIGURADO]")
                    .append("\n");

            resultado.append("SYNC_STATUS_ENABLED: ")
                    .append(config.isSyncStatusEnabled() ? "SIM" : "NAO")
                    .append("\n");

            resultado.append("\n=== Recomendacoes ===\n");

            if (serverUrl == null || serverUrl.isEmpty()) {
                resultado.append("[ATENCAO] SANKHYA_SERVER_URL nao configurado. Estrategia HTTP nao funcionara.\n");
            }
            if (user == null || user.isEmpty()) {
                resultado.append("[ATENCAO] SANKHYA_USER nao configurado. Autenticacao HTTP nao funcionara.\n");
            }
            if (password == null || password.isEmpty()) {
                resultado.append("[ATENCAO] SANKHYA_PASSWORD nao configurado. Autenticacao HTTP nao funcionara.\n");
            }

            resultado.append("\n[INFO] Ordem padrao: InternalAPI (somente)\n");
            resultado.append("[INFO] HTTP/ServiceInvoker so sao habilitados por opt-in (fastchannel.order.enableLegacyFallbacks=true).\n");

        } catch (Exception e) {
            resultado.append("\n[ERRO] Falha no teste: ").append(e.getMessage());
        }

        contexto.setMensagemRetorno(resultado.toString());
    }
}
