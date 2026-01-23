package br.com.bellube.fastchannel.job;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.service.LogService;
import br.com.bellube.fastchannel.service.OrderService;
import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Job Agendado para Importação de Pedidos do Fastchannel.
 *
 * Executa periodicamente (configurável) para buscar novos pedidos
 * da API Fastchannel e importá-los para o Sankhya.
 *
 * Configuração no Sankhya:
 * - Eventos Programáveis > Agendamento
 * - Classe: br.com.bellube.fastchannel.job.OrderImportJob
 * - Intervalo recomendado: 5-10 minutos
 */
public class OrderImportJob implements EventoProgramavelJava {

    private static final Logger log = Logger.getLogger(OrderImportJob.class.getName());

    @Override
    public void beforeInsert(PersistenceEvent event) throws Exception {
        // Not used
    }

    @Override
    public void beforeUpdate(PersistenceEvent event) throws Exception {
        // Not used
    }

    @Override
    public void beforeDelete(PersistenceEvent event) throws Exception {
        // Not used
    }

    @Override
    public void afterInsert(PersistenceEvent event) throws Exception {
        // Not used
    }

    @Override
    public void afterUpdate(PersistenceEvent event) throws Exception {
        // Not used
    }

    @Override
    public void afterDelete(PersistenceEvent event) throws Exception {
        // Not used
    }

    @Override
    public void beforeCommit(TransactionContext transactionContext) throws Exception {
        // Not used
    }

    public void executeScheduler() throws Exception {
        log.info("=== Iniciando Job de Importação de Pedidos Fastchannel ===");

        LogService logService = LogService.getInstance();
        FastchannelConfig config = FastchannelConfig.getInstance();

        try {
            // Verificar se integração está ativa
            if (!config.isAtivo()) {
                log.info("Integração Fastchannel desativada. Job ignorado.");
                return;
            }

            // Validar configuração
            if (config.getClientId() == null || config.getClientSecret() == null) {
                log.warning("Configuração Fastchannel incompleta. Job ignorado.");
                logService.warning(LogService.OP_ORDER_IMPORT, "Configuração incompleta");
                return;
            }

            // Executar importação
            OrderService orderService = new OrderService();
            int imported = orderService.importPendingOrders();

            String message = "Job concluído. " + imported + " pedidos importados.";
            log.info(message);
            logService.info(LogService.OP_ORDER_IMPORT, message);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro no Job de Importação de Pedidos", e);
            logService.error(LogService.OP_ORDER_IMPORT, "Erro no job", e);
            throw e;
        }

        log.info("=== Job de Importação de Pedidos Finalizado ===");
    }
}
