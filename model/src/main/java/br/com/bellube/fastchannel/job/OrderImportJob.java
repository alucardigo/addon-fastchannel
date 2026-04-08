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
 * Job Agendado para Importacao de Pedidos do Fastchannel.
 *
 * Executa periodicamente (configuravel) para buscar novos pedidos
 * da API Fastchannel e importa-los para o Sankhya.
 *
 * Configuracao no Sankhya:
 * - Eventos Programaveis > Agendamento
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
        log.info("=== Iniciando Job de Importacao de Pedidos Fastchannel ===");

        LogService logService = LogService.getInstance();
        FastchannelConfig config = FastchannelConfig.getInstance();

        try {
            // Verificar se integracao esta ativa
            if (!config.isAtivo()) {
                log.info("Integracao Fastchannel desativada. Job ignorado.");
                return;
            }

            // Validar configuracao
            if (config.getClientId() == null || config.getClientSecret() == null) {
                log.warning("Configuracao Fastchannel incompleta. Job ignorado.");
                logService.warning(LogService.OP_ORDER_IMPORT, "Configuracao incompleta");
                return;
            }

            // Executar importacao
            OrderService orderService = new OrderService();
            int imported = orderService.importPendingOrders();

            String message = "Job concluido. " + imported + " pedidos importados.";
            log.info(message);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro no Job de Importacao de Pedidos", e);
            logService.error(LogService.OP_ORDER_IMPORT, "Erro no job", e);
            throw e;
        }

        log.info("=== Job de Importacao de Pedidos Finalizado ===");
    }
}
