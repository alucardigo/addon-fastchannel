package br.com.bellube.fastchannel.listener;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.QueueService;
import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.vo.DynamicVO;

import java.math.BigDecimal;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Listener de Produto (TGFPRO).
 *
 * Captura alterações em produtos e enfileira para sincronização.
 * Também atualiza o De-Para quando REFERENCIA é alterada.
 *
 * Configuração no Sankhya:
 * - Eventos Programáveis > Listeners
 * - Entidade: Produto (TGFPRO)
 * - Eventos: afterInsert, afterUpdate
 */
public class ProdutoListener implements EventoProgramavelJava {

    private static final Logger log = Logger.getLogger(ProdutoListener.class.getName());

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
        processProdutoChange(event, FastchannelConstants.OPERATION_CREATE);
    }

    @Override
    public void afterUpdate(PersistenceEvent event) throws Exception {
        processProdutoChange(event, FastchannelConstants.OPERATION_UPDATE);
    }

    @Override
    public void afterDelete(PersistenceEvent event) throws Exception {
        processProdutoDelete(event);
    }

    @Override
    public void beforeCommit(TransactionContext transactionContext) throws Exception {
        // Not used
    }

    public void executeScheduler() throws Exception {
        // Not used - this is a listener, not a scheduler
    }

    private void processProdutoChange(PersistenceEvent event, String operation) {
        try {
            // Verificar se integração está ativa
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                return;
            }

            DynamicVO vo = (DynamicVO) event.getVo();
            BigDecimal codProd = vo.asBigDecimal("CODPROD");
            String referencia = vo.asString("REFERENCIA");
            String ativo = vo.asString("ATIVO");

            // Determinar SKU (REFERENCIA ou outro campo)
            String sku = referencia;
            if (sku == null || sku.isEmpty()) {
                log.fine("Produto " + codProd + " sem REFERENCIA. Ignorando.");
                return;
            }

            // Atualizar De-Para
            DeparaService deparaService = DeparaService.getInstance();
            deparaService.setMapping(DeparaService.TIPO_PRODUTO, codProd, sku);

            // Verificar se produto está ativo
            if (!"S".equals(ativo)) {
                // Produto inativado - enfileirar para zerar estoque
                QueueService queueService = QueueService.getInstance();
                queueService.enqueueStock(codProd, sku, BigDecimal.ZERO);
                log.info("Produto inativado: CODPROD " + codProd + " - Estoque será zerado");
                return;
            }

            // Enfileirar para sincronização
            QueueService queueService = QueueService.getInstance();
            queueService.enqueueProduct(codProd, sku, operation);

            log.info("Produto enfileirado: CODPROD " + codProd + " (SKU " + sku + ") - " + operation);

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao processar alteração de produto", e);
        }
    }

    private void processProdutoDelete(PersistenceEvent event) {
        try {
            // Verificar se integração está ativa
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                return;
            }

            DynamicVO vo = (DynamicVO) event.getVo();
            BigDecimal codProd = vo.asBigDecimal("CODPROD");

            // Obter SKU antes de remover do De-Para
            DeparaService deparaService = DeparaService.getInstance();
            String sku = deparaService.getSku(codProd);

            if (sku != null && !sku.isEmpty()) {
                // Enfileirar para zerar estoque
                QueueService queueService = QueueService.getInstance();
                queueService.enqueueStock(codProd, sku, BigDecimal.ZERO);

                // Remover do De-Para
                deparaService.removeMapping(DeparaService.TIPO_PRODUTO, codProd);

                log.info("Produto removido: CODPROD " + codProd + " (SKU " + sku + ")");
            }

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao processar exclusão de produto", e);
        }
    }
}
