package br.com.bellube.fastchannel.listener;

import br.com.bellube.fastchannel.config.FastchannelConfig;
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
 * Listener de Preço (TGFEXC - Exceção de Preço).
 *
 * Captura alterações de preço na tabela de preços e enfileira
 * para sincronização com o Fastchannel.
 *
 * Configuração no Sankhya:
 * - Eventos Programáveis > Listeners
 * - Entidade: ExcecaoPreco (TGFEXC)
 * - Eventos: afterInsert, afterUpdate
 */
public class PrecoListener implements EventoProgramavelJava {

    private static final Logger log = Logger.getLogger(PrecoListener.class.getName());

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
        processPrecoChange(event);
    }

    @Override
    public void afterUpdate(PersistenceEvent event) throws Exception {
        processPrecoChange(event);
    }

    @Override
    public void afterDelete(PersistenceEvent event) throws Exception {
        // Preço removido - pode ser necessário notificar
        processPrecoChange(event);
    }

    @Override
    public void beforeCommit(TransactionContext transactionContext) throws Exception {
        // Not used
    }

    public void executeScheduler() throws Exception {
        // Not used - this is a listener, not a scheduler
    }

    private void processPrecoChange(PersistenceEvent event) {
        try {
            // Verificar se integração está ativa
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                return;
            }

            DynamicVO vo = (DynamicVO) event.getVo();
            BigDecimal codProd = vo.asBigDecimal("CODPROD");
            BigDecimal nuTab = vo.asBigDecimal("NUTAB");

            // Verificar se é a tabela de preço configurada
            BigDecimal configNuTab = config.getNuTab();
            if (configNuTab != null && !configNuTab.equals(nuTab)) {
                return;
            }

            // Verificar se está ativo
            String ativo = vo.asString("ATIVO");
            if (!"S".equals(ativo)) {
                return;
            }

            // Obter SKU do produto
            DeparaService deparaService = DeparaService.getInstance();
            String sku = deparaService.getSkuWithFallback(codProd);

            if (sku == null || sku.isEmpty()) {
                log.fine("Produto " + codProd + " não tem SKU mapeado. Ignorando.");
                return;
            }

            // Enfileirar para sincronização
            QueueService queueService = QueueService.getInstance();
            queueService.enqueuePrice(codProd, sku);

            log.info("Preço enfileirado: CODPROD " + codProd + " (SKU " + sku + ")");

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao processar alteração de preço", e);
        }
    }
}
