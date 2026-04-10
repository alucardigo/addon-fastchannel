package br.com.bellube.fastchannel.listener;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.QueueService;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.PersistenceEventAdapter;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.studio.annotations.Listener;

import java.math.BigDecimal;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Listener de Produto (TGFPRO).
 *
 * Captura alteracoes em produtos e enfileira para sincronizacao.
 * Tambem atualiza o De-Para quando REFERENCIA e alterada.
 */
@Listener(instanceNames = {"Produto"})
public class ProdutoListener extends PersistenceEventAdapter {

    private static final Logger log = Logger.getLogger(ProdutoListener.class.getName());

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

    private void processProdutoChange(PersistenceEvent event, String operation) {
        try {
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                return;
            }

            DynamicVO vo = (DynamicVO) event.getVo();
            // [P2-8] event.getVo() pode vir null em cascata - evita NPE
            if (vo == null) {
                log.fine("ProdutoListener: event.getVo() null em insert/update, ignorando");
                return;
            }
            BigDecimal codProd = vo.asBigDecimal("CODPROD");
            String referencia = vo.asString("REFERENCIA");
            String ativo = vo.asString("ATIVO");

            String sku = referencia;
            if (sku == null || sku.isEmpty()) {
                log.fine("Produto " + codProd + " sem REFERENCIA. Ignorando.");
                return;
            }

            DeparaService deparaService = DeparaService.getInstance();
            deparaService.setMapping(DeparaService.TIPO_PRODUTO, codProd, sku);

            if (!"S".equals(ativo)) {
                QueueService queueService = QueueService.getInstance();
                queueService.enqueueStock(codProd, sku, BigDecimal.ZERO);
                log.info("Produto inativado: CODPROD " + codProd + " - Estoque sera zerado");
                return;
            }

            QueueService queueService = QueueService.getInstance();
            queueService.enqueueProduct(codProd, sku, operation);

            log.info("Produto enfileirado: CODPROD " + codProd + " (SKU " + sku + ") - " + operation);

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao processar alteracao de produto", e);
        }
    }

    private void processProdutoDelete(PersistenceEvent event) {
        try {
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                return;
            }

            DynamicVO vo = (DynamicVO) event.getVo();
            // [P2-8] event.getVo() pode vir null em cascata - evita NPE
            if (vo == null) {
                log.fine("ProdutoListener: event.getVo() null em delete, ignorando");
                return;
            }
            BigDecimal codProd = vo.asBigDecimal("CODPROD");

            DeparaService deparaService = DeparaService.getInstance();
            String sku = deparaService.getSku(codProd);

            if (sku != null && !sku.isEmpty()) {
                QueueService queueService = QueueService.getInstance();
                queueService.enqueueStock(codProd, sku, BigDecimal.ZERO);

                deparaService.removeMapping(DeparaService.TIPO_PRODUTO, codProd);

                log.info("Produto removido: CODPROD " + codProd + " (SKU " + sku + ")");
            }

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao processar exclusao de produto", e);
        }
    }
}
