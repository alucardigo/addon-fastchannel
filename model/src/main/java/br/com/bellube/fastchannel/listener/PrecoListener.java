package br.com.bellube.fastchannel.listener;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.PriceTableResolver;
import br.com.bellube.fastchannel.service.QueueService;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.PersistenceEventAdapter;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.studio.annotations.Listener;

import java.math.BigDecimal;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Listener de Preco (TGFEXC - Excecao de Preco).
 *
 * Captura alteracoes de preco na tabela de precos e enfileira
 * para sincronizacao com o Fastchannel.
 */
@Listener(instanceNames = {"ExcecaoPreco"})
public class PrecoListener extends PersistenceEventAdapter {

    private static final Logger log = Logger.getLogger(PrecoListener.class.getName());

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
        processPrecoChange(event);
    }

    private void processPrecoChange(PersistenceEvent event) {
        try {
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                return;
            }

            DynamicVO vo = (DynamicVO) event.getVo();
            BigDecimal codProd = vo.asBigDecimal("CODPROD");
            BigDecimal nuTab = vo.asBigDecimal("NUTAB");

            List<BigDecimal> eligibleTables = new PriceTableResolver().resolveEligibleTables();
            if (!eligibleTables.isEmpty()) {
                if (!eligibleTables.contains(nuTab)) {
                    log.fine("Tabela de preco " + nuTab + " fora da lista elegivel. Ignorando.");
                    return;
                }
            } else {
                BigDecimal configNuTab = config.getNuTab();
                if (configNuTab != null && !configNuTab.equals(nuTab)) {
                    return;
                }
            }

            String ativo = vo.asString("ATIVO");
            if (!"S".equals(ativo)) {
                return;
            }

            DeparaService deparaService = DeparaService.getInstance();
            if (!deparaService.isIntegracaoAutomaticaAtiva(DeparaService.TIPO_TABELA_PRECO, nuTab)) {
                log.fine("Tabela de preco " + nuTab + " com integracao automatica desabilitada.");
                return;
            }
            String sku = deparaService.getSkuForStock(codProd);
            if (sku != null) {
                sku = sku.trim();
            }

            if (sku == null || sku.isEmpty()) {
                log.fine("Produto " + codProd + " nao tem SKU mapeado. Ignorando.");
                return;
            }

            QueueService queueService = QueueService.getInstance();
            queueService.enqueuePrice(codProd, sku);

            log.info("Preco enfileirado: CODPROD " + codProd + " (SKU " + sku + ")");

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao processar alteracao de preco", e);
        }
    }
}
