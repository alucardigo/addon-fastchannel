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
 *
 * [FIX 2026-06-01 v1.2.90] O nome da instancia estava "ExcecaoPreco", que NAO EXISTE
 * no dicionario Sankhya — por isso o listener NUNCA disparava nas alteracoes de preco
 * feitas na tela de Gestao de Precos/Tabela. Confirmado no log de PROD: a entidade real
 * persistida para TGFEXC e "Excecao" (entityName="Excecao", crudListener=ExcecaoCrudListener,
 * campos VLRVENDA/NUTAB/CODPROD/TIPO). Sem este fix, as alteracoes pontuais da operadora
 * so chegavam na FC pelo AutoPriceChangesSweepJob (poll de 10 min) — lag de ate ~15 min.
 * Agora o listener dispara na hora e o preco vai pro outbox no proximo ciclo (~1 min).
 * Mantido "ExcecaoPreco" por seguranca (instancia inexistente apenas nunca dispara).
 */
@Listener(instanceNames = {"Excecao", "ExcecaoPreco"})
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
            // [P2-8] Em deletes em cascata, event.getVo() pode vir null - evita NPE silencioso
            if (vo == null) {
                log.fine("PrecoListener: event.getVo() null, ignorando evento");
                return;
            }
            BigDecimal codProd = vo.asBigDecimal("CODPROD");
            BigDecimal nuTab = vo.asBigDecimal("NUTAB");

            List<BigDecimal> eligibleTables = new PriceTableResolver().resolveEligibleTables();
            if (!eligibleTables.isEmpty()) {
                if (nuTab != null && !eligibleTables.contains(nuTab)) {
                    log.fine("Tabela de preco " + nuTab + " fora da lista elegivel. Ignorando.");
                    return;
                }
            } else {
                BigDecimal configNuTab = config.getNuTab();
                if (configNuTab != null && nuTab != null && !configNuTab.equals(nuTab)) {
                    return;
                }
            }

            DeparaService deparaService = DeparaService.getInstance();
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
