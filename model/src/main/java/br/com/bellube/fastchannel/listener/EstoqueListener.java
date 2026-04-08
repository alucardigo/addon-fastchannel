package br.com.bellube.fastchannel.listener;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.QueueService;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.PersistenceEventAdapter;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.studio.annotations.Listener;

import java.math.BigDecimal;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Listener de Estoque (TGFEST).
 *
 * Captura alteracoes de estoque e enfileira para sincronizacao
 * com o Fastchannel via Transactional Outbox Pattern.
 */
@Listener(instanceNames = {"Estoque"})
public class EstoqueListener extends PersistenceEventAdapter {

    private static final Logger log = Logger.getLogger(EstoqueListener.class.getName());

    @Override
    public void afterInsert(PersistenceEvent event) throws Exception {
        processEstoqueChange(event);
    }

    @Override
    public void afterUpdate(PersistenceEvent event) throws Exception {
        processEstoqueChange(event);
    }

    @Override
    public void afterDelete(PersistenceEvent event) throws Exception {
        processEstoqueChange(event);
    }

    private void processEstoqueChange(PersistenceEvent event) {
        try {
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                return;
            }

            DynamicVO vo = (DynamicVO) event.getVo();
            BigDecimal codProd = vo.asBigDecimal("CODPROD");
            BigDecimal codLocal = vo.asBigDecimal("CODLOCAL");
            BigDecimal codEmp = vo.asBigDecimal("CODEMP");
            BigDecimal estoque = vo.asBigDecimal("ESTOQUE");

            if (!isConfiguredLocalEmpresa(config, codLocal, codEmp)) {
                return;
            }

            DeparaService deparaService = DeparaService.getInstance();
            String sku = deparaService.getSkuForStock(codProd);

            if (sku == null || sku.isEmpty()) {
                log.fine("Produto " + codProd + " nao tem SKU mapeado. Ignorando.");
                return;
            }

            QueueService queueService = QueueService.getInstance();
            queueService.enqueueStock(codProd, sku, estoque, codEmp, codLocal);

            log.info("Estoque enfileirado: CODPROD " + codProd + " (SKU " + sku + ") = " + estoque);

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao processar alteracao de estoque", e);
        }
    }

    private boolean isConfiguredLocalEmpresa(FastchannelConfig config, BigDecimal codLocal, BigDecimal codEmp) {
        BigDecimal configCodLocal = config.getCodLocal();
        BigDecimal configCodEmp = config.getCodemp();

        if (configCodLocal == null && configCodEmp == null) {
            return true;
        }

        boolean localOk = configCodLocal == null || configCodLocal.equals(codLocal);
        boolean empOk = configCodEmp == null || configCodEmp.equals(codEmp);

        return localOk && empOk;
    }
}
