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
 * Listener de Estoque (TGFEST).
 *
 * Captura alterações de estoque e enfileira para sincronização
 * com o Fastchannel via Transactional Outbox Pattern.
 *
 * Configuração no Sankhya:
 * - Eventos Programáveis > Listeners
 * - Entidade: Estoque (TGFEST)
 * - Eventos: afterInsert, afterUpdate
 */
public class EstoqueListener implements EventoProgramavelJava {

    private static final Logger log = Logger.getLogger(EstoqueListener.class.getName());

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
        processEstoqueChange(event);
    }

    @Override
    public void afterUpdate(PersistenceEvent event) throws Exception {
        processEstoqueChange(event);
    }

    @Override
    public void afterDelete(PersistenceEvent event) throws Exception {
        // Considerar enviar estoque = 0 ao deletar
        processEstoqueChange(event);
    }

    @Override
    public void beforeCommit(TransactionContext transactionContext) throws Exception {
        // Not used
    }

    public void executeScheduler() throws Exception {
        // Not used - this is a listener, not a scheduler
    }

    private void processEstoqueChange(PersistenceEvent event) {
        try {
            // Verificar se integração está ativa
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                return;
            }

            DynamicVO vo = (DynamicVO) event.getVo();
            BigDecimal codProd = vo.asBigDecimal("CODPROD");
            BigDecimal codLocal = vo.asBigDecimal("CODLOCAL");
            BigDecimal codEmp = vo.asBigDecimal("CODEMP");
            BigDecimal estoque = vo.asBigDecimal("ESTOQUE");

            // Verificar se é o local e empresa configurados
            if (!isConfiguredLocalEmpresa(config, codLocal, codEmp)) {
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
            queueService.enqueueStock(codProd, sku, estoque);

            log.info("Estoque enfileirado: CODPROD " + codProd + " (SKU " + sku + ") = " + estoque);

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao processar alteração de estoque", e);
        }
    }

    private boolean isConfiguredLocalEmpresa(FastchannelConfig config, BigDecimal codLocal, BigDecimal codEmp) {
        BigDecimal configCodLocal = config.getCodLocal();
        BigDecimal configCodEmp = config.getCodemp();

        // Se não configurado, aceita qualquer local/empresa
        if (configCodLocal == null && configCodEmp == null) {
            return true;
        }

        boolean localOk = configCodLocal == null || configCodLocal.equals(codLocal);
        boolean empOk = configCodEmp == null || configCodEmp.equals(codEmp);

        return localOk && empOk;
    }
}
