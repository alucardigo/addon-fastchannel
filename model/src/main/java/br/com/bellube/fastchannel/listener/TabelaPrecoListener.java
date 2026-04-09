package br.com.bellube.fastchannel.listener;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.PriceTableResolver;
import br.com.bellube.fastchannel.service.QueueService;
import br.com.bellube.fastchannel.util.DBUtil;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.PersistenceEventAdapter;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.studio.annotations.Listener;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Listener de Tabela de Preco (TGFTAB).
 *
 * Captura alteracoes na propria tabela de precos (vigencia, ativacao, etc.)
 * e enfileira todos os produtos da tabela para re-sync com Fastchannel.
 *
 * Complementa o PrecoListener (ExcecaoPreco/TGFEXC) que so captura
 * alteracoes de preco por produto individual.
 */
@Listener(instanceNames = {"TabelaPreco"})
public class TabelaPrecoListener extends PersistenceEventAdapter {

    private static final Logger log = Logger.getLogger(TabelaPrecoListener.class.getName());

    @Override
    public void afterInsert(PersistenceEvent event) throws Exception {
        processTabelaChange(event);
    }

    @Override
    public void afterUpdate(PersistenceEvent event) throws Exception {
        processTabelaChange(event);
    }

    private void processTabelaChange(PersistenceEvent event) {
        try {
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                return;
            }

            DynamicVO vo = (DynamicVO) event.getVo();
            BigDecimal nuTab = vo.asBigDecimal("NUTAB");

            if (nuTab == null) {
                return;
            }

            // Verificar se esta tabela eh elegivel (mapeada no de-para FC)
            List<BigDecimal> eligibleTables = new PriceTableResolver().resolveEligibleTables();
            if (!eligibleTables.isEmpty() && !eligibleTables.contains(nuTab)) {
                log.fine("TGFTAB NUTAB " + nuTab + " nao esta na lista de tabelas elegiveis FC. Ignorando.");
                return;
            }

            // Buscar todos os produtos desta tabela e enfileirar para sync
            List<BigDecimal> codProds = fetchProductsInTable(nuTab);
            if (codProds.isEmpty()) {
                log.fine("TGFTAB NUTAB " + nuTab + " nao tem produtos para sincronizar.");
                return;
            }

            DeparaService deparaService = DeparaService.getInstance();
            QueueService queueService = QueueService.getInstance();
            int enqueued = 0;

            for (BigDecimal codProd : codProds) {
                String sku = deparaService.getSkuForStock(codProd);
                if (sku != null) {
                    sku = sku.trim();
                }
                if (sku == null || sku.isEmpty()) {
                    continue;
                }
                queueService.enqueuePrice(codProd, sku);
                enqueued++;
            }

            if (enqueued > 0) {
                log.info("TabelaPreco alterada: NUTAB " + nuTab + " - " + enqueued + " produtos enfileirados para sync de preco.");
            }

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao processar alteracao de tabela de preco", e);
        }
    }

    /**
     * Busca produtos que tem preco na tabela NUTAB via TGFEXC.
     */
    private List<BigDecimal> fetchProductsInTable(BigDecimal nuTab) {
        List<BigDecimal> codProds = new ArrayList<>();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                "SELECT DISTINCT CODPROD FROM TGFEXC WHERE NUTAB = ?");
            stmt.setBigDecimal(1, nuTab);
            rs = stmt.executeQuery();
            while (rs.next()) {
                codProds.add(rs.getBigDecimal("CODPROD"));
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar produtos da tabela NUTAB " + nuTab, e);
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
        return codProds;
    }
}
