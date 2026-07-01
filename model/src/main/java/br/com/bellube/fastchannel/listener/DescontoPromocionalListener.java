package br.com.bellube.fastchannel.listener;

import br.com.bellube.fastchannel.config.FastchannelConfig;
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
 * Listener de Descontos Promocionais (TGFDES) e Faixas de Desconto por Quantidade (TGFDPQ).
 *
 * <p>Captura alteracoes nas tabelas de Descontos Promocionais e enfileira o produto
 * para re-sincronizacao de preco (incluindo batches/faixas escalonadas) no FastChannel.
 *
 * <p>Escuta duas instancias:
 * <ul>
 *   <li><b>Desconto</b> (TGFDES) - cabecalho da promocao com CODPROD, CODTAB, datas, versao</li>
 *   <li><b>DescontoPorQuantidade</b> (TGFDPQ) - faixas de quantidade e preco escalonado</li>
 * </ul>
 *
 * <p>Antes deste listener (introduzido em 2026-04-24), alteracoes em Descontos
 * Promocionais/Faixas nao disparavam re-sync automatico — so o job diario atualizava
 * batches, e como esse job tambem nao estava agendado em TSIEVP, na pratica os
 * preco escalonados ficavam sempre desatualizados no FC ate alguem clicar "Sincronizar"
 * manualmente.
 */
@Listener(instanceNames = {"Desconto", "DescontoPorQuantidade"})
public class DescontoPromocionalListener extends PersistenceEventAdapter {

    private static final Logger log = Logger.getLogger(DescontoPromocionalListener.class.getName());

    @Override public void afterInsert(PersistenceEvent event) throws Exception { processChange(event); }
    @Override public void afterUpdate(PersistenceEvent event) throws Exception { processChange(event); }
    @Override public void afterDelete(PersistenceEvent event) throws Exception { processChange(event); }

    private void processChange(PersistenceEvent event) {
        try {
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                return;
            }

            DynamicVO vo = (DynamicVO) event.getVo();
            if (vo == null) {
                log.fine("DescontoPromocionalListener: event.getVo() null, ignorando");
                return;
            }

            BigDecimal codProd = resolveCodProd(vo);
            if (codProd == null) {
                log.fine("DescontoPromocionalListener: CODPROD nao resolvido");
                return;
            }

            DeparaService deparaService = DeparaService.getInstance();
            String sku = deparaService.getSkuForStock(codProd);
            if (sku != null) {
                sku = sku.trim();
            }
            if (sku == null || sku.isEmpty()) {
                log.fine("DescontoPromocionalListener: CODPROD " + codProd + " sem SKU mapeado. Ignorando.");
                return;
            }

            QueueService queueService = QueueService.getInstance();
            queueService.enqueuePrice(codProd, sku);
            log.info("[DescontoPromocional] Preco escalonado enfileirado: CODPROD "
                    + codProd + " (SKU " + sku + ")");

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao processar alteracao de Desconto Promocional", e);
        }
    }

    /**
     * Resolve CODPROD seja em Desconto (TGFDES tem CODPROD direto) ou em
     * DescontoPorQuantidade (TGFDPQ so tem NUPROMOCAO → precisa resolver via TGFDES).
     */
    private BigDecimal resolveCodProd(DynamicVO vo) {
        // Desconto (TGFDES): CODPROD direto no VO
        try {
            BigDecimal direct = vo.asBigDecimal("CODPROD");
            if (direct != null) return direct;
        } catch (Exception ignore) {
            // Se o VO for de TGFDPQ, nao tem CODPROD — cai no fallback abaixo
        }

        // DescontoPorQuantidade (TGFDPQ): resolver via NUPROMOCAO → TGFDES.CODPROD
        try {
            BigDecimal nuPromo = vo.asBigDecimal("NUPROMOCAO");
            if (nuPromo != null) {
                return fetchCodProdByNuPromocao(nuPromo);
            }
        } catch (Exception ignore) {
            /* tolerancia - alguns eventos de delete podem vir sem NUPROMOCAO */
        }

        return null;
    }

    private BigDecimal fetchCodProdByNuPromocao(BigDecimal nuPromo) {
        java.sql.Connection conn = null;
        java.sql.PreparedStatement ps = null;
        java.sql.ResultSet rs = null;
        try {
            conn = br.com.bellube.fastchannel.util.DBUtil.getConnection();
            ps = conn.prepareStatement(
                "SELECT TOP 1 CODPROD FROM TGFDES WHERE NUPROMOCAO = ? " +
                "ORDER BY ISNULL(NUVERSAO,0) DESC");
            ps.setBigDecimal(1, nuPromo);
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODPROD");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Erro ao resolver CODPROD via NUPROMOCAO=" + nuPromo, e);
        } finally {
            br.com.bellube.fastchannel.util.DBUtil.closeAll(rs, ps, conn);
        }
        return null;
    }
}
