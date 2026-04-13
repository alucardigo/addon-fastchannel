package br.com.bellube.fastchannel.listener;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.service.QueueService;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.PersistenceEventAdapter;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.studio.annotations.Listener;
import com.google.gson.Gson;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Listener de Volume/Transporte (TGFVOL).
 *
 * Captura eventos de expedicao para enviar dados de tracking
 * (codigo de rastreamento, transportadora) para o Fastchannel.
 */
@Listener(instanceNames = {"Volume"})
public class TrackingListener extends PersistenceEventAdapter {

    private static final Logger log = Logger.getLogger(TrackingListener.class.getName());
    private static final Gson gson = new Gson();

    @Override
    public void afterInsert(PersistenceEvent event) throws Exception {
        processTrackingChange(event);
    }

    @Override
    public void afterUpdate(PersistenceEvent event) throws Exception {
        processTrackingChange(event);
    }

    private void processTrackingChange(PersistenceEvent event) {
        try {
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                return;
            }

            if (!config.isSyncStatusEnabled()) {
                log.fine("Sincronizacao de status desabilitada. Tracking ignorado.");
                return;
            }

            DynamicVO vo = (DynamicVO) event.getVo();
            BigDecimal nuNota = vo.asBigDecimal("NUNOTA");

            // Tentar obter codigo de rastreamento
            String trackingCode = null;
            try {
                trackingCode = vo.asString("CODRASTREIO");
            } catch (Exception e) {
                // Campo pode nao existir em todas as instalacoes
                log.fine("Campo CODRASTREIO nao disponivel em TGFVOL");
            }

            if (trackingCode == null || trackingCode.trim().isEmpty()) {
                log.fine("Volume sem codigo de rastreamento para NUNOTA " + nuNota);
                return;
            }

            // Buscar orderId do pedido FastChannel vinculado
            String orderId = getOrderIdByNuNota(nuNota);
            if (orderId == null) {
                log.fine("NUNOTA " + nuNota + " nao esta vinculada a pedido FastChannel.");
                return;
            }

            // Tentar obter transportadora
            String carrierName = null;
            try {
                BigDecimal codParcTransp = vo.asBigDecimal("CODPARCTRANSP");
                if (codParcTransp != null) {
                    carrierName = getCarrierName(codParcTransp);
                }
            } catch (Exception e) {
                log.fine("Nao foi possivel obter transportadora: " + e.getMessage());
            }

            // Montar payload para a fila
            Map<String, String> payload = new HashMap<>();
            payload.put("orderId", orderId);
            payload.put("trackingCode", trackingCode.trim());
            if (carrierName != null) {
                payload.put("carrierName", carrierName);
            }

            QueueService queueService = QueueService.getInstance();
            queueService.enqueue(
                FastchannelConstants.ENTITY_TRACKING,
                FastchannelConstants.OPERATION_UPDATE,
                nuNota,
                orderId,
                gson.toJson(payload)
            );

            log.info("Tracking enfileirado: pedido " + orderId + " - rastreio " + trackingCode);

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao processar tracking", e);
        }
    }

    private String getOrderIdByNuNota(BigDecimal nuNota) {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT ORDER_ID FROM AD_FCPEDIDO WHERE NUNOTA = :nuNota");
            sql.setNamedParameter("nuNota", nuNota);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getString("ORDER_ID");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar OrderId por NUNOTA", e);
        } finally {
            if (rs != null) { try { rs.close(); } catch (Exception ignored) {} }
            if (jdbc != null) { try { jdbc.closeSession(); } catch (Exception ignored) {} }
        }
        return null;
    }

    private String getCarrierName(BigDecimal codParcTransp) {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT NOMEPARC FROM TGFPAR WHERE CODPARC = :codParc");
            sql.setNamedParameter("codParc", codParcTransp);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getString("NOMEPARC");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar transportadora", e);
        } finally {
            if (rs != null) { try { rs.close(); } catch (Exception ignored) {} }
            if (jdbc != null) { try { jdbc.closeSession(); } catch (Exception ignored) {} }
        }
        return null;
    }
}
