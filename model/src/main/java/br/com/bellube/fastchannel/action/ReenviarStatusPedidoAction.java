package br.com.bellube.fastchannel.action;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.http.FastchannelOrdersClient;
import br.com.bellube.fastchannel.service.LogService;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A??o para reenviar status do pedido para o Fastchannel.
 * ?til quando h? falha na sincroniza??o autom?tica.
 */
public class ReenviarStatusPedidoAction implements AcaoRotinaJava {

    private static final Logger log = Logger.getLogger(ReenviarStatusPedidoAction.class.getName());

    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        StringBuilder resultado = new StringBuilder();

        try {
            // Verificar se integra??o est? ativa
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                resultado.append("[ERRO] Integra??o n?o est? ativa!");
                contexto.setMensagemRetorno(resultado.toString());
                return;
            }

            // Obter registros selecionados
            Registro[] registros = contexto.getLinhas();

            if (registros == null || registros.length == 0) {
                resultado.append("[ERRO] Nenhum pedido selecionado.");
                contexto.setMensagemRetorno(resultado.toString());
                return;
            }

            resultado.append("=== Reenvio de Status para Fastchannel ===\n\n");
            resultado.append("Pedidos selecionados: ").append(registros.length).append("\n\n");

            FastchannelOrdersClient ordersClient = new FastchannelOrdersClient();
            int sucesso = 0;
            int falha = 0;

            for (Registro registro : registros) {
                String orderId = (String) registro.getCampo("ORDER_ID");
                BigDecimal nuNota = (BigDecimal) registro.getCampo("NUNOTA");
                String statusSkw = (String) registro.getCampo("STATUS_SKW");

                try {
                    // Mapear status Sankhya para Fastchannel
                    int fcStatus = mapStatusToFastchannel(statusSkw);

                    if (fcStatus == 0) {
                        resultado.append("[SKIP] Pedido ").append(orderId)
                                .append(": Status ").append(statusSkw).append(" n?o mapeado\n");
                        continue;
                    }

                    // Buscar dados adicionais se faturado
                    String mensagem = "Status atualizado manualmente";
                    if ("F".equals(statusSkw)) {
                        String nfInfo = buscarInfoNF(nuNota);
                        if (nfInfo != null) {
                            mensagem = "NF: " + nfInfo;
                        }
                    }

                    // Enviar para Fastchannel
                    ordersClient.updateOrderStatus(orderId, fcStatus, mensagem);

                    resultado.append("[OK] Pedido ").append(orderId)
                            .append(": Status ").append(fcStatus).append(" enviado\n");
                    sucesso++;

                    LogService.getInstance().info(LogService.OP_ORDER_IMPORT,
                            "Status reenviado: " + orderId, "Status: " + fcStatus);

                } catch (Exception e) {
                    resultado.append("[ERRO] Pedido ").append(orderId)
                            .append(": ").append(e.getMessage()).append("\n");
                    falha++;

                    log.log(Level.WARNING, "Erro ao reenviar status do pedido: " + orderId, e);
                }
            }

            resultado.append("\n=== Resultado ===\n");
            resultado.append("Sucesso: ").append(sucesso).append("\n");
            resultado.append("Falha: ").append(falha).append("\n");

        } catch (Exception e) {
            resultado.append("\n[ERRO] Falha no reenvio: ").append(e.getMessage());
            log.log(Level.SEVERE, "Erro no reenvio de status", e);
        }

        contexto.setMensagemRetorno(resultado.toString());
    }

    private int mapStatusToFastchannel(String statusSkw) {
        if (statusSkw == null) return 0;

        switch (statusSkw) {
            case "P": return 201; // Criado
            case "L": return 200; // Aprovado
            case "F": return 300; // NF Emitida
            case "C": return 400; // Cancelado
            default: return 0;
        }
    }

    private String buscarInfoNF(BigDecimal nuNota) {
        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT NUMNOTA, SERIENOTA FROM TGFCAB WHERE NUNOTA = :nuNota");
            sql.setNamedParameter("nuNota", nuNota);

            rs = sql.executeQuery();
            if (rs.next()) {
                String numnota = rs.getString("NUMNOTA");
                String serie = rs.getString("SERIENOTA");
                return numnota + "/" + serie;
            }

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar info NF", e);
        } finally {
            if (rs != null) {
                try { rs.close(); } catch (Exception ignored) {}
            }
        }
        return null;
    }
}

