package br.com.bellube.fastchannel.action;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.http.FastchannelOrdersClient;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Ação para consultar detalhes do pedido no Fastchannel.
 * Exibe informações atuais do pedido na API.
 */
public class ConsultarPedidoFCAction implements AcaoRotinaJava {

    private static final Logger log = Logger.getLogger(ConsultarPedidoFCAction.class.getName());

    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        StringBuilder resultado = new StringBuilder();

        try {
            // Verificar se integração está ativa
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                resultado.append("[ERRO] Integração não está ativa!");
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

            if (registros.length > 5) {
                resultado.append("[AVISO] Selecione no máximo 5 pedidos para consulta detalhada.\n");
                resultado.append("Mostrando apenas os 5 primeiros.\n\n");
            }

            resultado.append("=== Consulta de Pedidos no Fastchannel ===\n\n");

            FastchannelOrdersClient ordersClient = new FastchannelOrdersClient();
            int limit = Math.min(registros.length, 5);

            for (int i = 0; i < limit; i++) {
                Registro registro = registros[i];
                String orderId = (String) registro.getCampo("ORDER_ID");

                resultado.append("--- Pedido: ").append(orderId).append(" ---\n");

                try {
                    OrderDTO order = ordersClient.getOrder(orderId);

                    if (order != null) {
                        resultado.append("Status FC: ").append(order.getStatus()).append("\n");
                        resultado.append("Status Desc: ").append(order.getStatusDescription()).append("\n");
                        resultado.append("Data Criação: ").append(order.getCreatedAt()).append("\n");

                        if (order.getCustomer() != null) {
                            resultado.append("Cliente: ").append(order.getCustomer().getName()).append("\n");
                            resultado.append("CPF/CNPJ: ").append(order.getCustomer().getCpfCnpj()).append("\n");
                        }

                        resultado.append("Valor Total: R$ ").append(order.getTotal()).append("\n");
                        resultado.append("Frete: R$ ").append(order.getShippingCost()).append("\n");

                        if (order.getItems() != null) {
                            resultado.append("Qtd Itens: ").append(order.getItems().size()).append("\n");
                        }

                        if (order.getPaymentMethod() != null) {
                            resultado.append("Pagamento: ").append(order.getPaymentMethod()).append("\n");
                        }

                        if (order.getCarrierName() != null) {
                            resultado.append("Transportadora: ").append(order.getCarrierName()).append("\n");
                        }

                        resultado.append("[OK] Consulta realizada com sucesso\n");

                    } else {
                        resultado.append("[AVISO] Pedido não encontrado no Fastchannel\n");
                    }

                } catch (Exception e) {
                    resultado.append("[ERRO] ").append(e.getMessage()).append("\n");
                    log.log(Level.WARNING, "Erro ao consultar pedido: " + orderId, e);
                }

                resultado.append("\n");
            }

        } catch (Exception e) {
            resultado.append("\n[ERRO] Falha na consulta: ").append(e.getMessage());
            log.log(Level.SEVERE, "Erro na consulta de pedidos", e);
        }

        contexto.setMensagemRetorno(resultado.toString());
    }
}
