package br.com.bellube.fastchannel.service.strategy;

import br.com.bellube.fastchannel.dto.OrderDTO;

import java.math.BigDecimal;

/**
 * Interface para estrategias de criacao de pedidos no Sankhya.
 * Permite fallback automatico entre diferentes metodos de criacao.
 */
public interface OrderCreationStrategy {

    /**
     * Nome da estrategia para logs.
     */
    String getStrategyName();

    /**
     * Verifica se a estrategia esta disponivel/funcional.
     * @return true se pode ser usada
     */
    boolean isAvailable();

    /**
     * Cria um pedido no Sankhya.
     *
     * @param order Dados do pedido Fastchannel
     * @param codParc Codigo do parceiro
     * @param codTipVenda Codigo do tipo de venda
     * @param codVend Codigo do vendedor
     * @param codNat Codigo da natureza de operacao
     * @param codCenCus Codigo do centro de custo
     * @return NUNOTA criado
     * @throws Exception se falhar
     */
    BigDecimal createOrder(OrderDTO order, BigDecimal codParc,
                          BigDecimal codTipVenda, BigDecimal codVend,
                          BigDecimal codNat, BigDecimal codCenCus) throws Exception;
}
