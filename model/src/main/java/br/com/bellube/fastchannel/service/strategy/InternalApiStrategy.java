package br.com.bellube.fastchannel.service.strategy;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.dto.OrderItemDTO;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.jape.wrapper.fluid.FluidCreateVO;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Estrategia PREFERENCIAL: Usa API interna do Sankhya com JapeWrapper.
 * Similar ao VendaPixAdianta, usa APIs oficiais diretamente.
 */
public class InternalApiStrategy implements OrderCreationStrategy {

    private static final Logger log = Logger.getLogger(InternalApiStrategy.class.getName());

    private final FastchannelConfig config;
    private final DeparaService deparaService;

    public InternalApiStrategy() {
        this.config = FastchannelConfig.getInstance();
        this.deparaService = DeparaService.getInstance();
    }

    @Override
    public String getStrategyName() {
        return "InternalAPI";
    }

    @Override
    public boolean isAvailable() {
        try {
            // Verificar se consegue obter EntityFacade
            EntityFacade facade = EntityFacadeFactory.getCoreFacade();
            return facade != null;
        } catch (Exception e) {
            log.log(Level.WARNING, "InternalAPI nao disponivel", e);
            return false;
        }
    }

    @Override
    public BigDecimal createOrder(OrderDTO order, BigDecimal codParc,
                                  BigDecimal codTipVenda, BigDecimal codVend,
                                  BigDecimal codNat, BigDecimal codCenCus) throws Exception {

        log.info("[InternalAPI] Criando pedido " + order.getOrderId() + " usando API interna");

        try {
            EntityFacade facade = EntityFacadeFactory.getCoreFacade();

            // Usar transacao gerenciada pelo Sankhya
            facade.beginTransaction();

            try {
                // 1. Criar cabecalho
                BigDecimal nuNota = createCabecalho(order, codParc, codTipVenda, codVend, codNat, codCenCus);

                // 2. Criar itens
                createItens(nuNota, order);

                // Commit da transacao
                facade.commitTransaction();

                log.info("[InternalAPI] Pedido " + order.getOrderId() + " criado como NUNOTA " + nuNota);
                return nuNota;

            } catch (Exception e) {
                facade.rollbackTransaction();
                throw e;
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "[InternalAPI] Erro ao criar pedido via API interna", e);
            throw new Exception("Falha na criacao via API interna: " + e.getMessage(), e);
        }
    }

    private BigDecimal createCabecalho(OrderDTO order, BigDecimal codParc,
                                       BigDecimal codTipVenda, BigDecimal codVend,
                                       BigDecimal codNat, BigDecimal codCenCus) throws Exception {

        JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");

        BigDecimal codTipOper = config.getCodTipOper();
        BigDecimal codEmp = config.getCodemp();
        BigDecimal codLocal = config.getCodLocal();

        if (codTipOper == null) {
            throw new Exception("CODTIPOPER nao configurado");
        }
        if (codEmp == null) {
            throw new Exception("CODEMP nao configurado");
        }

        FluidCreateVO cabBuilder = cabDAO.create()
                .set("CODEMP", codEmp)
                .set("CODPARC", codParc)
                .set("CODTIPOPER", codTipOper)
                .set("DTNEG", new Timestamp(System.currentTimeMillis()))
                .set("TIPMOV", "P") // Pedido
                .set("STATUSNOTA", "L") // Liberado
                .set("AD_NUMFAST", truncate(order.getOrderId(), 50));

        // Campos opcionais
        if (codTipVenda != null) {
            cabBuilder = cabBuilder.set("CODTIPVENDA", codTipVenda);
        }
        if (codVend != null) {
            cabBuilder = cabBuilder.set("CODVEND", codVend);
        }
        if (codNat != null) {
            cabBuilder = cabBuilder.set("CODNAT", codNat);
        }
        if (codCenCus != null) {
            cabBuilder = cabBuilder.set("CODCENCUS", codCenCus);
        }
        if (codLocal != null) {
            cabBuilder = cabBuilder.set("CODLOCAL", codLocal);
        }

        // Valores
        if (order.getTotal() != null) {
            cabBuilder = cabBuilder.set("VLRNOTA", order.getTotal());
        }
        if (order.getDiscount() != null && order.getDiscount().compareTo(BigDecimal.ZERO) > 0) {
            cabBuilder = cabBuilder.set("VLRDESC", order.getDiscount());
        }
        if (order.getShippingCost() != null && order.getShippingCost().compareTo(BigDecimal.ZERO) > 0) {
            cabBuilder = cabBuilder.set("VLRFRETE", order.getShippingCost());
        }

        // Observacao
        String obs = buildObservacao(order);
        if (obs != null && !obs.isEmpty()) {
            cabBuilder = cabBuilder.set("OBSERVACAO", obs);
        }

        DynamicVO cabVO = cabBuilder.save();
        return cabVO.asBigDecimal("NUNOTA");
    }

    private void createItens(BigDecimal nuNota, OrderDTO order) throws Exception {
        JapeWrapper iteDAO = JapeFactory.dao("ItemNota");

        int sequencia = 1;
        for (OrderItemDTO item : order.getItems()) {
            BigDecimal codProd = deparaService.getCodProdBySkuOrEan(item.getSku());
            if (codProd == null) {
                throw new Exception("Produto nao encontrado para SKU: " + item.getSku());
            }

            String codVol = getVolumePadrao(codProd);

            FluidCreateVO itemBuilder = iteDAO.create()
                    .set("NUNOTA", nuNota)
                    .set("SEQUENCIA", new BigDecimal(sequencia))
                    .set("CODPROD", codProd)
                    .set("QTDNEG", item.getQuantity())
                    .set("VLRUNIT", item.getUnitPrice())
                    .set("CODVOL", codVol)
                    .set("ORIGPROD", "F"); // F = Fastchannel

            if (item.getDiscount() != null && item.getDiscount().compareTo(BigDecimal.ZERO) > 0) {
                BigDecimal totalSemDesc = item.getUnitPrice().multiply(item.getQuantity());
                if (totalSemDesc.compareTo(BigDecimal.ZERO) > 0) {
                    BigDecimal percDesc = item.getDiscount()
                            .divide(totalSemDesc, 4, BigDecimal.ROUND_HALF_UP)
                            .multiply(new BigDecimal("100"));
                    itemBuilder = itemBuilder.set("PERCDESC", percDesc);
                }
            }

            itemBuilder.save();
            sequencia++;
        }

        log.info("[InternalAPI] Criados " + order.getItems().size() + " itens para NUNOTA " + nuNota);
    }

    private String getVolumePadrao(BigDecimal codProd) {
        // TODO: Buscar CODVOL real do produto se necessario
        return "UN";
    }

    private String buildObservacao(OrderDTO order) {
        StringBuilder obs = new StringBuilder();
        obs.append("Pedido Fastchannel: ").append(order.getOrderId());

        if (order.getNotes() != null && !order.getNotes().isEmpty()) {
            obs.append(" | ").append(order.getNotes());
        }

        if (order.getShippingMethod() != null) {
            obs.append(" | Frete: ").append(order.getShippingMethod());
        }

        String result = obs.toString();
        return result.length() > 500 ? result.substring(0, 500) : result;
    }

    private String truncate(String str, int maxLength) {
        if (str == null) return null;
        return str.length() > maxLength ? str.substring(0, maxLength) : str;
    }
}
