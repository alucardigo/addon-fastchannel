package br.com.bellube.fastchannel.service.strategy;

import br.com.bellube.fastchannel.dto.OrderDTO;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Orquestrador de estrategias de criacao de pedidos.
 * Tenta estrategias em ordem de preferencia com fallback automatico:
 * 1. ServiceInvoker (preferencial - XML via CACSP.incluirNota, espelha legado Node.js
 *    gbi-app-integrador, deixa Sankhya calcular VLRTOT/VLRDESC/VLRNOTA via INFORMARPRECO="True")
 * 2. InternalAPI (fallback 1 - JAPE direto, mantido como rede de seguranca caso o XML falhe)
 * 3. HTTP (fallback 2 - chamada HTTP com autenticacao)
 *
 * <p><b>[2026-05-14 v1.2.77]</b> Trocada a ordem: ServiceInvoker (XML) virou primaria.
 * Motivo: a abordagem JAPE direta do InternalApiStrategy conflitava com o recalculo nativo
 * Sankhya (MGECOM/STP_CONFIRMANOTA2), exigindo 4 camadas de "repair" (forceItemValuesFromFc,
 * forceVlrNotaCorrect, repairVlrNotaAfterConfirmation, FCVlrNotaRepairJob) para manter o
 * cupom de desconto correto. O caminho XML segue a convencao Sankhya nativa - sem conflito,
 * sem necessidade de repairs. Os repairs continuam ativos como rede de seguranca para
 * pedidos antigos e para o caso do fallback InternalApi precisar ser usado.
 */
public class OrderCreationOrchestrator {

    private static final Logger log = Logger.getLogger(OrderCreationOrchestrator.class.getName());

    private final List<OrderCreationStrategy> strategies;

    public OrderCreationOrchestrator() {
        this.strategies = new ArrayList<>();

        // Ordem de preferencia (do melhor para o pior)
        strategies.add(new ServiceInvokerStrategy());   // 1. ServiceInvoker XML (preferencial - espelha legado)
        strategies.add(new InternalApiStrategy());      // 2. JAPE direto (fallback 1 - mantido como rede de seguranca)
        strategies.add(new HttpServiceStrategy());      // 3. HTTP (fallback 2 - ultimo recurso)
    }

    /**
     * Cria pedido tentando estrategias em ordem de preferencia.
     * Se uma estrategia falhar, tenta a proxima automaticamente.
     *
     * @param order Dados do pedido
     * @param codParc Codigo do parceiro
     * @param codTipVenda Codigo do tipo de venda
     * @param codVend Codigo do vendedor
     * @param codNat Codigo da natureza
     * @param codCenCus Codigo do centro de custo
     * @return NUNOTA criado
     * @throws Exception se todas as estrategias falharem
     */
    public BigDecimal createOrder(OrderDTO order, BigDecimal codParc,
                                  BigDecimal codTipVenda, BigDecimal codVend,
                                  BigDecimal codNat, BigDecimal codCenCus) throws Exception {

        log.info("=== Iniciando criacao de pedido " + order.getOrderId() + " com fallback automatico ===");

        // [CRIT-1] Idempotency: ANTES de tentar qualquer strategy, verificar se ja existe NUNOTA
        // com este AD_NUMFAST. Resolve os 1981 duplicados encontrados no homolog (33% das notas FC)
        // que aconteciam quando uma strategy comitava no DB mas a resposta perdia o NUNOTA, fazendo
        // o orchestrator cair para a proxima e criar OUTRA nota.
        BigDecimal existingNuNota = findExistingNuNotaByAdNumFast(order.getOrderId());
        if (existingNuNota != null) {
            log.info("=== IDEMPOTENT: pedido " + order.getOrderId() + " ja existe como NUNOTA "
                + existingNuNota + " (TGFCAB.AD_NUMFAST). Reutilizando. ===");
            return existingNuNota;
        }

        List<String> failedStrategies = new ArrayList<>();
        Exception rootException = null;
        Exception lastException = null;

        for (OrderCreationStrategy strategy : strategies) {
            try {
                // Verificar se estrategia esta disponivel
                if (!strategy.isAvailable()) {
                    log.warning("Estrategia " + strategy.getStrategyName() + " nao disponivel. Pulando.");
                    failedStrategies.add(strategy.getStrategyName() + " (indisponivel)");
                    continue;
                }

                log.info("Tentando estrategia: " + strategy.getStrategyName());

                // Tentar criar pedido
                BigDecimal nuNota = strategy.createOrder(order, codParc, codTipVenda, codVend, codNat, codCenCus);

                log.info("=== SUCESSO com estrategia " + strategy.getStrategyName() + " - NUNOTA: " + nuNota + " ===");
                return nuNota;

            } catch (Exception e) {
                lastException = e;
                if (rootException == null) {
                    rootException = e;
                }
                String errorMsg = "Estrategia " + strategy.getStrategyName() + " falhou: " + e.getMessage();
                log.log(Level.WARNING, errorMsg, e);
                failedStrategies.add(strategy.getStrategyName() + " (erro: " + e.getMessage() + ")");

                // Continuar para proxima estrategia
            }
        }

        // Se chegou aqui, todas as estrategias falharam
        String errorReport = buildErrorReport(order.getOrderId(), failedStrategies);
        log.severe(errorReport);

        throw new Exception(
            "TODAS as estrategias falharam para pedido " + order.getOrderId() + ". " +
            "Detalhes: " + String.join(" | ", failedStrategies) + ". " +
            "Erro raiz: " + (rootException != null ? rootException.getMessage() : "desconhecido") +
            ". Ultimo erro: " + (lastException != null ? lastException.getMessage() : "desconhecido"),
            rootException != null ? rootException : lastException
        );
    }

    /**
     * [CRIT-1] Idempotency check: busca NUNOTA existente em TGFCAB.AD_NUMFAST.
     * Falha de leitura nao bloqueia o fluxo (apenas loga warning) - eh fail-open por design,
     * para nao impedir importacao caso o DB esteja indisponivel.
     */
    private BigDecimal findExistingNuNotaByAdNumFast(String orderId) {
        if (orderId == null || orderId.isEmpty()) return null;
        java.sql.Connection conn = null;
        java.sql.PreparedStatement ps = null;
        java.sql.ResultSet rs = null;
        try {
            conn = br.com.bellube.fastchannel.util.DBUtil.getConnection();
            ps = conn.prepareStatement(
                "SELECT TOP 1 NUNOTA FROM TGFCAB WHERE AD_NUMFAST = ? ORDER BY NUNOTA DESC");
            ps.setString(1, orderId);
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal(1);
            }
            return null;
        } catch (Exception e) {
            log.log(Level.FINE, "Idempotency check AD_NUMFAST=" + orderId + " falhou (fail-open)", e);
            return null;
        } finally {
            br.com.bellube.fastchannel.util.DBUtil.closeAll(rs, ps, conn);
        }
    }

    /**
     * Constroi relatorio de erro detalhado.
     */
    private String buildErrorReport(String orderId, List<String> failedStrategies) {
        StringBuilder report = new StringBuilder();
        report.append("=== FALHA TOTAL na criacao do pedido ").append(orderId).append(" ===\n");
        report.append("Estrategias tentadas (").append(failedStrategies.size()).append("):\n");

        for (int i = 0; i < failedStrategies.size(); i++) {
            report.append("  ").append(i + 1).append(". ").append(failedStrategies.get(i)).append("\n");
        }

        report.append("===================================");
        return report.toString();
    }

    /**
     * Retorna lista de estrategias disponiveis.
     */
    public List<String> getAvailableStrategies() {
        List<String> available = new ArrayList<>();
        for (OrderCreationStrategy strategy : strategies) {
            if (strategy.isAvailable()) {
                available.add(strategy.getStrategyName());
            }
        }
        return available;
    }

    /**
     * Testa disponibilidade de todas as estrategias.
     * Util para diagnostico.
     */
    public String testStrategies() {
        StringBuilder result = new StringBuilder();
        result.append("=== Teste de Disponibilidade de Estrategias ===\n");

        for (OrderCreationStrategy strategy : strategies) {
            boolean available = strategy.isAvailable();
            result.append(strategy.getStrategyName())
                  .append(": ")
                  .append(available ? "DISPONIVEL" : "INDISPONIVEL")
                  .append("\n");
        }

        result.append("===============================================");
        return result.toString();
    }

}
