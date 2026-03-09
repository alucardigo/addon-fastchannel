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
 * 1. InternalAPI (preferencial - usa helpers do Sankhya)
 * 2. ServiceInvoker (fallback 1 - usa ServiceInvoker)
 * 3. HTTP (fallback 2 - chamada HTTP com autenticacao)
 */
public class OrderCreationOrchestrator {

    private static final Logger log = Logger.getLogger(OrderCreationOrchestrator.class.getName());

    private final List<OrderCreationStrategy> strategies;
    private final boolean legacyFallbacksEnabled;

    public OrderCreationOrchestrator() {
        this.strategies = new ArrayList<>();
        this.legacyFallbacksEnabled = isLegacyFallbacksEnabled();

        // Ordem de preferencia (do melhor para o pior)
        strategies.add(new InternalApiStrategy());      // 1. API Interna (preferencial)
        strategies.add(new ServiceInvokerStrategy());   // 2. ServiceInvoker (fallback 1)
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

    private boolean isLegacyFallbacksEnabled() {
        String configured = System.getProperty("fastchannel.order.enableLegacyFallbacks");
        if (configured == null || configured.trim().isEmpty()) {
            configured = System.getenv("FASTCHANNEL_ENABLE_LEGACY_FALLBACKS");
        }
        // Compatibilidade retroativa com a flag antiga.
        // Quando habilitada, libera InternalAPI como ultimo fallback.
        if (configured == null || configured.trim().isEmpty()) {
            configured = System.getProperty("fastchannel.order.enableInternalApiFallback");
        }
        if (configured == null || configured.trim().isEmpty()) {
            configured = System.getenv("FASTCHANNEL_ENABLE_INTERNAL_API_FALLBACK");
        }
        if (configured == null || configured.trim().isEmpty()) {
            return false;
        }
        return Boolean.parseBoolean(configured);
    }
}
