package br.com.bellube.fastchannel.service.strategy;

import br.com.bellube.fastchannel.dto.OrderDTO;
import org.junit.Test;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Testes unitarios para OrderCreationOrchestrator.
 *
 * O orquestrador instancia estrategias via construtor default, sem DI. Para
 * testar o fluxo de fallback usamos reflexao para injetar estrategias fake
 * em {@code strategies}. Isso mantem os testes 100% offline.
 */
public class OrderCreationOrchestratorTest {

    @Test
    public void defaultStrategies_areRegisteredInPreferenceOrder() throws Exception {
        OrderCreationOrchestrator orchestrator = new OrderCreationOrchestrator();
        List<OrderCreationStrategy> strategies = getStrategies(orchestrator);

        assertEquals(3, strategies.size());
        // Ordem documentada: Internal -> ServiceInvoker -> HTTP.
        assertTrue(strategies.get(0) instanceof InternalApiStrategy);
        assertTrue(strategies.get(1) instanceof ServiceInvokerStrategy);
        assertTrue(strategies.get(2) instanceof HttpServiceStrategy);
    }

    @Test
    public void testStrategies_returnsDiagnosticText() {
        OrderCreationOrchestrator orchestrator = new OrderCreationOrchestrator();
        String report = orchestrator.testStrategies();
        assertNotNull(report);
        assertTrue(report.contains("InternalAPI"));
        assertTrue(report.contains("ServiceInvoker"));
        assertTrue(report.contains("HTTP"));
    }

    @Test
    public void createOrder_fallsBackWhenFirstStrategyFails() throws Exception {
        OrderCreationOrchestrator orchestrator = new OrderCreationOrchestrator();

        FakeStrategy failing = new FakeStrategy("StratA", true, new RuntimeException("boom A"));
        FakeStrategy succeeding = new FakeStrategy("StratB", true, new BigDecimal("9001"));
        injectStrategies(orchestrator, Arrays.<OrderCreationStrategy>asList(failing, succeeding));

        BigDecimal nuNota = orchestrator.createOrder(buildOrder("ORD-1"),
                BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE);

        assertEquals(new BigDecimal("9001"), nuNota);
        assertTrue("primeira estrategia deve ter sido chamada", failing.wasCalled());
        assertTrue("segunda estrategia deve ter sido chamada apos fallback", succeeding.wasCalled());
    }

    @Test
    public void createOrder_skipsUnavailableStrategiesWithoutFailing() throws Exception {
        OrderCreationOrchestrator orchestrator = new OrderCreationOrchestrator();

        FakeStrategy unavailable = new FakeStrategy("Down", false, new BigDecimal("1"));
        FakeStrategy ok = new FakeStrategy("Ok", true, new BigDecimal("123"));
        injectStrategies(orchestrator, Arrays.<OrderCreationStrategy>asList(unavailable, ok));

        BigDecimal nuNota = orchestrator.createOrder(buildOrder("ORD-2"),
                BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE);

        assertEquals(new BigDecimal("123"), nuNota);
        assertFalse("estrategia indisponivel nao pode ser chamada", unavailable.wasCalled());
        assertTrue(ok.wasCalled());
    }

    @Test
    public void createOrder_whenAllStrategiesFail_throwsWithAggregatedDetails() throws Exception {
        OrderCreationOrchestrator orchestrator = new OrderCreationOrchestrator();

        FakeStrategy s1 = new FakeStrategy("A", true, new RuntimeException("falhou A"));
        FakeStrategy s2 = new FakeStrategy("B", true, new RuntimeException("falhou B"));
        FakeStrategy s3 = new FakeStrategy("C", true, new RuntimeException("falhou C"));
        injectStrategies(orchestrator, Arrays.<OrderCreationStrategy>asList(s1, s2, s3));

        try {
            orchestrator.createOrder(buildOrder("ORD-FAIL"),
                    BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE);
            fail("esperado Exception quando todas as estrategias falham");
        } catch (Exception e) {
            String message = e.getMessage();
            assertTrue(message.contains("TODAS as estrategias falharam"));
            assertTrue(message.contains("ORD-FAIL"));
            assertTrue(message.contains("falhou A"));
            assertTrue(message.contains("falhou B"));
            assertTrue(message.contains("falhou C"));
        }

        assertTrue(s1.wasCalled());
        assertTrue(s2.wasCalled());
        assertTrue(s3.wasCalled());
    }

    @Test
    public void getAvailableStrategies_filtersUnavailable() throws Exception {
        OrderCreationOrchestrator orchestrator = new OrderCreationOrchestrator();

        injectStrategies(orchestrator, Arrays.<OrderCreationStrategy>asList(
                new FakeStrategy("One", true, new BigDecimal("1")),
                new FakeStrategy("Two", false, new BigDecimal("2")),
                new FakeStrategy("Three", true, new BigDecimal("3"))
        ));

        List<String> available = orchestrator.getAvailableStrategies();
        assertEquals(2, available.size());
        assertTrue(available.contains("One"));
        assertTrue(available.contains("Three"));
        assertFalse(available.contains("Two"));
    }

    // =========================================================
    // helpers
    // =========================================================

    @SuppressWarnings("unchecked")
    private static List<OrderCreationStrategy> getStrategies(OrderCreationOrchestrator orchestrator) throws Exception {
        Field f = OrderCreationOrchestrator.class.getDeclaredField("strategies");
        f.setAccessible(true);
        return (List<OrderCreationStrategy>) f.get(orchestrator);
    }

    @SuppressWarnings("unchecked")
    private static void injectStrategies(OrderCreationOrchestrator orchestrator,
                                         List<OrderCreationStrategy> replacement) throws Exception {
        Field f = OrderCreationOrchestrator.class.getDeclaredField("strategies");
        f.setAccessible(true);
        List<OrderCreationStrategy> list = (List<OrderCreationStrategy>) f.get(orchestrator);
        list.clear();
        list.addAll(replacement);
    }

    private static OrderDTO buildOrder(String id) {
        OrderDTO order = new OrderDTO();
        order.setOrderId(id);
        return order;
    }

    /**
     * Fake simples de OrderCreationStrategy controlando availability e
     * resultado (valor ou excecao).
     */
    private static class FakeStrategy implements OrderCreationStrategy {
        private final String name;
        private final boolean available;
        private final Object outcome; // BigDecimal ou Throwable
        private boolean called = false;

        FakeStrategy(String name, boolean available, Object outcome) {
            this.name = name;
            this.available = available;
            this.outcome = outcome;
        }

        @Override
        public String getStrategyName() {
            return name;
        }

        @Override
        public boolean isAvailable() {
            return available;
        }

        @Override
        public BigDecimal createOrder(OrderDTO order, BigDecimal codParc,
                                      BigDecimal codTipVenda, BigDecimal codVend,
                                      BigDecimal codNat, BigDecimal codCenCus) throws Exception {
            called = true;
            if (outcome instanceof BigDecimal) {
                return (BigDecimal) outcome;
            }
            if (outcome instanceof Throwable) {
                Throwable t = (Throwable) outcome;
                if (t instanceof Exception) throw (Exception) t;
                throw new Exception(t);
            }
            throw new Exception("FakeStrategy outcome nao configurado");
        }

        boolean wasCalled() {
            return called;
        }
    }
}
