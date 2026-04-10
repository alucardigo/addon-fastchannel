package br.com.bellube.fastchannel.service;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Testes para QueueService em ambiente sem JAPE/mge-core.
 *
 * Invariantes exigidas pelo design (memoria reference_sku_rule_legacy.md e
 * project_fastchannel_analysis.md): nenhuma operacao publica deve lancar
 * excecao quando o banco nao estiver disponivel. O servico deve degradar
 * silenciosamente via DBUtil.closeAll e retornar contagens neutras.
 */
public class QueueServiceJdbcFallbackTest {

    @Test
    public void queueService_isSingleton() {
        QueueService a = QueueService.getInstance();
        QueueService b = QueueService.getInstance();
        assertNotNull(a);
        assertSame(a, b);
    }

    @Test
    public void countPending_doesNotThrowWithoutJape() {
        QueueService svc = QueueService.getInstance();
        int count = svc.countPending();
        // Sem banco, esperamos 0 (comportamento defensivo).
        assertTrue("countPending() deve retornar valor nao-negativo", count >= 0);
    }

    @Test
    public void countErrors_doesNotThrowWithoutJape() {
        QueueService svc = QueueService.getInstance();
        int count = svc.countErrors();
        assertTrue(count >= 0);
    }

    @Test
    public void countPendingByType_doesNotThrowWithoutJape() {
        QueueService svc = QueueService.getInstance();
        int count = svc.countPendingByType("PRODUCT");
        assertTrue(count >= 0);
    }

    @Test
    public void cleanupOldItems_doesNotThrowWithoutJape() {
        QueueService svc = QueueService.getInstance();
        int removed = svc.cleanupOldItems(30);
        assertTrue("cleanupOldItems() nao pode retornar negativo", removed >= 0);
    }

    @Test
    public void reactivateErrorItems_doesNotThrowWithoutJape() {
        QueueService svc = QueueService.getInstance();
        int count = svc.reactivateErrorItems(3);
        assertTrue(count >= 0);
    }

    @Test
    public void enqueueProduct_doesNotThrowWithoutJape() {
        QueueService svc = QueueService.getInstance();
        try {
            svc.enqueueProduct(new java.math.BigDecimal("1"), "SKU-01", "INSERT");
        } catch (Throwable t) {
            throw new AssertionError("enqueueProduct nao pode propagar excecao", t);
        }
    }

    @Test
    public void enqueueOrderStatus_doesNotThrowWithoutJape() {
        QueueService svc = QueueService.getInstance();
        try {
            svc.enqueueOrderStatus(new java.math.BigDecimal("4742473"), "ORD-1", 5);
        } catch (Throwable t) {
            throw new AssertionError("enqueueOrderStatus nao pode propagar excecao", t);
        }
    }
}
