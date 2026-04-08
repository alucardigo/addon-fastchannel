package br.com.bellube.fastchannel.listener;

import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.PersistenceEventAdapter;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.*;

/**
 * Tests for NotaFiscalListener structure.
 */
public class NotaFiscalListenerTest {

    @Test
    public void extendsPersistenceEventAdapter() {
        assertTrue("NotaFiscalListener must extend PersistenceEventAdapter",
                PersistenceEventAdapter.class.isAssignableFrom(NotaFiscalListener.class));
    }

    @Test
    public void hasAfterInsertMethod() throws NoSuchMethodException {
        Method method = NotaFiscalListener.class.getMethod("afterInsert", PersistenceEvent.class);
        assertNotNull(method);
    }

    @Test
    public void hasAfterUpdateMethod() throws NoSuchMethodException {
        Method method = NotaFiscalListener.class.getMethod("afterUpdate", PersistenceEvent.class);
        assertNotNull(method);
        assertEquals("afterUpdate should be declared in NotaFiscalListener",
                NotaFiscalListener.class, method.getDeclaringClass());
    }
}
