package br.com.bellube.fastchannel.listener;

import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.PersistenceEventAdapter;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.*;

/**
 * Tests for PrecoListener structure.
 */
public class PrecoListenerTest {

    @Test
    public void extendsPersistenceEventAdapter() {
        assertTrue("PrecoListener must extend PersistenceEventAdapter",
                PersistenceEventAdapter.class.isAssignableFrom(PrecoListener.class));
    }

    @Test
    public void hasAfterInsertMethod() throws NoSuchMethodException {
        Method method = PrecoListener.class.getMethod("afterInsert", PersistenceEvent.class);
        assertNotNull(method);
        assertEquals("afterInsert should be declared in PrecoListener",
                PrecoListener.class, method.getDeclaringClass());
    }

    @Test
    public void hasAfterUpdateMethod() throws NoSuchMethodException {
        Method method = PrecoListener.class.getMethod("afterUpdate", PersistenceEvent.class);
        assertNotNull(method);
        assertEquals("afterUpdate should be declared in PrecoListener",
                PrecoListener.class, method.getDeclaringClass());
    }

    @Test
    public void hasAfterDeleteMethod() throws NoSuchMethodException {
        Method method = PrecoListener.class.getMethod("afterDelete", PersistenceEvent.class);
        assertNotNull(method);
        assertEquals("afterDelete should be declared in PrecoListener",
                PrecoListener.class, method.getDeclaringClass());
    }

    @Test
    public void hasPrivateProcessMethod() throws Exception {
        Method method = PrecoListener.class.getDeclaredMethod("processPrecoChange", PersistenceEvent.class);
        assertNotNull("processPrecoChange must exist as private method", method);
    }
}
