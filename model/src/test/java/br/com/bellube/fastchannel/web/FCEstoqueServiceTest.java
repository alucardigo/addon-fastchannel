package br.com.bellube.fastchannel.web;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Testes unitarios para FCEstoqueService.
 * Valida existencia de metodos e assinaturas.
 */
public class FCEstoqueServiceTest {

    // --- Testes de existencia de metodos publicos ---

    @Test
    public void hasListMethod() throws Exception {
        Method m = FCEstoqueService.class.getMethod("list", Map.class);
        assertNotNull("Metodo list(Map) deve existir", m);
        assertEquals(Map.class, m.getReturnType());
    }

    @Test
    public void hasCompararFCMethod() throws Exception {
        Method m = FCEstoqueService.class.getMethod("compararFC", Map.class);
        assertNotNull("Metodo compararFC(Map) deve existir", m);
        assertEquals(Map.class, m.getReturnType());
    }

    @Test
    public void hasForcarSyncMethod() throws Exception {
        Method m = FCEstoqueService.class.getMethod("forcarSync", Map.class);
        assertNotNull("Metodo forcarSync(Map) deve existir", m);
        assertEquals(Map.class, m.getReturnType());
    }

    @Test
    public void hasReprocessarMethod() throws Exception {
        Method m = FCEstoqueService.class.getMethod("reprocessar", Map.class);
        assertNotNull("Metodo reprocessar(Map) deve existir", m);
        assertEquals(Map.class, m.getReturnType());
    }

    // --- Teste de instanciacao ---

    @Test
    public void canBeInstantiatedWithDefaultConstructor() throws Exception {
        FCEstoqueService instance = FCEstoqueService.class.getDeclaredConstructor().newInstance();
        assertNotNull("Deve ser instanciavel com construtor padrao", instance);
    }

    // --- Todos os metodos devem aceitar Map e retornar Map ---

    @Test
    public void allPublicMethodsFollowServicePattern() throws Exception {
        String[] methodNames = {"list", "compararFC", "forcarSync", "reprocessar"};

        for (String methodName : methodNames) {
            Method m = FCEstoqueService.class.getMethod(methodName, Map.class);
            assertEquals("Metodo " + methodName + " deve retornar Map",
                    Map.class, m.getReturnType());
            assertEquals("Metodo " + methodName + " deve ter 1 parametro",
                    1, m.getParameterCount());
        }
    }
}
