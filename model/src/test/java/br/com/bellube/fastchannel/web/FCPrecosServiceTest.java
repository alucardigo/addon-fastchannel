package br.com.bellube.fastchannel.web;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Testes unitarios para FCPrecosService.
 * Valida existencia de metodos, utilitarios e assinaturas.
 */
public class FCPrecosServiceTest {

    private final FCPrecosService service = new FCPrecosService();

    // --- Testes de existencia de metodos publicos ---

    @Test
    public void hasListMethod() throws Exception {
        Method m = FCPrecosService.class.getMethod("list", Map.class);
        assertNotNull("Metodo list(Map) deve existir", m);
        assertEquals(Map.class, m.getReturnType());
    }

    @Test
    public void hasCompararFCMethod() throws Exception {
        Method m = FCPrecosService.class.getMethod("compararFC", Map.class);
        assertNotNull("Metodo compararFC(Map) deve existir", m);
        assertEquals(Map.class, m.getReturnType());
    }

    @Test
    public void hasForcarSyncMethod() throws Exception {
        Method m = FCPrecosService.class.getMethod("forcarSync", Map.class);
        assertNotNull("Metodo forcarSync(Map) deve existir", m);
        assertEquals(Map.class, m.getReturnType());
    }

    @Test
    public void hasReprocessarMethod() throws Exception {
        Method m = FCPrecosService.class.getMethod("reprocessar", Map.class);
        assertNotNull("Metodo reprocessar(Map) deve existir", m);
        assertEquals(Map.class, m.getReturnType());
    }

    @Test
    public void hasSyncEmLoteMethod() throws Exception {
        Method m = FCPrecosService.class.getMethod("syncEmLote", Map.class);
        assertNotNull("Metodo syncEmLote(Map) deve existir", m);
        assertEquals(Map.class, m.getReturnType());
    }

    // --- Testes de utilitarios via reflexao ---

    @Test
    public void getIntReturnsDefaultWhenMissing() throws Exception {
        Method m = FCPrecosService.class.getDeclaredMethod("getInt", Map.class, String.class, int.class);
        m.setAccessible(true);

        Map<String, Object> params = new HashMap<>();
        int result = (int) m.invoke(service, params, "missing", 42);
        assertEquals("Deve retornar default quando chave nao existe", 42, result);
    }

    @Test
    public void getIntReturnsValueWhenPresent() throws Exception {
        Method m = FCPrecosService.class.getDeclaredMethod("getInt", Map.class, String.class, int.class);
        m.setAccessible(true);

        Map<String, Object> params = new HashMap<>();
        params.put("source", 2L);
        int result = (int) m.invoke(service, params, "source", 1);
        assertEquals("Deve retornar valor quando chave existe", 2, result);
    }

    @Test
    public void getIntHandlesStringNumber() throws Exception {
        Method m = FCPrecosService.class.getDeclaredMethod("getInt", Map.class, String.class, int.class);
        m.setAccessible(true);

        Map<String, Object> params = new HashMap<>();
        params.put("source", "3");
        int result = (int) m.invoke(service, params, "source", 1);
        assertEquals("Deve converter String para int", 3, result);
    }

    @Test
    public void getStringReturnsNullWhenMissing() throws Exception {
        Method m = FCPrecosService.class.getDeclaredMethod("getString", Map.class, String.class);
        m.setAccessible(true);

        Map<String, Object> params = new HashMap<>();
        String result = (String) m.invoke(service, params, "missing");
        assertNull("Deve retornar null quando chave nao existe", result);
    }

    @Test
    public void getStringReturnsValueWhenPresent() throws Exception {
        Method m = FCPrecosService.class.getDeclaredMethod("getString", Map.class, String.class);
        m.setAccessible(true);

        Map<String, Object> params = new HashMap<>();
        params.put("sku", "SKU001");
        String result = (String) m.invoke(service, params, "sku");
        assertEquals("SKU001", result);
    }

    // --- Testes de fonte de dados via list ---
    // O metodo list() roteia para 3 fontes diferentes baseado no parametro "source"

    @Test
    public void listMethodSignatureIsCorrect() throws Exception {
        // Verifica que o metodo list aceita Map e retorna Map
        Method m = FCPrecosService.class.getMethod("list", Map.class);
        assertEquals("Tipo de retorno deve ser Map", Map.class, m.getReturnType());
        assertEquals("Deve ter 1 parametro", 1, m.getParameterCount());
        assertEquals("Parametro deve ser Map", Map.class, m.getParameterTypes()[0]);
    }

    // --- Teste de instanciacao ---

    @Test
    public void canBeInstantiatedWithDefaultConstructor() throws Exception {
        FCPrecosService instance = FCPrecosService.class.getDeclaredConstructor().newInstance();
        assertNotNull("Deve ser instanciavel com construtor padrao", instance);
    }

    // --- Testes de metodos privados listFrom* ---

    @Test
    public void hasListFromSankhyaMethod() throws Exception {
        Method m = FCPrecosService.class.getDeclaredMethod("listFromSankhya", Map.class);
        m.setAccessible(true);
        assertNotNull("Metodo listFromSankhya deve existir", m);
    }

    @Test
    public void hasListFromAPIMethod() throws Exception {
        Method m = FCPrecosService.class.getDeclaredMethod("listFromAPI", Map.class);
        m.setAccessible(true);
        assertNotNull("Metodo listFromAPI deve existir", m);
    }

    @Test
    public void hasListFromQueueLogsMethod() throws Exception {
        Method m = FCPrecosService.class.getDeclaredMethod("listFromQueueLogs", Map.class);
        m.setAccessible(true);
        assertNotNull("Metodo listFromQueueLogs deve existir", m);
    }

    // --- Testes de column introspection fields ---

    @Test
    public void hasColumnIntrospectionFields() throws Exception {
        // Verifica que os campos de introspeccao de colunas existem
        assertNotNull(FCPrecosService.class.getDeclaredField("hasCodEmpColumn"));
        assertNotNull(FCPrecosService.class.getDeclaredField("hasAtivoColumn"));
        assertNotNull(FCPrecosService.class.getDeclaredField("hasDtInicColumn"));
        assertNotNull(FCPrecosService.class.getDeclaredField("hasDtFimColumn"));
    }
}
