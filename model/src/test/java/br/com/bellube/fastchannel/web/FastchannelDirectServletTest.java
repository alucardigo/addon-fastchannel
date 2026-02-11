package br.com.bellube.fastchannel.web;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Testes unitarios para FastchannelDirectServlet.
 * Valida que todos os services estao corretamente registrados
 * e que o roteamento funciona conforme esperado.
 */
public class FastchannelDirectServletTest {

    @SuppressWarnings("unchecked")
    private Map<String, ?> getServicesMap() throws Exception {
        Field f = FastchannelDirectServlet.class.getDeclaredField("services");
        f.setAccessible(true);
        return (Map<String, ?>) f.get(null);
    }

    // --- Verifica que o mapa de services foi inicializado ---

    @Test
    public void servicesMapIsNotEmpty() throws Exception {
        Map<String, ?> services = getServicesMap();
        assertNotNull("Mapa de services nao deve ser null", services);
        assertFalse("Mapa de services nao deve estar vazio", services.isEmpty());
    }

    // --- Dashboard ---

    @Test
    public void dashboardServiceRegistered() throws Exception {
        Map<String, ?> services = getServicesMap();
        assertNotNull("FCDashboardSP.snapshot deve estar registrado",
                services.get("FCDashboardSP.snapshot"));
    }

    // --- Config ---

    @Test
    public void configServicesRegistered() throws Exception {
        Map<String, ?> services = getServicesMap();
        assertNotNull("FCConfigSP.get", services.get("FCConfigSP.get"));
        assertNotNull("FCConfigSP.save", services.get("FCConfigSP.save"));
    }

    // --- Admin ---

    @Test
    public void adminServicesRegistered() throws Exception {
        Map<String, ?> services = getServicesMap();
        assertNotNull("FCAdminSP.testarConexao", services.get("FCAdminSP.testarConexao"));
        assertNotNull("FCAdminSP.importarPedidos", services.get("FCAdminSP.importarPedidos"));
        assertNotNull("FCAdminSP.processarFila", services.get("FCAdminSP.processarFila"));
    }

    // --- Pedidos ---

    @Test
    public void pedidosServicesRegistered() throws Exception {
        Map<String, ?> services = getServicesMap();
        assertNotNull("FCPedidosSP.list", services.get("FCPedidosSP.list"));
        assertNotNull("FCPedidosSP.get", services.get("FCPedidosSP.get"));
        assertNotNull("FCPedidosSP.reprocessar", services.get("FCPedidosSP.reprocessar"));
        assertNotNull("FCPedidosSP.consultarFC", services.get("FCPedidosSP.consultarFC"));
    }

    // --- Fila ---

    @Test
    public void filaServicesRegistered() throws Exception {
        Map<String, ?> services = getServicesMap();
        assertNotNull("FCFilaSP.stats", services.get("FCFilaSP.stats"));
        assertNotNull("FCFilaSP.list", services.get("FCFilaSP.list"));
        assertNotNull("FCFilaSP.reprocessar", services.get("FCFilaSP.reprocessar"));
        assertNotNull("FCFilaSP.limparErros", services.get("FCFilaSP.limparErros"));
    }

    // --- Logs ---

    @Test
    public void logsServicesRegistered() throws Exception {
        Map<String, ?> services = getServicesMap();
        assertNotNull("FCLogsSP.list", services.get("FCLogsSP.list"));
        assertNotNull("FCLogsSP.limpar", services.get("FCLogsSP.limpar"));
    }

    // --- De-Para avancado (FCDeParaSP - tela de-para.html) ---

    @Test
    public void deParaServicesRegistered() throws Exception {
        Map<String, ?> services = getServicesMap();
        assertNotNull("FCDeParaSP.list deve estar registrado",
                services.get("FCDeParaSP.list"));
        assertNotNull("FCDeParaSP.save deve estar registrado",
                services.get("FCDeParaSP.save"));
        assertNotNull("FCDeParaSP.remove deve estar registrado",
                services.get("FCDeParaSP.remove"));
        assertNotNull("FCDeParaSP.listDisponiveis deve estar registrado",
                services.get("FCDeParaSP.listDisponiveis"));
    }

    // --- De-Para original (FCDeparaSP - tela depara.html) ---

    @Test
    public void deparaOriginalServicesRegistered() throws Exception {
        Map<String, ?> services = getServicesMap();
        assertNotNull("FCDeparaSP.listEmpresas deve estar registrado",
                services.get("FCDeparaSP.listEmpresas"));
        assertNotNull("FCDeparaSP.listLocais deve estar registrado",
                services.get("FCDeparaSP.listLocais"));
        assertNotNull("FCDeparaSP.listTabelasPreco deve estar registrado",
                services.get("FCDeparaSP.listTabelasPreco"));
        assertNotNull("FCDeparaSP.listMappings deve estar registrado",
                services.get("FCDeparaSP.listMappings"));
        assertNotNull("FCDeparaSP.saveMappings deve estar registrado",
                services.get("FCDeparaSP.saveMappings"));
    }

    // --- Estoque (NOVOS) ---

    @Test
    public void estoqueServicesRegistered() throws Exception {
        Map<String, ?> services = getServicesMap();
        assertNotNull("FCEstoqueSP.list deve estar registrado",
                services.get("FCEstoqueSP.list"));
        assertNotNull("FCEstoqueSP.compararFC deve estar registrado",
                services.get("FCEstoqueSP.compararFC"));
        assertNotNull("FCEstoqueSP.forcarSync deve estar registrado",
                services.get("FCEstoqueSP.forcarSync"));
        assertNotNull("FCEstoqueSP.reprocessar deve estar registrado",
                services.get("FCEstoqueSP.reprocessar"));
    }

    // --- Precos (NOVOS) ---

    @Test
    public void precosServicesRegistered() throws Exception {
        Map<String, ?> services = getServicesMap();
        assertNotNull("FCPrecosSP.list deve estar registrado",
                services.get("FCPrecosSP.list"));
        assertNotNull("FCPrecosSP.compararFC deve estar registrado",
                services.get("FCPrecosSP.compararFC"));
        assertNotNull("FCPrecosSP.forcarSync deve estar registrado",
                services.get("FCPrecosSP.forcarSync"));
        assertNotNull("FCPrecosSP.reprocessar deve estar registrado",
                services.get("FCPrecosSP.reprocessar"));
        assertNotNull("FCPrecosSP.syncEmLote deve estar registrado",
                services.get("FCPrecosSP.syncEmLote"));
    }

    // --- Testes de contagem total ---

    @Test
    public void totalServiceCount() throws Exception {
        Map<String, ?> services = getServicesMap();
        // Dashboard(1) + Config(2) + Admin(3) + Pedidos(4) + Fila(4) + Logs(2)
        // + DeParaAvancado(4) + DeParaOriginal(5) + Estoque(4) + Precos(5) = 34
        assertEquals("Total de services registrados deve ser 34", 34, services.size());
    }

    // --- Teste do metodo hasService ---

    @Test
    public void hasServiceReturnsTrueForRegistered() {
        assertTrue("hasService deve retornar true para service registrado",
                FastchannelDirectServlet.hasService("FCDashboardSP.snapshot"));
        assertTrue("hasService deve retornar true para FCDeparaSP.listEmpresas",
                FastchannelDirectServlet.hasService("FCDeparaSP.listEmpresas"));
        assertTrue("hasService deve retornar true para FCDeParaSP.list",
                FastchannelDirectServlet.hasService("FCDeParaSP.list"));
    }

    @Test
    public void hasServiceReturnsFalseForUnregistered() {
        assertFalse("hasService deve retornar false para service nao registrado",
                FastchannelDirectServlet.hasService("Inexistente.naoExiste"));
    }

    // --- Testes de ServiceInfo ---

    @Test
    public void serviceInfoHasCorrectClassAndMethod() throws Exception {
        Map<String, ?> services = getServicesMap();
        Object info = services.get("FCDeParaSP.list");
        assertNotNull(info);

        // Verificar via reflexao que ServiceInfo tem os campos corretos
        Field classField = info.getClass().getDeclaredField("serviceClass");
        classField.setAccessible(true);
        Class<?> clazz = (Class<?>) classField.get(info);
        assertEquals("Service class deve ser FCDeparaService",
                FCDeparaService.class, clazz);

        Field methodField = info.getClass().getDeclaredField("methodName");
        methodField.setAccessible(true);
        String methodName = (String) methodField.get(info);
        assertEquals("Method name deve ser 'list'", "list", methodName);
    }

    // --- Testes de JSON parsing/serialization ---

    @Test
    public void escapeJsonHandlesSpecialChars() throws Exception {
        FastchannelDirectServlet servlet = new FastchannelDirectServlet();
        Method m = FastchannelDirectServlet.class.getDeclaredMethod("escapeJson", String.class);
        m.setAccessible(true);

        assertEquals("", m.invoke(servlet, (String) null));
        assertEquals("hello", m.invoke(servlet, "hello"));
        assertEquals("line\\none", m.invoke(servlet, "line\none"));
        assertEquals("tab\\there", m.invoke(servlet, "tab\there"));
        assertEquals("quote\\\"here", m.invoke(servlet, "quote\"here"));
        assertEquals("back\\\\slash", m.invoke(servlet, "back\\slash"));
    }

    @Test
    public void toJsonSerializesBasicTypes() throws Exception {
        FastchannelDirectServlet servlet = new FastchannelDirectServlet();
        Method m = FastchannelDirectServlet.class.getDeclaredMethod("toJson", Object.class);
        m.setAccessible(true);

        assertEquals("null", m.invoke(servlet, (Object) null));
        assertEquals("\"hello\"", m.invoke(servlet, "hello"));
        assertEquals("42", m.invoke(servlet, 42));
        assertEquals("true", m.invoke(servlet, true));
        assertEquals("false", m.invoke(servlet, false));
    }

    @Test
    public void parseJsonHandlesEmptyAndBasicInput() throws Exception {
        FastchannelDirectServlet servlet = new FastchannelDirectServlet();
        Method m = FastchannelDirectServlet.class.getDeclaredMethod("parseJson", String.class);
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, Object> empty = (Map<String, Object>) m.invoke(servlet, "{}");
        assertNotNull(empty);
        assertTrue(empty.isEmpty());

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) m.invoke(servlet, "{\"tipo\":\"EMPRESA\",\"codSankhya\":1}");
        assertNotNull(result);
        assertEquals("EMPRESA", result.get("tipo"));
        assertEquals(1L, result.get("codSankhya"));
    }

    // --- Verificacao de que cada service registrado tem o metodo correspondente ---

    @Test
    public void allRegisteredServicesHaveValidMethods() throws Exception {
        Map<String, ?> services = getServicesMap();

        for (Map.Entry<String, ?> entry : services.entrySet()) {
            String key = entry.getKey();
            Object info = entry.getValue();

            Field classField = info.getClass().getDeclaredField("serviceClass");
            classField.setAccessible(true);
            Class<?> clazz = (Class<?>) classField.get(info);

            Field methodField = info.getClass().getDeclaredField("methodName");
            methodField.setAccessible(true);
            String methodName = (String) methodField.get(info);

            // Verifica que o metodo existe na classe
            try {
                Method method = clazz.getMethod(methodName, Map.class);
                assertNotNull("Metodo " + methodName + " deve existir em " + clazz.getSimpleName()
                        + " (service: " + key + ")", method);
            } catch (NoSuchMethodException e) {
                fail("Metodo " + methodName + "(Map) NAO encontrado em "
                        + clazz.getSimpleName() + " para service " + key);
            }
        }
    }

    // --- Verificacao de que cada service pode ser instanciado ---

    @Test
    public void allRegisteredServicesAreInstantiable() throws Exception {
        Map<String, ?> services = getServicesMap();

        for (Map.Entry<String, ?> entry : services.entrySet()) {
            String key = entry.getKey();
            Object info = entry.getValue();

            Field classField = info.getClass().getDeclaredField("serviceClass");
            classField.setAccessible(true);
            Class<?> clazz = (Class<?>) classField.get(info);

            try {
                Object instance = clazz.getDeclaredConstructor().newInstance();
                assertNotNull("Service " + key + " deve ser instanciavel", instance);
            } catch (Exception e) {
                fail("Nao foi possivel instanciar " + clazz.getSimpleName()
                        + " para service " + key + ": " + e.getMessage());
            }
        }
    }
}
