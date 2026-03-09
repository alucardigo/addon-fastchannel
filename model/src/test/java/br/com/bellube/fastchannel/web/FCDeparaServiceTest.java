package br.com.bellube.fastchannel.web;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FCDeparaServiceTest {

    @Test
    public void mergeMappings_prefersExistingCodExterno() {
        List<Map<String, Object>> base = java.util.Arrays.asList(map("codSankhya", 1, "descricao", "Empresa A"));
        List<Map<String, Object>> mappings = java.util.Arrays.asList(map("codSankhya", 1, "codExterno", "21"));

        List<Map<String, Object>> merged = FCDeparaService.merge(base, mappings);
        assertEquals("21", merged.get(0).get("codExterno"));
    }

    @Test
    public void serviceRegistry_containsDeparaEndpoints() {
        assertTrue(FastchannelDirectServlet.hasService("FCDeparaSP.listEmpresas"));
        assertTrue(FastchannelDirectServlet.hasService("FCDeparaSP.listLocais"));
        assertTrue(FastchannelDirectServlet.hasService("FCDeparaSP.listTabelasPreco"));
        assertTrue(FastchannelDirectServlet.hasService("FCDeparaSP.listMappings"));
        assertTrue(FastchannelDirectServlet.hasService("FCDeparaSP.saveMappings"));
    }

    @Test
    public void choosePriceTableDescriptionColumn_prefersFirstMatch() {
        List<String> available = java.util.Arrays.asList("CODTAB", "DESCRICAO", "DTVIGOR");
        assertEquals("DESCRICAO", FCDeparaService.choosePriceTableDescriptionColumn(available));
    }

    private static Map<String, Object> map(Object... values) {
        Map<String, Object> result = new HashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            result.put(values[i].toString(), values[i + 1]);
        }
        return result;
    }
}
