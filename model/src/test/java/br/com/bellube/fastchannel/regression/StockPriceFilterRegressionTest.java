package br.com.bellube.fastchannel.regression;

import br.com.bellube.fastchannel.web.FCEstoqueService;
import br.com.bellube.fastchannel.web.FCPrecosService;
import org.junit.Test;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;

public class StockPriceFilterRegressionTest {

    @Test
    public void regression_estoqueFilters_doNotForceEmptyWhenNoConnection() throws Exception {
        FCEstoqueService service = new FCEstoqueService();
        StringBuilder where = new StringBuilder("1=1");
        List<Object> params = new ArrayList<>();

        Method empresa = FCEstoqueService.class.getDeclaredMethod(
                "appendConfiguredEmpresasFilter",
                StringBuilder.class, List.class, Connection.class, String.class);
        empresa.setAccessible(true);
        empresa.invoke(service, where, params, null, "E.CODEMP");

        Method local = FCEstoqueService.class.getDeclaredMethod(
                "appendConfiguredLocaisFilter",
                StringBuilder.class, List.class, Connection.class, String.class);
        local.setAccessible(true);
        local.invoke(service, where, params, null, "E.CODLOCAL");

        assertFalse(where.toString().contains("1 = 0"));
    }

    @Test
    public void regression_precosFilters_doNotForceEmptyWhenNoConnection() throws Exception {
        FCPrecosService service = new FCPrecosService();
        StringBuilder where = new StringBuilder("1=1");
        List<Object> params = new ArrayList<>();

        Method empresa = FCPrecosService.class.getDeclaredMethod(
                "appendConfiguredEmpresasFilter",
                StringBuilder.class, List.class, Connection.class, String.class);
        empresa.setAccessible(true);
        empresa.invoke(service, where, params, null, "E.CODEMP");

        Method local = FCPrecosService.class.getDeclaredMethod(
                "appendConfiguredLocaisFilter",
                StringBuilder.class, List.class, Connection.class, String.class, String.class);
        local.setAccessible(true);
        local.invoke(service, where, params, null, "E.CODPROD", "E.CODEMP");

        assertFalse(where.toString().contains("1 = 0"));
    }
}
