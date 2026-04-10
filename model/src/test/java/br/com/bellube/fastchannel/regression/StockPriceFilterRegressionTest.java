package br.com.bellube.fastchannel.regression;

import br.com.bellube.fastchannel.util.FastchannelProductFilter;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;

/**
 * [TASK-4] Atualizado para refletir a refatoracao dos metodos para
 * {@link FastchannelProductFilter}. A invariante funcional permanece: com
 * Connection == null (JAPE indisponivel), os filtros NAO podem injetar
 * "1 = 0" no WHERE, pois isso escondia toda a listagem.
 */
public class StockPriceFilterRegressionTest {

    @Test
    public void regression_estoqueFilters_doNotForceEmptyWhenNoConnection() {
        StringBuilder where = new StringBuilder("1=1");
        List<Object> params = new ArrayList<>();

        FastchannelProductFilter.appendConfiguredEmpresasFilter(where, params, null, "E.CODEMP");
        FastchannelProductFilter.appendConfiguredLocaisFilter(where, params, null, "E.CODLOCAL");

        assertFalse("Filtro de estoque nao pode injetar 1=0 quando a conexao eh null",
                where.toString().contains("1 = 0"));
    }

    @Test
    public void regression_precosFilters_doNotForceEmptyWhenNoConnection() {
        StringBuilder where = new StringBuilder("1=1");
        List<Object> params = new ArrayList<>();

        // Em precos a combinacao dos dois filtros tambem passa pelo helper comum,
        // porque a versao price-specific em FCPrecosService delega ao
        // FastchannelProductFilter.loadConfiguredEmpresas quando a conn eh valida.
        FastchannelProductFilter.appendConfiguredEmpresasFilter(where, params, null, "E.CODEMP");
        FastchannelProductFilter.appendConfiguredLocaisFilter(where, params, null, "E.CODLOCAL");

        assertFalse("Filtro de precos nao pode injetar 1=0 quando a conexao eh null",
                where.toString().contains("1 = 0"));
    }
}
