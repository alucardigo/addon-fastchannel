package br.com.bellube.fastchannel.regression;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HomologFixRegressionTest {

    @Test
    public void orderService_mustNotAdvanceLastOrderSyncBlindlyToNow() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/OrderService.java");

        assertFalse("LAST_ORDER_SYNC nao pode mais avancar cegamente para o horario atual",
                src.contains("config.updateLastOrderSync(new Timestamp(System.currentTimeMillis()))"));
        assertTrue("Fluxo deve usar progresso da varredura da API para decidir o cursor",
                src.contains("OrderImportBatchProgress progress = importPendingOrdersFromCursor(lastSync, pageSize);"));
        assertTrue("Fluxo deve atualizar cursor apenas quando houver nextCursor valido",
                src.contains("if (progress.hasNewCursor(lastSync)) {"));
    }

    @Test
    public void orderService_mustUseDatabaseMetadataToProtectOrderMappingColumns() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/OrderService.java");

        assertTrue("OrderService deve carregar tamanhos reais das colunas da AD_FCPEDIDO",
                src.contains("loadOrderMappingFieldSizes()"));
        assertTrue("OrderService deve consultar DatabaseMetaData para truncamento resiliente",
                src.contains("DatabaseMetaData"));
        assertTrue("OrderService deve truncar conforme o tamanho da coluna",
                src.contains("truncateToColumn("));
    }

    @Test
    public void estoqueAndPrecosLists_mustNotHideItemsWithoutProductMapping() throws Exception {
        String estoque = readMainSource("br/com/bellube/fastchannel/web/FCEstoqueService.java");
        String precos = readMainSource("br/com/bellube/fastchannel/web/FCPrecosService.java");

        assertFalse("Listagem de estoque nao deve aplicar filtro oculto por produto mapeado",
                estoque.contains("appendMappedProductFilter(where, queryParams, conn, \"P.CODPROD\");"));
        assertFalse("Listagem local de precos nao deve aplicar filtro oculto por produto mapeado",
                precos.contains("appendMappedProductFilter(where, queryParams, conn, \"E.CODPROD\");"));
    }

    @Test
    public void frontendScreens_mustParseNaiveBackendTimestampAndShowRangePagination() throws Exception {
        assertTrue(readWebSource("dashboard.html").contains("function parseBackendDate(dateStr)"));
        assertTrue(readWebSource("estoque.html").contains("function parseBackendDate(dateStr)"));
        assertTrue(readWebSource("fila.html").contains("function parseBackendDate(dateStr)"));
        assertTrue(readWebSource("logs.html").contains("function parseBackendDate(dateStr)"));
        assertTrue(readWebSource("pedidos.html").contains("function parseBackendDate(dateStr)"));
        assertTrue(readWebSource("precos.html").contains("function parseBackendDate(dateStr)"));

        assertTrue("Tela de precos deve mostrar intervalo real da pagina",
                readWebSource("precos.html").contains("`Mostrando ${start}-${end} de ${total}`"));
        assertTrue("Tela de logs deve mostrar intervalo real da pagina",
                readWebSource("logs.html").contains("`Mostrando ${start}-${end} de ${total}`"));
        assertTrue("Tela da fila deve mostrar intervalo real da pagina",
                readWebSource("fila.html").contains("`Mostrando ${start}-${end} de ${total}`"));
    }

    private String readMainSource(String relativePath) throws IOException {
        Path path = Paths.get("src", "main", "java").resolve(relativePath);
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    private String readWebSource(String fileName) throws IOException {
        Path path = Paths.get("..", "vc", "src", "main", "webapp", "html5", "fastchannel", fileName);
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }
}
