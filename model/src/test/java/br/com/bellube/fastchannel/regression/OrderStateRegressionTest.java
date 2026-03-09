package br.com.bellube.fastchannel.regression;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Regressao: garante que a importacao nao marque pedido como
 * confirmado/faturado no momento da inclusao.
 */
public class OrderStateRegressionTest {

    @Test
    public void orderService_mustKeepCabecalhoPendingAndNotFaturado() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/OrderService.java");

        assertTrue("OrderService deve forcar STATUSNOTA='P' no cabecalho",
                src.contains("updateVO = updateVO.set(\"STATUSNOTA\", \"P\")"));
        assertTrue("OrderService deve forcar PENDENTE='S' no cabecalho",
                src.contains("updateVO = updateVO.set(\"PENDENTE\", \"S\")"));
        assertTrue("OrderService deve limpar DTFATUR no cabecalho",
                src.contains("updateVO = updateVO.set(\"DTFATUR\", null)"));
    }

    @Test
    public void orderService_mustKeepItensNotEntregueAndPending() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/OrderService.java");

        assertTrue("OrderService deve forcar QTDENTREGUE=0 no item",
                src.contains("updateVO = updateVO.set(\"QTDENTREGUE\", BigDecimal.ZERO)"));
        assertTrue("OrderService deve forcar STATUSNOTA='P' no item",
                src.contains("updateVO = updateVO.set(\"STATUSNOTA\", \"P\")"));
    }

    @Test
    public void internalApiStrategy_mustCreateCabecalhoPending() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java");

        assertTrue("InternalApiStrategy deve criar cabecalho com STATUSNOTA='P'",
                src.contains("cabBuilder = cabBuilder.set(\"STATUSNOTA\", \"P\")"));
        assertTrue("InternalApiStrategy deve criar cabecalho com PENDENTE='S'",
                src.contains("cabBuilder = cabBuilder.set(\"PENDENTE\", \"S\")"));
    }

    @Test
    public void internalApiStrategy_mustCreateItensNotEntregueAndPending() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java");

        assertTrue("InternalApiStrategy deve criar item com QTDENTREGUE=0",
                src.contains("itemBuilder = itemBuilder.set(\"QTDENTREGUE\", BigDecimal.ZERO)"));
        assertTrue("InternalApiStrategy deve criar item com STATUSNOTA='P'",
                src.contains("itemBuilder = itemBuilder.set(\"STATUSNOTA\", \"P\")"));
    }

    @Test
    public void mustNotReintroduceConfirmedOrFaturadoDefaults() throws Exception {
        String orderService = readMainSource("br/com/bellube/fastchannel/service/OrderService.java");
        String internalApi = readMainSource("br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java");

        assertFalse("Nao pode voltar STATUSNOTA='L' no fluxo de importacao",
                orderService.contains("set(\"STATUSNOTA\", \"L\")") || internalApi.contains("set(\"STATUSNOTA\", \"L\")"));
        assertFalse("Nao pode voltar PENDENTE='N' no fluxo de importacao",
                orderService.contains("set(\"PENDENTE\", \"N\")") || internalApi.contains("set(\"PENDENTE\", \"N\")"));
        assertFalse("Nao pode voltar QTDENTREGUE com quantidade negociada",
                internalApi.contains("set(\"QTDENTREGUE\", quantity)") || internalApi.contains("set(\"QTDENTREGUE\", qtdNeg)"));
    }

    @Test
    public void internalApiStrategy_mustUsePartnerPreferredSeller() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java");

        assertTrue("CODVEND deve priorizar TGFPAR.CODVEND",
                src.contains("BigDecimal codVendParceiro = resolveCodVendByParc(codParc);"));
        assertTrue("CODVEND deve ser sobrescrito pelo vendedor do parceiro quando existir",
                src.contains("if (!isNullOrZero(codVendParceiro)) {"));
    }

    @Test
    public void internalApiStrategy_mustNotFallbackToRandomNutabWhenPreferredExists() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java");

        assertTrue("Nao deve cair no fallback geral de NUTAB quando ha NUTAB preferencial",
                src.contains("if (isNullOrZero(preferredNuTab) && (isNullOrZero(data.nuTab) || data.precoBase == null))"));
        assertTrue("NUTAB deve ser normalizado para versao ativa mais recente",
                src.contains("data.nuTab = normalizeNuTabToLatestActive(data.nuTab);"));
    }

    private String readMainSource(String relativeMainJavaPath) throws IOException {
        Path fromRepoRoot = Paths.get("model", "src", "main", "java")
                .resolve(relativeMainJavaPath);
        if (Files.exists(fromRepoRoot)) {
            return new String(Files.readAllBytes(fromRepoRoot), StandardCharsets.UTF_8);
        }

        Path fromModelRoot = Paths.get("src", "main", "java")
                .resolve(relativeMainJavaPath);
        if (Files.exists(fromModelRoot)) {
            return new String(Files.readAllBytes(fromModelRoot), StandardCharsets.UTF_8);
        }

        throw new IOException("Arquivo fonte nao encontrado: " + relativeMainJavaPath);
    }
}
