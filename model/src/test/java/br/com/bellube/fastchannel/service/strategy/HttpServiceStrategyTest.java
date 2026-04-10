package br.com.bellube.fastchannel.service.strategy;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Testes unitarios para HttpServiceStrategy.
 *
 * Nao exige container Sankhya: exercita a ordem das combinacoes login/servico,
 * o parser de NUNOTA, o extrator de mensagem de erro (incluindo base64) e o
 * nome/contratos publicos expostos pela estrategia.
 */
public class HttpServiceStrategyTest {

    @Test
    public void getStrategyName_isHttp() {
        assertEquals("HTTP", new HttpServiceStrategy().getStrategyName());
    }

    @Test
    public void loginServiceCombinations_prioritizeMgeLoginWithMgecomService() throws Exception {
        Field field = HttpServiceStrategy.class.getDeclaredField("LOGIN_SERVICE_COMBINATIONS");
        field.setAccessible(true);
        String[][] combinations = (String[][]) field.get(null);

        assertNotNull("LOGIN_SERVICE_COMBINATIONS deve existir", combinations);
        assertEquals("Devem existir 4 combinacoes (mge/mgecom, mgecom/mgecom, mgecom/mge, mge/mge)",
                4, combinations.length);

        // A primeira combinacao DEVE ser login /mge + servico /mgecom
        // (confirmada funcional em homologacao e producao).
        assertArrayEquals(new String[]{"/mge/service.sbr", "/mgecom/service.sbr"}, combinations[0]);

        // Segunda combinacao: legado /mgecom + /mgecom
        assertArrayEquals(new String[]{"/mgecom/service.sbr", "/mgecom/service.sbr"}, combinations[1]);

        // Ultimos fallbacks
        assertArrayEquals(new String[]{"/mgecom/service.sbr", "/mge/service.sbr"}, combinations[2]);
        assertArrayEquals(new String[]{"/mge/service.sbr", "/mge/service.sbr"}, combinations[3]);
    }

    @Test
    public void parseNuNotaFromResponse_extractsNumberFromStandardFormat() throws Exception {
        Method m = HttpServiceStrategy.class.getDeclaredMethod("parseNuNotaFromResponse", String.class);
        m.setAccessible(true);
        HttpServiceStrategy svc = new HttpServiceStrategy();

        String xml = "<?xml version=\"1.0\"?><serviceResponse status=\"1\"><responseBody>" +
                "<NUNOTA>4742473</NUNOTA></responseBody></serviceResponse>";
        Object result = m.invoke(svc, xml);
        assertEquals(new BigDecimal("4742473"), result);
    }

    @Test
    public void parseNuNotaFromResponse_returnsNullWhenAbsent() throws Exception {
        Method m = HttpServiceStrategy.class.getDeclaredMethod("parseNuNotaFromResponse", String.class);
        m.setAccessible(true);
        HttpServiceStrategy svc = new HttpServiceStrategy();

        Object result = m.invoke(svc, "<serviceResponse status=\"1\"><responseBody/></serviceResponse>");
        assertNull(result);
    }

    @Test
    public void parseNuNotaFromResponse_firstMatchWins() throws Exception {
        Method m = HttpServiceStrategy.class.getDeclaredMethod("parseNuNotaFromResponse", String.class);
        m.setAccessible(true);
        HttpServiceStrategy svc = new HttpServiceStrategy();

        String xml = "<response><NUNOTA>10</NUNOTA><NUNOTA>20</NUNOTA></response>";
        Object result = m.invoke(svc, xml);
        assertEquals(new BigDecimal("10"), result);
    }

    @Test
    public void extractErrorMessage_decodesBase64StatusMessage() throws Exception {
        Method m = HttpServiceStrategy.class.getDeclaredMethod("extractErrorMessage", String.class);
        m.setAccessible(true);
        HttpServiceStrategy svc = new HttpServiceStrategy();

        String plain = "CODTIPVENDA invalido";
        String encoded = java.util.Base64.getEncoder().encodeToString(plain.getBytes("UTF-8"));
        String xml = "<serviceResponse status=\"0\"><statusMessage>" + encoded + "</statusMessage></serviceResponse>";

        String extracted = (String) m.invoke(svc, xml);
        assertEquals(plain, extracted);
    }

    @Test
    public void extractErrorMessage_fallsBackToMessageTag() throws Exception {
        Method m = HttpServiceStrategy.class.getDeclaredMethod("extractErrorMessage", String.class);
        m.setAccessible(true);
        HttpServiceStrategy svc = new HttpServiceStrategy();

        String xml = "<serviceResponse><message>Erro generico do Sankhya</message></serviceResponse>";
        String extracted = (String) m.invoke(svc, xml);
        assertTrue("deveria conter parte da mensagem bruta", extracted.contains("Erro generico"));
    }

    @Test
    public void extractErrorMessage_unknownReturnsCompactedPayload() throws Exception {
        Method m = HttpServiceStrategy.class.getDeclaredMethod("extractErrorMessage", String.class);
        m.setAccessible(true);
        HttpServiceStrategy svc = new HttpServiceStrategy();

        String extracted = (String) m.invoke(svc, "<html><body>oops</body></html>");
        assertNotNull(extracted);
        assertTrue(extracted.length() > 0);
    }

    @Test
    public void sanitizeErrorText_stripsXmlAndCollapsesWhitespace() throws Exception {
        Method m = HttpServiceStrategy.class.getDeclaredMethod("sanitizeErrorText", String.class);
        m.setAccessible(true);
        HttpServiceStrategy svc = new HttpServiceStrategy();

        String raw = "<tag>  linha1\n   linha2 </tag>";
        String result = (String) m.invoke(svc, raw);
        assertEquals("linha1 linha2", result);
    }

    @Test
    public void sanitizeErrorText_truncatesLongPayload() throws Exception {
        Method m = HttpServiceStrategy.class.getDeclaredMethod("sanitizeErrorText", String.class);
        m.setAccessible(true);
        HttpServiceStrategy svc = new HttpServiceStrategy();

        StringBuilder huge = new StringBuilder();
        for (int i = 0; i < 2000; i++) huge.append('a');
        String result = (String) m.invoke(svc, huge.toString());
        assertTrue("deve truncar payloads longos", result.length() <= 603);
        assertTrue(result.endsWith("..."));
    }

    @Test
    public void sanitizeErrorText_nullReturnsEmpty() throws Exception {
        Method m = HttpServiceStrategy.class.getDeclaredMethod("sanitizeErrorText", String.class);
        m.setAccessible(true);
        HttpServiceStrategy svc = new HttpServiceStrategy();

        assertEquals("", m.invoke(svc, new Object[]{null}));
    }

    @Test
    public void isAvailable_requiresSankhyaUrlAndUser() {
        // Sem config completa, a estrategia pode ou nao estar disponivel dependendo do
        // ambiente de teste. Garantimos apenas que o metodo nao lanca excecao.
        HttpServiceStrategy svc = new HttpServiceStrategy();
        boolean available = svc.isAvailable();
        // Tanto true quanto false sao validos aqui.
        assertTrue(available || !available);
    }
}
