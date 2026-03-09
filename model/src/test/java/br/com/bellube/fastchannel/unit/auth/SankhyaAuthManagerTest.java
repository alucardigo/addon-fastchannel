package br.com.bellube.fastchannel.unit.auth;

import br.com.bellube.fastchannel.service.auth.SankhyaAuthManager;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.*;

/**
 * Testes unitarios para SankhyaAuthManager.
 * Valida construcao de XML de login e extracao de JSESSIONID.
 */
public class SankhyaAuthManagerTest {

    // ===================== Login XML =====================

    @Test
    public void buildLoginXml_containsKeepConnectedS() throws Exception {
        String xml = invokeBuildLoginXml("SUP", "admin");
        assertTrue("XML deve conter KEEPCONNECTED=S", xml.contains("<KEEPCONNECTED>S</KEEPCONNECTED>"));
    }

    @Test
    public void buildLoginXml_containsServiceRequestWithCapitalR() throws Exception {
        String xml = invokeBuildLoginXml("SUP", "admin");
        assertTrue("XML deve usar serviceRequest (R maiusculo)", xml.contains("serviceRequest"));
        assertFalse("XML nao deve usar servicerequest (r minusculo)", xml.contains("servicerequest"));
    }

    @Test
    public void buildLoginXml_containsNOMUSU() throws Exception {
        String xml = invokeBuildLoginXml("FAST", "pwd");
        assertTrue(xml.contains("<NOMUSU>FAST</NOMUSU>"));
    }

    @Test
    public void buildLoginXml_containsINTERNO() throws Exception {
        String xml = invokeBuildLoginXml("SUP", "mypass");
        assertTrue(xml.contains("<INTERNO>mypass</INTERNO>"));
    }

    @Test
    public void buildLoginXml_escapesSpecialChars() throws Exception {
        String xml = invokeBuildLoginXml("user<>&", "pass\"'");
        assertTrue(xml.contains("&lt;"));
        assertTrue(xml.contains("&amp;"));
        assertTrue(xml.contains("&gt;"));
        assertTrue(xml.contains("&quot;"));
        assertTrue(xml.contains("&apos;"));
    }

    // ===================== JSESSIONID Extraction =====================

    @Test
    public void extractJsessionId_fromXmlLowercase() throws Exception {
        String xml = "<response><jsessionid>ABC123</jsessionid></response>";
        assertEquals("ABC123", invokeExtractJsessionId(xml));
    }

    @Test
    public void extractJsessionId_fromXmlUppercase() throws Exception {
        String xml = "<response><JSESSIONID>DEF456</JSESSIONID></response>";
        assertEquals("DEF456", invokeExtractJsessionId(xml));
    }

    @Test
    public void extractJsessionId_fromMgeSessionJson() throws Exception {
        String json = "{\"status\":\"1\",\"mgeSession\":\"GHI789\"}";
        assertEquals("GHI789", invokeExtractJsessionId(json));
    }

    @Test
    public void extractJsessionId_fromMgeSessionQueryStyle() throws Exception {
        String text = "OK mgeSession=JKL012.route1";
        assertEquals("JKL012.route1", invokeExtractJsessionId(text));
    }

    @Test
    public void extractJsessionId_notFound_returnsNull() throws Exception {
        assertNull(invokeExtractJsessionId("<response><status>1</status></response>"));
    }

    @Test
    public void extractJsessionId_emptyString_returnsNull() throws Exception {
        assertNull(invokeExtractJsessionId(""));
    }

    // ===================== Server URL Normalization =====================

    @Test
    public void normalizeServerBaseUrl_removesTrailingSlash() throws Exception {
        assertEquals("http://server:8080", invokeNormalizeServerBaseUrl("http://server:8080/"));
    }

    @Test
    public void normalizeServerBaseUrl_removesMgeSuffix() throws Exception {
        assertEquals("http://server:8080", invokeNormalizeServerBaseUrl("http://server:8080/mge"));
    }

    @Test
    public void normalizeServerBaseUrl_removesMgecomSuffix() throws Exception {
        assertEquals("http://server:8080", invokeNormalizeServerBaseUrl("http://server:8080/mgecom"));
    }

    @Test
    public void normalizeServerBaseUrl_plainUrl_unchanged() throws Exception {
        assertEquals("http://server:8080", invokeNormalizeServerBaseUrl("http://server:8080"));
    }

    @Test
    public void normalizeServerBaseUrl_null_returnsNull() throws Exception {
        assertNull(invokeNormalizeServerBaseUrl(null));
    }

    // ===================== Service Path Normalization =====================

    @Test
    public void normalizeServicePath_null_returnsDefault() throws Exception {
        assertEquals("/mge/service.sbr", invokeNormalizeServicePath(null));
    }

    @Test
    public void normalizeServicePath_empty_returnsDefault() throws Exception {
        assertEquals("/mge/service.sbr", invokeNormalizeServicePath("  "));
    }

    @Test
    public void normalizeServicePath_alreadyComplete_unchanged() throws Exception {
        assertEquals("/mgecom/service.sbr", invokeNormalizeServicePath("/mgecom/service.sbr"));
    }

    @Test
    public void normalizeServicePath_pathWithoutSlash_prependsSlash() throws Exception {
        String result = invokeNormalizeServicePath("mge/service.sbr");
        assertTrue(result.startsWith("/"));
    }

    @Test
    public void normalizeServicePath_pathWithoutServiceSbr_appendsIt() throws Exception {
        String result = invokeNormalizeServicePath("/mgecom");
        assertTrue(result.endsWith("/service.sbr"));
    }

    // ===================== Helpers =====================

    private String invokeBuildLoginXml(String username, String password) throws Exception {
        SankhyaAuthManager mgr = new SankhyaAuthManager();
        Method m = SankhyaAuthManager.class.getDeclaredMethod("buildLoginXml", String.class, String.class);
        m.setAccessible(true);
        return (String) m.invoke(mgr, username, password);
    }

    private String invokeExtractJsessionId(String xml) throws Exception {
        SankhyaAuthManager mgr = new SankhyaAuthManager();
        Method m = SankhyaAuthManager.class.getDeclaredMethod("extractJsessionId", String.class);
        m.setAccessible(true);
        return (String) m.invoke(mgr, xml);
    }

    private String invokeNormalizeServerBaseUrl(String url) throws Exception {
        SankhyaAuthManager mgr = new SankhyaAuthManager();
        Method m = SankhyaAuthManager.class.getDeclaredMethod("normalizeServerBaseUrl", String.class);
        m.setAccessible(true);
        return (String) m.invoke(mgr, url);
    }

    private String invokeNormalizeServicePath(String path) throws Exception {
        SankhyaAuthManager mgr = new SankhyaAuthManager();
        Method m = SankhyaAuthManager.class.getDeclaredMethod("normalizeServicePath", String.class);
        m.setAccessible(true);
        return (String) m.invoke(mgr, path);
    }
}
