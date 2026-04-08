package br.com.bellube.fastchannel.http;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Validates BUG-SSL fix: SSL insecure mode defaults to SECURE (not insecure)
 * when no system property or env var is set.
 *
 * The fix on line 305 of FastchannelHttpClient ensures that a null/empty
 * configured value does NOT enable insecure mode:
 * {@code boolean insecure = configured != null && !configured.trim().isEmpty() && Boolean.parseBoolean(configured);}
 */
public class FastchannelHttpClientSslTest {

    private static final String SSL_PROPERTY = "fastchannel.ssl.insecure";

    private Field sslSocketFactoryField;
    private Field hostnameVerifierField;
    private Method configureSslMethod;

    @Before
    public void setUp() throws Exception {
        // Reset static fields to null before each test
        sslSocketFactoryField = FastchannelHttpClient.class.getDeclaredField("insecureSslSocketFactory");
        sslSocketFactoryField.setAccessible(true);
        sslSocketFactoryField.set(null, null);

        hostnameVerifierField = FastchannelHttpClient.class.getDeclaredField("insecureHostnameVerifier");
        hostnameVerifierField.setAccessible(true);
        hostnameVerifierField.set(null, null);

        configureSslMethod = FastchannelHttpClient.class.getDeclaredMethod("configureSslIfNeeded",
                java.net.HttpURLConnection.class);
        configureSslMethod.setAccessible(true);

        // Ensure property is clean
        System.clearProperty(SSL_PROPERTY);
    }

    @After
    public void tearDown() throws Exception {
        System.clearProperty(SSL_PROPERTY);
        // Reset static fields back to null
        sslSocketFactoryField.set(null, null);
        hostnameVerifierField.set(null, null);
    }

    /**
     * BUG-SSL regression: when no system property is set, SSL should use
     * the default (secure) socket factory. The insecure fields must remain null.
     */
    @Test
    public void sslSecureByDefault() throws Exception {
        // No property set - should remain secure
        FastchannelHttpClient client = new FastchannelHttpClient();

        // Use a mock-like HttpsURLConnection via a simple stub
        HttpsURLConnection mockConnection = createStubHttpsConnection();
        configureSslMethod.invoke(client, mockConnection);

        SSLSocketFactory factory = (SSLSocketFactory) sslSocketFactoryField.get(null);
        HostnameVerifier verifier = (HostnameVerifier) hostnameVerifierField.get(null);

        assertNull("insecureSslSocketFactory should be null when SSL is secure by default", factory);
        assertNull("insecureHostnameVerifier should be null when SSL is secure by default", verifier);
    }

    /**
     * When fastchannel.ssl.insecure=true, the insecure SSL factory should be created.
     */
    @Test
    public void sslInsecureWhenExplicitlyEnabled() throws Exception {
        System.setProperty(SSL_PROPERTY, "true");

        FastchannelHttpClient client = new FastchannelHttpClient();
        HttpsURLConnection mockConnection = createStubHttpsConnection();
        configureSslMethod.invoke(client, mockConnection);

        SSLSocketFactory factory = (SSLSocketFactory) sslSocketFactoryField.get(null);
        HostnameVerifier verifier = (HostnameVerifier) hostnameVerifierField.get(null);

        assertNotNull("insecureSslSocketFactory should be created when insecure=true", factory);
        assertNotNull("insecureHostnameVerifier should be created when insecure=true", verifier);
    }

    /**
     * When fastchannel.ssl.insecure=false, SSL should remain secure.
     */
    @Test
    public void sslSecureWhenExplicitlyDisabled() throws Exception {
        System.setProperty(SSL_PROPERTY, "false");

        FastchannelHttpClient client = new FastchannelHttpClient();
        HttpsURLConnection mockConnection = createStubHttpsConnection();
        configureSslMethod.invoke(client, mockConnection);

        SSLSocketFactory factory = (SSLSocketFactory) sslSocketFactoryField.get(null);
        HostnameVerifier verifier = (HostnameVerifier) hostnameVerifierField.get(null);

        assertNull("insecureSslSocketFactory should be null when insecure=false", factory);
        assertNull("insecureHostnameVerifier should be null when insecure=false", verifier);
    }

    /**
     * Creates a minimal HttpsURLConnection stub using a real URL.
     * The connection is never actually opened; it is only passed to
     * configureSslIfNeeded which checks instanceof and sets factories.
     */
    private HttpsURLConnection createStubHttpsConnection() throws Exception {
        java.net.URL url = new java.net.URL("https://localhost");
        return (HttpsURLConnection) url.openConnection();
    }
}
