package br.com.bellube.fastchannel.unit.service;

import br.com.bellube.fastchannel.http.FastchannelPriceClient;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.PriceService;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.math.BigDecimal;

import static org.junit.Assert.*;

/**
 * Testes unitarios para PriceService.
 * Valida logica de determinacao de canal e roteamento.
 */
public class PriceServiceTest {

    @Test
    public void determineChannel_mappedDist_returnsDistribution() throws Exception {
        PriceService svc = createServiceWithMockDepara("DIST_TABLE");
        FastchannelPriceClient.Channel result = invokeDetermineChannel(svc, new BigDecimal("100"), "SKU001");
        assertEquals(FastchannelPriceClient.Channel.DISTRIBUTION, result);
    }

    @Test
    public void determineChannel_skuPrefixD_returnsDistribution() throws Exception {
        PriceService svc = createServiceWithMockDepara(null);
        FastchannelPriceClient.Channel result = invokeDetermineChannel(svc, new BigDecimal("100"), "D-SKU001");
        assertEquals(FastchannelPriceClient.Channel.DISTRIBUTION, result);
    }

    @Test
    public void determineChannel_noMapping_noPrefix_returnsConsumption() throws Exception {
        PriceService svc = createServiceWithMockDepara(null);
        FastchannelPriceClient.Channel result = invokeDetermineChannel(svc, new BigDecimal("100"), "SKU001");
        assertEquals(FastchannelPriceClient.Channel.CONSUMPTION, result);
    }

    @Test
    public void determineChannel_lowercaseD_returnsDistribution() throws Exception {
        PriceService svc = createServiceWithMockDepara(null);
        FastchannelPriceClient.Channel result = invokeDetermineChannel(svc, new BigDecimal("100"), "d-sku");
        assertEquals(FastchannelPriceClient.Channel.DISTRIBUTION, result);
    }

    @Test
    public void channelEnum_hasExpectedValues() {
        assertNotNull(FastchannelPriceClient.Channel.DISTRIBUTION);
        assertNotNull(FastchannelPriceClient.Channel.CONSUMPTION);
        assertEquals(2, FastchannelPriceClient.Channel.values().length);
    }

    @Test
    public void priceClient_defaultChannel_isConsumption() {
        FastchannelPriceClient client = new FastchannelPriceClient();
        assertEquals(FastchannelPriceClient.Channel.CONSUMPTION, client.getChannel());
    }

    @Test
    public void priceClient_distributionChannel() {
        FastchannelPriceClient client = new FastchannelPriceClient(FastchannelPriceClient.Channel.DISTRIBUTION);
        assertEquals(FastchannelPriceClient.Channel.DISTRIBUTION, client.getChannel());
    }

    // ===================== PriceResolver - toCentavos =====================

    @Test
    public void priceResolver_toCentavos_convertsCorrectly() throws Exception {
        br.com.bellube.fastchannel.service.PriceResolver resolver = new br.com.bellube.fastchannel.service.PriceResolver();
        Method m = resolver.getClass().getDeclaredMethod("toCentavos", BigDecimal.class);
        m.setAccessible(true);
        BigDecimal result = (BigDecimal) m.invoke(resolver, new BigDecimal("15.99"));
        assertEquals(new BigDecimal("1599"), result);
    }

    @Test
    public void priceResolver_toCentavos_null_returnsNull() throws Exception {
        br.com.bellube.fastchannel.service.PriceResolver resolver = new br.com.bellube.fastchannel.service.PriceResolver();
        Method m = resolver.getClass().getDeclaredMethod("toCentavos", BigDecimal.class);
        m.setAccessible(true);
        assertNull(m.invoke(resolver, (BigDecimal) null));
    }

    // ===================== Helpers =====================

    private PriceService createServiceWithMockDepara(String mappedValue) throws Exception {
        // DeparaServiceProvider e o construtor interno do PriceService sao package-private,
        // entao usamos reflection completa para instanciar.
        Class<?> providerClass = Class.forName("br.com.bellube.fastchannel.service.PriceService$DeparaServiceProvider");
        java.lang.reflect.Constructor<?> provCtor = providerClass.getDeclaredConstructor();
        provCtor.setAccessible(true);
        Object provider = provCtor.newInstance();

        // Precisamos de um PriceService funcional com determineChannel testavel.
        // Como nao podemos mock DeparaService (construtor privado), usamos reflection
        // para criar o PriceService e trocar o campo deparaService apos construcao.
        Constructor<PriceService> ctor = PriceService.class.getDeclaredConstructor(
                providerClass,
                br.com.bellube.fastchannel.service.PriceResolver.class,
                br.com.bellube.fastchannel.service.PriceTableResolver.class,
                FastchannelPriceClient.class,
                FastchannelPriceClient.class);
        ctor.setAccessible(true);
        PriceService svc = ctor.newInstance(provider,
                new br.com.bellube.fastchannel.service.PriceResolver(),
                new br.com.bellube.fastchannel.service.PriceTableResolver(),
                new FastchannelPriceClient(FastchannelPriceClient.Channel.DISTRIBUTION),
                new FastchannelPriceClient(FastchannelPriceClient.Channel.CONSUMPTION));

        // Substituir deparaService no PriceService por um mock via reflection
        java.lang.reflect.Field deparaField = PriceService.class.getDeclaredField("deparaService");
        deparaField.setAccessible(true);
        // Criar instancia de DeparaService usando reflection (construtor privado)
        Constructor<DeparaService> deparaCtor = DeparaService.class.getDeclaredConstructor();
        deparaCtor.setAccessible(true);
        DeparaService mockDepara = deparaCtor.newInstance();
        // Pre-popular cache para evitar que getCodigoExterno acesse BD (EntityFacadeFactory nao existe no teste).
        // Para mappedValue null, usamos "" como sentinel - ambos resultam em "nao e DIST" no determineChannel.
        java.lang.reflect.Field cacheField = DeparaService.class.getDeclaredField("cacheSankhyaToExterno");
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.Map<String, java.util.Map<BigDecimal, String>> cache =
                (java.util.Map<String, java.util.Map<BigDecimal, String>>) cacheField.get(mockDepara);
        java.util.concurrent.ConcurrentHashMap<BigDecimal, String> typeCache = new java.util.concurrent.ConcurrentHashMap<>();
        typeCache.put(new BigDecimal("100"), mappedValue != null ? mappedValue : "");
        cache.put(DeparaService.TIPO_TABELA_PRECO, typeCache);
        deparaField.set(svc, mockDepara);
        return svc;
    }

    private FastchannelPriceClient.Channel invokeDetermineChannel(PriceService svc, BigDecimal codProd, String sku) throws Exception {
        Method m = PriceService.class.getDeclaredMethod("determineChannel", BigDecimal.class, String.class);
        m.setAccessible(true);
        return (FastchannelPriceClient.Channel) m.invoke(svc, codProd, sku);
    }
}
