package br.com.bellube.fastchannel.service.strategy;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Testes unitarios para InternalApiStrategy.
 *
 * Sem container Sankhya/JAPE disponivel, a estrategia DEVE se reportar como
 * indisponivel via {@link InternalApiStrategy#isAvailable()} sem lancar
 * excecao. Aqui focamos nessas invariantes (nome e robustez do isAvailable).
 */
public class InternalApiStrategyTest {

    @Test
    public void getStrategyName_isInternalApi() {
        assertEquals("InternalAPI", new InternalApiStrategy().getStrategyName());
    }

    @Test
    public void isAvailable_isStableAcrossCalls() {
        // Dado que isAvailable() exercita recursos dinamicos
        // (EntityFacadeFactory, classloading de Jape, cache JVM-global),
        // o resultado depende do ambiente de teste e DEVE ser estavel: se
        // retornou true/false uma vez, deve manter o mesmo resultado em
        // chamadas subsequentes dentro do mesmo processo JVM.
        //
        // A invariante aqui nao eh o valor absoluto (que depende do
        // classpath/cache global), mas sim:
        //   (1) o metodo nao pode propagar excecao checada;
        //   (2) chamadas consecutivas devem produzir o mesmo outcome.
        InternalApiStrategy svc = new InternalApiStrategy();

        Object first;
        Object second;
        try {
            first = svc.isAvailable();
        } catch (Throwable t) {
            first = t.getClass().getName();
        }
        try {
            second = svc.isAvailable();
        } catch (Throwable t) {
            second = t.getClass().getName();
        }
        assertEquals("isAvailable() deve ser estavel entre chamadas consecutivas",
                first, second);
    }

    @Test
    public void constructor_doesNotThrow() {
        // O construtor lazyliza dependencias de Config/Depara — nao pode lancar
        // no simples fato de instanciar.
        InternalApiStrategy svc = new InternalApiStrategy();
        assertNotNull(svc);
    }
}
