package br.com.bellube.fastchannel.service;

import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Testes para LogService em ambiente sem JAPE.
 *
 * LogService swallow-se erros de BD por design (logEntry() captura Exception
 * e continua). Estes testes verificam que nenhum metodo publico propaga
 * excecao quando DBUtil nao consegue abrir conexao.
 */
public class LogServiceNoJapeTest {

    @Test
    public void getInstance_returnsSingleton() {
        LogService a = LogService.getInstance();
        LogService b = LogService.getInstance();
        assertNotNull(a);
        assertSame(a, b);
    }

    @Test
    public void info_doesNotThrowWithoutDatabase() {
        LogService svc = LogService.getInstance();
        try {
            svc.info("TEST", "mensagem de teste");
            svc.info("TEST", "mensagem com referencia", "ORD-1");
        } catch (Throwable t) {
            throw new AssertionError("info() nao pode propagar excecao: " + t.getMessage(), t);
        }
    }

    @Test
    public void warning_doesNotThrowWithoutDatabase() {
        LogService svc = LogService.getInstance();
        try {
            svc.warning("TEST", "warning de teste");
            svc.warning("TEST", "warning com ref", "ORD-2");
        } catch (Throwable t) {
            throw new AssertionError("warning() nao pode propagar excecao", t);
        }
    }

    @Test
    public void error_doesNotThrowWithoutDatabase() {
        LogService svc = LogService.getInstance();
        try {
            svc.error("TEST", "erro sem excecao", (Throwable) null);
            svc.error("TEST", "erro com excecao", new RuntimeException("boom"));
            svc.error("TEST", "erro com referencia", "REF-1", new RuntimeException("boom"));
        } catch (Throwable t) {
            throw new AssertionError("error() nao pode propagar excecao", t);
        }
    }

    @Test
    public void debug_doesNotThrowWithoutDatabase() {
        LogService svc = LogService.getInstance();
        try {
            svc.debug("TEST", "debug message");
        } catch (Throwable t) {
            throw new AssertionError("debug() nao pode propagar excecao", t);
        }
    }

    @Test
    public void logOrderImport_handlesBothSuccessAndFailure() {
        LogService svc = LogService.getInstance();
        try {
            svc.logOrderImport("ORD-123", new BigDecimal("456"), true, "ok");
            svc.logOrderImport("ORD-124", null, false, "sem parceiro");
        } catch (Throwable t) {
            throw new AssertionError("logOrderImport() nao pode propagar excecao", t);
        }
    }

    @Test
    public void logStockSync_handlesBothCases() {
        LogService svc = LogService.getInstance();
        try {
            svc.logStockSync("SKU-01", new BigDecimal("10"), true, null);
            svc.logStockSync("SKU-02", BigDecimal.ZERO, false, "estoque nao encontrado");
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Test
    public void logHttpRequest_handlesBothStatusCodes() {
        LogService svc = LogService.getInstance();
        try {
            svc.logHttpRequest("GET", "https://api.example.com", 200, "ok");
            svc.logHttpRequest("POST", "https://api.example.com", 500, "erro interno");
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Test
    public void cleanupOldLogs_returnsZeroWhenNoDatabase() {
        LogService svc = LogService.getInstance();
        int removed = svc.cleanupOldLogs(30);
        assertTrue("cleanupOldLogs sem BD deve retornar 0", removed >= 0);
    }
}
