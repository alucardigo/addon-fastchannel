package br.com.bellube.fastchannel.service;

import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Testes da camada de idempotencia do ORDER_IMPORT.
 *
 * O objetivo aqui e validar comportamento executavel dos helpers puros
 * (sem banco real) e manter apenas os invariantes estruturais que dependem
 * da SQL montada em fonte.
 */
public class OrderServiceIdempotencyTest {

    @Test
    public void claimFactory_claimedStartsWithoutNuNotaAndWithoutProcessingFlag() throws Exception {
        Object claim = invokeClaimFactory("claimed", null);

        assertFalse(invokeClaimHasExistingNuNota(claim));
        assertNull(invokeClaimGetExistingNuNota(claim));
        assertFalse(invokeClaimIsAlreadyProcessing(claim));
    }

    @Test
    public void claimFactory_reusedCarriesExistingNuNota() throws Exception {
        BigDecimal nuNota = new BigDecimal("4742473");
        Object claim = invokeClaimFactory("reused", nuNota);

        assertTrue(invokeClaimHasExistingNuNota(claim));
        assertEquals(nuNota, invokeClaimGetExistingNuNota(claim));
        assertFalse(invokeClaimIsAlreadyProcessing(claim));
    }

    @Test
    public void claimFactory_alreadyProcessingSetsOnlyProcessingFlag() throws Exception {
        Object claim = invokeClaimFactory("alreadyProcessing", null);

        assertFalse(invokeClaimHasExistingNuNota(claim));
        assertNull(invokeClaimGetExistingNuNota(claim));
        assertTrue(invokeClaimIsAlreadyProcessing(claim));
    }

    @Test
    public void activeProcessingClaim_recentProcessandoIsActive() throws Exception {
        Timestamp recent = new Timestamp(System.currentTimeMillis() - 60_000L);
        Object snapshot = newSnapshot("4490", null, null, "PROCESSANDO", recent);

        assertTrue(invokeIsActiveProcessingClaim(snapshot));
    }

    @Test
    public void activeProcessingClaim_nullTimestampStillCountsAsActive() throws Exception {
        Object snapshot = newSnapshot("4490", null, null, "PROCESSANDO", null);

        assertTrue(invokeIsActiveProcessingClaim(snapshot));
    }

    @Test
    public void activeProcessingClaim_staleProcessandoIsNotActive() throws Exception {
        long timeoutMillis = resolveClaimTimeoutMinutes() * 60L * 1000L;
        Timestamp stale = new Timestamp(System.currentTimeMillis() - timeoutMillis - 5_000L);
        Object snapshot = newSnapshot("4490", null, null, "PROCESSANDO", stale);

        assertFalse(invokeIsActiveProcessingClaim(snapshot));
    }

    @Test
    public void activeProcessingClaim_nonProcessandoStatusIsNotActive() throws Exception {
        Timestamp recent = new Timestamp(System.currentTimeMillis() - 60_000L);
        Object snapshot = newSnapshot("4490", null, null, "ERRO", recent);

        assertFalse(invokeIsActiveProcessingClaim(snapshot));
    }

    @Test
    public void activeProcessingClaim_nullSnapshotIsNotActive() throws Exception {
        assertFalse(invokeIsActiveProcessingClaim(null));
    }

    @Test
    public void uniqueViolation_detectsConstraintNameInExceptionChain() throws Exception {
        Exception root = new Exception("Violacao da constraint UK_FCPEDIDO_ORDERID");

        assertTrue(invokeIsOrderMappingUniqueViolation(root));
    }

    @Test
    public void uniqueViolation_detectsSqlState23000() throws Exception {
        SQLException root = new SQLException("duplicate key", "23000");

        assertTrue(invokeIsOrderMappingUniqueViolation(root));
    }

    @Test
    public void uniqueViolation_detectsNestedDuplicateKeyword() throws Exception {
        RuntimeException root = new RuntimeException("wrapper",
                new IllegalStateException("duplicate entry for order id"));

        assertTrue(invokeIsOrderMappingUniqueViolation(root));
    }

    @Test
    public void uniqueViolation_genericErrorReturnsFalse() throws Exception {
        Exception root = new Exception("timeout de conexao");

        assertFalse(invokeIsOrderMappingUniqueViolation(root));
    }

    @Test
    public void takeOverSql_preservesCodParcAndUsesStaleCutoff() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/OrderService.java");
        int start = src.indexOf("private boolean takeOverOrderMappingClaim(");
        int end = src.indexOf("\n    private ", start + 1);
        String body = end > 0 ? src.substring(start, end) : src.substring(start);

        assertTrue(body.contains("NUNOTA = NULL"));
        assertFalse(body.contains("CODPARC = NULL"));
        assertTrue(body.contains("DH_IMPORTACAO < :staleCutoff"));
        assertTrue(body.contains("STATUS_IMPORT_PROCESSANDO"));
    }

    @Test
    public void finalUpsert_stillTargetsSameRowByOrderId() throws Exception {
        String src = readMainSource("br/com/bellube/fastchannel/service/OrderService.java");
        int start = src.indexOf("private void upsertOrderMapping(");
        int end = src.indexOf("\n    private ", start + 1);
        String body = end > 0 ? src.substring(start, end) : src.substring(start);

        assertTrue(body.contains("UPDATE AD_FCPEDIDO SET"));
        assertTrue(body.contains("WHERE ORDER_ID = :orderId"));
        assertTrue(body.contains("NUNOTA = :nuNota, CODPARC = :codParc"));
    }

    private boolean invokeIsActiveProcessingClaim(Object snapshot) throws Exception {
        OrderService service = new OrderService();
        Method method = OrderService.class.getDeclaredMethod("isActiveProcessingClaim", orderMappingSnapshotClass());
        method.setAccessible(true);
        return (Boolean) method.invoke(service, snapshot);
    }

    private boolean invokeIsOrderMappingUniqueViolation(Throwable error) throws Exception {
        OrderService service = new OrderService();
        Method method = OrderService.class.getDeclaredMethod("isOrderMappingUniqueViolation", Throwable.class);
        method.setAccessible(true);
        return (Boolean) method.invoke(service, error);
    }

    private Object invokeClaimFactory(String methodName, BigDecimal nuNota) throws Exception {
        Method method;
        Class<?> claimClass = orderImportClaimClass();
        if ("reused".equals(methodName)) {
            method = claimClass.getDeclaredMethod(methodName, BigDecimal.class);
            method.setAccessible(true);
            return method.invoke(null, nuNota);
        }
        method = claimClass.getDeclaredMethod(methodName);
        method.setAccessible(true);
        return method.invoke(null);
    }

    private boolean invokeClaimHasExistingNuNota(Object claim) throws Exception {
        Method method = orderImportClaimClass().getDeclaredMethod("hasExistingNuNota");
        method.setAccessible(true);
        return (Boolean) method.invoke(claim);
    }

    private BigDecimal invokeClaimGetExistingNuNota(Object claim) throws Exception {
        Method method = orderImportClaimClass().getDeclaredMethod("getExistingNuNota");
        method.setAccessible(true);
        return (BigDecimal) method.invoke(claim);
    }

    private boolean invokeClaimIsAlreadyProcessing(Object claim) throws Exception {
        Method method = orderImportClaimClass().getDeclaredMethod("isAlreadyProcessing");
        method.setAccessible(true);
        return (Boolean) method.invoke(claim);
    }

    private Object newSnapshot(String orderId, BigDecimal nuNota, BigDecimal codParc,
                               String statusImport, Timestamp dhImportacao) throws Exception {
        Constructor<?> ctor = orderMappingSnapshotClass().getDeclaredConstructor(
                String.class, BigDecimal.class, BigDecimal.class, String.class, Timestamp.class);
        ctor.setAccessible(true);
        return ctor.newInstance(orderId, nuNota, codParc, statusImport, dhImportacao);
    }

    private long resolveClaimTimeoutMinutes() throws Exception {
        Field field = OrderService.class.getDeclaredField("ORDER_IMPORT_CLAIM_TIMEOUT_MINUTES");
        field.setAccessible(true);
        return ((Number) field.get(null)).longValue();
    }

    private Class<?> orderImportClaimClass() throws Exception {
        return Class.forName("br.com.bellube.fastchannel.service.OrderService$OrderImportClaim");
    }

    private Class<?> orderMappingSnapshotClass() throws Exception {
        return Class.forName("br.com.bellube.fastchannel.service.OrderService$OrderMappingSnapshot");
    }

    private String readMainSource(String relativeMainJavaPath) throws Exception {
        Path fromRepoRoot = Paths.get("model", "src", "main", "java").resolve(relativeMainJavaPath);
        if (Files.exists(fromRepoRoot)) {
            return new String(Files.readAllBytes(fromRepoRoot), StandardCharsets.UTF_8);
        }

        Path fromModelRoot = Paths.get("src", "main", "java").resolve(relativeMainJavaPath);
        if (Files.exists(fromModelRoot)) {
            return new String(Files.readAllBytes(fromModelRoot), StandardCharsets.UTF_8);
        }

        throw new IllegalStateException("Arquivo fonte nao encontrado: " + relativeMainJavaPath);
    }
}
