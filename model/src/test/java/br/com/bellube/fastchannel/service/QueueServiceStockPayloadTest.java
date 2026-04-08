package br.com.bellube.fastchannel.service;

import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertTrue;

public class QueueServiceStockPayloadTest {

    @Test
    public void includesCodEmpAndCodLocalInPayload() {
        QueueService queueService = QueueService.getInstance();
        String payload = queueService.buildStockPayload(
                "SKU1",
                new BigDecimal("5"),
                new BigDecimal("26"),
                new BigDecimal("99000000"),
                "2",
                "21"
        );
        assertTrue(payload.contains("\"codEmp\":26"));
        assertTrue(payload.contains("\"codLocal\":99000000"));
    }
}
