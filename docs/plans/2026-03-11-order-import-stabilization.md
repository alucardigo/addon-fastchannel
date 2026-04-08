# Order Import Stabilization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** restaurar a importacao de pedidos sem SELECT invalido e sem race de reprocessamento.

**Architecture:** remover lock SQL multi-statement incompatível com ConnectionProxy e substituir por idempotencia baseada em AD_FCPEDIDO.ORDER_ID. Reprocessamento manual deve aceitar PROCESSANDO vencido e manter uma unica posse por pedido.

**Tech Stack:** Java, JAPE, NativeSql, SQL Server, Gradle, JUnit.

---

### Task 1: Eliminar o lock incompatível
- Modificar `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Remover `sp_getapplock`/`sp_releaseapplock`
- Introduzir claim por `ORDER_ID`

### Task 2: Fechar race no mapeamento
- Modificar `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Trocar `SELECT then INSERT/UPDATE` por `UPDATE first + INSERT + unique-violation fallback`
- Adicionar `PROCESSANDO` com timeout de retomada

### Task 3: Ajustar retry administrativo
- Modificar `model/src/main/java/br/com/bellube/fastchannel/web/FCAdminService.java`
- Permitir reprocesso de `PROCESSANDO` vencido

### Task 4: Cobrir regressao
- Modificar `model/src/test/java/br/com/bellube/fastchannel/regression/OrderStateRegressionTest.java`
- Validar ausencia de `sp_getapplock`
- Validar idempotencia/claim textual e retry de `PROCESSANDO`

### Task 5: Verificar
- Rodar `./gradlew.bat :model:test --tests br.com.bellube.fastchannel.regression.OrderStateRegressionTest`
- Rodar `./gradlew.bat :model:test --tests br.com.bellube.fastchannel.regression.CriticalFixesRegressionTest`
- Rodar `./gradlew.bat :model:test --tests br.com.bellube.fastchannel.regression.*`
