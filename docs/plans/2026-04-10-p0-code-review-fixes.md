# P0 Code Review Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Corrigir os P0 ainda abertos no fluxo crítico de pedidos do addon-fastchannel identificados pelos testes de regressão de code review.

**Architecture:** O primeiro ajuste endurece o `InternalApiStrategy` para falhar cedo quando chaves compostas versionadas (`DHTIPOPER`/`DHTIPVENDA`) não puderem ser resolvidas, eliminando fallback silencioso com timestamp atual. O segundo ajuste torna o `OrderStatusSyncJob` compatível com schemas sem `NUNOTA_FATURA`/`NUCHAVE_NFE`, preservando aliases previsíveis no `SELECT` dinâmico para o restante do job.

**Tech Stack:** Java 8, Gradle, Sankhya JAPE/JdbcWrapper, JUnit 4.

---

### Task 1: Fail-fast de Header Versionado

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/regression/OrderStateRegressionTest.java`

**Step 1: Executar teste de regressão que já falha**

Run: `./gradlew.bat test --tests "br.com.bellube.fastchannel.regression.OrderStateRegressionTest.internalApiStrategy_mustFailFastWhenHeaderCompositeKeysAreIncomplete"`
Expected: FAIL porque o código atual ainda usa fallback com `new java.sql.Timestamp(System.currentTimeMillis())`.

**Step 2: Implementar o ajuste mínimo**

Editar `InternalApiStrategy.createCabecalho(...)` para:
- manter o `throw new Exception("CODPARC nao resolvido para o pedido ...")`
- substituir o fallback silencioso de `DHTIPOPER` por `throw new Exception("DHTIPOPER nao resolvido para CODTIPOPER ...")`
- substituir o fallback silencioso de `DHTIPVENDA` por `throw new Exception("DHTIPVENDA nao resolvido para CODTIPVENDA ...")`
- não introduzir fallback aleatório de header.

**Step 3: Reexecutar o teste**

Run: `./gradlew.bat test --tests "br.com.bellube.fastchannel.regression.OrderStateRegressionTest.internalApiStrategy_mustFailFastWhenHeaderCompositeKeysAreIncomplete"`
Expected: PASS.

### Task 2: SELECT Dinâmico Compatível com Schema Antigo

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/job/OrderStatusSyncJob.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/regression/OrderStateRegressionTest.java`

**Step 1: Executar teste de regressão que já falha**

Run: `./gradlew.bat test --tests "br.com.bellube.fastchannel.regression.OrderStateRegressionTest.orderStatusSyncJob_mustTolerateSchemaWithoutNunotaFatura"`
Expected: FAIL porque o SQL dinâmico atual usa alias `NF_NUMERO`/`NF_CHAVE` mesmo quando a coluna antiga não existe.

**Step 2: Implementar o ajuste mínimo**

Editar `OrderStatusSyncJob.executeScheduler()` para:
- montar o `SELECT` dinâmico com aliases estáveis usando `CAST(NULL AS VARCHAR(50)) AS NUNOTA_FATURA` e `CAST(NULL AS VARCHAR(50)) AS NUCHAVE_NFE` quando necessário
- ler `rs.getString("NUNOTA_FATURA")` e `rs.getString("NUCHAVE_NFE")` no bloco de envio de NF
- manter a prioridade de colunas novas (`NF_NUMERO`, `NF_CHAVE`) quando existirem, apenas re-aliasando para os nomes estáveis.

**Step 3: Reexecutar o teste**

Run: `./gradlew.bat test --tests "br.com.bellube.fastchannel.regression.OrderStateRegressionTest.orderStatusSyncJob_mustTolerateSchemaWithoutNunotaFatura"`
Expected: PASS.

### Task 3: Verificação Integrada

**Files:**
- Modify: `docs/plans/2026-04-10-p0-code-review-fixes.md`

**Step 1: Rodar a suíte direcionada**

Run: `./gradlew.bat test --tests "br.com.bellube.fastchannel.regression.OrderStateRegressionTest" --tests "br.com.bellube.fastchannel.service.OrderServiceNoJapeTest" --tests "br.com.bellube.fastchannel.service.OrderXmlBuilderTest" --tests "br.com.bellube.fastchannel.http.FastchannelOrdersClientTest"`
Expected: PASS nos testes direcionados desta rodada.

**Step 2: Registrar evidência**

Run: `git diff -- model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java model/src/main/java/br/com/bellube/fastchannel/job/OrderStatusSyncJob.java`
Expected: diff curto, focado nesses dois P0.
