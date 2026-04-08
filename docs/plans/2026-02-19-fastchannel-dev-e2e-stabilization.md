# Fastchannel Dev E2E Stabilization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fazer a importacao de pedidos Fastchannel -> Sankhya funcionar em dev e garantir sincronizacao Sankhya -> Fastchannel de produtos/precos/estoque (incluindo tabela de preco ID 9 e novo estoque de teste na API).

**Architecture:** Validar primeiro infraestrutura local (MSSQL + WildFly + deploy do addon), depois executar fluxos pelos mesmos endpoints/SPs usados pela UI (`/addon-fastchannel/fc-direct` e servicos `FC*SP`). Corrigir apenas pontos bloqueadores no backend (servicos web, clients HTTP e resolvers) com foco em comportamento equivalente ao legado.

**Tech Stack:** Java 8, Gradle, Sankhya Addon Studio, WildFly, MSSQL (Docker), Fastchannel REST API.

### Task 1: Runtime baseline (MSSQL + WildFly + deploy limpo)

**Files:**
- Modify: `scripts/dev/restart-local-stack.ps1` (criar se nao existir)
- Modify: `scripts/dev/check-local-stack.ps1` (criar se nao existir)
- Verify: `X:/Wildfly_Clean/wildfly_producao/standalone/log/server.log`

**Step 1: Escrever script de restart do ambiente local**

```powershell
# scripts/dev/restart-local-stack.ps1
docker start skdev-mssql | Out-Null
Get-Process java -ErrorAction SilentlyContinue | Where-Object {
  $_.Path -like "*wildfly*"
} | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Process cmd.exe -ArgumentList '/c', 'X:\Wildfly_Clean\START_SERVER_LOCAL.bat'
```

**Step 2: Executar restart**

Run: `pwsh -NoProfile -File scripts/dev/restart-local-stack.ps1`
Expected: MSSQL em `1433`, WildFly em `8080` e `9990`.

**Step 3: Verificar boot e deploy**

Run: `rg -n "WFLYSRV0025|Addon-FastChannel|WFLYUT0021" X:/Wildfly_Clean/wildfly_producao/standalone/log/server.log`
Expected: boot completo e addon sem `.failed`.

**Step 4: Commit**

```bash
git add scripts/dev/restart-local-stack.ps1 scripts/dev/check-local-stack.ps1
git commit -m "chore: add local stack restart and health check scripts"
```

### Task 2: Endpoint e servicos administrativos (health + conexao Fastchannel)

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FastchannelDirectServlet.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCAdminService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/config/FastchannelConfig.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/web/FastchannelDirectServletTest.java`

**Step 1: Escrever teste de roteamento basico do servlet**

```java
@Test
public void shouldRouteFcAdminGetAndTestConnection() {
    // mock request serviceName=FCAdminSP serviceMethod=get
    // assert response json has success=true
}
```

**Step 2: Executar teste e validar falha (se existir gap)**

Run: `./gradlew :model:test --tests "*FastchannelDirectServletTest"`
Expected: FAIL se rota/metodo ainda estiver incorreto.

**Step 3: Corrigir parsing de request e roteamento para FCAdminService**

```java
String serviceName = req.getParameter("serviceName");
String serviceMethod = req.getParameter("serviceMethod");
if ("FCAdminSP".equals(serviceName)) {
    dispatchAdmin(serviceMethod, bodyJson);
}
```

**Step 4: Reexecutar teste**

Run: `./gradlew :model:test --tests "*FastchannelDirectServletTest"`
Expected: PASS.

**Step 5: Commit**

```bash
git add model/src/main/java/br/com/bellube/fastchannel/web/FastchannelDirectServlet.java model/src/main/java/br/com/bellube/fastchannel/web/FCAdminService.java model/src/main/java/br/com/bellube/fastchannel/config/FastchannelConfig.java model/src/test/java/br/com/bellube/fastchannel/web/FastchannelDirectServletTest.java
git commit -m "fix: stabilize admin/direct endpoint routing and config resolution"
```

### Task 3: Importacao de pedidos Fastchannel -> Sankhya

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/http/FastchannelOrdersClient.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/OrderCreationOrchestrator.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/service/OrderServiceImportTest.java`

**Step 1: Escrever teste de importacao com pedido mock**

```java
@Test
public void shouldImportOrderAndCreateNufinWhenValidMockPayload() {
    // mock Fastchannel order payload
    // assert order persisted and mapped in depara
}
```

**Step 2: Rodar teste de importacao**

Run: `./gradlew :model:test --tests "*OrderServiceImportTest"`
Expected: FAIL reproduzindo erro atual.

**Step 3: Corrigir transacao/documento/parceiro fallback no fluxo de importacao**

```java
jdbcWrapper.openSession();
jdbcWrapper.openTransaction();
try {
    BigDecimal codParc = resolveOrCreatePartnerWithValidDocument(order);
    BigDecimal nunota = createOrderViaOrchestrator(order, codParc);
    jdbcWrapper.commitTransaction();
} catch (Exception e) {
    jdbcWrapper.rollbackTransaction();
    throw e;
}
```

**Step 4: Revalidar localmente via SP**

Run: `Invoke-RestMethod -Method Post -Uri "http://localhost:8080/addon-fastchannel/fc-direct?serviceName=FCAdminSP&serviceMethod=importarPedidos" -ContentType "application/json" -Body "{}"`
Expected: retorno com sucesso e itens importados.

**Step 5: Commit**

```bash
git add model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java model/src/main/java/br/com/bellube/fastchannel/http/FastchannelOrdersClient.java model/src/main/java/br/com/bellube/fastchannel/service/strategy/OrderCreationOrchestrator.java model/src/test/java/br/com/bellube/fastchannel/service/OrderServiceImportTest.java
git commit -m "fix: make order import resilient with transaction-safe partner/order creation"
```

### Task 4: Precos e estoque Sankhya -> Fastchannel (tabela 9 e estoque novo)

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCEstoqueService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/http/FastchannelPriceClient.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/http/FastchannelStockClient.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/PriceResolver.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/StockResolver.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/web/FCPrecosServiceTest.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/web/FCEstoqueServiceTest.java`

**Step 1: Escrever testes de sincronizacao de preco (incluindo escalonado) e estoque**

```java
@Test
public void shouldSyncPriceTableNineIncludingTierPrices() { }

@Test
public void shouldCreateAndSyncNewStockInFastchannel() { }
```

**Step 2: Executar testes**

Run: `./gradlew :model:test --tests "*FCPrecosServiceTest" --tests "*FCEstoqueServiceTest"`
Expected: FAIL onde houver lacuna de payload/endpoint.

**Step 3: Ajustar payloads/headers para API Fastchannel e forcar tabela 9 em dev**

```java
if (devMode && requestedPriceTableId == null) {
    requestedPriceTableId = BigDecimal.valueOf(9);
}
priceClient.syncPriceTable(requestedPriceTableId, prices, tierPrices);
stockClient.createOrUpdateStock(testWarehouseCode, items);
```

**Step 4: Validar via SPs de tela**

Run: `Invoke-RestMethod -Method Post -Uri "http://localhost:8080/addon-fastchannel/fc-direct?serviceName=FCPrecosSP&serviceMethod=sincronizarPrecos" -ContentType "application/json" -Body "{\"priceTableId\":9}"`
Expected: sucesso com quantidade de precos enviada.

Run: `Invoke-RestMethod -Method Post -Uri "http://localhost:8080/addon-fastchannel/fc-direct?serviceName=FCEstoqueSP&serviceMethod=sincronizarEstoque" -ContentType "application/json" -Body "{\"createTestStock\":true}"`
Expected: sucesso com estoque criado/atualizado na Fastchannel.

**Step 5: Commit**

```bash
git add model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java model/src/main/java/br/com/bellube/fastchannel/web/FCEstoqueService.java model/src/main/java/br/com/bellube/fastchannel/http/FastchannelPriceClient.java model/src/main/java/br/com/bellube/fastchannel/http/FastchannelStockClient.java model/src/main/java/br/com/bellube/fastchannel/service/PriceResolver.java model/src/main/java/br/com/bellube/fastchannel/service/StockResolver.java model/src/test/java/br/com/bellube/fastchannel/web/FCPrecosServiceTest.java model/src/test/java/br/com/bellube/fastchannel/web/FCEstoqueServiceTest.java
git commit -m "feat: sync price table 9 and test stock to fastchannel from sankhya"
```

### Task 5: Verificacao final e evidencias

**Files:**
- Modify: `docs/validation/2026-02-19-fastchannel-dev-e2e.md` (criar)

**Step 1: Rodar build/test/deploy final**

Run: `./gradlew clean build`
Expected: BUILD SUCCESSFUL.

**Step 2: Rodar bateria de SPs em sequencia**

Run:
1. `FCConfigSP.get`
2. `FCAdminSP.testarConexao`
3. `FCAdminSP.importarPedidos`
4. `FCPrecosSP.sincronizarPrecos` (tabela 9)
5. `FCEstoqueSP.sincronizarEstoque` (estoque teste)

Expected: todos com sucesso.

**Step 3: Documentar evidencias**

```markdown
- data/hora
- request resumido
- response resumido
- trechos de log server.log
- ids de pedido/preco/estoque processados
```

**Step 4: Commit**

```bash
git add docs/validation/2026-02-19-fastchannel-dev-e2e.md
git commit -m "docs: add dev end-to-end validation evidence for fastchannel integration"
```

