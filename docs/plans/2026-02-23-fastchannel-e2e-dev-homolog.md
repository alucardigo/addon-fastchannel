# Fastchannel E2E Dev + Homolog Stabilization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Concluir o fluxo ponta a ponta da integração: importar pedidos Fastchannel para Sankhya e sincronizar produtos/preços (incluindo escalonado na tabela 9) e estoque Sankhya para Fastchannel em dev e homolog.

**Architecture:** Primeiro estabilizar e validar o fluxo local na worktree `merge-unify` com evidência de logs e resposta de SPs; em seguida replicar validação em homolog via acesso SSH/Tailscale e endpoint MCP always-on. O comportamento do legado será a referência para payloads, fallback de criação de pedido e regras de de/para.

**Tech Stack:** Java 8, Gradle, Sankhya Add-on Studio, WildFly, MSSQL, Fastchannel REST API, PowerShell, SSH/Tailscale.

### Task 1: Baseline de execução e diagnóstico reproduzível

**Files:**
- Read: `model/src/main/java/br/com/bellube/fastchannel/web/FastchannelDirectServlet.java`
- Read: `model/src/main/java/br/com/bellube/fastchannel/web/FCAdminService.java`
- Read: `X:/Wildfly_Clean/wildfly_producao/standalone/log/server.log`
- Read: `X:/IntegracaoFastchannel/APPFASTCHANNEL/gbi-app-integrador-main/gbi-app-integrador-main`

**Step 1: Validar estado da worktree**

Run: `git status --short --branch`
Expected: branch `merge/unify-fastchannel` com alterações existentes preservadas.

**Step 2: Compilar módulo model**

Run: `./gradlew :model:compileJava`
Expected: `BUILD SUCCESSFUL`.

**Step 3: Reproduzir fluxo de importação de pedidos via endpoint real**

Run: `Invoke-RestMethod -Method Post -Uri "http://localhost:8080/addon-fastchannel/fc-direct?serviceName=FCAdminSP&serviceMethod=importarPedidos" -ContentType "application/json" -Body "{}"`
Expected: erro reproduzido ou sucesso parcial com mensagem detalhada.

**Step 4: Capturar stacktrace no server.log**

Run: `rg -n "FCAdminSP|importarPedidos|OrderService|ERROR|Exception" X:/Wildfly_Clean/wildfly_producao/standalone/log/server.log`
Expected: evidência do ponto exato de falha atual.

### Task 2: Fechar importação de pedidos Fastchannel -> Sankhya

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/http/FastchannelOrdersClient.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/OrderCreationOrchestrator.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/ServiceInvokerStrategy.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/service/`

**Step 1: Escrever/ajustar teste para cenário de importação com mock de pedido**

Run: `./gradlew :model:test --tests "*Order*"`
Expected: teste reproduzindo o erro atual antes da correção.

**Step 2: Corrigir regra bloqueante no `OrderService`**

Implementar o mínimo para passar no cenário falho (transação ativa, parceiro/documento fallback, criação da nota).

**Step 3: Garantir estratégia de criação de pedido alinhada ao legado**

Ordem obrigatória: `ServiceInvoker (CACSP.incluirNota)` primeiro, `InternalAPI` como fallback.

**Step 4: Reexecutar testes afetados**

Run: `./gradlew :model:test --tests "*Order*"`
Expected: PASS nos testes do fluxo de importação.

### Task 3: Sincronização de preços Sankhya -> Fastchannel (tabela 9 + escalonado)

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/http/FastchannelPriceClient.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/PriceResolver.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/PriceBatchResolver.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/web/FCPrecosServiceTest.java`

**Step 1: Cobrir cenário de tabela 9 e preço escalonado em teste**

Run: `./gradlew :model:test --tests "*FCPrecos*"`
Expected: FAIL inicial para lacunas de payload/regra.

**Step 2: Ajustar payload Fastchannel para preço base + escalonado**

Aplicar regras do legado e forçar tabela ID 9 em ambiente dev quando ausente no request.

**Step 3: Validar chamada de sincronização real**

Run: `Invoke-RestMethod -Method Post -Uri "http://localhost:8080/addon-fastchannel/fc-direct?serviceName=FCPrecosSP&serviceMethod=sincronizarPrecos" -ContentType "application/json" -Body '{"priceTableId":9}'`
Expected: sucesso e evidência de itens enviados.

### Task 4: Sincronização de estoque Sankhya -> Fastchannel (novo estoque de teste)

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCEstoqueService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/StockResolver.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/http/`
- Test: `model/src/test/java/br/com/bellube/fastchannel/web/FCEstoqueServiceTest.java`

**Step 1: Cobrir cenário de criação de estoque de teste na API**

Run: `./gradlew :model:test --tests "*FCEstoque*"`
Expected: FAIL inicial se houver divergência de contrato.

**Step 2: Corrigir endpoint/payload de estoque no client Fastchannel**

Implementar criação/atualização de estoque de teste conforme API alvo.

**Step 3: Validar sincronização real via SP**

Run: `Invoke-RestMethod -Method Post -Uri "http://localhost:8080/addon-fastchannel/fc-direct?serviceName=FCEstoqueSP&serviceMethod=sincronizarEstoque" -ContentType "application/json" -Body '{"createTestStock":true}'`
Expected: sucesso com confirmação de criação/atualização.

### Task 5: Build, deploy e validação local completa

**Files:**
- Modify: `docs/validation/2026-02-23-fastchannel-e2e-dev-homolog.md` (create)

**Step 1: Build completo**

Run: `./gradlew clean build`
Expected: `BUILD SUCCESSFUL`.

**Step 2: Deploy local e smoke dos serviços principais**

Run: endpoints `FCConfigSP.get`, `FCAdminSP.importarPedidos`, `FCPrecosSP.sincronizarPrecos`, `FCEstoqueSP.sincronizarEstoque`.
Expected: todos retornando sucesso funcional.

**Step 3: Registrar evidências**

Salvar request/response resumidos, IDs gerados e trechos relevantes de log.

### Task 6: Validação em homolog via SSH/Tailscale

**Files:**
- Modify: `docs/validation/2026-02-23-fastchannel-e2e-dev-homolog.md`
- Read: `X:/mcp-ssh-root/PLAYBOOK_AI_REMOTE_OPS.md`

**Step 1: Verificar saúde do servidor homolog**

Run: `ssh_healthcheck` (MCP) ou `systemctl status wildfly_teste --no-pager`.
Expected: `wildfly_teste active`, portas 8080/9990 ativas.

**Step 2: Executar smoke em homolog**

Chamar os mesmos SPs e validar importação/sincronização com dados reais disponíveis.

**Step 3: Consolidar resultado final**

Classificar cada fluxo como `OK`, `OK com ressalva`, ou `Falha bloqueante`, com causa e próximo patch.
