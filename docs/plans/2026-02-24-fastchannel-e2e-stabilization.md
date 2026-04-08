# Fastchannel E2E Stabilization (Dev + Homolog) Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fechar a integração ponta a ponta para importar pedidos da Fastchannel no Sankhya e sincronizar produtos/preços/estoque do Sankhya para Fastchannel, com evidência técnica em dev e homolog.

**Architecture:** Executar em trilhas sequenciais curtas: (1) baseline e reprodução do erro atual, (2) correção focada no fluxo de pedidos e estratégias de criação de nota, (3) validação dos fluxos de preço (tabela 9) e estoque, (4) revalidação em homolog com logs e resultados dos endpoints.

**Tech Stack:** Java 8, Gradle, Sankhya Add-on Studio, WildFly, MSSQL, Fastchannel API, PowerShell, SSH/Tailscale.

### Task 1: Baseline e reprodução atual

**Files:**
- Read: `AGENTS.md`
- Read: `docs/plans/2026-02-23-fastchannel-e2e-dev-homolog.md`
- Read: `model/src/main/java/br/com/bellube/fastchannel/web/FCAdminService.java`
- Read: `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`

**Step 1: Validar branch/worktree e alterações existentes**
Run: `git status --short --branch`
Expected: branch `merge/unify-fastchannel` com alterações preservadas.

**Step 2: Compilar para confirmar baseline técnico**
Run: `./gradlew.bat :model:compileJava`
Expected: `BUILD SUCCESSFUL`.

**Step 3: Reproduzir importação via endpoint real**
Run: `Invoke-RestMethod -Method Post -Uri "http://localhost:8080/addon-fastchannel/fc-direct?serviceName=FCAdminSP.importarPedidos"`
Expected: sucesso ou erro reproduzível com stacktrace no log.

### Task 2: Correção do fluxo de importação de pedidos

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/OrderCreationOrchestrator.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/ServiceInvokerStrategy.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java`

**Step 1: Garantir ordem de estratégia alinhada ao legado**
Run: `rg -n "ServiceInvokerStrategy|InternalApiStrategy" model/src/main/java/br/com/bellube/fastchannel/service/strategy/OrderCreationOrchestrator.java`
Expected: `ServiceInvoker` primeiro, `InternalApi` fallback.

**Step 2: Corrigir bloqueio atual (transação/documento/parceiro) no `OrderService`**
Run: `./gradlew.bat :model:test --tests "*Order*"`
Expected: testes do fluxo de pedido sem falha bloqueante.

**Step 3: Build de segurança após patch**
Run: `./gradlew.bat :model:compileJava`
Expected: compilação limpa.

### Task 3: Preço e estoque para Fastchannel

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/PriceResolver.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/http/FastchannelPriceClient.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCEstoqueService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/StockResolver.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/http/FastchannelStockClient.java`

**Step 1: Validar sync de preço com tabela 9 (e cenário escalonado)**
Run: `Invoke-RestMethod -Method Post -Uri "http://localhost:8080/addon-fastchannel/fc-direct?serviceName=FCPrecosSP.sincronizarPrecos" -ContentType "application/json" -Body '{"priceTableId":9}'`
Expected: sucesso com itens sincronizados.

**Step 2: Validar sync de estoque com criação de estoque teste**
Run: `Invoke-RestMethod -Method Post -Uri "http://localhost:8080/addon-fastchannel/fc-direct?serviceName=FCEstoqueSP.sincronizarEstoque" -ContentType "application/json" -Body '{"createTestStock":true}'`
Expected: sucesso com confirmação de criação/atualização.

### Task 4: Deploy e validação em homolog

**Files:**
- Read: `X:/mcp-ssh-root/PLAYBOOK_AI_REMOTE_OPS.md`
- Create: `docs/validation/2026-02-24-fastchannel-e2e-dev-homolog.md`

**Step 1: Validar saúde de homolog (WildFly/portas)**
Run: comandos do playbook (`systemctl`, `curl`, `journalctl`).
Expected: `wildfly_teste` ativo e endpoints respondendo.

**Step 2: Executar bateria de SPs em homolog**
Run: `FCAdminSP.importarPedidos`, `FCPrecosSP.sincronizarPrecos`, `FCEstoqueSP.sincronizarEstoque`.
Expected: status funcional com evidência em log/retorno.

**Step 3: Consolidar resultado final por fluxo**
Registrar cada fluxo como `OK`, `OK com ressalva` ou `Falha bloqueante`.
