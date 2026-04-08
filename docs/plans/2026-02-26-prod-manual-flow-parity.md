# Fastchannel Pedido Manual-Flow Parity Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Ajustar a importação de pedidos Fastchannel para reproduzir o comportamento de pedido manual no Sankhya, sem bypass de gatilhos/regras, corrigindo mapeamentos de cabeçalho/itens e removendo duplicidades.

**Architecture:** Centralizar as regras de criação em `OrderService` e estratégias (`ServiceInvoker`/`InternalAPI`) garantindo que todos os campos críticos de `TGFCAB/TGFITE` sejam resolvidos antes da tentativa de confirmação. Reforçar validações de de-para (TOP, tipo de venda/prazo, local de estoque) e aplicar fallback seguro para ambientes sem campos adicionais via migração.

**Tech Stack:** Java 8+, Sankhya Add-on SDK, SQL Server, Gradle, Fastchannel API.

### Task 1: Diagnóstico legado x merge-unify

**Files:**
- Modify: `docs/plans/2026-02-26-prod-functional-loop-stabilization.md`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/ServiceInvokerStrategy.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java`

**Step 1: Mapear pontos obrigatórios do legado**
Run: `rg -n "TOP|CODVEND|AD_CODVENDEXEC|CIF_FOB|NUTAB|TGFEXC|TGFTPV|observ" X:/IntegracaoFastchannel/APPFASTCHANNEL/gbi-app-integrador-main/gbi-app-integrador-main -g "*.java"`
Expected: Lista de fontes com regras de cabeçalho/item e prazo.

**Step 2: Mapear implementação atual**
Run: `rg -n "TOP|CODVEND|AD_CODVENDEXEC|CIF_FOB|NUTAB|TGFEXC|TGFTPV|observ|confirm" X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java -g "*.java"`
Expected: Pontos de divergência localizados.

**Step 3: Documentar gaps**
Registrar no plano operacional os gaps encontrados (campos faltantes, confirmação indevida, anti-duplicidade).

### Task 2: Cabeçalho do pedido (TGFCAB)

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderXmlBuilder.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/service/OrderServiceHeaderRulesTest.java`

**Step 1: Criar teste de regras de cabeçalho**
Cobrir: `TOP=403`, `CODVEND=167`, `AD_CODVENDEXEC=167`, `CIF_FOB='C'`, número Fast em observação interna, remoção da observação pública.

**Step 2: Implementar mapeamento mínimo**
Adicionar/forçar campos no objeto/XML de criação respeitando regras do Sankhya.

**Step 3: Bloquear confirmação em erro de validação**
Quando falhar validação de prazo/local/regras, não confirmar pedido.

**Step 4: Rodar teste**
Run: `./gradlew :model:test --tests "*OrderServiceHeaderRulesTest"`
Expected: PASS.

### Task 3: Itens do pedido (TGFITE)

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/ServiceInvokerStrategy.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/service/OrderServiceItemRulesTest.java`

**Step 1: Criar teste para campos de item**
Cobrir preenchimento de `VLRCUS`, `USOPROD`, `CUSTO`, `ATUALESTTERC`, `TERCEIRO`, `PRECOBASE` e `NUTAB` via `TGFEXC`.

**Step 2: Implementar resolução de NUTAB/TGFEXC**
Aplicar resolução por empresa/tabela do site e fallback explícito com log.

**Step 3: Aplicar estoque reserva**
Resolver `CODLOCAL` com priorização de de-para de estoque reserva.

**Step 4: Rodar teste**
Run: `./gradlew :model:test --tests "*OrderServiceItemRulesTest"`
Expected: PASS.

### Task 4: Prazo/Tipo de venda e anti-duplicidade

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/http/FastchannelOrdersClient.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCAdminService.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/service/OrderImportDedupTest.java`

**Step 1: Validar prazo via TGFTPV.AD_IDFAST**
Mapear `paymentConditionId` da Fast para tipo de venda/prazo válido.

**Step 2: Anti-duplicidade por pedido Fast**
Antes de incluir, consultar se já existe pedido importado para o mesmo ID Fast (incluindo legado).

**Step 3: Não confirmar quando validação falhar**
Persistir erro detalhado em log/fila e marcar como não confirmado.

**Step 4: Rodar teste**
Run: `./gradlew :model:test --tests "*OrderImportDedupTest"`
Expected: PASS.

### Task 5: Migração de campos adicionais e compatibilidade ambiente

**Files:**
- Modify: `dbscripts/V11.xml`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/util/DbColumnSupport.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/util/DbColumnSupportTest.java`

**Step 1: Declarar criação de campos obrigatórios ausentes**
Garantir scripts de migração para ambientes sem os campos adicionais.

**Step 2: Implementar fallback controlado**
Se campo faltar, não quebrar importação; registrar orientação operacional.

**Step 3: Rodar teste**
Run: `./gradlew :model:test --tests "*DbColumnSupportTest"`
Expected: PASS.

### Task 6: Build, publicação e validação funcional em produção

**Files:**
- Modify: `build.gradle`
- Modify: `docs/validation/prod-e2e-2026-02-26.md`

**Step 1: Atualizar versão**
Incrementar `ADDON_VERSION`.

**Step 2: Build**
Run: `./gradlew clean :model:compileJava -x test`
Expected: BUILD SUCCESSFUL.

**Step 3: Publicação**
Run: `./gradlew --no-daemon publishAddon "-PADDON_VERSION=<nova>" "-PADDON_LICENSE_ID=2997960" "-Pemail=suporteti@bellube.com.br" "-Ppassword=102030" "-Ppublish=true"`
Expected: addon atualizado na Área Dev.

**Step 4: Teste funcional em loop**
Executar ciclo: importar pedido Fast -> validar no Sankhya UI/DB -> sincronizar preço via Sankhya -> conferir na Fast UI -> sincronizar estoque via Sankhya -> conferir Fast UI.

**Step 5: Registrar evidências**
Salvar evidências de logs e resultados em `docs/validation/prod-e2e-2026-02-26.md`.
