# Fastchannel Prod Publish + Validation Loop Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Publicar a versão mais nova do addon e validar em produção o ciclo completo de pedidos, preços e estoque com evidência cruzada Fastchannel x Sankhya x banco.

**Architecture:** Aplicar publicação via Gradle (AreaDev), instalar em produção, executar SPs/ações da integração e validar resultado por UI humana (Sankhya/Fastchannel) e DBExplorer (TGFCAB/TGFITE/filas/logs). Correções de código são iteradas até os critérios funcionais fecharem.

**Tech Stack:** Java Addon Studio (Sankhya), Gradle, MSSQL (DBExplorer), Fastchannel API/Portal, Playwright MCP.

### Task 1: Baseline e publicação

**Files:**
- Modify: `build.gradle`
- Verify: `docs/plans/2026-02-26-prod-publish-validation-loop.md`

1. Confirmar versão-alvo em `build.gradle`.
2. Compilar `:model:compileJava -x test`.
3. Publicar via `publishAddon` com credenciais AreaDev.
4. Confirmar sucesso de upload/publicação no output.

### Task 2: Instalação/ativação em produção

**Files:**
- Verify: `Addon-FastChannel.ear/extension.xml`
- Verify: `META-INF/addon-fastchannel-extension.xml`

1. Validar em UI Sankhya que addon está instalado e ativo em Minhas Soluções.
2. Recarregar tela e confirmar versão instalada.
3. Confirmar endpoints do addon respondendo em produção.

### Task 3: Teste funcional de pedidos

**Files:**
- Verify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Verify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/OrderCreationOrchestrator.java`

1. Executar importação de pedidos pendentes.
2. Capturar mapeamento `orderId -> NUNOTA`.
3. Validar no Portal de Vendas/UI que pedidos foram criados corretamente.
4. Validar TGFCAB/TGFITE no DBExplorer (TOP, vendedor FAST, campos de custo/preço/observação).

### Task 4: Teste funcional de preço e estoque

**Files:**
- Verify: `model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java`
- Verify: `model/src/main/java/br/com/bellube/fastchannel/web/FCEstoqueService.java`
- Verify: `model/src/main/java/br/com/bellube/fastchannel/job/OutboxProcessorJob.java`

1. Alterar centavos no Sankhya (tabela SITE/mapeada) e disparar sincronização.
2. Confirmar reflexo na Fastchannel (`/pricetable/products/3`) e reverter para valor original.
3. Validar sincronização de estoque para CD ativo na Fastchannel.
4. Conferir filas e logs (sem erro recorrente).

### Task 5: Correções e novo ciclo

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/**`
- Modify: `dbscripts/V*.xml`

1. Para cada falha funcional, corrigir código/migração.
2. Recompilar, republicar e repetir Tasks 2-4.
3. Encerrar apenas com evidência final de sucesso nos três fluxos.
