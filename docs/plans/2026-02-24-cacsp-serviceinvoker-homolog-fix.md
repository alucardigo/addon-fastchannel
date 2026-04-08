# CACSP ServiceInvoker Homolog Fix Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fazer a importacao de pedidos funcionar no homolog (Fastchannel -> Sankhya) e confirmar sincronizacao de preco/estoque (Sankhya -> Fastchannel) com evidencia em UI + banco.

**Architecture:** Preservar a estrategia oficial do legado (CACSP.incluirNota) com fallback robusto quando ServiceInvoker nao estiver disponivel no runtime. Validar ponta a ponta por caminho humano no Sankhya (menu/cliques), e cruzar com logs e banco de homolog para garantir consistencia operacional.

**Tech Stack:** Add-on Sankhya (Java), WildFly, MSSQL, Fastchannel API, Playwright MCP para validacao UI.

### Task 1: Diagnostico comparativo de estrategia de pedido

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/OrderCreationOrchestrator.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/ServiceInvokerStrategy.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/HttpServiceStrategy.java`
- Reference: `X:/IntegracaoFastchannel/APPFASTCHANNEL/gbi-app-integrador-main/gbi-app-integrador-main/models/fastChannel/erp/sankhya.js`

**Step 1: Ler fluxo legado e mapear endpoint/headers usados no CACSP.incluirNota**

**Step 2: Verificar no merge-unify a ordem de fallback e condicoes de indisponibilidade (ClassNotFound/HTTP provider)**

**Step 3: Ajustar estrategia para reproduzir comportamento legado em homolog sem quebrar caminhos existentes**

**Step 4: Compilar modulo**
Run: `./gradlew :model:compileJava` (ou `gradlew.bat :model:compileJava` no Windows)
Expected: `BUILD SUCCESSFUL`

### Task 2: Publicacao e validacao tecnica em homolog

**Files:**
- Runtime target: `/home/bellube.rodrigo.faria/...` (homolog)
- Logs: `/home/mgeweb/wildfly_teste/standalone/log/server.log`

**Step 1: Gerar EAR da worktree merge-unify**

**Step 2: Publicar no WildFly homolog e validar deploy `.deployed`**

**Step 3: Executar importacao de pedido via UI e confirmar ausencia de erro de provedor/service invoker**

**Step 4: Validar NUNOTA criada e estado sincronizado no banco**

### Task 3: Validacao humana por cliques (sem URL direta interna)

**Files:**
- UI path: `Portal de Vendas -> Pedidos` e abas `Precos`, `Estoque`

**Step 1: Login no Sankhya com `sup/Azsxdc` e abrir addon por menu**

**Step 2: Em Pedidos, abrir detalhe do erro, reprocessar e validar sucesso**

**Step 3: Em Precos e Estoque, acionar filtros/sincronizacoes e validar status `OK/ENVIADO`**

**Step 4: Cruzar com Fastchannel UI (quando autenticao permitir) e banco homolog**

### Task 4: Evidencias e fechamento

**Files:**
- Create/Modify: `docs/validation/2026-02-24-homolog-e2e-evidence.md`

**Step 1: Registrar pedido(s) importado(s) com NUNOTA, timestamps e logs-chave**

**Step 2: Registrar SKU/tabela de preco/estoque validados (incluindo tabela solicitada pelo negocio)**

**Step 3: Listar riscos remanescentes e recomendacoes objetivas**
