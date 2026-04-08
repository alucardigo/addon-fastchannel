# Fastchannel UI Exhaustive E2E Automation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Criar script automatizado exaustivo de validação de UI no homolog cobrindo todo o fluxo operacional do Addon Fastchannel como usuário humano.

**Architecture:** Script Node + Playwright em modo procedural com captura de evidências por etapa (screenshot + JSON report), tratamento automático de diálogos de confirmação/alerta e validações de sucesso por mensagem/estado da grade.

**Tech Stack:** Node.js, Playwright, Sankhya MGE, Addon Fastchannel UI.

### Task 1: Scaffold do runner E2E

**Files:**
- Create: `scripts/e2e_fastchannel_ui_homolog.mjs`

**Step 1: Criar parser de argumentos e contexto de execução**
- Base URL, usuário, senha, headless, timeout, output-dir.

**Step 2: Criar infraestrutura de evidência**
- Pasta de execução com timestamp.
- `report.json` com passos, status e diálogos capturados.
- screenshots por etapa.

### Task 2: Implementar fluxo completo por clique

**Files:**
- Modify: `scripts/e2e_fastchannel_ui_homolog.mjs`

**Step 1: Login no MGE + entrada no Addon via menu**
- Login `sup/Azsxdc`.
- Fechar modal de restauração de sessão, se aparecer.
- Pesquisar `Addon-FastChannel` e abrir `Pedidos` pelo menu.

**Step 2: Cobrir operações-chave**
- Pedidos: importar + abrir detalhe + reprocessar.
- Preços: selecionar item + sincronizar selecionados.
- Estoque: filtrar/carregar + selecionar item + forçar sync.
- Fila: processar fila.
- Dashboard: testar conexão.
- Logs: validar presença de registros.

**Step 3: Garantir robustez para latência alta**
- Espera por alertas longos (timeout maior).
- Captura de confirm + alert na mesma ação.
- fallback de seletores.

### Task 3: Documentar execução

**Files:**
- Create: `docs/validation/2026-02-23-fastchannel-ui-exhaustive-e2e.md`

**Step 1: Adicionar comandos de execução**
- Exemplo com homolog.
- Exemplo headless e headful.

**Step 2: Explicar artefatos gerados**
- Onde fica `report.json`.
- Onde ficam screenshots.
