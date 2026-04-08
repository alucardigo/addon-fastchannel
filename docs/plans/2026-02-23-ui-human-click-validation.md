# UI Human-Click Validation Plan (Homolog) Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Revalidar o Addon Fastchannel no homolog navegando como humano (menus/botões), sem chamar rotas internas diretamente.

**Architecture:** Usar automação de navegador para reproduzir o mesmo caminho de usuário final: login no MGE, abertura do Addon pelo menu, execução dos botões de operação em cada tela e conferência de alertas/resultados visuais. O foco é validar comportamento real de UI + backend integrado.

**Tech Stack:** Sankhya MGE (web), Addon Fastchannel, Playwright MCP.

### Task 1: Preparar sessão e acessar módulo via UI

**Files:**
- Create: `docs/plans/2026-02-23-ui-human-click-validation.md`
- Test: `N/A`

**Step 1: Abrir página inicial do MGE**
- Navegar para `http://100.72.97.11:8080/mge/`.

**Step 2: Login pelo formulário**
- Preencher usuário `sup` e senha `Azsxdc`.
- Clicar botão de entrar.

**Step 3: Abrir menu do sistema e entrar no Addon**
- Clicar ícone/menu lateral.
- Clicar em `Addon-FastChannel`.
- Clicar em `Operacoes`.
- Clicar em `Pedidos`.

### Task 2: Validar Pedidos (importação/reprocessamento)

**Files:**
- Modify: `N/A`
- Test: `UI homolog / Pedidos`

**Step 1: Executar importação por botão**
- Clicar `Importar Novos Pedidos`.
- Confirmar diálogo.
- Aguardar retorno com paciência.

**Step 2: Validar resultado visual**
- Registrar alerta de sucesso/erro.
- Conferir atualização de linhas/status na grade.

**Step 3: Abrir detalhe e reprocessar um pedido com falha (se existir)**
- Clicar `Detalhes`.
- Clicar `Reprocessar`.
- Validar alerta e mudança de status.

### Task 3: Validar Preços e Estoque

**Files:**
- Modify: `N/A`
- Test: `UI homolog / Precos / Estoque / Fila`

**Step 1: Preços**
- Ir em `Precos` via menu do addon.
- Selecionar item.
- Clicar `Sincronizar Selecionados`.
- Confirmar diálogo e registrar alerta final.

**Step 2: Estoque**
- Ir em `Estoque` via menu do addon.
- Filtrar/selecionar item.
- Clicar `Forcar Sync Selecionados`.
- Confirmar diálogo e registrar retorno.

**Step 3: Fila**
- Ir em `Fila de Sincronizacao`.
- Clicar `Processar Fila Agora`.
- Confirmar diálogo e validar `pendentes antes/depois`.

### Task 4: Validar Dashboard/Logs e fechar evidências

**Files:**
- Modify: `N/A`
- Test: `UI homolog / Dashboard / Logs`

**Step 1: Dashboard**
- Clicar `Dashboard`.
- Clicar `Testar Conexao`.
- Registrar alerta com resultado.

**Step 2: Logs**
- Clicar `Logs`.
- Validar presença de registros recentes de importação/sincronização.

**Step 3: Entrega**
- Consolidar resultados com: alertas vistos, ações executadas por clique e eventuais erros de regra de negócio.
