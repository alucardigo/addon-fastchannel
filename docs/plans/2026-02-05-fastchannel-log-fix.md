# Fastchannel Log Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Corrigir os problemas apontados nos logs do add-on Fastchannel (falha de deploy, endpoints 405/404, erros de schema SQL) e estabilizar telas de Configuracao/Precos/Pedidos/Dashboard.

**Architecture:** Ajustar schema do banco para alinhar com o codigo (campos esperados em AD_FCPEDIDO e AD_FCLOG), garantir rotas corretas do `fc-direct` pelo contexto `/addon-fastchannel`, e eliminar duplicidades de inicializacao (scheduler/provider). Validar dependencia externa "Place" antes do deploy do WAR.

**Tech Stack:** Java/Wildfly, SQL Server, scripts SQL/V2.xml, HTML5/JS, JBoss deployment.

---

### Task 1: Confirmar dependencias externas e contexto de deploy

**Files:**
- Read: `Addon-FastChannel.ear\META-INF\application.xml`
- Read: `build.gradle`
- Read: `vc\src\main\webapp\html5\fastchannel\*.html`

**Step 1: Validar context-root definido no EAR**

Run:
```powershell
Get-Content -Path X:\IntegracaoFastchannel\APPFASTCHANNEL\.worktrees\merge-unify\Addon-FastChannel.ear\META-INF\application.xml
```
Expected: `<context-root>/addon-fastchannel</context-root>`.

**Step 2: Validar que o build gera `jboss-web.xml` com `/addon-fastchannel`**

Run:
```powershell
rg -n "context-root" X:\IntegracaoFastchannel\APPFASTCHANNEL\.worktrees\merge-unify\build.gradle
```
Expected: `context-root` igual a `/addon-fastchannel`.

**Step 3: Confirmar chamadas JS para `fc-direct`**

Run:
```powershell
rg -n "resolveAddonContexts|fc-direct" X:\IntegracaoFastchannel\APPFASTCHANNEL\.worktrees\merge-unify\vc\src\main\webapp\html5\fastchannel
```
Expected: As telas usam `resolveAddonContexts()` e constroem `${base}/fc-direct`.

---

### Task 2: Corrigir schema do banco para AD_FCPEDIDO

**Files:**
- Modify: `dbscripts\V2.xml`
- Create: `dbscripts\migration_add_fc_pedido_fields.sql`
- Update: `docs\MIGRATION_SERVICE_BASED_ORDERS.md`

**Step 1: Adicionar colunas faltantes no `V2.xml` (instalacao nova)**

Edit `dbscripts\V2.xml` para incluir em `AD_FCPEDIDO` as colunas esperadas pelo codigo:
- `STATUS_IMPORT` (VARCHAR/CHAR)
- `VALOR_TOTAL` (NUMERIC/DECIMAL)
- `DH_PEDIDO` (DATETIME/TIMESTAMP)
- `NOME_CLIENTE` (VARCHAR)
- `CPF_CNPJ` (VARCHAR)
- `VALOR_FRETE` (NUMERIC/DECIMAL)
- `ERRO_MSG` (VARCHAR)

**Step 2: Criar migration para banco existente (SQL Server)**

Create `dbscripts\migration_add_fc_pedido_fields.sql` com:
- `ALTER TABLE AD_FCPEDIDO ADD STATUS_IMPORT ...`
- `ALTER TABLE AD_FCPEDIDO ADD VALOR_TOTAL ...`
- `ALTER TABLE AD_FCPEDIDO ADD DH_PEDIDO ...`
- `ALTER TABLE AD_FCPEDIDO ADD NOME_CLIENTE ...`
- `ALTER TABLE AD_FCPEDIDO ADD CPF_CNPJ ...`
- `ALTER TABLE AD_FCPEDIDO ADD VALOR_FRETE ...`
- `ALTER TABLE AD_FCPEDIDO ADD ERRO_MSG ...`

**Step 3: Atualizar doc**

Update `docs\MIGRATION_SERVICE_BASED_ORDERS.md` para refletir nomes/paths reais dos scripts.

**Step 4: Verificar migracao (SQL)**

Run (no ambiente de DB):
```sql
SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'AD_FCPEDIDO';
```
Expected: lista contendo `STATUS_IMPORT`, `VALOR_TOTAL`, `DH_PEDIDO`, `NOME_CLIENTE`, `CPF_CNPJ`, `VALOR_FRETE`, `ERRO_MSG`.

---

### Task 3: Corrigir schema do banco para AD_FCLOG (DH_LOG)

**Files:**
- Modify: `dbscripts\V2.xml`
- Create: `dbscripts\migration_add_fc_log_fields.sql`
- Update: `docs\MIGRATION_SERVICE_BASED_ORDERS.md`

**Step 1: Padronizar coluna de data em `AD_FCLOG`**

Decision: manter o padrao do codigo (`DH_LOG`). Atualizar `V2.xml` para criar `DH_LOG` (e manter `DH_REGISTRO` se necessario por compatibilidade).

**Step 2: Criar migration**

Create `dbscripts\migration_add_fc_log_fields.sql` com:
- `ALTER TABLE AD_FCLOG ADD DH_LOG DATETIME2 DEFAULT CURRENT_TIMESTAMP` (SQL Server)
- Copiar valores: `UPDATE AD_FCLOG SET DH_LOG = DH_REGISTRO WHERE DH_LOG IS NULL` (se existir `DH_REGISTRO`).
- Ajustar indice para `DH_LOG` (criar `IDX_FCLOG_DH` em `DH_LOG`).

**Step 3: Verificar migracao (SQL)**

Run:
```sql
SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'AD_FCLOG';
```
Expected: lista contendo `DH_LOG`.

---

### Task 4: Validar endpoints e evitar 405 em `/mge/addon-fastchannel`

**Files:**
- Modify: `vc\src\main\webapp\html5\fastchannel\*.html` (se necessario)
- Update: `README.md`

**Step 1: Confirmar que `/addon-fastchannel` e o unico contexto valido**

If os logs confirmarem 405 em `/mge/addon-fastchannel`, ajustar `resolveAddonContexts()` para priorizar `/addon-fastchannel` e usar `/mge/addon-fastchannel` apenas quando estiver comprovadamente disponivel.

**Step 2: Padronizar documentacao**

Update `README.md` e instrucoes de acesso para usar `/addon-fastchannel/html5/fastchannel/...`.

**Step 3: Verificacao**

Acessar:
- `/addon-fastchannel/html5/fastchannel/config.html`
- `/addon-fastchannel/html5/fastchannel/precos.html`
- `/addon-fastchannel/html5/fastchannel/pedidos.html`

Expected: chamadas `fc-direct` retornam 200 e listagens carregam.

---

### Task 5: Resolver falha de deploy "Nao foi possivel estabelecer conexao com o Place"

**Files:**
- Inspect: `wildfly_producao\standalone\configuration\*` (config do ambiente)
- Inspect: `model\src\main\java` (configuracao do Place se existir)

**Step 1: Localizar configuracao do Place**

Search por `Place` no codigo/config:
```powershell
rg -n "Place" X:\IntegracaoFastchannel\APPFASTCHANNEL\.worktrees\merge-unify
```
Expected: localizar classe/propriedade de conexao.

**Step 2: Verificar credenciais/endpoint no ambiente**

Confirmar host/porta/credenciais em configs do Wildfly. Corrigir se necessario.

**Step 3: Re-deploy e validar**

Expected: nao ocorrer `ModuleBootLoaderException` na inicializacao do modulo.

---

### Task 6: Eliminar duplicidades de inicializacao (JAPE provider e Scheduler)

**Files:**
- Inspect: `model\src\main\resources\META-INF\addon-fastchannel-jape-cfg.xml`
- Inspect: `model\src\main\resources\META-INF\addon-fastchannel-extension.xml`

**Step 1: Verificar se o modulo esta sendo carregado duas vezes**

Procurar duplicidade em logs de deploy e em `application.xml` (dois web modules, etc.).

**Step 2: Ajustar registro do scheduler**

Se o scheduler registra sempre em cada reload, adicionar guarda para evitar re-registro quando ja existir (ou garantir que nao existam dois contextos carregando o mesmo modulo).

---

### Task 7: Verificacao final (logs e telas)

**Files:**
- Read: `X:\Wildfly_Clean\wildfly_producao\standalone\log\server.log`
- Read: `X:\Wildfly_Clean\wildfly_producao\standalone\log\access\*.log`

**Step 1: Validar ausencia de erros criticos**

Run:
```powershell
rg -n -i "fastchannel|addon-fastchannel" X:\Wildfly_Clean\wildfly_producao\standalone\log\server.log | rg -i "error|warn|warning"
```
Expected: sem `ModuleBootLoaderException`, sem `STATUS_IMPORT`/`DH_LOG` invalidos, sem `Scheduler already registered`.

**Step 2: Validar HTTP**

Run:
```powershell
rg -n "addon-fastchannel" X:\Wildfly_Clean\wildfly_producao\standalone\log\access -g "access*.log" | rg "\s(4\d\d|5\d\d)\s"
```
Expected: sem 404/405/500 nas rotas `/addon-fastchannel/*` durante uso normal.

---

**Execution Handoff**

Plan complete and saved to `docs/plans/2026-02-05-fastchannel-log-fix.md`. Two execution options:

1. Subagent-Driven (this session) - I dispatch fresh subagent per task, review between tasks, fast iteration
2. Parallel Session (separate) - Open new session with executing-plans, batch execution with checkpoints

Which approach?
