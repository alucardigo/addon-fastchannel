# Fastchannel Addon Fix Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Corrigir erros do add-on Fastchannel (importacao de pedidos, dashboards, UI de fontes e desinstalacao) e garantir que as telas reflitam o banco corretamente.

**Architecture:** Ajustar schema e configuracao via migracoes V*.xml, corrigir cache/reload de configuracao, adicionar fallback de mapeamentos e validar integracoes com deploy local. Resolver dependencias de WPM/Place para fluxo de desinstalacao.

**Tech Stack:** Java (WildFly), Gradle Add-on Studio, SQL Server (Docker), WPM/Place

---

### Task 1: Capturar evidencias atuais (reprodutivel)

**Files:**
- Inspect: `X:/Wildfly_Clean/wildfly_producao/standalone/log/server.log`

**Step 1: Erros do add-on**

Run: `rg -n "addon-fastchannel|fastchannel" X:/Wildfly_Clean/wildfly_producao/standalone/log/server.log | rg -n "ERROR|WARN|Exception|Failed|invalid|nao" | Select-Object -Last 200`
Expected: erros de importacao de pedidos (CODEMP nao resolvido, NUNOTA NULL) e avisos de ddmigration.

**Step 2: Estado do banco**

Run:
- `docker exec skdev-mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U SANKHYA -P developer -d jiva -Q "SELECT TOP 1 * FROM AD_FCCONFIG"`
- `docker exec skdev-mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U SANKHYA -P developer -d jiva -Q "SELECT COUNT(*) AS CNT FROM AD_FCDEPARA"`
- `docker exec skdev-mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U SANKHYA -P developer -d jiva -Q "SELECT COUNT(*) AS CNT FROM TGFEXC"`
Expected: `AD_FCDEPARA` vazio e `TGFEXC` vazio.

---

### Task 2: Corrigir schema (migracoes V*.xml)

**Files:**
- Create: `dbscripts/V7.xml`

**Step 1: Escrever migracao para ajustar colunas**

Include no `V7.xml`:
- Alterar `AD_FCPEDIDO.NUNOTA` para permitir `NULL`.
- Adicionar colunas `UI_SOURCE_DEFAULT`, `UI_ENABLE_SOURCE_2`, `UI_ENABLE_SOURCE_3` em `AD_FCCONFIG` se nao existirem.

**Step 2: Garantir compatibilidade**

Manter `executar="SE_NAO_EXISTIR"` para colunas novas. Para `NUNOTA`, usar `ALTER TABLE ... ALTER COLUMN` compativel com MSSQL/Oracle.

**Step 3: Deploy e validacao**

Run: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/gradlew.bat -p X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify deployAddon -x test`
Expected: migracao aplicada sem erros no `server.log`.

---

### Task 3: Atualizar cache de configuracao apos salvar

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCConfigService.java`

**Step 1: Failing test (manual)**

Manual: salvar configuracao `ATIVO = N`, abrir dashboard em ate 10s e observar status ainda "Conectado".
Expected: status nao muda devido a cache.

**Step 2: Implementar reload**

Depois do `stmt.executeUpdate()` em `save`, chamar `FastchannelConfig.getInstance().reload()`.

**Step 3: Verificar**

Manual: salvar configuracao `ATIVO = N` e confirmar no dashboard que status muda sem esperar 5 minutos.

---

### Task 4: Fallback de mapeamentos de pedido

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/FastchannelHeaderMappingService.java`

**Step 1: Failing test (manual)**

Manual: importar pedido com `AD_FCDEPARA` vazio e `AD_FCCONFIG` preenchido (CODEMP, CODTIPOPER, TIPNEG). Esperado: erro `CODEMP nao resolvido`.

**Step 2: Implementar fallback**

Se `resolveByKeys` retornar `null`, usar valores de `FastchannelConfig` (CODEMP, CODTIPOPER, TIPNEG, CODNAT, CODCENCUS, CODVEND, CODPARC_PADRAO) antes de lancar erro.

**Step 3: Verificar**

Manual: repetir importacao e confirmar que o pedido cria `NUNOTA` quando config esta preenchida.

---

### Task 5: Documentar e aplicar de-para obrigatorio

**Files:**
- Update: `docs/ANALYSIS_COMPARISON_LEGACY.md` (ou novo doc curto)

**Step 1: Adicionar instrucao de de-para minimo**

Inserir exemplo:
- `EMPRESA`, `TOP_PEDIDO`, `TIPNEG` usando chaves `R:<ResellerId>`, `S:<StorageId>` ou `S:<StorageId>|R:<ResellerId>`.

**Step 2: SQL de exemplo**

Adicionar exemplos de `INSERT INTO AD_FCDEPARA` para Reseller 21 e Storage 2.

---

### Task 6: Habilitar fontes de precos na UI

**Files:**
- Modify: `vc/src/main/webapp/html5/fastchannel/config.html`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCConfigService.java`

**Step 1: Adicionar campos de UI**

Adicionar checkbox/select para:
- `UI_ENABLE_SOURCE_2`
- `UI_ENABLE_SOURCE_3`
- `UI_SOURCE_DEFAULT`

**Step 2: Garantir leitura/escrita**

Ampliar `FCConfigService.get/save` para expor esses campos (ja parcialmente suportado pelo backend).

**Step 3: Verificar**

Manual: habilitar fonte 2/3 na configuracao e ver opcoes aparecerem em `precos.html`.

---

### Task 7: Desinstalacao via WPM/Place

**Files:**
- Inspect (server): `X:/Wildfly_Clean/wildfly_producao/standalone/deployments/wpm.war.failed`
- Inspect (server): `X:/Wildfly_Clean/wildfly_producao/standalone/deployments/placemm.ear.failed`

**Step 1: Diagnostico**

Confirmar erro de timeout de deploy (600s).

**Step 2: Reprocesso de deploy**

Remover `.failed`, garantir recursos e reiniciar o WildFly para re-deploy de `wpm.war` e `placemm.ear`.

**Step 3: Verificar UI de desinstalacao**

Abrir WPM e confirmar que aparece botao de desinstalar o add-on.

---

### Task 8: Validacao final + publicacao

**Files:**
- Inspect: `X:/Wildfly_Clean/wildfly_producao/standalone/log/server.log`

**Step 1: Smoke tests**

- `FCAdminSP.testarConexao`
- `FCPedidosSP.list`
- `FCPrecosSP.list` (source 1/2)
- Dashboard com status correto.

**Step 2: Publicar exts**

Run: `./gradlew publishAddon -Pusuario=<email> -Psenha=<senha> -PpublishPlace=true -PprivateKey=<path>`
Expected: gerar `.exts` em `build/libs`.

**Step 3: Verificar logs**

Run: `rg -n "addon-fastchannel|fastchannel" X:/Wildfly_Clean/wildfly_producao/standalone/log/server.log | rg -n "ERROR|WARN|Exception" | Select-Object -Last 50`
Expected: sem erros do add-on.
