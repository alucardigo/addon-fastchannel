# Fastchannel Log Issues Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Eliminar erros do addon Fastchannel no deploy e corrigir falhas de leitura de dados nas telas (configuração, fila e logs).

**Architecture:** Corrigir inconsistências entre schema do banco e o código, ajustar empacotamento do EAR para evitar dupla inicialização, e garantir arquivos auxiliares no caminho esperado pelo loader.

**Tech Stack:** Java, Wildfly, Gradle, SQL Server/Oracle, ddmigration.

---

### Task 1: Mapear divergências de schema vs. código (baseline)

**Files:**
- Review: `model/src/main/java/br/com/bellube/fastchannel/web/FCFilaService.java`
- Review: `model/src/main/java/br/com/bellube/fastchannel/web/FCLogsService.java`
- Review: `model/src/main/java/br/com/bellube/fastchannel/config/FastchannelConfig.java`
- Review: `datadictionary/AD_FCQUEUE.xml`
- Review: `datadictionary/AD_FCLOG.xml`
- Review: `datadictionary/AD_FCCONFIG.xml`
- Review: `dbscripts/V2.xml`

**Step 1: Write the failing test**

```text
Manual repro checklist:
- Abrir tela Fila: erros de coluna 'TIPO_ENTIDADE'
- Abrir tela Logs: erros de coluna 'STACKTRACE'
- Abrir Config: erro de coluna 'SYNC_STATUS_ENABLED'
```

**Step 2: Run test to verify it fails**

Run: `rg -n "TIPO_ENTIDADE|STACKTRACE|SYNC_STATUS_ENABLED" X:\Wildfly_Clean\wildfly_producao\standalone\log\server.log`
Expected: FAIL with SQLServerException for the above columns.

**Step 3: Document root causes**

```text
- AD_FCQUEUE no schema usa ENTITY_TYPE/ENTITY_KEY/LAST_ERROR, mas o código consulta TIPO_ENTIDADE/SKU/ULTIMO_ERRO.
- AD_FCLOG tem DETALHES e DH_REGISTRO, mas o código consulta STACKTRACE.
- AD_FCCONFIG não contém SYNC_STATUS_ENABLED (nem SANKHYA_*), mas o código tenta ler.
```

**Step 4: Run test to verify it passes**

Run: `rg -n "TIPO_ENTIDADE|STACKTRACE|SYNC_STATUS_ENABLED" X:\Wildfly_Clean\wildfly_producao\standalone\log\server.log`
Expected: no new errors after later tasks are deployed.

**Step 5: Commit**

```bash
# No commit in this task (baseline only)
```

### Task 2: Corrigir leitura da fila (FCFilaService) para colunas reais

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCFilaService.java`

**Step 1: Write the failing test**

```java
// pseudo-test: should query AD_FCQUEUE columns that exist
// expected SQL uses ENTITY_TYPE, ENTITY_ID, ENTITY_KEY, LAST_ERROR
```

**Step 2: Run test to verify it fails**

Run: `rg -n "FCFilaService" X:\Wildfly_Clean\wildfly_producao\standalone\log\server.log`
Expected: SQLServerException for 'TIPO_ENTIDADE'.

**Step 3: Write minimal implementation**

```java
// Adjust SQL to map existing columns:
// ENTITY_TYPE -> tipo
// ENTITY_ID -> referencia
// ENTITY_KEY -> sku
// LAST_ERROR -> ultimoErro
```

**Step 4: Run test to verify it passes**

Run: `gradlew.bat deployAddon -x test`
Expected: deploy OK; open Fila page; no SQLServerException for 'TIPO_ENTIDADE'.

**Step 5: Commit**

```bash
git add model/src/main/java/br/com/bellube/fastchannel/web/FCFilaService.java
git commit -m "fix: align FCFilaService with AD_FCQUEUE schema"
```

### Task 3: Corrigir leitura de logs (FCLogsService) para DETALHES

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCLogsService.java`

**Step 1: Write the failing test**

```java
// pseudo-test: should query DETALHES instead of STACKTRACE
```

**Step 2: Run test to verify it fails**

Run: `rg -n "FCLogsService" X:\Wildfly_Clean\wildfly_producao\standalone\log\server.log`
Expected: SQLServerException for 'STACKTRACE'.

**Step 3: Write minimal implementation**

```java
// Replace STACKTRACE column with DETALHES
// Keep response field name "stacktrace" for UI compatibility
```

**Step 4: Run test to verify it passes**

Run: `gradlew.bat deployAddon -x test`
Expected: deploy OK; open Logs page; no SQLServerException for 'STACKTRACE'.

**Step 5: Commit**

```bash
git add model/src/main/java/br/com/bellube/fastchannel/web/FCLogsService.java
git commit -m "fix: read AD_FCLOG.DETALHES instead of STACKTRACE"
```

### Task 4: Adicionar colunas faltantes em AD_FCCONFIG (ddmigration)

**Files:**
- Modify: `dbscripts/V3.xml` (or new `dbscripts/V4.xml`)
- Optionally modify: `dbscripts/migration_*.sql`

**Step 1: Write the failing test**

```text
Manual: abrir tela Configuracao e observar erro SYNC_STATUS_ENABLED.
```

**Step 2: Run test to verify it fails**

Run: `rg -n "SYNC_STATUS_ENABLED" X:\Wildfly_Clean\wildfly_producao\standalone\log\server.log`
Expected: SQLServerException for SYNC_STATUS_ENABLED.

**Step 3: Write minimal implementation**

```sql
-- Add missing columns to AD_FCCONFIG
-- SYNC_STATUS_ENABLED CHAR(1) DEFAULT 'N'
-- SANKHYA_SERVER_URL VARCHAR(300)
-- SANKHYA_USER VARCHAR(100)
-- SANKHYA_PASSWORD VARCHAR(200)
```

**Step 4: Run test to verify it passes**

Run: `gradlew.bat deployAddon -x test`
Expected: ddmigration executes column scripts; Config screen loads without SQL errors.

**Step 5: Commit**

```bash
git add dbscripts/V3.xml
# or V4.xml if new
# plus any SQL helpers
git commit -m "fix: add missing AD_FCCONFIG columns"
```

### Task 5: Resolver duplicidade de JAPE provider e Scheduler

**Files:**
- Modify: `build.gradle`
- Review: `build/dist/ejb/addon-fastchannel-model.jar`

**Step 1: Write the failing test**

```text
Deploy -> server.log has JAPE: Provedor de dados ja existe + Scheduler already registered.
```

**Step 2: Run test to verify it fails**

Run: `rg -n "Addon-FastChannel-dwf|Scheduler already registered" X:\Wildfly_Clean\wildfly_producao\standalone\log\server.log`
Expected: errors present on deploy.

**Step 3: Write minimal implementation**

```text
Option A (preferred): place addon-fastchannel-model.jar only once in EAR (ear/lib) and remove from web WAR injection.
Option B: keep in EJB only, and add ear/lib copy for web classpath.
```

**Step 4: Run test to verify it passes**

Run: `gradlew.bat deployAddon -x test`
Expected: no JAPE duplicate provider error; no Scheduler already registered error.

**Step 5: Commit**

```bash
git add build.gradle
git commit -m "fix: avoid duplicate model jar to prevent double init"
```

### Task 6: Corrigir caminho de dashboards embutidos

**Files:**
- Modify: `build.gradle` or add resource under `vc/src/main/webapp/WEB-INF/resources/`
- Review: `vc/src/main/resources/META-INF/addon-fastchannel-dashboards/dashboards-descriptor.xml`

**Step 1: Write the failing test**

```text
server.log: Não foi encontrado arquivo /WEB-INF/resources/addon-fastchannel-embedded-dashboards-cfg.xml
```

**Step 2: Run test to verify it fails**

Run: `rg -n "addon-fastchannel-embedded-dashboards-cfg" X:\Wildfly_Clean\wildfly_producao\standalone\log\server.log`
Expected: missing file log entry.

**Step 3: Write minimal implementation**

```text
Ensure build copies addon-fastchannel-embedded-dashboards-cfg.xml into WEB-INF/resources.
```

**Step 4: Run test to verify it passes**

Run: `gradlew.bat deployAddon -x test`
Expected: no missing dashboards cfg log entry for addon-fastchannel.

**Step 5: Commit**

```bash
git add build.gradle vc/src/main/webapp/WEB-INF/resources/addon-fastchannel-embedded-dashboards-cfg.xml
# if file added

git commit -m "fix: include embedded dashboards cfg in resources"
```
