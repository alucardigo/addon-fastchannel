# Fastchannel Addon Stabilization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the fastchannel addon deploy cleanly in dev and eliminate runtime errors in logs (schema, migrations, initialization, dashboards).

**Architecture:** Align DB schema with runtime queries via standard V*.xml migrations, ensure addon registration/Place handling is valid for dev, and remove duplicate initialization triggers in the WAR packaging. Validate by deploy + log inspection.

**Tech Stack:** Java (WildFly), Gradle, WPM ddmigration, SQL Server

---

### Task 1: Capture current deploy failures and log evidence

**Files:**
- Inspect: `X:/Wildfly_Clean/wildfly_producao/standalone/log/server.log`

**Step 1: Re-run deploy**

Run: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/gradlew.bat -p X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify deployAddon -x test`
Expected: deployment attempt in `server.log`.

**Step 2: Extract error lines**

Run: `Select-String -Path X:/Wildfly_Clean/wildfly_producao/standalone/log/server.log -Pattern "addon-fastchannel|fastchannel" | Select-String -Pattern "ERROR|WARN|Exception|Failed|nao|invalid"`
Expected: errors for Place/addon registration, scheduler duplicate, JAPE provider, missing columns.

**Step 3: Archive evidence**

Copy the latest error snippets into a scratch file for comparison.

---

### Task 2: Fix dev add-on registration / Place dependency

**Files:**
- Inspect: `X:/Wildfly_Clean/wildfly_producao/standalone/configuration/*`
- Inspect: `Addon-FastChannel.ear/META-INF/jboss-app.xml`

**Step 1: Locate add-on allowlist / Place config**

Run: `rg -n "addon|place|loja" X:/Wildfly_Clean/wildfly_producao/standalone/configuration`
Expected: a file/property that controls add-on registration or Place connectivity.

**Step 2: Add dev allowlist entry**

If a local allowlist exists, add an entry for `addon-fastchannel`.

Example (properties file):
```
addon-fastchannel=true
```

Example (json array):
```
"addon-fastchannel"
```

**Step 3: If no allowlist exists, disable Place check in dev**

Search for a flag in config or JVM args that disables Place validation and set it for this dev server.

**Step 4: Redeploy**

Run: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/gradlew.bat -p X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify deployAddon -x test`
Expected: no `ModuleBootLoaderException` and no `O addon addon-fastchannel nao consta na lista`.

---

### Task 3: Eliminate duplicate initialization (scheduler + JAPE provider)

**Files:**
- Inspect: `vc/src/main/webapp/WEB-INF/web.xml`
- Inspect: `build.gradle`

**Step 1: Verify web.xml listeners**

Run: `rg -n "AddonModuleServletContextListener|TinyEJBModuleLoader" vc/src/main/webapp/WEB-INF/web.xml`
Expected: each listener appears once.

**Step 2: Verify build inject step**

Ensure `injectExtensionIntoWar` does not append duplicate listener blocks or duplicate WEB-INF entries.

Minimal guard in `build.gradle`:
```
if (xmlText.count(earMarker) > 1) {
    // remove duplicate block
}
```

**Step 3: Redeploy and confirm single init**

Run: `Select-String -Path X:/Wildfly_Clean/wildfly_producao/standalone/log/server.log -Pattern "Modulo 'addon-fastchannel' esta sendo inicializado" | Select-Object -Last 4`
Expected: a single init cycle per deploy and no `Scheduler already registered` or `JAPE: Provedor de dados ja existe`.

---

### Task 4: Package embedded dashboard config correctly

**Files:**
- Modify: `build.gradle`
- Create: `vc/src/main/webapp/WEB-INF/resources/addon-fastchannel-embedded-dashboards-cfg.xml`

**Step 1: Add dashboards cfg file if missing**

Add minimal config file:
```
<embedded-dashboards/>
```

**Step 2: Ensure WAR packaging copies to WEB-INF/resources**

In `build.gradle` `injectExtensionIntoWar`, copy to:
```
WEB-INF/resources/addon-fastchannel-embedded-dashboards-cfg.xml
```

**Step 3: Redeploy and verify log**

Run: `rg -n "embedded-dashboards" X:/Wildfly_Clean/wildfly_producao/standalone/log/server.log`
Expected: no "Nao foi encontrado arquivo ... embedded-dashboards".

---

### Task 5: Standardize migrations to V*.xml and align schema

**Files:**
- Modify: `dbscripts/V5.xml`
- Modify: `dbscripts/V6.xml`
- Review: `dbscripts/migration_add_fc_*.sql`

**Step 1: Convert nonstandard SQL migrations into V*.xml**

Move content from:
- `dbscripts/migration_add_fc_config_defaults.sql`
- `dbscripts/migration_add_fc_log_fields.sql`
- `dbscripts/migration_add_fc_pedido_fields.sql`
into new `V5.xml`/`V6.xml` scripts following existing V2 format.

**Step 2: Ensure columns used by runtime exist**

Include DDL for:
- `AD_FCPEDIDO.STATUS_IMPORT`
- `AD_FCLOG.DH_LOG`
- `AD_FCCONFIG.SYNC_STATUS_ENABLED`
- `AD_FCFILA.ENTITY_TYPE/ENTITY_ID/ENTITY_KEY/LAST_ERROR` (or adjust to actual column names)
- `AD_FCLOG.DETALHES` (or update code to match DB)

**Step 3: Redeploy and confirm migrations**

Expected log lines:
- `Processando script COLUMN AD_FCPEDIDO.STATUS_IMPORT...`
- `Processando script COLUMN AD_FCLOG.DH_LOG...`

---

### Task 6: Align service queries with schema

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCDashboardService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCFilaService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCLogsService.java`

**Step 1: Update SQL column references**

Ensure the dashboard queries match the schema created in Task 5.
Example patch (pattern):
```
SELECT COUNT(1) FROM AD_FCPEDIDO WHERE STATUS_IMPORT = ?
```

**Step 2: Rebuild and redeploy**

Run: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/gradlew.bat -p X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify deployAddon -x test`
Expected: no SQLServerException for invalid column names.

---

### Task 7: Final verification

**Files:**
- Inspect: `X:/Wildfly_Clean/wildfly_producao/standalone/log/server.log`

**Step 1: Check errors**

Run: `Select-String -Path X:/Wildfly_Clean/wildfly_producao/standalone/log/server.log -Pattern "addon-fastchannel|fastchannel" | Select-String -Pattern "ERROR|WARN|Exception|Failed|invalid" | Select-Object -Last 50`
Expected: no fastchannel-related errors.

**Step 2: Smoke test pages**

Load fastchannel dashboard, prices, orders, queue, and logs pages and confirm data appears.

**Step 3: Commit**

```
git add dbscripts/V5.xml dbscripts/V6.xml build.gradle vc/src/main/webapp/WEB-INF/resources/addon-fastchannel-embedded-dashboards-cfg.xml model/src/main/java/br/com/bellube/fastchannel/web/*.java
git commit -m "fix: stabilize fastchannel addon deploy and schema"
```
