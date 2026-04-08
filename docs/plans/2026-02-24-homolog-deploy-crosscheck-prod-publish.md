# Homolog Deploy, Cross-Check and Portal Publish Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Deploy the updated addon to homolog, validate order import and sync flows with recent records, cross-check Fastchannel x Addon x DB, and publish addon in Sankhya portal for production testing.

**Architecture:** Reuse the already working homolog stack and replace only the integration model artifact in a complete EAR layout to avoid missing-module failures. Validate from UI-first (human click flow), then confirm with DB and Fastchannel API/portal evidence.

**Tech Stack:** Sankhya Add-on (Java/Gradle), WildFly, MSSQL, Fastchannel API/Portal, Playwright browser automation.

### Task 1: Prepare and deploy correct homolog artifact
**Files:**
- Modify: `/home/bellube.rodrigo.faria/addon-fastchannel-dist/lib/addon-fastchannel-model.jar`
- Verify: `/home/mgeweb/wildfly_teste/standalone/deployments/addon-fastchannel.ear`

1. Build `model` in `/home/bellube.rodrigo.faria/merge-unify` with JDK8.
2. Copy generated jar into complete dist EAR structure.
3. Repack/copy full EAR to WildFly deployments.
4. Confirm `.deployed` and no `.failed`.

### Task 2: Homolog smoke and recent-record validation
**Files:**
- Verify logs: `/home/mgeweb/wildfly_teste/standalone/log/server.log`

1. Validate endpoints and addon services healthy.
2. Trigger import/sync from Sankhya UI (Pedidos, Preços, Estoque).
3. Validate recent records in UI with status and detailed error context.
4. Capture whether customer/CNPJ is shown in logs/history.

### Task 3: Cross-check Fastchannel x Addon x DB
**Files:**
- Query DB in homolog (`SANKHYA_TESTE`).

1. Pull recent order/sync rows from addon tables.
2. Cross-check with Fastchannel portal/API objects.
3. Validate product/price/stock mapping consistency and table usage.
4. Report mismatches with concrete record IDs.

### Task 4: Production portal publication
**Files:**
- Use Sankhya Add-on Studio publish flow.

1. Publish addon via portal with provided credentials/licensing.
2. Confirm publication result and version identifiers.
3. Document rollback path and validation checklist for prod test.
