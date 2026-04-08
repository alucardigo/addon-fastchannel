# Fastchannel Merge & Price Alignment Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Unify worktrees into main, fix price unit consistency, enforce SQL Server pricing, support multi-table + batch prices, and prepare for integration testing.

**Architecture:** Cherry-pick and merge cohesive worktree changes into a clean merge branch, then apply price flow updates (SNK_GET_PRECO via SQL Server), multi-table table resolution, batch price sync, and UI normalization for units. Keep JAPE InternalAPI and validate flow.

**Tech Stack:** Java (Sankhya Extension), JAPE/NativeSql, SQL Server, Fastchannel API, Gradle.

---

### Task 1: Create OpenSpec change proposal for merge + pricing alignment

**Files:**
- Create: `openspec/changes/update-fastchannel-merge-unify/proposal.md`
- Create: `openspec/changes/update-fastchannel-merge-unify/tasks.md`
- Create: `openspec/changes/update-fastchannel-merge-unify/design.md`
- Create: `openspec/changes/update-fastchannel-merge-unify/specs/fastchannel/spec.md`

**Step 1:** Draft proposal (why/what/impact).
**Step 2:** Draft design (merge strategy, pricing flow, batch sync).
**Step 3:** Draft tasks checklist and spec deltas.
**Step 4:** Note: `openspec` CLI unavailable; validate manually.

---

### Task 2: Identify worktree commits and select cherry-picks

**Files:**
- Inspect: `.worktrees/add-estoque-precos-ui`
- Inspect: `.worktrees/fc-addon-fixes`
- Inspect: `.worktrees/compat-fastchannel-legacy`

**Step 1:** List commit history per worktree.
**Step 2:** Identify clean commits vs uncommitted changes.
**Step 3:** Cherry-pick clean commits into merge branch.
**Step 4:** For uncommitted changes, port manually (file diff).

---

### Task 3: Normalize price unit handling (centavos vs decimal)

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/PriceResolver.java`
- Modify: `vc/src/main/webapp/html5/fastchannel/precos.html`

**Step 1:** Define canonical units (decimal) for UI and API comparisons.
**Step 2:** Adjust UI to show decimal; only convert to centavos where needed for Fastchannel payload.
**Step 3:** Update compare logic to prevent false divergences.
**Step 4:** Manual verification (no JUnit available).

---

### Task 4: Enforce SQL Server pricing function

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/PriceResolver.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/job/OutboxProcessorJob.java`

**Step 1:** Ensure SNK_GET_PRECO uses `[sankhya].SNK_GET_PRECO(..., GETDATE())` only.
**Step 2:** Remove Oracle/catalog detection.
**Step 3:** Validate compile.

---

### Task 5: Multi-table price sync (AD_TIPO_FAST / PRICE_TABLE_IDS)

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/job/OutboxProcessorJob.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/PriceTableResolver.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/QueueService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java`

**Step 1:** Resolve eligible tables via config + AD_TIPO_FAST.
**Step 2:** For each table, enqueue/update price for SKU.
**Step 3:** Ensure payload includes table mapping if needed.

---

### Task 6: Batch price sync for all eligible tables

**Files:**
- Create/Modify: `model/src/main/java/br/com/bellube/fastchannel/job/OutboxProcessorJob.java`
- Create: `model/src/main/java/br/com/bellube/fastchannel/http/FastchannelPriceClient.java` (batch method)
- Create: `model/src/main/java/br/com/bellube/fastchannel/service/PriceBatchResolver.java`

**Step 1:** Implement SQL Server query equivalent to legacy batch price query.
**Step 2:** Build payload for `/prices/{id}/batches`.
**Step 3:** Send per eligible price table.

---

### Task 7: Validate InternalAPI (JAPE) flow

**Files:**
- Review: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java`
- Modify: if needed for mandatory fields.

**Step 1:** Confirm required fields are set (CODTIPOPER, CODTIPVENDA, CODVEND, CODNAT, CODCENCUS).
**Step 2:** Keep JapeSession transaction (no EntityFacade begin/commit).

---

### Task 8: Documentation & migration alignment

**Files:**
- Modify: `docs/ANALYSIS_COMPARISON_LEGACY.md`
- Modify: `docs/MIGRATION_SERVICE_BASED_ORDERS.md`
- Modify: `docs/fastchannel/*` if needed

**Step 1:** Document multi-table and batch price behavior.
**Step 2:** Document unit normalization and SQL Server assumptions.

---

### Task 9: Build verification

**Step 1:** Run `./gradlew assemble`.
**Step 2:** Note that tests are skipped due to missing JUnit; report.

