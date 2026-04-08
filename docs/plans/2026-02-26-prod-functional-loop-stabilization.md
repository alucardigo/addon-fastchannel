# Prod Functional Loop Stabilization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Stabilize production flow for Fastchannel→Sankhya orders and Sankhya→Fastchannel price/stock sync with repeatable publish-install-test loop.

**Architecture:** Keep strategy order `ServiceInvoker > InternalAPI > HTTP`, harden failure handling, and remove runtime edge cases that abort order creation. Preserve SKU behavior based on business rule (reference vs product code) and dynamic company filtering by de-para mappings.

**Tech Stack:** Java (Addon Studio), Sankhya Jape/API, Gradle, AreaDev publish API/UI, Fastchannel API/UI.

### Task 1: Diagnose current production failures

**Files:**
- Modify: `docs/validation/production-functional-evidence.md`
- Read: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java`
- Read: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/ServiceInvokerStrategy.java`

**Step 1:** Capture latest failing order IDs and messages from addon UI logs.

**Step 2:** Confirm exact failing paths (ServiceInvoker unavailable vs InternalAPI business error).

**Step 3:** Record findings with timestamp and order IDs.

### Task 2: Patch order creation resilience

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/ServiceInvokerStrategy.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/service/strategy/`

**Step 1:** Add guards for item fields that can trigger calculation/division issues in Sankhya rules.

**Step 2:** Expand ServiceInvoker reflection compatibility and fallback behavior.

**Step 3:** Build and run tests.

### Task 3: Validate price/stock listing and sync filters

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCEstoqueService.java`

**Step 1:** Ensure query filters only show companies mapped in de-para with integration enabled.

**Step 2:** Preserve SKU generation rule from brand setting (`AD_FASTREF`).

**Step 3:** Validate endpoint responses and UI tables.

### Task 4: Publish + install + production functional loop

**Files:**
- Modify: `build.gradle`
- Modify: `docs/validation/production-functional-evidence.md`

**Step 1:** Bump addon version and run `./gradlew clean build -x test publishAddon ...`.

**Step 2:** Promote release in AreaDev.

**Step 3:** Install in production via Minhas Soluções.

**Step 4:** Execute loop tests:
- Order import (Fastchannel→Sankhya)
- Price sync (Sankhya→Fastchannel) with cent variation and rollback
- Stock sync (Sankhya→Fastchannel)

**Step 5:** Record evidence with IDs, timestamps, and UI screenshots/observations.
