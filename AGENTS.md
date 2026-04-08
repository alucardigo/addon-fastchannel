# AGENTS.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Existing AI/Spec Instructions

- **CLAUDE.md / OpenSpec**: This repo is wired for spec-driven changes via OpenSpec.
  - When a request involves proposals, specs, or non-trivial architectural/behavior changes, first consult `openspec/AGENTS.md`.
  - Follow the three-stage workflow there (proposal → implementation → archive) instead of implementing large changes ad hoc.
  - Bug fixes, small refactors, and config tweaks usually do **not** require a new proposal.

## Core Stack and Build/Deploy

- **Platform**: Sankhya Addon (Java EE/WildFly), using the official `br.com.sankhya.addonstudio` Gradle plugin.
- **Main modules**:
  - `model/`: Java business logic, integration, jobs, listeners, and web servlet endpoints.
  - `vc/`: Web UI (HTML5 + JS/CSS) packaged as the addon web module.
- **Addon identity**:
  - Extension ID: `addon-fastchannel` (see `META-INF/addon-fastchannel-extension.xml`).
  - Context roots used by the UI/servlet: `/mge/addon-fastchannel` and `/addon-fastchannel`.

### Gradle tasks

These are the key tasks you should rely on; do not invent alternatives unless the user asks for something different.

- **Standard build (no deploy)**
  - From the repo root:
    - On Windows PowerShell:
      ```bash
      ./gradlew.bat clean build
      ```
    - On Unix-like shells:
      ```bash
      ./gradlew clean build
      ```
- **Addon packaging and deployment to a local WildFly/Sankhya server**
  - `build.gradle` is configured with the `snkmodule` extension. It reads the server folder from either:
    - `local.properties` → `snk.serverFolder=<absolute-path-to-wildfly-root>`; or
    - environment variable `SNK_SERVER_FOLDER`.
  - The deployment flow is encapsulated in the customized `deployAddon` task:
    - It patches `extension.xml` (`patchExtension`).
    - Syncs the extension and dbscripts into the build output (`syncExtensionResources`, `syncDbScripts`).
    - Injects the extension/datadictionary into the WAR and prepares the EAR (`injectExtensionIntoWar`).
    - Cleans any previous `.deployed/.failed/.dodeploy/.isdeploying` markers and copies the EAR under WildFly's `standalone/deployments` (`prepareDeployDir`).
    - Waits for the `.deployed` marker, failing on `.failed` or timeout.
  - Typical workflow (once `snk.serverFolder` is configured):
    ```bash
    ./gradlew.bat deployAddon
    ```
  - This is the preferred way for agents to build + deploy when asked to "publish" or "deploy" the addon in a dev environment.

### Database / DDL

- Database integration is done via Sankhya datadictionary and explicit db scripts:
  - `datadictionary/*.xml` define entities such as `AD_FCCONFIG`, `AD_FCQUEUE`, `AD_FCPEDIDO`, `AD_FCLOGS`, `AD_FCDEPARA` etc.
  - `dbscripts/V*.xml` plus `dbscripts/migration_*.sql` are copied into `build/dist/dbscripts` and embedded in the WAR (`syncDbScripts` and `injectExtensionIntoWar`).
  - `addon.autoDDL=false` in `build.gradle`: the project expects these scripts/datadictionary files to drive schema creation/migration, not automatic DDL.

## Running and Testing the Integration (Conceptual)

There is no conventional JUnit/Gradle test suite in this repo. "Tests" in practice are:

- **Sankhya-side jobs/listeners** (must be wired through the Sankhya UI):
  - `br.com.bellube.fastchannel.job.OrderImportJob` implements `EventoProgramavelJava` and is intended to be scheduled via **Eventos Programáveis > Agendamento**.
  - Entity listeners in `model/src/main/java/br/com/bellube/fastchannel/listener` hook into core tables:
    - `ProdutoListener` → `TGFPRO` (products).
    - `PrecoListener` → `TGFEXC` (price exceptions / price tables).
    - `EstoqueListener` → `TGFEST` (stock).
  - These listeners enqueue changes into the Fastchannel queue using `QueueService` and `AD_FCQUEUE`.

- **HTML5 UI screens** under `vc/src/main/webapp/html5/fastchannel` (all routed through `FastchannelDirectServlet`):
  - `dashboard.html` – health, queue stats, recent logs, and quick actions (test connection, import orders, process queue).
  - `config.html` – centralised configuration bound to `AD_FCCONFIG` and `FastchannelConfig`.
  - `pedidos.html` – order monitoring and reprocessing.
  - `estoque.html` – stock sync monitoring and comparison.
  - `precos.html` – price sync monitoring, comparison, and batch sync.
  - `fila.html` – queue browser and maintenance.
  - `logs.html` – integration logs.

As an agent, when asked to "test" something, prefer to:

- Reason about the execution flow (listeners → queue → jobs → HTTP client to Fastchannel) rather than inventing unit tests.
- When environment access is available, guide the user to:
  - Trigger `OrderImportJob.executeScheduler()` via Sankhya scheduler.
  - Use the HTML5 screens through the `/mge/addon-fastchannel/html5/fastchannel/*.html` endpoints to exercise the flows.

## High-Level Architecture

At a high level, the addon implements a full Fastchannel ↔ Sankhya integration with these layers:

### 1. Configuration & Constants

- `model/src/main/java/br/com/bellube/fastchannel/config`:
  - `FastchannelConfig`: central configuration singleton that reads from `AD_FCCONFIG` and caches values such as:
    - API base URL, OAuth client credentials, and timeouts.
    - Batch sizes, flags (e.g., whether to sync status), and mapping parameters (company, operation, price table, stock location, etc.).
    - It also persists sync cursors (e.g., last order sync timestamp).
  - `FastchannelConstants`: numeric and string constants representing Fastchannel order statuses, operation codes, and default fallbacks.
  - `SourceConfig`: environment-/source-specific overrides for configuration (used by price/stock helpers and header mapping logic).

**Agent note**: When behavior seems configurable, always look at `FastchannelConfig`/`SourceConfig` first before hard-coding.

### 2. DTOs and HTTP Clients

- `model/src/main/java/br/com/bellube/fastchannel/dto`:
  - Rich DTOs for orders, items, customers, addresses, tracking, prices, stock, and queue entries; these map Fastchannel payloads to Sankhya semantics.
- `model/src/main/java/br/com/bellube/fastchannel/http`:
  - `FastchannelHttpClient`: low-level HTTP client that handles base URL, headers, timeouts, and error handling.
  - `FastchannelOrdersClient`, `FastchannelPriceClient`, `FastchannelStockClient`: typed clients over the HTTP base for each domain.

**Agent note**: Use these clients instead of making raw HTTP calls from new code.

### 3. Authentication

- `model/src/main/java/br/com/bellube/fastchannel/auth/FastchannelTokenManager`:
  - Manages OAuth2 tokens for the Fastchannel API.
  - Handles caching, expiry checks, and refresh; backed by `FastchannelConfig` for credentials and endpoints.
- `model/src/main/java/br/com/bellube/fastchannel/service/auth/SankhyaAuthManager`:
  - Manages integration with Sankhya session/auth when invoking internal services/HTTP endpoints.

### 4. De-Para and Header Mapping

- `DeparaService` (in `service` package):
  - Central de-para manager backed by `AD_FCDEPARA`.
  - Supports mapping between SKUs/EAN and `CODPROD`, price tables, stock locations, and reseller-specific mappings.
  - Exposes helper methods including: `getCodProdBySkuOrEan`, `getSku`, `getSkuWithFallback`, `getSkuForStock`, and write APIs like `setMapping/removeMapping`.
- `FastchannelHeaderMappingService`:
  - Maps incoming order context to all required header fields for order creation in Sankhya, returning a `ResolvedHeader` object containing:
    - `codEmp`, `codTipOper`, `codTipVenda`, `codVend`, `codNat`, `codCenCus`, and (optionally) `codParc`.
  - Uses `DeparaService`, `SourceConfig`, and partner lookup helpers to reconcile reseller/customer data.

**Agent note**: For any new integration scenario that needs consistent mappings (e.g., new stock strategy), prefer to extend `DeparaService` and `FastchannelHeaderMappingService` rather than duplicating SQLs in random services.

### 5. Order Import Flow

- `OrderService` is the central orchestrator for importing Fastchannel orders:
  - `importPendingOrders()`:
    - Uses `FastchannelOrdersClient` to page through pending orders since `FastchannelConfig.getLastOrderSync()`.
    - Skips already-imported orders via `isOrderAlreadyImported()` (lookup in `AD_FCPEDIDO`).
    - For each order, optionally re-fetches full details, then calls `importOrder(order)`.
    - After success, logs via `LogService` and optionally marks the order as synced back to Fastchannel (`ordersClient.markAsSynced`) depending on configuration.
    - Updates the last-sync cursor if at least one order was imported.
  - `importOrder(OrderDTO)`:
    - Validates and normalizes monetary fields.
    - Resolves header info (`FastchannelHeaderMappingService.resolve`).
    - Finds or creates the partner (`TGFPAR`) and address (`Endereco`) as needed.
    - Validates **all** products ahead of time using `DeparaService`.
    - Delegates to `importOrderViaService(...)` which uses `OrderCreationOrchestrator`.
    - On success, calls `upsertOrderMapping(...)` to persist into `AD_FCPEDIDO` with rich metadata and logs.
  - `OrderCreationOrchestrator` (in `service.strategy`):
    - Implements a layered strategy for actually creating the Sankhya order, in this order:
      1. `InternalApiStrategy` – uses in-process Java APIs / model-core to build the order via higher-level services.
      2. `ServiceInvokerStrategy` – uses `SankhyaServiceInvoker` to call the `CACSP.incluirNota` service by reflection.
      3. `HttpServiceStrategy` – falls back to HTTP calls against the MGE service endpoint if service invocation fails.
    - Each strategy receives the normalized `OrderDTO` and resolved header info and is expected to return the created `NUNOTA` or throw.

**Agent note**: When modifying how orders are created, change `OrderCreationOrchestrator` and the specific strategies instead of touching `OrderService`’s higher-level orchestration.

### 6. Queue and Outbox

- `QueueService` (service layer, not fully listed here but central):
  - Encapsulates reads/writes to `AD_FCQUEUE` and implements the transactional outbox pattern for stock/price/product events.
  - Provides helpers like `enqueueProduct`, `enqueuePrice`, `enqueueStock` and queue-processing operations.
- `OutboxProcessorJob`:
  - Scheduled job that scans `AD_FCQUEUE` and dispatches batched syncs to Fastchannel using the HTTP clients.
  - Handles backoff, error counters, and status updates to keep the queue consistent.

### 7. Listeners (Stock/Price/Product)

- `ProdutoListener`:
  - For `TGFPRO` insert/update/delete events.
  - Updates de-para when `REFERENCIA` changes.
  - When a product is inactivated, enqueues a stock event with quantity zero.
  - On active products, enqueues product sync operations via `QueueService`.
- `PrecoListener`:
  - For `TGFEXC` (price exceptions) relevant to the configured price table (`FastchannelConfig.getNuTab()`).
  - Ensures the record is active and has a valid SKU via `DeparaService`, then enqueues a price sync.
- `EstoqueListener`:
  - For `TGFEST` changes filtered by configured company and local stock (`FastchannelConfig.getCodemp()` / `getCodLocal()`).
  - Resolves SKU via `DeparaService.getSkuForStock` and enqueues stock sync with quantity and location.

### 8. Web Services and UI Routing

- `FastchannelDirectServlet` (`model/src/main/java/br/com/bellube/fastchannel/web/FastchannelDirectServlet.java`):
  - Acts as a thin service registry and JSON adapter between HTML5 screens and backend services.
  - Registered in the web module under `/fc-direct` and used by all HTML5 pages.
  - Maintains a static map of `serviceName → (class, method)` for service classes like:
    - `FCDashboardService`, `FCConfigService`, `FCAdminService`, `FCPedidosService`, `FCEstoqueService`, `FCPrecosService`, `FCFilaService`, `FCLogsService`.
  - Reads JSON payload, invokes the target method via reflection, and serializes the result back to JSON.
- HTML5 pages under `vc/src/main/webapp/html5/fastchannel`:
  - Use a small JavaScript client that:
    - Detects the correct addon context root (`/mge/addon-fastchannel` vs `/addon-fastchannel`).
    - Extracts the `mgeSession` from `SNKCONTEXT`, query params, or cookies.
    - Calls `fc-direct?serviceName=...` with `outputType=json` and attaches the session.

**Agent note**: When adding new frontend capabilities, extend the service map in `FastchannelDirectServlet` and implement a corresponding `*Service` class + HTML/JS wiring; keep the JSON contract simple and documented in code.

## How Future Agents Should Work Here

- For **big changes** (new capabilities, behavior changes, or architecture work):
  - Read `openspec/project.md` and `openspec/AGENTS.md`.
  - Create/extend an OpenSpec change under `openspec/changes/<change-id>/` and get it validated before implementing (unless the user explicitly opts out).
- For **small changes and bug fixes**:
  - Work directly in `model/` and/or `vc/`, using the existing patterns:
    - Reuse `FastchannelConfig`, `DeparaService`, and `FastchannelHeaderMappingService` for any config/mapping needs.
    - Keep new outbound Fastchannel calls inside the HTTP client classes.
    - For new UI features, wire them through `FastchannelDirectServlet` and a dedicated `*Service` class.
- When asked for **build/deploy/test commands**, prefer the documented Gradle tasks and flows above instead of inventing new ones.
