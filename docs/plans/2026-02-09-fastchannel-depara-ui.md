# De-Para Fastchannel UI Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan.

**Goal:** Add a dedicated De-Para UI (Empresas/Locais/Tabelas de Preco) backed by AD_FCDEPARA, with filters and upsert, and improve price table selection in the Precos screen.

**Architecture:** Add a new backend service (FCDeparaService) exposed via FastchannelDirectServlet to list Sankhya entities + existing mappings and to persist updates (upsert). Frontend adds a new depara.html with tabs and filters, and updates navigation and Precos filters to use price-table dropdown.

**Tech Stack:** Java (Sankhya JAPE/NativeSql), HTML5/JS, existing fc-direct JSON API.

---

### Task 1: Backend De-Para Service (list + upsert + cache invalidation)

**Files:**
- Create: `model/src/main/java/br/com/bellube/fastchannel/web/FCDeparaService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/DeparaService.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/web/FCDeparaServiceTest.java`

**Step 1: Write the failing test**

```java
// FCDeparaServiceTest.java
@Test
void mergeMappings_prefersExistingCodExterno() {
    List<Map<String, Object>> base = List.of(map("codSankhya", 1, "descricao", "Empresa A"));
    List<Map<String, Object>> mappings = List.of(map("codSankhya", 1, "codExterno", "21"));

    List<Map<String, Object>> merged = FCDeparaService.merge(base, mappings);
    assertEquals("21", merged.get(0).get("codExterno"));
}
```

**Step 2: Run test to verify it fails**

Run: `./gradlew :model:test --tests "br.com.bellube.fastchannel.web.FCDeparaServiceTest"`
Expected: FAIL (class/method not found)

**Step 3: Write minimal implementation**

```java
// FCDeparaService.java (static helper)
static List<Map<String, Object>> merge(List<Map<String, Object>> base, List<Map<String, Object>> mappings) {
    Map<String, String> byCod = new HashMap<>();
    for (Map<String, Object> m : mappings) {
        if (m.get("codSankhya") != null && m.get("codExterno") != null) {
            byCod.put(m.get("codSankhya").toString(), m.get("codExterno").toString());
        }
    }
    for (Map<String, Object> b : base) {
        String key = b.get("codSankhya") != null ? b.get("codSankhya").toString() : null;
        if (key != null && byCod.containsKey(key)) {
            b.put("codExterno", byCod.get(key));
        }
    }
    return base;
}
```

**Step 4: Run test to verify it passes**

Run: `./gradlew :model:test --tests "br.com.bellube.fastchannel.web.FCDeparaServiceTest"`
Expected: PASS

**Step 5: Implement full service**

Add endpoints:
- `listEmpresas` (TSIEMP)
- `listLocais` (TGFLOC)
- `listTabelasPreco` (TGFTAB latest by CODTAB)
- `listMappings` (AD_FCDEPARA by TIPO_ENTIDADE)
- `saveMappings` (upsert AD_FCDEPARA for each row)

Ensure `saveMappings` invalidates cache: `DeparaService.getInstance().invalidateCache()`.

**Step 6: Commit**

```bash
git add model/src/main/java/br/com/bellube/fastchannel/web/FCDeparaService.java \
        model/src/main/java/br/com/bellube/fastchannel/service/DeparaService.java \
        model/src/test/java/br/com/bellube/fastchannel/web/FCDeparaServiceTest.java

git commit -m "feat: add backend de-para service"
```

---

### Task 2: Expose FCDeparaService in fc-direct

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FastchannelDirectServlet.java`

**Step 1: Write the failing test**

Add to test:
```java
@Test
void serviceRegistry_containsDeparaEndpoints() {
    assertTrue(FastchannelDirectServlet.hasService("FCDeparaSP.listEmpresas"));
}
```

**Step 2: Run test to verify it fails**

Run: `./gradlew :model:test --tests "br.com.bellube.fastchannel.web.FCDeparaServiceTest"`
Expected: FAIL

**Step 3: Implement minimal code**

Register services in static block:
```java
services.put("FCDeparaSP.listEmpresas", new ServiceInfo(FCDeparaService.class, "listEmpresas"));
services.put("FCDeparaSP.listLocais", new ServiceInfo(FCDeparaService.class, "listLocais"));
services.put("FCDeparaSP.listTabelasPreco", new ServiceInfo(FCDeparaService.class, "listTabelasPreco"));
services.put("FCDeparaSP.listMappings", new ServiceInfo(FCDeparaService.class, "listMappings"));
services.put("FCDeparaSP.saveMappings", new ServiceInfo(FCDeparaService.class, "saveMappings"));
```

Add a `hasService` helper for tests if needed.

**Step 4: Run test to verify it passes**

Run: `./gradlew :model:test --tests "br.com.bellube.fastchannel.web.FCDeparaServiceTest"`
Expected: PASS

**Step 5: Commit**

```bash
git add model/src/main/java/br/com/bellube/fastchannel/web/FastchannelDirectServlet.java \
        model/src/test/java/br/com/bellube/fastchannel/web/FCDeparaServiceTest.java

git commit -m "feat: expose de-para endpoints"
```

---

### Task 3: De-Para UI page with filters and save

**Files:**
- Create: `vc/src/main/webapp/html5/fastchannel/depara.html`
- Modify: `vc/src/main/webapp/html5/fastchannel/dashboard.html`
- Modify: `vc/src/main/webapp/html5/fastchannel/config.html`
- Modify: `vc/src/main/webapp/html5/fastchannel/pedidos.html`
- Modify: `vc/src/main/webapp/html5/fastchannel/estoque.html`
- Modify: `vc/src/main/webapp/html5/fastchannel/precos.html`
- Modify: `vc/src/main/webapp/html5/fastchannel/fila.html`
- Modify: `vc/src/main/webapp/html5/fastchannel/logs.html`

**Step 1: Write failing UI spec tests (manual)**

Manual check:
- New tab “De-Para” appears in all pages nav.
- De-Para page loads Empresas/Locais/Tabelas with filters and editable COD_EXTERNO.
- Save persists changes (re-open shows same values).

**Step 2: Implement UI**

- Add tabs (Empresas/Locais/Tabelas) with filter inputs (codigo + descricao).
- On load, call `FCDeparaSP.listEmpresas` and `FCDeparaSP.listMappings` (tipo=EMPRESA), then merge.
- Same for Locais (TIPO=STOCK_STORAGE) and Tabelas (TIPO=TABELA_PRECO).
- Provide “Salvar alterações” button that sends only changed rows to `FCDeparaSP.saveMappings`.

**Step 3: Manual validation**

Open `/addon-fastchannel/html5/fastchannel/depara.html` and save a mapping; confirm persisted in DB.

**Step 4: Commit**

```bash
git add vc/src/main/webapp/html5/fastchannel/depara.html \
        vc/src/main/webapp/html5/fastchannel/dashboard.html \
        vc/src/main/webapp/html5/fastchannel/config.html \
        vc/src/main/webapp/html5/fastchannel/pedidos.html \
        vc/src/main/webapp/html5/fastchannel/estoque.html \
        vc/src/main/webapp/html5/fastchannel/precos.html \
        vc/src/main/webapp/html5/fastchannel/fila.html \
        vc/src/main/webapp/html5/fastchannel/logs.html

git commit -m "feat: add de-para UI"
```

---

### Task 4: Price tables dropdown + fix price listing expectations

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java`
- Modify: `vc/src/main/webapp/html5/fastchannel/precos.html`

**Step 1: Write failing test**

Add to `FCDeparaServiceTest`:
```java
@Test
void listTabelasPreco_returnsLatestByCodtab() {
    // stub by calling helper with mock rows or verify SQL builder string if needed
}
```

**Step 2: Implement backend helper**

- Add `FCPrecosSP.listTabelasPreco` (or reuse `FCDeparaSP.listTabelasPreco`) returning list of {nuTab, codTab, descricao, dtVigor}.
- Ensure `precos.html` uses dropdown populated with this list and sets `nuTab` on filter.

**Step 3: Manual validation**

- Precos page shows table list in dropdown.
- Filtering by table returns items from TGFEXC.

**Step 4: Commit**

```bash
git add model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java \
        vc/src/main/webapp/html5/fastchannel/precos.html

git commit -m "feat: price table dropdown"
```

---

### Task 5: Deploy & validate

**Files:**
- None

**Step 1: Build and deploy locally**

Run: `./gradlew clean deployAddon -x test`
Expected: deploy success

**Step 2: Manual checks**

- De-Para page loads and saves.
- Price tables show in Precos page.
- Existing config values remain unchanged.

**Step 3: Commit (if any deploy scripts changed)**

```bash
git status
```

```
