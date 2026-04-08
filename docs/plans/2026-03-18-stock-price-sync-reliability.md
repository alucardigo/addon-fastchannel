# Stock & Price Sync Reliability — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Garantir que envios de estoque e preco para a Fastchannel sejam verificados com GET apos PUT, logados corretamente, e que a resolucao de SKU seja consistente em todos os fluxos.

**Architecture:** Extrair logica de "enviar + verificar + logar" em metodos reutilizaveis nos Services de estoque e preco. O `forcarSync` e o `compararFC` passam a usar a mesma cadeia: PUT -> GET verificativo -> log. A resolucao de SKU usa `getSkuForStock()` como fonte unica de verdade.

**Tech Stack:** Java 8, Sankhya SDK (JdbcWrapper, NativeSql), API REST Fastchannel (OAuth2 + subscription keys)

---

## Problema Diagnosticado

1. **`forcarSync` nao grava log** — nao chama `LogService` apos envio
2. **`forcarSync` nao verifica** — aceita HTTP 200 do PUT sem confirmar com GET
3. **`compararFC` retorna `estoqueFastchannel: null`** — GET da FC pode estar falhando silenciosamente (exception swallowed no loop de candidates)
4. **Resolucao de SKU inconsistente** — `resolveOutboundSku` prioriza de-para, mas listeners/jobs usam `getSkuForStock` (regra da marca). Isso causou o bug do produto 4342 sobrescrevendo 9102.
5. **Precos tem os mesmos problemas** — `FCPrecosService.forcarSync` nao loga nem verifica

---

## Task 1: Unificar resolucao de SKU no FCEstoqueService

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCEstoqueService.java:625-644`

**Step 1: Alterar `resolveOutboundSku` para priorizar `getSkuForStock`**

A logica atual prioriza o de-para generico. Deve priorizar `getSkuForStock()` (regra da marca), que e a mesma usada por `EstoqueListener` e `StockFullSyncJob`.

```java
private String resolveOutboundSku(BigDecimal codProd, String fallbackSku) {
    if (codProd != null) {
        DeparaService depara = DeparaService.getInstance();
        // Priorizar regra da marca (mesma logica que EstoqueListener/StockFullSyncJob)
        String skuByRule = normalizeSku(depara.getSkuForStock(codProd));
        if (skuByRule != null && !skuByRule.isEmpty()) {
            return skuByRule;
        }
        // Fallback: de-para explicito
        String mapped = normalizeSku(depara.getCodigoExternoAtivo(DeparaService.TIPO_PRODUTO, codProd));
        if (mapped != null && !mapped.isEmpty()) {
            return mapped;
        }
    }
    String normalizedFallback = normalizeSku(fallbackSku);
    if (normalizedFallback != null && !normalizedFallback.isEmpty()) {
        return normalizedFallback;
    }
    return null;
}
```

**Step 2: Verificar que compila**

Run: `cd X:\IntegracaoFastchannel\APPFASTCHANNEL\.worktrees\merge-unify && gradlew.bat :model:compileJava`
Expected: BUILD SUCCESSFUL

**Step 3: Commit**

```bash
git add model/src/main/java/br/com/bellube/fastchannel/web/FCEstoqueService.java
git commit -m "fix: priorizar getSkuForStock na resolucao de SKU outbound de estoque"
```

---

## Task 2: Adicionar log e verificacao no forcarSync de estoque

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCEstoqueService.java:290-371`

**Step 1: Refatorar o loop do forcarSync para logar e verificar**

Dentro do loop `for (Map<String, Object> item : items)`, apos a linha `stockClient.updateStock(skuOutbound, estoque, storageId, resellerId)` (linha 345), adicionar verificacao GET e log:

Substituir o bloco `try` interno (linhas 314-352) por:

```java
try {
    String sku = asString(item.get("sku"));
    boolean skuProvided = sku != null && !sku.isEmpty();
    BigDecimal codProd = asBigDecimal(item.get("codProd"));
    BigDecimal codLocal = asBigDecimal(item.get("codLocal"));
    BigDecimal codEmp = asBigDecimal(item.get("codEmp"));

    if (codProd == null && (sku == null || sku.isEmpty())) {
        throw new Exception("sku/codProd ausente");
    }
    if (codProd == null) codProd = findCodProdBySku(conn, sku);
    if (sku == null || sku.isEmpty()) sku = findSkuByCodProd(conn, codProd);
    String skuOutbound = chooseOutboundSku(codProd, sku, skuProvided);
    if (skuOutbound == null || skuOutbound.isEmpty()) {
        throw new Exception("SKU nao resolvido para CODPROD " + codProd);
    }

    if (codLocal == null) codLocal = config.getCodLocal();
    if (codEmp == null) codEmp = config.getCodemp();
    DeparaService depara = DeparaService.getInstance();
    String storageId = resolveStorageId(depara, config, codLocal);
    String resellerId = resolveResellerId(depara, config, codEmp);
    if (storageId == null || storageId.isEmpty()) {
        throw new Exception("StorageId nao mapeado para CODLOCAL " + codLocal);
    }
    if (resellerId == null || resellerId.isEmpty()) {
        throw new Exception("ResellerId nao mapeado para CODEMP " + codEmp);
    }

    BigDecimal estoque = new StockResolver().resolve(codProd, codEmp, codLocal);
    if (estoque == null) estoque = BigDecimal.ZERO;

    // PUT - enviar estoque
    stockClient.updateStock(skuOutbound, estoque, storageId, resellerId);

    // GET - verificar se o valor chegou na FC
    LogService logService = LogService.getInstance();
    StockDTO verificacao = null;
    try {
        verificacao = stockClient.getStock(skuOutbound);
    } catch (Exception getEx) {
        log.warning("GET verificativo falhou para SKU " + skuOutbound + ": " + getEx.getMessage());
    }

    if (verificacao != null && verificacao.getQuantity() != null) {
        BigDecimal fcQty = verificacao.getQuantity();
        if (fcQty.compareTo(estoque) == 0) {
            logService.logStockSync(skuOutbound, estoque, true,
                    "forcarSync OK. Sankhya=" + estoque + " FC=" + fcQty
                            + " StorageId=" + storageId + " ResellerId=" + resellerId);
        } else {
            logService.logStockSync(skuOutbound, estoque, true,
                    "forcarSync DIVERGENTE. Enviado=" + estoque + " FC retornou=" + fcQty
                            + " StorageId=" + storageId + " ResellerId=" + resellerId);
        }
    } else {
        logService.logStockSync(skuOutbound, estoque, true,
                "forcarSync enviado (PUT OK), GET verificativo nao retornou dados."
                        + " StorageId=" + storageId + " ResellerId=" + resellerId);
    }

    ok++;
} catch (Exception e) {
    errors++;
    String itemRef = item.get("sku") != null ? String.valueOf(item.get("sku")) : String.valueOf(item.get("codProd"));
    errorDetails.add(itemRef + ": " + e.getMessage());
    log.log(Level.WARNING, "Falha ao sincronizar estoque", e);
    try {
        LogService.getInstance().logStockSync(
                itemRef, BigDecimal.ZERO, false, "forcarSync ERRO: " + e.getMessage());
    } catch (Exception logEx) {
        log.log(Level.FINE, "Falha ao gravar log de erro", logEx);
    }
}
```

**Step 2: Atualizar o response para incluir dados da verificacao**

Antes de `result.put("success", ...)` (linha 354), adicionar campo de verificacao:

```java
result.put("verified", errors == 0);
```

**Step 3: Verificar que compila**

Run: `gradlew.bat :model:compileJava`
Expected: BUILD SUCCESSFUL

**Step 4: Commit**

```bash
git add model/src/main/java/br/com/bellube/fastchannel/web/FCEstoqueService.java
git commit -m "feat: adicionar log e verificacao GET apos PUT no forcarSync de estoque"
```

---

## Task 3: Melhorar compararFC de estoque — diagnostico quando GET falha

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCEstoqueService.java:240-278`

**Step 1: Logar erros no loop de candidatos em vez de engolir**

Substituir o bloco do loop (linhas 240-254) por:

```java
StockDTO fcStock = null;
String skuUsado = null;
List<String> tentativas = new ArrayList<>();
for (String candidate : skuCandidates) {
    if (candidate == null || candidate.isEmpty()) continue;
    try {
        StockDTO fetched = client.getStock(candidate);
        if (fetched != null) {
            fcStock = fetched;
            skuUsado = candidate;
            break;
        } else {
            tentativas.add(candidate + ":404/vazio");
        }
    } catch (Exception ex) {
        tentativas.add(candidate + ":" + ex.getMessage());
    }
}
```

Depois, no bloco `else` da linha 276-278, incluir diagnostico:

```java
} else {
    result.put("estoqueFastchannel", null);
    if (!tentativas.isEmpty()) {
        result.put("fcDiagnostico", "Nenhum SKU encontrado na FC. Tentativas: " + tentativas);
    }
}
```

**Step 2: Verificar que compila**

Run: `gradlew.bat :model:compileJava`

**Step 3: Commit**

```bash
git add model/src/main/java/br/com/bellube/fastchannel/web/FCEstoqueService.java
git commit -m "fix: diagnosticar falhas no GET da FC no compararFC de estoque"
```

---

## Task 4: Unificar resolucao de SKU no FCPrecosService

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java`

**Step 1: Localizar e alterar `resolveOutboundSku` (se existir) para priorizar `getSkuForStock`**

Aplicar a mesma logica da Task 1: priorizar `getSkuForStock()` sobre de-para generico.

Se o FCPrecosService tiver seu proprio `resolveOutboundSku`, aplicar a mesma mudanca:

```java
private String resolveOutboundSku(BigDecimal codProd, String fallbackSku) {
    if (codProd != null) {
        DeparaService depara = DeparaService.getInstance();
        String skuByRule = normalizeSku(depara.getSkuForStock(codProd));
        if (skuByRule != null && !skuByRule.isEmpty()) {
            return skuByRule;
        }
        String mapped = normalizeSku(depara.getCodigoExternoAtivo(DeparaService.TIPO_PRODUTO, codProd));
        if (mapped != null && !mapped.isEmpty()) {
            return mapped;
        }
    }
    String normalizedFallback = normalizeSku(fallbackSku);
    if (normalizedFallback != null && !normalizedFallback.isEmpty()) {
        return normalizedFallback;
    }
    return null;
}
```

**Step 2: Verificar que compila**

Run: `gradlew.bat :model:compileJava`

**Step 3: Commit**

```bash
git add model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java
git commit -m "fix: priorizar getSkuForStock na resolucao de SKU outbound de precos"
```

---

## Task 5: Adicionar log e verificacao no forcarSync de precos

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java`

**Step 1: No forcarSync de precos, adicionar apos o envio:**

1. GET verificativo via `FastchannelPriceClient.getPrice(skuOutbound)`
2. Log via `LogService.logPriceSync(sku, success, details)`
3. Incluir no details se houve divergencia entre valor enviado e retornado

Seguir o mesmo padrao da Task 2 (PUT -> GET verificativo -> log com resultado).

**Step 2: Verificar que compila**

Run: `gradlew.bat :model:compileJava`

**Step 3: Commit**

```bash
git add model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java
git commit -m "feat: adicionar log e verificacao GET apos PUT no forcarSync de precos"
```

---

## Task 6: Melhorar compararFC de precos — diagnostico quando GET falha

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java`

**Step 1: Aplicar o mesmo padrao da Task 3 ao compararFC de precos**

1. No loop de candidatos, logar erros em vez de engolir
2. Retornar campo `fcDiagnostico` quando nenhum SKU retornou dados da FC

**Step 2: Verificar que compila**

Run: `gradlew.bat :model:compileJava`

**Step 3: Commit**

```bash
git add model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java
git commit -m "fix: diagnosticar falhas no GET da FC no compararFC de precos"
```

---

## Task 7: Teste integrado via API

**Step 1: Chamar forcarSync de estoque para produto 9102**

```
POST http://172.16.127.11:8080/addon-fastchannel/fc-direct?serviceName=FCEstoqueSP.forcarSync
Body: {"codProd":9102,"codEmp":26,"codLocal":99000000}
```

Esperado: `success: true, processed: 1, verified: true`

**Step 2: Verificar log foi gravado no banco**

```sql
SELECT TOP 3 * FROM AD_FCLOG
WHERE OPERACAO = 'STOCK_SYNC' AND REFERENCIA = '31013353'
ORDER BY DH_REGISTRO DESC
```

Esperado: novo registro com nivel INFO e mensagem contendo "forcarSync"

**Step 3: Chamar compararFC para confirmar**

```
POST http://172.16.127.11:8080/addon-fastchannel/fc-direct?serviceName=FCEstoqueSP.compararFC
Body: {"codProd":9102,"codEmp":26,"codLocal":99000000}
```

Esperado: `estoqueFastchannel` com valor numerico (nao null), ou `fcDiagnostico` explicando o que falhou

**Step 4: Repetir para precos**

```
POST http://172.16.127.11:8080/addon-fastchannel/fc-direct?serviceName=FCPrecosSP.forcarSync
Body: {"codProd":9102}
```

Esperado: `success: true` com log gravado

**Step 5: Commit final**

```bash
git commit -m "test: validar sync estoque/preco com log e verificacao para produto 9102"
```

---

## Resumo de Arquivos Alterados

| Arquivo | Tasks | Mudanca |
|---------|-------|---------|
| `FCEstoqueService.java` | 1, 2, 3 | SKU unificado, log+verificacao no forcarSync, diagnostico no compararFC |
| `FCPrecosService.java` | 4, 5, 6 | SKU unificado, log+verificacao no forcarSync, diagnostico no compararFC |

## Nota sobre o GET retornando null

O GET da API de Stock Fastchannel (`/stock-management/v1/stock/{sku}`) pode usar uma subscription key diferente da de distribuicao. Se o GET continuar falhando apos as mudancas, investigar se e necessario usar `SUBSCRIPTION_KEY_CONSUMPTION` em vez de `SUBSCRIPTION_KEY_DISTRIBUTION` para leitura. A task 3 adicionara diagnostico para revelar isso.
