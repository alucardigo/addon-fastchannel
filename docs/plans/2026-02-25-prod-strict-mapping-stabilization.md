# Fastchannel Production Strict Mapping Stabilization Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Garantir integração confiável em produção entre Sankhya e Fastchannel, com preços/estoque exportados do Sankhya e pedidos importados da Fastchannel sem uso de dados de homologação.

**Architecture:** Endurecer o fluxo de configuração para modo estrito de produção: remover fallbacks implícitos para tabela/local/produto, exigir mapeamentos válidos (de-para e campos adicionais) e tornar falhas explícitas/logáveis. Preservar compatibilidade com legado para `CACSP.incluirNota` via ServiceInvoker, com fallback controlado para InternalAPI apenas quando aplicável.

**Tech Stack:** Java (Addon Studio), Sankhya Jape/ServiceInvoker, Gradle, MSSQL, Fastchannel REST API.

### Task 1: Auditar defaults e fallbacks perigosos

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/config/FastchannelConfig.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/PriceTableResolver.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/QueueService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java`

1. Revisar resolução de `NUTAB`, `CODLOCAL`, `StorageId` e `CODPROD` para identificar qualquer fallback automático silencioso.
2. Converter fallback silencioso em falha explícita com log estruturado (`AD_FCLOG`) quando dado obrigatório não existir.
3. Garantir que nenhum valor de homologação exista como default no código executável.

### Task 2: Endurecer mapeamento de preços para produção

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/PriceResolver.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/PriceTableResolver.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java`

1. Exigir `NUTAB` resolvido por configuração/`AD_TIPO_FAST`/campo adicional sem fallback inválido.
2. Validar de-para de tabela Sankhya x ID Fastchannel antes do envio.
3. Em erro de mapeamento, bloquear envio e logar causa com contexto (`SKU`, `NUTAB`, `PRICE_TABLE_ID`).

### Task 3: Endurecer mapeamento de estoque para produção

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/QueueService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/StockResolver.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCEstoqueService.java`

1. Exigir `CODLOCAL` e `StorageId` válidos; sem fallback para local não mapeado.
2. Garantir consistência SKU/produto antes de atualizar estoque na Fastchannel.
3. Melhorar payload/log para auditoria de divergências (produto, local, saldo, storage).

### Task 4: Fortalecer importação de pedidos

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/OrderCreationOrchestrator.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/ServiceInvokerStrategy.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java`

1. Manter prioridade do legado (`ServiceInvoker/CACSP.incluirNota`).
2. Impedir importação com item sem mapeamento válido (sem cair em produto mínimo).
3. Incluir no log da falha os identificadores mínimos para suporte: `orderId`, `customer`, `documento`, `sku`, `codProd`, `codLocal`.

### Task 5: Build e validação técnica

**Files:**
- Modify: `build.gradle` (apenas se houver bump de versão)

1. Rodar compilação e testes do módulo.
2. Gerar EAR e validar integridade do pacote.
3. Preparar artefato para deploy de produção.

### Task 6: Validação funcional em produção

**Files:**
- Create/Update: `docs/validation/2026-02-25-prod-strict-mapping-validation.md`

1. Validar por UI humana no Sankhya produção (`skw.bellube.com.br`) fluxos de preço, estoque e pedidos.
2. Cruzar evidência com UI Fastchannel (price table e estoque) e DBExplorer produção.
3. Registrar evidência com data/hora, entidades afetadas e resultado.
