# InternalApiStrategy 4490 Regression Fix Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Corrigir a regressão da importação do pedido 4490, eliminando lookup inválido de TipoVenda/NUTAB e impedindo criação com chaves compostas incompletas no Sankhya.

**Architecture:** O ajuste ficará concentrado em `InternalApiStrategy`, removendo resolução frágil de PK parcial e adicionando guards explícitos antes do `save()` do cabeçalho. A cobertura será feita por testes de regressão focados em impedir o retorno de `findByPK` incorreto e da persistência com datas versionadas ausentes.

**Tech Stack:** Java, Jape/Sankhya SDK, JUnit 4, Gradle.

---

### Task 1: Cobrir regressão por teste

**Files:**
- Modify: `model/src/test/java/br/com/bellube/fastchannel/regression/OrderStateRegressionTest.java`

**Step 1:** Adicionar teste falhando para impedir `findByPK(codTipVenda)` em `TipoVenda`.

**Step 2:** Adicionar teste falhando para exigir guard de `DHTIPVENDA`/`DHTIPOPER` antes do `save()`.

**Step 3:** Executar o teste alvo e confirmar falha.

### Task 2: Corrigir resolução de TipoVenda e guards de cabeçalho

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java`

**Step 1:** Reescrever `findTipoVendaByCodTipVenda` para buscar por código e selecionar a versão mais recente por `DHALTER`.

**Step 2:** Adicionar falha explícita quando `codParc`, `codTipOper`/`DHTIPOPER` ou `codTipVenda`/`DHTIPVENDA` estiverem inconsistentes.

**Step 3:** Reduzir caminho frágil em resolução de `NUTAB` para não depender de lookup que estoure em produção antes do fallback seguro.

### Task 3: Validar

**Files:**
- Modify: `build.gradle` apenas se necessário para teste

**Step 1:** Rodar os testes de regressão relevantes.

**Step 2:** Rodar um subconjunto adicional dos testes existentes de regressão da estratégia.

**Step 3:** Revisar diff final e preparar publish só depois de evidência verde.
