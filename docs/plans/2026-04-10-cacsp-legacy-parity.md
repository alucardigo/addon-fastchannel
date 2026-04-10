# CACSP Legacy Parity Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Garantir que o XML enviado pelo addon-fastchannel para `CACSP.incluirNota` tenha paridade exata com o legado relevante.

**Architecture:** A auditoria compara a montagem do XML de nota em [OrderXmlBuilder.java](X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/service/OrderXmlBuilder.java) e o envio HTTP em [HttpServiceStrategy.java](X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/service/strategy/HttpServiceStrategy.java) contra as implementações legadas em `models/erp/sankhya.js` e `models/fastChannel/erp/sankhya.js`. Se houver divergência objetiva e de baixo risco, o ajuste será feito no builder do addon, preservando o restante da orquestração.

**Tech Stack:** Java 8+/Gradle no addon, JavaScript/Node.js no legado, XML de serviço Sankhya `CACSP.incluirNota`.

---

### Task 1: Mapear XML do addon

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderXmlBuilder.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/HttpServiceStrategy.java`

**Step 1: Ler montagem do XML no addon**

Run: `Get-Content "X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/service/OrderXmlBuilder.java"`
Expected: localizar todos os campos de `cabecalho` e `item` enviados em `buildIncluirNotaXml`.

**Step 2: Ler envio HTTP no addon**

Run: `Get-Content "X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/service/strategy/HttpServiceStrategy.java"`
Expected: confirmar endpoint, query params, headers e charset usados ao postar o XML.

### Task 2: Mapear XML do legado

**Files:**
- Read: `X:/gbi-app-integrador-main/gbi-app-integrador-main/models/erp/sankhya.js`
- Read: `X:/gbi-app-integrador-main/gbi-app-integrador-main/models/fastChannel/erp/sankhya.js`

**Step 1: Localizar o fluxo legado relevante**

Run: `rg -n "CACSP\\.incluirNota|<serviceRequest serviceName=\"CACSP\\.incluirNota\"|<itens INFORMARPRECO" "X:/gbi-app-integrador-main/gbi-app-integrador-main/models"`
Expected: identificar os pontos onde o legado realmente monta o XML enviado ao Sankhya.

**Step 2: Extrair a lista de campos do legado**

Run: leitura dos blocos em `models/erp/sankhya.js` e `models/fastChannel/erp/sankhya.js`
Expected: inventário explícito dos campos de cabeçalho e item, incluindo hardcodes e condicionais.

### Task 3: Comparar campo a campo

**Files:**
- Modify: `C:/Users/suporteti/.claude/projects/X--/memory/report_legacy_comparison.md`

**Step 1: Consolidar matriz de paridade**

Run: comparação manual entre o XML do addon e do legado
Expected: tabela com campos enviados em ambos, somente no legado e somente no addon.

**Step 2: Identificar divergências acionáveis**

Run: revisão de formatação e defaults
Expected: lista de campos faltantes, campos extras, diferenças de formato, valores hardcoded e potenciais regressões.

### Task 4: Corrigir bug claro e simples no addon

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderXmlBuilder.java`

**Step 1: Criar ajuste mínimo**

Run: editar apenas o builder se a divergência for objetiva
Expected: XML do addon alinhado ao legado relevante sem mexer em fluxos paralelos desnecessários.

**Step 2: Verificar diff**

Run: `git -C "X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify" diff -- model/src/main/java/br/com/bellube/fastchannel/service/OrderXmlBuilder.java`
Expected: diff pequeno e focado em paridade do XML.

### Task 5: Registrar evidências

**Files:**
- Modify: `C:/Users/suporteti/.claude/projects/X--/memory/report_legacy_comparison.md`

**Step 1: Gerar relatório final**

Run: salvar relatório com campos, divergências e patches sugeridos
Expected: documento markdown consumível, com referências de arquivo/linha e diff sugerido.

**Step 2: Registrar verificação**

Run: `git -C "X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify" diff --stat`
Expected: resumo dos arquivos alterados, se houver fix aplicado.
