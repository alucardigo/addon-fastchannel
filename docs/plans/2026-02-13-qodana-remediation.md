# Qodana Findings Remediation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remover os 30 achados atuais do Qodana com baixo risco de regressão, priorizando problemas de fluxo/condição antes de refactors cosméticos.

**Architecture:** A correção será feita por grupos de regra e por contexto de negócio, começando em autenticação/HTTP/strategy (maior risco funcional), depois removendo atribuições redundantes repetitivas e finalizando com melhorias de legibilidade. Cada bloco fecha com execução de testes unitários e novo scan Qodana.

**Tech Stack:** Java, Gradle, JUnit, Qodana CLI (`qodana-jvm`)

### Task 1: Baseline Reprodutível

**Files:**
- Read: `docs/qodana-analysis-2026-02-13.md`
- Modify: `docs/plans/2026-02-13-qodana-remediation.md`
- Verify Artifact: `C:/Users/suporteti/AppData/Local/JetBrains/Qodana/54c30f0d-5fa750da/results/qodana.sarif.json`

**Step 1: Validar baseline do scan**

Run: `qodana view -f "C:/Users/suporteti/AppData/Local/JetBrains/Qodana/54c30f0d-5fa750da/results/qodana.sarif.json"`
Expected: 30 problemas listados.

**Step 2: Confirmar pontos de maior risco**

Run: `rg -n "Condition|already assigned|initializer 'null'" model/src/main/java/br/com/bellube/fastchannel`
Expected: ocorrências nos arquivos de auth/http/strategy/servlet/queue.

**Step 3: Commit de checkpoint (sem código)**

```bash
git add docs/qodana-analysis-2026-02-13.md docs/plans/2026-02-13-qodana-remediation.md
git commit -m "docs: add qodana baseline and remediation plan"
```

### Task 2: Corrigir `ConstantValue` em autenticação e HTTP (risco alto)

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/auth/FastchannelTokenManager.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/http/FastchannelHttpClient.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/auth/`
- Test: `model/src/test/java/br/com/bellube/fastchannel/http/`

**Step 1: Escrever/ajustar testes para cenários de nulidade e exceção**

Objetivo: garantir comportamento com `snippet`, `lastException` e fallback de erro.

**Step 2: Corrigir condições sempre verdadeiras/falsas**

Ação: remover verificações tautológicas e simplificar o fluxo condicional sem alterar retorno esperado.

**Step 3: Rodar testes focados**

Run: `./gradlew test --tests "*FastchannelTokenManager*" --tests "*FastchannelHttpClient*"`
Expected: PASS.

**Step 4: Commit**

```bash
git add model/src/main/java/br/com/bellube/fastchannel/auth/FastchannelTokenManager.java model/src/main/java/br/com/bellube/fastchannel/http/FastchannelHttpClient.java model/src/test/java/br/com/bellube/fastchannel/auth/ model/src/test/java/br/com/bellube/fastchannel/http/
git commit -m "fix: remove constant-value branches in auth and http flows"
```

### Task 3: Corrigir `ConstantValue` em strategy/invoker/servlet (risco alto)

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/SankhyaServiceInvoker.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/HttpServiceStrategy.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FastchannelDirectServlet.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/service/`
- Test: `model/src/test/java/br/com/bellube/fastchannel/web/`

**Step 1: Cobrir cenários de erro/response em testes**

Objetivo: provar comportamento de fallback e mensagens em integração HTTP e servlet.

**Step 2: Eliminar checks impossíveis**

Ação: substituir blocos impossíveis por caminho direto e logs claros.

**Step 3: Rodar suíte parcial**

Run: `./gradlew test --tests "*ServiceInvoker*" --tests "*HttpServiceStrategy*" --tests "*InternalApiStrategy*" --tests "*DirectServlet*"`
Expected: PASS.

**Step 4: Commit**

```bash
git add model/src/main/java/br/com/bellube/fastchannel/service/SankhyaServiceInvoker.java model/src/main/java/br/com/bellube/fastchannel/service/strategy/HttpServiceStrategy.java model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java model/src/main/java/br/com/bellube/fastchannel/web/FastchannelDirectServlet.java model/src/test/java/br/com/bellube/fastchannel/service/ model/src/test/java/br/com/bellube/fastchannel/web/
git commit -m "fix: remove impossible conditions in strategy, invoker and servlet"
```

### Task 4: Corrigir `DataFlowIssue` (reatribuições redundantes)

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/auth/FastchannelTokenManager.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/FastchannelHeaderMappingService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FastchannelDirectServlet.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/service/FastchannelHeaderMappingServiceTest.java`

**Step 1: Ajustar atribuições repetidas**

Ação: remover reatribuições idênticas e manter só a atribuição efetiva.

**Step 2: Validar resultado funcional**

Run: `./gradlew test --tests "*FastchannelHeaderMappingServiceTest*"`
Expected: PASS.

**Step 3: Commit**

```bash
git add model/src/main/java/br/com/bellube/fastchannel/auth/FastchannelTokenManager.java model/src/main/java/br/com/bellube/fastchannel/service/FastchannelHeaderMappingService.java model/src/main/java/br/com/bellube/fastchannel/web/FastchannelDirectServlet.java model/src/test/java/br/com/bellube/fastchannel/service/FastchannelHeaderMappingServiceTest.java
git commit -m "refactor: remove redundant data-flow assignments"
```

### Task 5: Corrigir `UnusedAssignment` (redundâncias e valores não usados)

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/listener/NotaFiscalListener.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/QueueService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCConfigService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java`

**Step 1: Remover inicializações redundantes**

Ação: eliminar `jdbc = null`, `exists = false`, `found = false` quando não agregam valor.

**Step 2: Corrigir variáveis atribuídas e não lidas**

Ação: em `NotaFiscalListener`, usar `message` de fato (log/retorno) ou remover variável.

**Step 3: Validar compilação e testes do módulo**

Run: `./gradlew test`
Expected: PASS.

**Step 4: Commit**

```bash
git add model/src/main/java/br/com/bellube/fastchannel/listener/NotaFiscalListener.java model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java model/src/main/java/br/com/bellube/fastchannel/service/QueueService.java model/src/main/java/br/com/bellube/fastchannel/web/FCConfigService.java model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java
git commit -m "refactor: remove unused assignments and redundant initializers"
```

### Task 6: Limpeza de moderados + validação final Qodana

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/DeparaService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FastchannelDirectServlet.java`
- Output: `docs/qodana-analysis-post-fix.md`

**Step 1: Aplicar refactors de legibilidade**

Ação: ajustar `while` para `do-while` quando apropriado e extrair parte comum do `if`.

**Step 2: Rodar scan final**

Run: `qodana scan --print-problems`
Expected: queda relevante dos 30 findings (ideal: 0).

**Step 3: Documentar delta pós-correção**

Ação: criar `docs/qodana-analysis-post-fix.md` com antes/depois por regra.

**Step 4: Commit**

```bash
git add model/src/main/java/br/com/bellube/fastchannel/service/DeparaService.java model/src/main/java/br/com/bellube/fastchannel/web/FastchannelDirectServlet.java docs/qodana-analysis-post-fix.md
git commit -m "chore: finalize qodana remediation and publish delta report"
```

## Orchestration Evidence
- Skill loaded: `writing-plans`
- Skill loaded: `multi-cli-agent-orchestrator`
- Skill loaded: `loki-mode`
- Router dry-run executed:
  - `C:/Users/suporteti/.codex/skills/multi-cli-agent-orchestrator/scripts/orchestrate-router.ps1`
  - Output log dir: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/.orchestration/logs/`

