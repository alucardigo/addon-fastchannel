# Functional Review Handoff For Claude Code

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Corrigir os principais problemas de funcionalidade identificados na revisão do addon-fastchannel, priorizando confiabilidade operacional primeiro e desempenho depois. Segurança está explicitamente fora do escopo desta rodada.

**Scope:** Este plano cobre somente os achados com impacto funcional direto ou custo operacional relevante:
- reclaim de pedidos travados em fallback JDBC
- processamento duplicado na fila por claim não atômico
- falha do último recurso de gravação em `AD_FCPEDIDO`
- perda de itens na listagem local de preços
- regressão da tela de fila
- replay histórico desnecessário no catch-up de `IsSynched`

**Architecture:** Os ajustes mais críticos ficam em três áreas: `OrderService` para robustez de importação, `QueueService`/`OutboxProcessorJob` para integridade do outbox. O objetivo é corrigir primeiro tudo que pode deixar pedido travado ou duplicado, depois restaurar a superfície funcional da UI e por fim reduzir trabalho remoto redundante.

**Tech Stack:** Java 8, Gradle, Sankhya JAPE/JdbcWrapper, HTML5/JS, JUnit 4.

---

## Task 1: Retomar Claims Travados No Fallback JDBC

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/regression/OrderStateRegressionTest.java`

**Problem**

O caminho `claimOrderImportJdbc(...)` só assume registros com `STATUS_IMPORT='ERRO'`. Se um pedido cair em `PROCESSANDO` e o fluxo estiver em fallback JDBC, o retry automático não recupera esse claim envelhecido.

**Step 1: Cobrir o caso com teste de regressão**

Adicionar um teste que valide:
- registro existente em `AD_FCPEDIDO` com `ORDER_ID` igual
- `NUNOTA` nulo
- `STATUS_IMPORT='PROCESSANDO'`
- `DH_IMPORTACAO` mais antigo que `ORDER_IMPORT_CLAIM_TIMEOUT_MINUTES`
- resultado esperado: o claim JDBC deve ser retomado e atualizado para `PROCESSANDO` novamente

**Step 2: Alinhar a lógica JDBC com a lógica JAPE**

Editar `claimOrderImportJdbc(...)` para aceitar takeover quando:
- `STATUS_IMPORT IN ('ERRO', 'PENDENTE', null/'')`
- ou `STATUS_IMPORT='PROCESSANDO'` com `DH_IMPORTACAO` vencido

Não mudar o comportamento para `PROCESSANDO` ativo dentro da janela de timeout.

**Step 3: Verificar compatibilidade**

Confirmar que:
- pedidos já com `NUNOTA` continuam retornando `OrderImportClaim.reused(...)`
- pedidos em `PROCESSANDO` recente continuam bloqueados

---

## Task 2: Tornar O Claim Da Fila Atômico

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/QueueService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/job/OutboxProcessorJob.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/job/OutboxProcessorJobTest.java`

**Problem**

Hoje o fluxo faz:
1. `SELECT ... WHERE STATUS='PENDENTE'`
2. depois `UPDATE` separado para `PROCESSANDO`

Isso permite que duas execuções paralelas peguem o mesmo item.

**Step 1: Introduzir claim explícito no serviço**

Adicionar uma API no `QueueService` que faça claim com verificação de status, por exemplo:
- `boolean tryMarkAsProcessing(BigDecimal idQueue)`

Esse método deve atualizar para `PROCESSANDO` somente se o status ainda for `PENDENTE`, com `WHERE IDQUEUE = ? AND STATUS = 'PENDENTE'`, retornando se houve claim real.

**Step 2: Adaptar o job**

No loop do `OutboxProcessorJob`:
- substituir o `markAsProcessing(...)` cego pelo novo método atômico
- se o claim falhar, pular o item silenciosamente com log em `FINE` ou `INFO`

**Step 3: Validar o comportamento**

Cobrir ao menos estes casos:
- claim bem-sucedido processa o item
- claim perdido não processa o item
- o restante do fluxo de sucesso/erro continua marcando `ENVIADO`, `ERRO` etc.

---

## Task 3: Corrigir O Último Recurso De Persistência De Pedido

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/regression/HomologFixRegressionTest.java`

**Problem**

`forceStatusUpdateFull(...)` usa `truncateToColumn(...)` para `STATUS_IMPORT` e `ERRO_MSG`, mas escreve `NOME_CLIENTE` e `CPF_CNPJ` sem truncamento. Esse método é o fallback usado quando o upsert normal falha, então ele precisa ser resiliente.

**Step 1: Ajustar o fallback**

Aplicar `truncateToColumn(...)` também em:
- `nome`
- `cpf`

Usar os limites já resolvidos por `OrderMappingFieldSizes`.

**Step 2: Garantir simetria**

Aplicar a correção tanto no:
- `UPDATE`
- `INSERT`

**Step 3: Cobrir por teste**

Adicionar teste de regressão que comprove que valores acima do tamanho de coluna não explodem esse fallback.

---

## Task 4: Corrigir A Listagem Local De Preços

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/regression/StockPriceFilterRegressionTest.java`

**Step 1: Atualizar regressão quebrada**

`StockPriceFilterRegressionTest` hoje falha por `NoSuchMethodException`, indicando quebra de contrato/refatoração.

Corrigir uma destas opções:
- atualizar a suíte para refletir a nova extração via `FastchannelProductFilter`, desde que a regra funcional continue garantida

---

## Task 5: Formalizar a Remoção da Tela De Fila 

**Files:**
- Modify or restore: `vc/src/main/webapp/html5/fastchannel/fila.html`
- Review: `model/src/main/java/br/com/bellube/fastchannel/web/FastchannelDirectServlet.java`
- Test: `model/src/test/java/br/com/bellube/fastchannel/regression/HomologFixRegressionTest.java`

**Problem**

`fila.html` foi removido da branch, mas:
- o backend `FCFilaSP.*` continua ativo
- `AGENTS.md` ainda descreve a tela de fila como parte do addon
- a regressão de frontend falha com `NoSuchFileException`

**Step 1: Decidir a direção**

2. A remoção foi intencional, remover também o contrato restante:
   - ajustar testes
   - atualizar documentação
   - revisar navegação e endpoints expostos

**Step 2: Validar a regressão**

Reexecutar `HomologFixRegressionTest` e garantir que a falha de `fila.html` desapareça.

---

## Task 6: Parar O Replay Histórico De `markAsSynced`

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/job/OrderStatusSyncJob.java`
- Review: `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`

**Problem**

Parte 2 do `OrderStatusSyncJob` faz `SELECT ORDER_ID, NUNOTA FROM AD_FCPEDIDO WHERE STATUS_IMPORT='SUCESSO'` e chama `markAsSynced(...)` para todo o histórico em toda execução.

**Step 1: Introduzir um critério incremental**

Implementar uma forma de evitar replay completo do histórico. Exemplos aceitáveis:
- coluna/flag local indicando que o `markAsSynced` já foi enviado
- uso de cursor temporal consistente
- reaproveitar informação persistida que já diferencie pedidos ainda não marcados

**Step 2: Preservar compatibilidade**

Garantir que:
- pedidos recém-importados continuem sendo marcados
- o catch-up ainda recupere falhas reais
- a rotina não faça full scan e full replay indefinidamente

**Step 3: Validar por evidência**

Gerar diff e, se possível, teste que demonstre que itens já sincronizados não são reenviados a cada rodada.

---

## Task 7: Verificação Final

**Files:**
- Modify: `docs/plans/2026-04-10-functional-review-handoff-for-claude-code.md`

**Step 1: Rodar a suíte direcionada**

Executar pelo menos:

```bash
./gradlew.bat test --tests "br.com.bellube.fastchannel.regression.HomologFixRegressionTest" --tests "br.com.bellube.fastchannel.regression.StockPriceFilterRegressionTest" --tests "br.com.bellube.fastchannel.job.OutboxProcessorJobTest"
```

Se criar testes novos, incluí-los também.

**Step 2: Rodar uma verificação mais ampla**

Executar:

```bash
./gradlew.bat test
```

Registrar quais falhas eram pré-existentes e quais foram eliminadas nesta rodada.

**Step 3: Registrar evidência**

No fechamento da implementação, incluir:
- testes executados
- resultado
- lista curta dos arquivos alterados
- qualquer risco residual que tenha ficado de fora desta rodada

---

## Priority Order

Implementar nesta ordem:

1. Task 1: reclaim de pedido travado
2. Task 2: claim atômico da fila
3. Task 3: fallback resiliente de `AD_FCPEDIDO`
4. Task 5: remoção completa tela de fila
5. Task 4: correção da listagem local de preços
6. Task 6: otimização do catch-up de `IsSynched`

---

## Acceptance Criteria

- Pedido em `PROCESSANDO` vencido volta a ser reprocessável também no fallback JDBC.
- Fila não processa o mesmo item duas vezes por corrida concorrente normal.
- Fallback `forceStatusUpdateFull(...)` suporta nomes/documentos longos sem quebrar.
- Tela local de preços oculta produto só porque ele existe em `NUTAB` antigo da mesma família.
- `fila.html` volta a existir e passar na regressão, ou sua remoção fica consistentemente refletida em backend, testes e documentação.
- `OrderStatusSyncJob` deixa de reenviar `markAsSynced` para o histórico inteiro a cada execução.
- `HomologFixRegressionTest` e `StockPriceFilterRegressionTest` deixam de falhar por regressão introduzida nesta branch.
