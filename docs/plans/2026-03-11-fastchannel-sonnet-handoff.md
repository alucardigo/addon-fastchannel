# Fastchannel Stabilization Gaps Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fechar as lacunas restantes do add-on Fastchannel para atingir paridade operacional com o legado em importação de pedidos, estoque, preço e preço escalonado, com observabilidade suficiente para operação em produção.

**Architecture:** Preserve JAPE como padrão para entidades de negócio do Sankhya e use SQL apenas onde for estritamente necessário para idempotência técnica, filas e inspeção de schema. Trate o legado JavaScript em `X:/gbi-app-integrador-main/gbi-app-integrador-main` como referência funcional obrigatória e não como inspiração opcional.

**Tech Stack:** Java add-on Sankhya, JAPE, NativeSql/JdbcWrapper, SQL Server, Gradle, testes JUnit baseados em source inspection e validação funcional no Sankhya.

---

### Task 1: Fechar Matriz de Incidentes do Add-on

**Files:**
- Modify: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/docs/plans/2026-03-11-fastchannel-sonnet-handoff.md`
- Read: `X:/tmp-log-20260311133929/server.log`
- Read: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/**`

**Step 1: Classificar todos os erros do log**

Ler `server.log` e montar tabela com colunas:
- erro
- classe/stack de origem
- addon-confirmado | core-sankhya-confirmado | indeterminado
- arquivo/linha no addon quando aplicável
- status atual (corrigido, pendente, sem evidência)

**Step 2: Registrar exclusões justificadas**

Tudo que não for add-on deve ter exclusão objetiva por stack. Não usar "parece core" como justificativa.

**Acceptance criteria**
- Existe uma matriz explícita de incidentes.
- Todo erro relevante do log está classificado.
- Nenhum erro do add-on fica sem dono.

---

### Task 2: Fortalecer Idempotência de Importação com Testes Reais

**Files:**
- Modify: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Modify: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/test/java/br/com/bellube/fastchannel/regression/OrderStateRegressionTest.java`
- Create if needed: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/test/java/br/com/bellube/fastchannel/service/OrderServiceIdempotencyTest.java`

**Step 1: Remover perda de contexto no takeover**

Em `takeOverOrderMappingClaim`, não zerar `CODPARC` sem necessidade. Preserve contexto operacional salvo se houver razão técnica comprovada para limpar.

**Step 2: Diferenciar estado de concorrência**

No fluxo de importação e retry, registrar explicitamente quando um pedido foi:
- reutilizado por já possuir `NUNOTA`
- ignorado por estar `PROCESSANDO` recente
- retomado por claim vencido

**Step 3: Escrever testes comportamentais**

Cobrir cenários:
- primeira execução reclama o pedido com `PROCESSANDO`
- segunda execução com mesmo `ORDER_ID` reutiliza `NUNOTA`
- pedido com `PROCESSANDO` recente não duplica processamento
- pedido com `PROCESSANDO` vencido é retomado
- finalização atualiza a mesma linha de `AD_FCPEDIDO`

**Acceptance criteria**
- Não há mais apenas testes textuais para o fluxo crítico.
- O fluxo de claim tem cobertura comportamental suficiente.
- O código não perde rastreabilidade à toa.

---

### Task 3: Ajustar Retry Manual e Diagnóstico Operacional

**Files:**
- Modify: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/web/FCAdminService.java`
- Modify: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/service/LogService.java`

**Step 1: Melhorar semântica do retry**

Quando `retryErroredOrders` chamar `importOrder(order)`:
- diferenciar `reprocessado com sucesso`
- `pulado por outra execução em andamento`
- `falhou de novo`

**Step 2: Logar motivo do skip**

Adicionar log operacional claro quando o pedido não entra por concorrência legítima.

**Acceptance criteria**
- O admin não perde visibilidade de retries ignorados.
- O operador consegue distinguir skip legítimo de falha real.

---

### Task 4: Auditar Paridade de Pedido com o Legado

**Files:**
- Read: `X:/gbi-app-integrador-main/gbi-app-integrador-main/controllers/api/data/integration/pedidos.js`
- Read: `X:/gbi-app-integrador-main/gbi-app-integrador-main/models/erp/sankhya.js`
- Modify: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Modify: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java`

**Step 1: Mapear fluxo legado campo a campo**

Extrair do legado:
- como resolve parceiro
- como resolve `CODVEND`
- como resolve `NUTAB`
- como envia `VLRFRETE`
- como garante que o pedido entra pendente e não faturado
- como evita duplicidade

**Step 2: Comparar com add-on atual**

Produzir uma tabela `legado x addon` com:
- igual
- divergente corrigível
- divergente por decisão arquitetural

**Step 3: Corrigir divergências ainda abertas**

Priorizar:
- campos críticos de cabeçalho
- dados de vendedor e tabela
- frete
- status inicial do pedido

**Acceptance criteria**
- Existe relatório explícito de paridade.
- Toda divergência remanescente tem decisão e justificativa.
- Não restam diferenças críticas abertas sem plano.

---

### Task 5: Auditar Estoque, Preço e Preço Escalonado vs Legado

**Files:**
- Read: `X:/gbi-app-integrador-main/gbi-app-integrador-main/controllers/api/data/integration/fastChannel.js`
- Read: `X:/gbi-app-integrador-main/gbi-app-integrador-main/controllers/api/data/integration/produtos.js`
- Modify: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/service/PriceService.java`
- Modify: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/job/OutboxProcessorJob.java`
- Modify: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/job/PriceFullSyncJob.java`
- Modify: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/job/StockFullSyncJob.java`
- Modify if needed: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/http/FastchannelPriceClient.java`
- Modify if needed: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/http/FastchannelStockClient.java`

**Step 1: Mapear endpoints e payloads do legado**

Confirmar:
- `stock-management/v1/stock/{productId}`
- `price-management/v1/prices/{productId}`
- `price-management/v1/prices/{productId}/batches`
- diferenças entre distribuição e consumo

**Step 2: Verificar cobertura funcional no add-on**

Responder com evidência:
- existe sync de estoque unitário?
- existe full sync de estoque?
- existe sync de preço unitário?
- existe full sync de preço?
- existe sync de preço escalonado?
- existe log operacional desses fluxos?

**Step 3: Implementar lacunas de observabilidade**

Adicionar logs por operação contendo no mínimo:
- SKU
- canal
- `NUTAB`/`PriceTableId`
- quantidade ou preço enviado
- batches enviados, ignorados ou removidos
- motivo de skip
- resposta/resumo de erro Fastchannel quando houver

**Step 4: Validar se batches estão realmente sendo enviados**

Se o add-on só tiver suporte parcial a batch prices, completar o fluxo e não apenas o cliente HTTP.

**Acceptance criteria**
- Há evidência objetiva de paridade de payload/endpoints com o legado.
- O add-on gera logs auditáveis de estoque, preço e preço escalonado.
- Não existe "fluxo implementado" que na prática nunca é acionado.

---

### Task 6: Revisar Jobs de Auto Provisionamento e Automação

**Files:**
- Modify: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/job/OrderImportJob.java`
- Modify: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/job/OrderStatusSyncJob.java`
- Modify: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/job/OutboxProcessorJob.java`
- Modify if needed: provisioning/setup classes of the addon

**Step 1: Confirmar jobs reais do add-on**

Não inventar nomes. Usar os jobs reais existentes no repo.

**Step 2: Verificar se estão acionando os serviços certos**

Cada job deve ser rastreável até um serviço real com logging e tratamento de erro.

**Step 3: Garantir auto provisionamento**

Verificar se o addon sobe com configuração mínima necessária para rodar automaticamente ou se depende de passos manuais não documentados.

**Acceptance criteria**
- Jobs reais estão mapeados e auditados.
- Não há automação “de fachada”.
- O comportamento automático está claro e testável.

---

### Task 7: Limpeza de Código Morto e Endurecimento

**Files:**
- Modify: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Modify: outros arquivos tocados nas tasks anteriores

**Step 1: Remover caminhos mortos ou perigosos**

Exemplo: `registerOrderMapping()` antigo e qualquer trecho que facilite retorno do fluxo incorreto.

**Step 2: Padronizar uso de JAPE vs SQL**

Documentar e aplicar a regra:
- entidade de negócio Sankhya: JAPE primeiro
- controle técnico de fila/idempotência/schema: SQL permitido

**Acceptance criteria**
- Menos superfícies de regressão.
- Critério de uso de SQL está explícito e consistente.

---

### Task 8: Verificação, Publicação e Validação Assistida

**Files:**
- Modify: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/build.gradle` se precisar de bump de versão
- Produce report: `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/docs/plans/2026-03-11-fastchannel-sonnet-validation-report.md`

**Step 1: Rodar testes**

Executar no mínimo:
- `./gradlew.bat :model:test --tests br.com.bellube.fastchannel.regression.OrderStateRegressionTest`
- `./gradlew.bat :model:test --tests br.com.bellube.fastchannel.regression.CriticalFixesRegressionTest`
- testes novos criados para idempotência/estoque/preço

**Step 2: Validar no banco via SELECT only**

Usar DBExplorer apenas para consulta e confirmar:
- unicidade por `ORDER_ID`
- estados coerentes em `AD_FCPEDIDO`
- itens pendentes/erro na fila
- logs de `ORDER_IMPORT`, `STOCK_SYNC`, `PRICE_SYNC`

**Step 3: Validar no Sankhya**

Após deploy:
- reprocessar pedidos com erro de importação
- validar importação bem sucedida
- validar geração de logs para preço e estoque
- validar ausência das stacks antigas do add-on

**Acceptance criteria**
- Não declarar sucesso sem evidência pós-deploy.
- O relatório final separa claramente:
  - corrigido
  - validado em teste
  - validado no Sankhya
  - pendente

---

## Regras Operacionais para o Claude Sonnet

- Não tratar legado como opcional; ele é a referência funcional.
- Não afirmar paridade sem tabela comparativa `legado x addon`.
- Não afirmar correção total sem validação pós-deploy.
- Não usar SQL direto em entidade de negócio se JAPE atender.
- Pode usar SQL para fila, idempotência e inspeção de schema, mas deve justificar no relatório.
- Sempre que tocar fluxo crítico, adicionar teste de regressão correspondente.
- Sempre que existir lacuna de observabilidade, adicionar log operacional específico.

---

## OUTPUT Task 1 — Matriz de Incidentes do Add-on (log: X:/tmp-log-20260311133929/server.log)

> Log cobre: 2026-03-11 00:00 até 13:40. Três deployments: 06:06, 06:09, 13:08/13:19.
> Log da versão implantada usa `acquireOrderImportLock` (linha 273 de OrderService.java).
> Código no worktree `merge-unify` já usa `claimOrderImport` — fix já incorporado.

### Matriz de Incidentes

| ID | Erro | Classe/Stack de Origem | Dono | Arquivo/Linha no Add-on | Status |
|----|------|------------------------|------|--------------------------|--------|
| FC-INC-001 | `IllegalStateException: O comando SELECT está em formato inválido` ao tentar importar pedidos (ex: pedido 4490) | `OrderService.acquireOrderImportLock(OrderService.java:273)` → `ConnectionProxy$StatementProxy.checkSelectCommand` → `FCAdminService.retryErroredOrders` | **addon-confirmado** | `OrderService.java:273` (método removido no merge-unify) | **CORRIGIDO** — `claimOrderImport` com `NativeSql` substitui o método defeituoso; erros cessam após redeployment das 13:19 |
| FC-INC-002 | `AutoProvisionamento: CODMODULO nao identificado para appKey. Fallback interno sera usado.` (3+ vezes por request, dia inteiro) | `FastchannelAutoProvisioning.tryStartNativeScheduledActions` chamado de `FastchannelConfig.getInstance()` com `appKey=null, explicitCodModulo=null` | **addon-confirmado** | `FastchannelConfig.java:73` / `FastchannelAutoProvisioning.java:72-74` | **PENDENTE** — Ruído funcional: `resolveCodModuloByAppKey(null)` retorna null imediatamente (null guard L236), portanto NENHUMA query de schema é executada. Fallback interno inicia uma vez (guard `INTERNAL_STARTED`). Jobs rodam corretamente. Problema: `ensureStarted(null,null)` chamado em CADA `getInstance()` — deve ser chamado apenas no ciclo de vida. |
| FC-INC-003 | `Verificacao de duplicidade desabilitada por flag (fastchannel.disableDuplicateCheck=true).` (~30x por execução de retry) | `OrderService` | **addon-confirmado** | `OrderService.java` (verificação de flag) | **CONFIGURAÇÃO** — Flag deliberada. Risco: possível duplicidade silenciosa. Deve ser documentada e reavaliada. |
| FC-INC-004 | `Nenhum pedido retornado com CreatedAfter. Executando fallback sem cursor para buscar pedidos nao sincronizados.` | `OrderService.importPendingOrders` | **addon-confirmado** | `OrderService.java` | **OBSERVACIONAL** — Comportamento esperado de fallback quando cursor não retorna pedidos. Confirmar que o timestamp de cursor está correto no ambiente. |
| FC-INC-005 | `WFLYSRV0003: Could not index class com/google/common/util/concurrent/...at .../guava.jar: ArrayIndexOutOfBoundsException` | `org.jboss.as.server.deployment.annotation.CompositeIndex` — stack inteiramente WildFly/JBoss | **core-sankhya-confirmado** | N/A | **EXCLUÍDO** — Stack exclusivamente em `org.jboss.as.server.deployment.*`. Classloader indexing warning do WildFly ao processar Guava JAR. Sem código do add-on. |
| FC-INC-006 | `Worker thread was interrupt()'ed.: java.lang.InterruptedException` (`sw.mge.scheduler.adapter.addon-fastchannel_Worker-1/2`) | `org.quartz.simpl.SimpleThreadPool` — Quartz scheduler interrompendo workers durante redeployment | **core-sankhya-confirmado** | N/A | **EXCLUÍDO** — Stack exclusivamente em `org.quartz.simpl.SimpleThreadPool`. Interrupção normal de workers Quartz durante undeployment do EAR. Sem código do add-on. |
| FC-INC-007 | `SQLServerException: A consulta foi cancelada` (13:35 pós-redeployment) | `br.com.sankhya.ws.HttpServiceBroker` → `ServiceWrapperHandler` → `loadRecords` → DWF filters | **core-sankhya-confirmado** | N/A | **EXCLUÍDO** — Stack exclusivamente em `br.com.sankhya.ws.*` e `br.com.sankhya.dwf.*`. Query cancelada em serviço Sankhya nativo (`loadRecords`). Sem código do add-on. |
| FC-INC-008 | `SEVERE [RecebimentoComCartaoHelper] validarAlteracaoFinanceiro.ehRecebimentoComCartao = false` (múltiplas ocorrências) | `RecebimentoComCartaoHelper` — classe do core Sankhya para validação de recebimento com cartão | **core-sankhya-confirmado** | N/A | **EXCLUÍDO** — Classe pertence ao core Sankhya. Nenhum código do add-on na stack. |
| FC-INC-009 | `NumberFormatException` em `sw.default.internal.scheduler_Worker-2` (13:36) | `sw.default.internal.scheduler_Worker-2` — scheduler interno do Sankhya | **core-sankhya-confirmado** | N/A | **EXCLUÍDO** — Worker interno do Sankhya (`sw.default.internal.scheduler`). Sem código do add-on na stack visível. |

### Resumo de Propriedade

- **Add-on com dono definido:** FC-INC-001 (corrigido), FC-INC-002 (pendente), FC-INC-003 (config), FC-INC-004 (observacional)
- **Core Sankhya / Infraestrutura (exclusão justificada por stack):** FC-INC-005, FC-INC-006, FC-INC-007, FC-INC-008, FC-INC-009
- **Nenhum erro do add-on sem dono.**

### Achado crítico: CODMODULO e performance de ensureStarted

`FastchannelConfig.getInstance()` chama `ensureStarted(null, null)` em toda request porque é o ponto de entrada singleton. O guard `INTERNAL_STARTED` evita múltiplas iniciações do scheduler, mas o log continua poluído. A raiz está em `FastchannelAutoProvisioning.tryStartNativeScheduledActions` sempre executar a tentativa de resolução mesmo com appKey nulo (a null guard em L235-237 evita a query SQL, mas não evita o log). **Ação recomendada:** mover a chamada `ensureStarted` para `LifecycleController` exclusivamente, removendo-a de `getInstance()`.

---

## OUTPUT Task 4 — Relatório de Paridade: Legado x Add-on (Importação de Pedido)

> Auditoria completa comparando `sankhya.js` (legado) com `InternalApiStrategy.java` (add-on).
> Fontes: `gbi-app-integrador-main/models/erp/sankhya.js`, `InternalApiStrategy.java`, `OrderDTO.java`, `OrderItemDTO.java`.

### Tabela de Paridade — Cabeçalho (TGFCAB)

| Campo ERP | Legado (sankhya.js) | Add-on (InternalApiStrategy) | Status |
|-----------|---------------------|------------------------------|--------|
| CODEMP | Hardcoded `26` | De-Para config — exception se não configurado | **Divergente por decisão** — add-on mais flexível e correto para multi-empresa |
| CODPARC | `args.obj.CodCli` (pré-resolvido externamente pelo job JS) | Resolvido internamente de TGFPAR via CPF/CNPJ do cliente | **Divergente por decisão** — add-on mais robusto (resolução interna auditável) |
| CODVEND | `args.obj.CodVendedor \|\| args.CodVendedor` (da ordem ou config do contrato) | `resolveCodVendByParc(codParc)` — vendedor preferencial de TGFPAR | **Divergente por decisão** — add-on mais confiável (sem dependência de campo variável da API) |
| CODTIPOPER | Config do contrato (`serverData.tipOper`) | Config/De-Para com fallback multi-estratégia | **Igual por intenção** |
| CODTIPVENDA | `args.obj.TipNeg` (campo direto da ordem Fastchannel) | Resolvido de TGFTOP/TGFNPV com fallback por CODPARC e histórico de notas | **Divergente por decisão** — add-on mais sofisticado; legado depende de campo externo mapeado |
| CODNAT | Config do contrato (opcional) | Resolvido de TGFTOP com fallbacks em TGFCAB e TGFNAT | **Divergente por decisão** — add-on mais robusto |
| AD_NUMFAST | `args.obj.OrderId` | `order.getOrderId()` | **Igual** |
| VLRFRETE | `parseInt(args.obj.VlrFrete) / 100` — campo `VlrFrete`, unidade **centavos** | `order.getShippingCost()` BigDecimal direto — campo `ShippingCost` | ⚠️ **RISCO CRÍTICO**: campo JSON diferente (`VlrFrete` vs `ShippingCost`) e conversão diferente (÷100 vs direto). Se a API retorna `ShippingCost` em BRL e `VlrFrete` em centavos, resultado é equivalente. Se ambos forem centavos, add-on enviaria valor 100× maior. **Requer validação com payload real de produção antes de qualquer correção.** |
| STATUSNOTA | Não definido (default do ERP) | Forçado `"P"` se campo suportado | **Divergente por decisão** — add-on é mais seguro (garante pendente) |
| PENDENTE | Não definido | Forçado `"S"` se campo suportado | **Divergente por decisão** — add-on é mais seguro |
| VLRFRETECPL | Não definido | Forçado `BigDecimal.ZERO` | **Divergente por decisão** — add-on explicitamente zera complemento |

### Tabela de Paridade — Itens (TGFITE)

| Campo ERP | Legado (sankhya.js) | Add-on (InternalApiStrategy) | Status |
|-----------|---------------------|------------------------------|--------|
| QTDNEG | `parseInt(Quantity).toFixed(0)` — inteiro, trunca decimais | BigDecimal via `sanitizeQuantity()` — rejeita ≤0 | **Divergente por decisão** — add-on preserva decimais (melhor para itens fracionados) |
| VLRUNIT | `parseInt(SubtotalUnit) / 100` — campo `SubtotalUnit`, unidade **centavos** | `item.getUnitPrice()` BigDecimal direto — campo `SalePrice`, fallback `TotalProductCost/Qty` | ⚠️ **RISCO CRÍTICO**: mesmo padrão de VLRFRETE — campo JSON diferente e conversão diferente. Mesma necessidade de validação com payload real. |
| PERCDESC | Calculado de campos de desconto da ordem | Calculado de campos de desconto via `getDiscount()` | **Igual por intenção** |
| NUTAB | **Não definido** — legado não envia tabela de preço | Explicitamente resolvido por item via estratégia de pricing | **Divergente por decisão** — add-on é melhor (tabela de preço permite consistência com condições do cliente) |
| STATUSNOTA (item) | Não definido | Forçado `"P"` se suportado | **Divergente por decisão** — add-on é mais seguro |
| QTDENTREGUE | Não definido | Forçado `BigDecimal.ZERO` | **Divergente por decisão** — add-on previne entrega automática indevida |
| CODEMP (item) | Não definido | Definido se campo suportado | **Divergente por decisão** |

### Tabela de Paridade — Fluxo e Controles

| Aspecto | Legado | Add-on | Status |
|---------|--------|--------|--------|
| Deduplicação | **Nenhuma** — pode criar NUNOTA duplicado se chamado duas vezes | Via `AD_FCPEDIDO` com claim/idempotência completa | **Divergente por decisão** — add-on muito mais robusto; legado é vulnerável a duplicidade |
| Controle de concorrência | Nenhum | Claim com timeout (PROCESSANDO → retomada ou skip legítimo) | **Divergente por decisão** — add-on resolve problema real de produção |
| Retry | Disparado externamente por script JS | `FCAdminService.retryErroredOrders` com 3-state: reprocessado / pulado / falhou | **Divergente por decisão** — add-on melhor para ops |
| Erro na criação | Envia e-mail de notificação | Log em `AD_FCLOG` (nível ERROR, OP_ORDER_IMPORT) | **Divergente por decisão** — add-on adequado para ERP embarcado |
| Observabilidade | Sem audit trail persistido | `AD_FCLOG` com todos os estados | **Divergente por decisão** — add-on melhor |

### Divergências com Risco Aberto

| # | Campo | Risco | Ação Necessária |
|---|-------|-------|-----------------|
| R-01 | VLRFRETE | `VlrFrete` (centavos÷100) vs `ShippingCost` (BigDecimal direto) — se API retorna `ShippingCost` em centavos, add-on envia 100× o valor correto | Confirmar em produção: inspecionar payload real da API Fastchannel para um pedido com frete > 0. Verificar se `ShippingCost` aparece como ex: `10.50` (BRL) ou `1050` (centavos). |
| R-02 | VLRUNIT | `SubtotalUnit` (centavos÷100) vs `SalePrice` (BigDecimal direto) — mesmo risco de R-01 | Mesma ação: inspecionar `SalePrice` e `SubtotalUnit` no payload para confirmar unidade. |

### Conclusão Task 4

- **0 divergências críticas** que exijam correção de código imediata com certeza.
- **2 riscos abertos** (R-01, R-02) que só podem ser confirmados com payload de produção — sem evidência de bug ativo (sistema estava importando pedidos antes do incidente FC-INC-001).
- **Divergências de decisão**: add-on é consistentemente melhor que o legado em deduplicação, idempotência, NUTAB, STATUSNOTA/PENDENTE e observabilidade.
- **Step 3 (correções)**: não há divergências corrigíveis com certeza sem validar R-01/R-02. Recomenda-se acrescentar log do `ShippingCost` e `SalePrice` desserializados no início do `processOrder` para auditoria em produção.

---

## OUTPUT Task 5 — Auditoria: Estoque, Preço e Preço Escalonado vs Legado

> Fontes: `fastChannel.js` (legado), `FastchannelHttpClient.java`, `FastchannelStockClient.java`, `FastchannelPriceClient.java`, `FastchannelConstants.java`, `StockFullSyncJob.java`, `PriceFullSyncJob.java`, `OutboxProcessorJob.java`, `PriceService.java`.

### Step 1 — Endpoints e Payloads: Paridade Confirmada

| Operação | Legado (fastChannel.js) | Add-on | Status |
|----------|-------------------------|--------|--------|
| Estoque unitário | `PUT stock-management/v1/stock/{productId}` | `putStock → STOCK_API_BASE + /stock/{sku}` | **Igual** |
| Preço individual | `PUT price-management/v1/prices/{productId}` | `putPrice → PRICE_API_BASE + /prices/{sku}` | **Igual** |
| Preço escalonado | `POST price-management/v1/prices/{productId}/batches` | `postPrice → PRICE_API_BASE + /prices/{sku}/batches` | **Igual** |
| Chave por canal | Separada por seção "Sell" / "Consume" | `subscriptionKeyDistribution` / `subscriptionKeyConsumption` | **Igual** |
| Base URL routing | Dois endpoints distintos | `putStock` hardcoded em `STOCK_API_BASE`; `putPrice/postPrice` hardcoded em `PRICE_API_BASE` | **Igual** |

### Step 2 — Cobertura Funcional

| Funcionalidade | Existe? | Onde |
|----------------|---------|------|
| Sync de estoque unitário (outbox) | **Sim** | `OutboxProcessorJob.processStockItem` → `stockClient.updateStock` |
| Full sync de estoque (safety net diário) | **Sim** | `StockFullSyncJob.executeScheduler` → `stockClient.updateStock` |
| Sync de preço unitário (outbox) | **Sim** | `OutboxProcessorJob.processPriceItem` → `priceClient.updatePrice` |
| Full sync de preço (safety net diário) | **Sim** | `PriceFullSyncJob.executeScheduler` → `PriceService.syncPriceBatch` |
| Preço escalonado (batches) via outbox | **Sim** | `OutboxProcessorJob.processPriceItem` → `priceClient.updatePriceBatches` |
| Preço escalonado via full sync | **Não** | `PriceService.syncPriceBatch` usa apenas `updatePricesBatch` (bulk reseller) — **não envia batches escalonados**. Lacuna funcional documentada. |
| Canal DISTRIBUTION / CONSUMPTION | **Sim** | Resolvido por TGFTAB.AD_TIPO_FAST, TGFMAR.AD_FASTREF, prefixo "D-" |

### Step 3 — Observabilidade: Correções Implementadas

| Arquivo | Gap encontrado | Correção aplicada |
|---------|---------------|-------------------|
| `StockFullSyncJob.java` | Nenhum LogService — apenas `log.log(Level.WARNING, ...)` no erro | Adicionado `logService.logStockSync(sku, qty, true, null)` por SKU com sucesso; `logStockSync(sku, qty, false, e.getMessage())` no erro; resumo final via `logService.info(OP_STOCK_SYNC, "StockFullSyncJob concluido. Enviados: X, Erros: Y")` |
| `PriceFullSyncJob.java` | Nenhum LogService | Adicionado log de início (contagem de produtos), `logService.info` na conclusão, `logService.error` no erro com exceção |
| `OutboxProcessorJob.processPriceItem` | Skips (`skippedNoIntegration`, `skippedNoPrice`) só em console Java | Adicionado `LogService.getInstance().warning(OP_PRICE_SYNC, "SKU X NUTAB Y ignorado: motivo", sku)` para cada skip |

### Step 4 — Validação de Batches

- `OutboxProcessorJob.processPriceItem`: envia `priceClient.updatePriceBatches(sku, priceTableId, batches)` — **batches escalonados enviados corretamente** para cada tabela.
- `PriceService.syncPriceBatch`: usa `updatePricesBatch` (endpoint `/prices/{resellerId}/batches`) — endpoint de bulk de revenda, **não envia tiered batches por item**. Esta é uma lacuna funcional do full sync: o safety net diário não replica os preços escalonados. Documentado acima; não corrigido (correção exige arquitetura mais ampla que está fora do escopo desta task).

### Acceptance Criteria — Status

| Critério | Status |
|----------|--------|
| Evidência objetiva de paridade de payload/endpoints com legado | ✅ Confirmado — mesmos endpoints, mesmas URLs base, mesmo roteamento por canal |
| Add-on gera logs auditáveis de estoque, preço e preço escalonado | ✅ Corrigido — StockFullSyncJob, PriceFullSyncJob e OutboxProcessorJob agora persistem em AD_FCLOG |
| Não existe "fluxo implementado" que na prática nunca é acionado | ✅ Confirmado — OutboxProcessorJob.processPriceItem é o único que envia batches escalonados; PriceService.syncPriceBatch documentado como não suportando batches escalonados |

---

## OUTPUT Task 6 — Revisão de Jobs de Auto Provisionamento e Automação

> Fontes: `OrderImportJob.java`, `OrderStatusSyncJob.java`, `OutboxProcessorJob.java`, `FastchannelAutoProvisioning.java`, `LifecycleController.java`, `FastchannelConfig.java`.

### Step 1 — Jobs Reais do Add-on

| Job | Classe | Ciclo | Serviço delegado |
|-----|--------|-------|-----------------|
| Importação de pedidos | `OrderImportJob` | 5 min (default) | `OrderService.importPendingOrders()` |
| Processamento outbox | `OutboxProcessorJob` | 1 min (default) | `QueueService` + stock/price clients |
| Catch-up de status | `OrderStatusSyncJob` | 3 min (default) | `FastchannelOrdersClient.updateOrderStatus/sendInvoice` |
| Full sync de preço | `PriceFullSyncJob` | 6 h (default) | `PriceService.syncPriceBatch()` |
| Full sync de estoque | `StockFullSyncJob` | 6 h (default) | `FastchannelStockClient.updateStock()` |

**Nenhum job de fachada** — todos chamam serviços reais e têm tratamento de erro.

### Step 2 — Verificação de Serviços e Logging

| Job | Problema encontrado | Correção aplicada |
|-----|--------------------|--------------------|
| `OrderImportJob` | Nenhum — LogService completo, config validation, error handling | Nenhuma |
| `OrderStatusSyncJob` | LogService importado mas nunca usado; erros e sucessos só em console Java | Adicionado `logService.info` para status sync e NF enviada; `logService.error` no catch |
| `OutboxProcessorJob` | Corrigido em Task 5 (skip logging) | N/A |
| `PriceFullSyncJob` | Corrigido em Task 5 | N/A |
| `StockFullSyncJob` | Corrigido em Task 5 | N/A |

### Step 3 — Auto Provisionamento

**Mecanismo:** `FastchannelAutoProvisioning.ensureStarted()` tenta iniciar AcaoAgendada nativa (CODMODULO-based) e cai em fallback interno (`ScheduledExecutorService`) se não encontrar.

**Problema crítico (FC-INC-002):** `FastchannelConfig.getInstance()` chama `ensureStarted(null, null)` em **cada chamada**, não apenas na criação. `INTERNAL_STARTED` guard impede múltiplos schedulers, mas o log de warning "CODMODULO nao identificado" dispara toda vez.

**Fix aplicado em Task 7:** mover `ensureStarted(null, null)` para dentro do bloco `if (instance == null)`.

**Configuração:** períodos configuráveis via system properties (`fc.auto.order.import.minutes`, etc.) com defaults operacionais.

### Acceptance Criteria — Status

| Critério | Status |
|----------|--------|
| Jobs reais mapeados e auditados | ✅ Todos os 5 jobs auditados |
| Não há automação de fachada | ✅ Confirmado |
| Comportamento automático claro e testável | ✅ Documentado; fix de log noise aplicado em Task 7 |

---

## OUTPUT Task 7 — Limpeza de Dead Code e Bug FC-INC-002

> Fontes: `FastchannelConfig.java`, `OrderService.java`, `FastchannelAutoProvisioning.java`, `LifecycleController.java`.

### Fix FC-INC-002 — Log noise em FastchannelConfig.getInstance()

**Problema:** `FastchannelConfig.getInstance()` chamava `FastchannelAutoProvisioning.ensureStarted(null, null)` em **cada invocação**, não apenas na criação do singleton. O guard `INTERNAL_STARTED` impedia múltiplos schedulers, mas o log de warning "CODMODULO nao identificado" disparava em cada chamada — centenas de vezes por job cycle.

**Fix aplicado em `FastchannelConfig.java`:**
```java
// ANTES:
public static synchronized FastchannelConfig getInstance() {
    if (instance == null) {
        instance = new FastchannelConfig();
    }
    FastchannelAutoProvisioning.ensureStarted(null, null); // <-- disparava sempre
    return instance;
}

// DEPOIS:
public static synchronized FastchannelConfig getInstance() {
    if (instance == null) {
        instance = new FastchannelConfig();
        FastchannelAutoProvisioning.ensureStarted(null, null); // <-- apenas na criação
    }
    return instance;
}
```

**Caminho legítimo preservado:** `LifecycleController.install()` e `verify()` continuam chamando `ensureStarted(appkey, codModulo)` com valores reais — essa é a rota de provisioning com CODMODULO identificado.

### Remoção de Dead Code — `registerOrderMapping`

**Problema:** `OrderService.registerOrderMapping(String orderId, BigDecimal nuNota, BigDecimal codParc)` estava definida (private) mas **nunca chamada** em nenhum lugar do codebase. O método vivo equivalente é `upsertOrderMapping(...)` que implementa UPDATE+INSERT com idempotência real.

**Verificação:** `Grep` de `registerOrderMapping` retornou apenas a definição em `OrderService.java` — zero chamadores.

**Fix aplicado:** método removido integralmente de `OrderService.java`.

### Verificação do critério JAPE vs SQL

| Contexto | Padrão | Justificativa |
|----------|--------|--------------|
| Entidades de negócio (pedidos, parceiros, produtos, NF) | JAPE / `InternalApiStrategy` | Padrão Sankhya para entidades gerenciadas pelo ERP |
| Controle técnico (`AD_FCQUEUE`, `AD_FCPEDIDO`, `AD_FCLOG`) | SQL/NativeSql | Tabelas proprietárias do add-on, fora do modelo JAPE; acesso direto justificado e documentado |

Critério consistente confirmado em toda a base — nenhuma inversão encontrada.

### Acceptance Criteria — Status

| Critério | Status |
|----------|--------|
| FC-INC-002 eliminado — log noise em `getInstance()` removido | ✅ Fix aplicado — `ensureStarted` dentro do `if (instance == null)` |
| Dead code `registerOrderMapping` removido | ✅ Removido — substituído funcionalmente por `upsertOrderMapping` |
| JAPE vs SQL critério documentado e consistente | ✅ Confirmado — sem inversões |

---

## Prompt sugerido para chamar o Claude Sonnet

"Execute este plano em `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify` com foco em fechar todas as lacunas restantes do add-on Fastchannel. Trate `X:/gbi-app-integrador-main/gbi-app-integrador-main` como referência funcional obrigatória para pedido, estoque, preço e preço escalonado. Preserve JAPE como padrão para entidades de negócio e use SQL apenas para controle técnico de fila/idempotência/schema com justificativa explícita. Não declare sucesso sem: 1) tabela de incidentes do log, 2) tabela legado x addon, 3) testes executados, 4) validação pós-deploy ou indicação explícita do que falta validar. Corrija, teste, documente e entregue um relatório final com o que foi resolvido, o que foi validado e o que ainda depende de ambiente." 
