# Matriz Legado x Add-on x Status x Evidência

## Escopo

Comparativo consolidado entre o legado em `X:/gbi-app-integrador-main/gbi-app-integrador-main` e o add-on atual em `X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify`, com foco em:
- importação de pedidos
- estoque
- preço unitário
- preço escalonado
- observabilidade
- auto-provisionamento

Critério de status:
- `OK`: paridade ou comportamento equivalente comprovado no código
- `PARCIAL`: existe implementação, mas com lacuna funcional ou evidência insuficiente
- `DIVERGENTE`: comportamento do add-on não replica o legado
- `SEM EVIDÊNCIA`: o relatório afirma, mas a validação atual não comprovou

## Matriz

| Área | Comportamento no legado | Estado no add-on | Status | Evidência |
|------|--------------------------|------------------|--------|-----------|
| Importação de pedidos | Busca pedidos usando `IgnoreCreationDate=true` | `FastchannelOrdersClient` não envia `IgnoreCreationDate` | DIVERGENTE | [`FastchannelOrdersClient.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/http/FastchannelOrdersClient.java), [`fastChannel.js`](/X:/gbi-app-integrador-main/gbi-app-integrador-main/controllers/api/data/integration/fastChannel.js) |
| Importação de pedidos | Filtra pedidos com `CurrentStatusTypeId != 2` | DTO e fluxo atual não implementam esse filtro | DIVERGENTE | [`OrderDTO.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/dto/OrderDTO.java), [`OrderService.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java), [`pedidos.js`](/X:/gbi-app-integrador-main/gbi-app-integrador-main/controllers/api/data/integration/pedidos.js) |
| Importação de pedidos | Usa TOP/configuração legada para roteamento do pedido | `FastchannelHeaderMappingService` e fallbacks forçam `403` em caminhos críticos | DIVERGENTE | [`FastchannelHeaderMappingService.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/service/FastchannelHeaderMappingService.java), [`InternalApiStrategy.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java), [`OrderXmlBuilder.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/service/OrderXmlBuilder.java) |
| Importação de pedidos | Parceiro deve ser o correto do pedido | `resolveFallbackCodParc()` pega o primeiro cliente da base | DIVERGENTE | [`OrderService.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java) |
| Importação de pedidos | Evita duplicidade | Claim idempotente por `AD_FCPEDIDO.ORDER_ID` existe | PARCIAL | Código em [`OrderService.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java), mas cobertura real ainda insuficiente |
| Importação de pedidos | Reprocessamento distingue falha x em andamento | `FCAdminService` agora loga skip de retry | PARCIAL | [`FCAdminService.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/web/FCAdminService.java), porém resposta final ainda pode mascarar retries falhos/pulados |
| Estoque incremental | Envia `StorageId`, `StorageName`, `ResellerId`, `ResellerName` | Pipeline de fila preserva `storageId/resellerId` e cliente suporta payload compatível | OK | [`OutboxProcessorJob.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/job/OutboxProcessorJob.java), [`FastchannelStockClient.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/http/FastchannelStockClient.java), [`Produtos.js`](/X:/gbi-app-integrador-main/gbi-app-integrador-main/models/mssqlModels/sankhya/Produtos.js) |
| Estoque full sync | Granularidade equivalente a storage/reseller do legado | Full sync usa `codemp/codLocal` globais e `updateStock(sku, qty)` | PARCIAL | [`StockFullSyncJob.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/job/StockFullSyncJob.java) |
| Preço unitário | Atualiza preço por SKU/canal | Add-on implementa distribuição e consumo com `FastchannelPriceClient` | OK | [`PriceService.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/service/PriceService.java), [`FastchannelPriceClient.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/http/FastchannelPriceClient.java) |
| Preço escalonado | Reconcilia batches remotos: lista, remove obsoletos, cria válidos | Add-on envia batches atuais por POST, sem reconciliação completa | DIVERGENTE | [`OutboxProcessorJob.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/job/OutboxProcessorJob.java), [`FastchannelPriceClient.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/http/FastchannelPriceClient.java), [`fastChannel.js`](/X:/gbi-app-integrador-main/gbi-app-integrador-main/controllers/api/data/integration/fastChannel.js) |
| Logs de pedido | Legado gera trilha operacional por contexto | Add-on agora gera `ORDER_IMPORT` e retry skip em `AD_FCLOG` | PARCIAL | [`LogService.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/service/LogService.java), falta validação em ambiente |
| Logs de estoque | Observabilidade por SKU | `StockFullSyncJob` e outbox agora registram sucesso/erro por SKU | PARCIAL | [`StockFullSyncJob.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/job/StockFullSyncJob.java), [`OutboxProcessorJob.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/job/OutboxProcessorJob.java) |
| Logs de preço | Observabilidade por SKU/NUTAB | Outbox e full sync agora têm logs, inclusive skips | PARCIAL | [`OutboxProcessorJob.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/job/OutboxProcessorJob.java), [`PriceFullSyncJob.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/job/PriceFullSyncJob.java) |
| Auto-provisionamento | Subida automática sem ruído excessivo | `ensureStarted(null, null)` foi movido para a criação do singleton | PARCIAL | [`FastchannelConfig.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/main/java/br/com/bellube/fastchannel/config/FastchannelConfig.java), ainda falta validar log real/WildFly |
| Cobertura de testes | Evidência funcional deve sustentar claims | Relatório fala em 164 testes; execução local comprovada nesta revisão mostrou apenas 33 testes XML gerados | SEM EVIDÊNCIA | `model/buildGradle/test-results/test/*.xml` |
| Idempotência/concurrency | Deve provar comportamento em runtime | Teste novo continua sendo inspeção de fonte | SEM EVIDÊNCIA | [`OrderServiceIdempotencyTest.java`](/X:/IntegracaoFastchannel/APPFASTCHANNEL/.worktrees/merge-unify/model/src/test/java/br/com/bellube/fastchannel/service/OrderServiceIdempotencyTest.java) |

## Conclusão

### O que está comprovadamente melhor
- remoção da causa raiz do `SELECT inválido` no `ORDER_IMPORT`
- incremento de observabilidade em jobs de estoque, preço e status de pedido
- retry administrativo com log explícito para skip por concorrência
- auto-provisionamento menos ruidoso

### O que impede aprovação de publish agora
1. `TOP`/roteamento de cabeçalho com override indevido para `403`
2. fallback de parceiro inseguro (`primeiro cliente da base`)
3. coleta de pedidos sem `IgnoreCreationDate=true`
4. ausência do filtro `CurrentStatusTypeId != 2`
5. preço escalonado sem reconciliação equivalente ao legado
6. full sync de estoque sem prova de paridade por storage/reseller
7. relatório superdeclara cobertura de testes
8. testes de idempotência ainda não validam comportamento real

## Uso recomendado

Esse arquivo deve ser tratado como base objetiva para o próximo ciclo de correção. O publish só deve ser reconsiderado depois que os itens `DIVERGENTE` e `SEM EVIDÊNCIA` forem resolvidos ou reclassificados com validação de ambiente.
