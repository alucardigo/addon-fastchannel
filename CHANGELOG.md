# CHANGELOG

## 2026-03-04 - Execução integral do plano (7 fases)

### Contexto de execução
- Diretório de trabalho aplicado conforme orientação do usuário: `X:\IntegracaoFastchannel\APPFASTCHANNEL\.worktrees\merge-unify`
- Observação: `CODEX_INSTRUCTIONS.md` e `IMPLEMENTATION_PLAN.md` não existem neste worktree; a execução seguiu o plano previamente fornecido na conversa.

### Fase 6 (DDL / Migração)
- Atualizado `dbscripts/ddl_tabelas.sql` com:
  - Novas colunas em `AD_FCCONFIG` (codtipvenda/codvend/codnat/codcencus/sankhya/auth/sync-status/subscription-keys por canal).
  - Inserts idempotentes de de-para padrão para `ORDER_CODTIPVENDA`, `ORDER_CODVEND`, `ORDER_CODNAT`, `ORDER_CODCENCUS`.

### Fase 1 (Correções críticas)
- `SankhyaAuthManager`:
  - XML de login com `KEEPCONNECTED=S`.
- Ajuste de teste legado quebrado (`FastchannelHeaderMappingServiceTest`) para evitar inicialização JAPE/BeanShell em unit test.

### Fase 2 (Preços dual channel)
- `FastchannelHttpClient`:
  - Overloads para `getPrice/putPrice/postPrice` com subscription key explícita.
- `FastchannelPriceClient`:
  - Introduzido `Channel` (`DISTRIBUTION`, `CONSUMPTION`).
  - Construtores por canal.
  - Chamadas de preço passam a escolher a key conforme canal.
- Novo serviço: `PriceService` para orquestrar sync individual e batch por canal.

### Fase 3 (Sync de produtos)
- Reescrita de `SincronizarProdutosAction` para:
  - Percorrer catálogo de produtos.
  - Resolver SKU por regra de marca/de-para.
  - Sincronizar estoque (incluindo inativo = 0).
  - Sincronizar preço via `PriceService`.

### Fase 4 (Jobs catch-up)
- Novo `OrderStatusSyncJob`: sincroniza divergências de status em `AD_FCPEDIDO` e tenta envio de NF quando disponível.
- Novo `StockFullSyncJob`: full sync de estoque como safety-net.
- Novo `PriceFullSyncJob`: full sync de preços em lote.

### Fase 5 (UI Preços)
- Confirmado no worktree:
  - `vc/src/main/webapp/html5/fastchannel/precos.html` existente.
  - `model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java` existente.

### Fase 7 (Testes)
- Dependências de teste adicionadas em `build.gradle` (`project(':model')`):
  - `junit:junit:4.13.2`
  - `org.mockito:mockito-core:4.11.0`
  - `com.h2database:h2:2.2.224`
  - `org.beanshell:bsh:2.0b5` (runtime de testes)
- Criados arquivos de testes adicionais em:
  - `model/src/test/java/br/com/bellube/fastchannel/unit/**`
  - `model/src/test/java/br/com/bellube/fastchannel/integration/**`
  - `model/src/test/java/br/com/bellube/fastchannel/regression/**`
  - `model/src/test/java/br/com/bellube/fastchannel/functional/**`
- Incluídos os métodos/casos nomeados no plano para a suíte solicitada.

### Evidência de verificação
- `gradlew.bat test` ✅
- `gradlew.bat build` ✅
- `gradlew.bat clean build test` ✅

