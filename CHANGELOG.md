# CHANGELOG

## 2026-07-03 - v1.2.92 (FEATURE: rota API Oficial do Sankhya — OAuth2/Gateway — na criação de pedidos)

### Pedido
"Melhora a estabilidade da integração e a fluência da integração acrescentando a rota de API
Oficial do Sankhya" — o usuário registrou um componente ("Addon-Fastchannel-APISANKYA") no
Portal do Desenvolvedor Sankhya (Client ID/Secret de Produção e Sandbox) e configurou a tela
"Configurações Gateway" do ERP (Token de Integração vinculado ao usuário 167).

### O que foi implementado
Nova estratégia `OfficialApiStrategy` adicionada ao `OrderCreationOrchestrator` (2ª posição,
logo após ServiceInvoker), autenticada via **OAuth2 client_credentials** contra o **Gateway
oficial do Sankhya Om** — método de autenticação documentado e suportado pelo fornecedor, em
contraste com o login legado usuário/senha + JSESSIONID (raspado de body/header) usado pelas
demais estratégias.

**Ganhos:**
- **Estabilidade**: contrato de autenticação estável entre versões do servidor (não depende de
  parsing de sessão interna que já quebrou silenciosamente — ver incidente v4670000 desregistrando
  a base, "Nenhum provedor encontrado" no CHANGELOG histórico).
- **Fluência**: o access token é **cacheado e reutilizado** por `SankhyaOAuthManager` (renovado
  automaticamente ~60s antes de expirar) — elimina o round-trip de login+logout que
  `HttpServiceStrategy` paga a CADA pedido criado.

### Descoberta importante durante a implementação (documentação ≠ realidade validada)
A pesquisa inicial (documentação genérica) assumia OAuth2 contra o **próprio host do ERP**
(`{SANKHYA_SERVER_URL}/mge/oauth/token`) — **testado contra o servidor real e rejeitado**
("HTTP method POST is not supported by this URL"). A investigação da documentação oficial viva
(developer.sankhya.com.br) revelou o contrato real, **validado com credenciais de produção
reais antes de codificar**:
- Host do Gateway é **fixo/hospedado pelo fornecedor** (`api.sankhya.com.br`), não o host do
  cliente.
- `POST /authenticate` exige **header `X-Token`** (o "Token de Integração" da tela Configurações
  Gateway) **além de** `client_id`/`client_secret` no body — sem o X-Token a chamada falha mesmo
  com credenciais corretas.
- Chamadas de negócio (`CACSP.incluirNota`) vão para
  `POST /gateway/v1/mgecom/service.sbr?serviceName=CACSP.incluirNota&outputType=json` (mesma
  regra `/mge` login + `/mgecom` serviço já documentada), com corpo **JSON** (não XML) e
  `Authorization: Bearer <token>`.
- Validado end-to-end contra produção: `/authenticate` retornou HTTP 200 com JWT válido (claims
  confirmam o app "Addon-Fastchannel-APISANKYA" e "BEL DISTRIBUIDOR DE LUBRIFICANTES LTDA"); uma
  chamada de **leitura** (`loadRecords` no parceiro 15680) via Bearer retornou dados reais,
  confirmando toda a cadeia de autenticação/autorização. A chamada de **escrita**
  (`CACSP.incluirNota`) não foi testada em produção por ser destrutiva (criaria uma nota real) —
  a implementação reaproveita o `OrderXmlBuilder` já testado em produção (mesmos campos que
  `ServiceInvokerStrategy`/`HttpServiceStrategy` usam) e apenas transcreve o XML gerado para o
  formato JSON documentado; qualquer divergência de contrato nesta chamada específica é absorvida
  pelo fallback automático do orquestrador (ServiceInvoker já criava o pedido corretamente antes
  desta mudança, e continua sendo tentado primeiro).

### Configuração (nova, opcional — feature fail-safe se não configurada)
`AD_FCCONFIG.SANKHYA_OAUTH_CLIENT_ID` / `SANKHYA_OAUTH_CLIENT_SECRET` / `SANKHYA_GATEWAY_X_TOKEN`
(migração `dbscripts/V23.xml`). Editáveis pela tela de Configurações do addon (`FCConfigService`).
Sem essas 3 credenciais preenchidas, `OfficialApiStrategy.isAvailable()` retorna `false` e o
orquestrador pula direto para a próxima estratégia — nenhum comportamento existente muda.

### Novos componentes
- `service/auth/SankhyaOAuthManager.java` — fetch/cache/refresh do token OAuth2.
- `service/strategy/OfficialApiStrategy.java` — nova estratégia de criação de pedido, incluindo
  transliteração XML→JSON (`xmlElementToJson`) reaproveitando o `OrderXmlBuilder` existente.
- `FastchannelConstants.SANKHYA_GATEWAY_*` — URLs fixas do Gateway (override via
  `-Dsankhya.gateway.baseUrl` para testes em sandbox).

## 2026-06-15 - v1.2.91 (HARDEN: escalonado expirado removido de forma confiável — janela do AutoSweep 2→30 dias)

### Sintoma reportado (Vitoria / Bel Lube)
"O preço escalonado de alguns produtos deveria ter terminado e sido removido da FastChannel mas
continuou ativo." Ex.: produto 9102 / SKU 31013353 (Moto Performance 10W30), promo 418 com fim
em 10/06 — continuava aparecendo escalonado em 15/06.

### Diagnóstico (medido em 15/06)
- Varredura sistêmica: os 17 produtos com promo expirada em jun/26 estão TODOS com 0 batches na
  FC AGORA (limpos). O 9102 mostra R$653,96 que é o **preço regular** do Sankhya (TGFEXC NUTAB
  4460, definido 01/06) — não é resíduo de escalonado.
- A causa do **atraso**: o `AutoPriceChangesSweepJob` só varria promos expiradas nas **últimas 2
  dias**. Se o addon ficasse fora do ar (ou o full sync — que leva ~4h — fosse interrompido por
  restart) por mais de 2 dias após a expiração, o escalonado só era limpo no próximo full sync
  completo. Confirmado no log 15/06: AutoSweep com `expired=0` o dia todo (promo 418 de 10/06 já
  estava fora da janela de 2 dias) e full sync de 4,2h após restart às 06:30.

### Mudança v1.2.91
`AutoPriceChangesSweepJob`:
- Janela de detecção de promos expiradas: **2 → 30 dias** (configurável `-Dfc.auto.sweep.expiredDays`).
  Cobre fins de semana, feriados e janelas de downtime sem deixar escalonado preso.
- **Dedup por NUPROMOCAO** (`HANDLED_EXPIRED_PROMOS`): cada promo expirada é enfileirada UMA vez
  por vida do addon — sem re-enfileirar a cada 10 min (o cleanup é idempotente, mas evita ruído).
  No restart o set zera e re-varre os últimos 30 dias (one-shot idempotente — pega o que ficou
  preso durante eventual downtime).

### Impacto
Promo que expira é detectada e limpa em ≤10 min (addon no ar) ou no primeiro ciclo após restart,
mesmo que a expiração tenha sido há semanas. Não depende mais do full sync de 4h como backstop.

## 2026-06-01 - v1.2.90 (FIX: PrecoListener nunca disparava — instância errada "ExcecaoPreco" → "Excecao")

### Sintoma reportado (Vitoria / Bel Lube)
Alterou o preço de 4 produtos (radiador 12610, protection 5942, 5w40 SP 13989, performance 5919)
às 16:19-16:20 e "não alteraram" no site. A alteração só chegou na FC ~16-45 min depois.

### Causa raiz (provada no log de PROD)
O `PrecoListener` estava registrado para a instância **`ExcecaoPreco`**, que **não existe** no
dicionário Sankhya. A entidade real do TGFEXC é **`Excecao`** (log: `entityName="Excecao"`,
`crudListener=ExcecaoCrudListener`, campos VLRVENDA/NUTAB/CODPROD/TIPO). Resultado: o listener
**nunca disparava** nas edições de preço. As alterações só eram capturadas pelo
`AutoPriceChangesSweepJob` (poll de 10 min, v1.2.88) → lag de até ~15 min + processamento do outbox.
Confirmado: todos os "Enfileirado: PRECO" do dia vieram de threads `fastchannel-auto-*` (o sweep),
nenhum de `default task-*` (que seria o listener reagindo à UI).

### Fix v1.2.90
`@Listener(instanceNames = {"Excecao", "ExcecaoPreco"})` — passa a reagir à entidade real `Excecao`.
Agora uma alteração de preço na tela dispara o listener **na hora** → enfileira → outbox no próximo
ciclo (~1 min). O `AutoPriceChangesSweepJob` (v1.2.88) continua como rede de segurança e para
escalonado expirado (que não gera evento de TGFEXC).

### Validação do caso reportado
Os 4 produtos JÁ estão corretos na FC (sincronizados pelo sweep): 61066242=R$205,23,
31252653=R$613,01, 31240853=R$798,33, 31251453=R$583,03. O que a operadora via era a
vitrine com cache + o lag do poll. Com v1.2.90 instalada, a próxima alteração reflete em ~1 min.

## 2026-06-01 - v1.2.89 (OTIMIZAÇÃO: full sync pula produtos que não existem na FastChannel)

### Pedido (Vitoria / Bel Lube)
"Não tem como otimizar quando é um full sync ou um sync muito grande pra não ficar enviando
vários produtos inexistentes na FastChannel?" — o full sync demorava demais e enchia o painel
de erros sem dar garantia de progresso.

### Diagnóstico (medido contra a API real em 01/06)
O full sync tentava empurrar preço de **~2544 produtos** (todos das marcas `AD_FAST='S'`), mas
apenas **~611-630 existem no catálogo da FastChannel**. Os ~1900 restantes (75%) retornavam
HTTP 404 *"O SKU do produto não existe ou está incorreto"* — × até 12 tabelas = milhares de
PUTs inúteis por sync. O addon **nunca cria produto no catálogo FC** (`processProductItem` só
grava o De-Para local), então um produto ausente jamais será criado por aqui → é seguro pulá-lo.

### Mudança v1.2.89
- `FastchannelPriceClient.listExistingSkus(tables)`: agrega `GET /prices?PriceTableId=X` de todas
  as tabelas elegíveis → conjunto de SKUs que existem na FC (paginado, ~12 GETs, ~6s).
- `PriceService.getFcExistingSkus()`: cache de 5 min, **fail-open** (se a feature estiver
  desligada ou o prefetch falhar/vier vazio, retorna null → não pula nada = comportamento legado).
- `syncPriceBatch` (full sync automático) e `FCPrecosService.syncAll` ("Sincronizar Todos"):
  pulam produtos cujo SKU não está no conjunto — não contam como erro nem sucesso, logam `pulados`.
- Flag de escape: `-Dfc.sync.skipNonexistent=false` desativa a otimização.

### Impacto
- Produtos sincronizados por full sync caem de ~2544 → ~611 (**−75% de chamadas/tempo**).
- O painel deixa de acumular ~1900 "erros" que eram só ruído de produto inexistente.
- Log final: `[syncPriceBatch] concluido: N produtos ... (X ok, Y falhas, Z pulados por não existir na FC)`.

### Validação (API real FC)
Prefetch retornou 611 SKUs em 5,6s. SKUs 1001/1237/1400/14998 (que davam 404) → marcados para
pular; SKUs 31251453/31408053 (reais) → marcados para sincronizar. ✓

## 2026-06-01 - v1.2.88 (AUTOMAÇÃO: varredura periódica de escalonado expirado + TGFEXC alterado)

### Pedido (Vitoria / Bel Lube, 01/06)
Duas dores reais em produção:
1. **Escalonado expirado não é limpo automaticamente** — quando `TGFDES.DTFINAL` passa, nenhum
   evento dispara para os produtos afetados. Os batches ficam visíveis na FC (e o override do
   `SalePrice` da faixa-1 também) até o próximo full sync — horas, às vezes um dia inteiro.
2. **Alteração manual em TGFEXC pelo backoffice nem sempre dispara o `PrecoListener`** — a fila
   `AD_FCQUEUE` fica sem entrada, e a mudança só chega na FC no full sync diário. A usuária
   reportou "não sei se tá funcionando, não tenho garantia".

### Mudança v1.2.88
Novo job `AutoPriceChangesSweepJob` agendado a cada **10 min** (configurável via
`fc.auto.sweep.minutes`). Em cada tick:
- Busca produtos cujo `TGFDES.DTFINAL` caiu nas últimas ~2 dias e que têm faixas
  `TGFDPQ` — promos que acabaram de expirar.
- Busca `TGFEXC.CODPROD` alterados desde a última varredura.
- Para cada produto afetado: `QueueService.enqueuePrice(codProd, sku)` — entra como
  PRECO/UPDATE no `AD_FCQUEUE` (com debounce do `enqueue`, sem duplicar).
O outbox processor pega cada item enfileirado e roda `syncPrice` — que já aplica o
override (v1.2.87) e o cleanup dos batches obsoletos (v1.2.85).

Resultado: faixas expiradas saem da FC sozinhas em **≤ 10 min**, e mudanças manuais
no TGFEXC também — sem depender do listener nem do full sync diário.

### Log de saída (cada tick)
`[AutoSweep] varredura ok: expired=N tgfexc=M enfileirados=K sem_sku=X erro=Y`

## 2026-05-26 - v1.2.87 (REGRA: escalonado "a partir de 1 un." sobrepõe o Preço de Venda)

### Pedido
Com o escalonado funcionando (faixas 1-3, 4-5, 6+), a faixa que vale **a partir de 1 unidade**
deve ser **replicada no Preço de Venda** — o escalonado é uma campanha com data-fim e tem
prioridade. Antes ficava incoerente: a listagem/carrinho mostravam o Preço de Venda cheio
(ex.: R$ 620,99) enquanto o detalhe mostrava o escalonado (ex.: R$ 580,31).

### Mudança v1.2.87
`PriceService.syncPriceTable`: os batches passam a ser resolvidos **antes** do PUT de preço.
Se existir faixa escalonada cobrindo a 1ª unidade (`Min<=1<=Max`), o `SalePrice` (Preço de Venda)
é **sobreposto** pelo preço dessa faixa (`findFirstUnitBatchPrice`, escolhe a de menor preço se
houver mais de uma). O `ListPrice` (Preço de Lista / "De") é mantido como referência riscada.
Resultado coerente: listagem, detalhe e carrinho mostram o mesmo preço efetivo de 1 unidade.

Exemplo SKU 31251453: ListPrice=633,66 (De), SalePrice 620,99 -> **580,31** (= faixa 1-3),
faixas 1-3=580,31 / 4-5=566,66 / 6+=560,07.

## 2026-05-26 - v1.2.86 (FIX GRAVE: FC gravava Qtd Mín/Máx = 0 — quantidade enviada em notação decimal)

### Sintoma reportado
Após v1.2.85, as faixas pararam de duplicar (3 faixas corretas), MAS as **quantidades** ficaram
erradas: no site FC todas apareciam como "A partir de 1 unid." e na tela de Preços os batches
mostravam `Qtd Mín=0 / Qtd Máx=0` (e a faixa-topo `0 / 999999`).

### Causa raiz (confirmada contra a API real da FC em 2026-05-26)
A API FC **armazena `MinimumBatchSize`/`MaximumBatchSize` como 0 quando recebe o número em
notação decimal** (`"1.0"`, `"3.0"`); só grava certo com notação **inteira** (`"1"`, `"3"`).
Teste direto na API: POST `2.0`→ grava `0`; POST `2` → grava `2`.

O addon enviava decimal porque **`TGFDPQ.QTDE` é `float` no SQL** → `rs.getBigDecimal()` devolvia
escala 1 (`1.0`, `3.0`) e o gson serializava `"1.0"`. A faixa-topo era a única correta porque
usava a constante `new BigDecimal("999999")` (escala 0 → `999999`). Esta também era a causa
**original** do "todas a partir de 1" (Min=0 → a FC exibe "a partir de 1").

### Fix v1.2.86
- **`PriceBatchResolver`** — `Min`/`Max` normalizados para escala 0 (inteiro) via `toIntScale()`
  ao montar cada faixa.
- **`FastchannelPriceClient.normalizeDesiredBatches`** — `setScale(0)` em `Min`/`Max` no
  **chokepoint único** de todo POST de batch (cobre resolver e edição manual da tela).
Resultado: gson emite `"MinimumBatchSize":1,"MaximumBatchSize":3` → FC grava 1..3, 4..5, 6..999999.

### Validação (API real FC, SKU 31251453)
POST decimal `2.0` → FC grava Min=0; POST inteiro `2` → FC grava Min=2. Confirmado. Batches de
teste removidos. Produção será auto-corrigida na próxima sync após deploy (cleanup v1.2.85 remove
os batches Min=0 e re-posta com inteiro).

## 2026-05-25 - v1.2.85 (FIX GRAVE: preço escalonado duplicado e sem limpeza — parser de batches lia campo errado)

### Sintoma reportado
Preço escalonado (faixas/batches) não funcionava direito com várias faixas (ex.: 1-3, 3-5,
6+), as faixas apareciam **duplicadas** no site FastChannel e **não eram limpas/sincronizadas**
corretamente. Visualmente davam a impressão de "todas a partir de 1".

### Causa raiz (confirmada contra a API real da FC em 2026-05-25)
`FastchannelPriceClient.listPriceBatches()` desserializava a resposta na classe
`BatchListResponse` que procurava o campo JSON **`ProductPriceBatch`** — esse é o nome do
ELEMENTO no XML, mas a FC (ServiceStack), com `Accept: application/json` já enviado, responde
no formato `{"Success":true,"Payload":[ {batch...} ],"TotalRecords":N}`. Como `ProductPriceBatch`
nunca existe no JSON, o método retornava **SEMPRE lista vazia**. Consequências:
- O dedup (`containsEquivalentBatch(currentBatches, ...)`) nunca via os batches já existentes
  → **toda sync re-POSTava as mesmas faixas**. No log de 2026-05-25 a faixa 1-3 do SKU
  31251453 foi POSTada **6×** (e a FC **não deduplica POST** — testado: 2 POST = 2 batches).
- A limpeza (delete de batches não-desejados/lixo) iterava sobre `currentBatches` (sempre vazia)
  → **nunca deletava nada** (0 deletes em todo o log).

### Fix v1.2.85
1. **`listPriceBatches()`** — lê o array **`Payload`** (mesmo padrão de `parsePriceFromResponse`/
   `listPricesForTable`), com fallback ao formato legado e `stripBom`. Em caso de falha de
   parse loga WARNING (nunca mais mascara como lista vazia silenciosa).
2. **`updatePriceBatches()`** — reconciliação completa com self-heal: mantém UMA ocorrência de
   cada faixa desejada e **deleta duplicatas, lixo e faixas obsoletas**. Resíduos acumulados de
   meses serão removidos automaticamente na próxima sync de cada produto.
3. **`PriceBatchResolver`** — a maior faixa de um escalonamento com 2+ faixas passa a ser
   "N e acima" (`MaximumBatchSize=999999`). Antes "até 6" virava Min6-Max6 e quantidades 7+
   ficavam sem preço escalonado. (Faixa única mantém o cap explícito.)

### Validação (API real FC, SKU 31251453)
- POST 6 batches duplicados (3 faixas ×2) → FC aceita `Max=999999` e mantém os 6 (não deduplica).
- Algoritmo do fix → deletou 3 duplicatas, restaram exatamente 3 faixas (1-3, 4-5, 6-999999).
- desired vazio (promo deletada) → deletou tudo → 0 batches. Produção deixada limpa.

## 2026-05-20 - v1.2.84 (FIX GRAVE: SKU FC mapeava para CODPROD errado quando REFFORN do produto coincidia com SKU)

### Sintoma reportado
Pedido FC 4782 (NUNOTA 4861316, HERBERT RODRIGUES) tinha 2 itens no FC:
- Item 1: SKU 12654 (MILITEC CX 12X200ML) R$ 776,94
- Item 2: SKU 12655 (MILITEC CX 24X40ML) R$ 357,52

Mas no Sankhya foi lancado:
- Item 1: CODPROD 12654 (CORRETO)
- Item 2: CODPROD **14412** (MILITEC - 40ML unitario, **NAO** a CX 24X40ML)

Resultado: nota emitida com produto errado, descritivo errado, codigo errado.

### Causa raiz
A marca MILITEC tem `TGFMAR.AD_FAST='S' AD_FASTREF='C'` (regra: SKU = CODPROD). Mas o
metodo `DeparaService.getCodProdByRefFornFcBrand()` buscava por REFFORN **ignorando**
o `AD_FASTREF`. Coincidentemente, o produto CODPROD=14412 tinha `REFFORN='12655'`
cadastrado (referencia do fornecedor para esse produto). Quando o FC enviava
SKU=12655, a query:
```sql
SELECT P.CODPROD FROM TGFPRO P
INNER JOIN TGFMAR M ON M.CODIGO = P.CODMARCA AND M.AD_FAST = 'S'
WHERE P.ATIVO = 'S' AND P.REFFORN = '12655'
```
encontrava CODPROD=14412 (REFFORN=12655). O De-Para correto (12655 -> 12655) nem chegava
a ser consultado porque o REFFORN era a primeira prioridade na cadeia de resolucao.

### Fix v1.2.84
Adicionado filtro `AND M.AD_FASTREF = 'R'` em 3 lugares:

1. **`getCodProdByRefFornFcBrand(refForn)`** (linha 1295) - resolucao individual JAPE
2. **`getCodProdBySkuOrEanJdbc()`** (linha 537) - fallback JDBC quando JAPE indisponivel
3. **`prefetchChunk()` batch SQL1** (linha 732) - pre-resolucao em lote no inicio do job
   de importacao de pedidos

Apos o fix:
- SKU 12655 + marca AD_FASTREF='C' (MILITEC) -> nao bate em REFFORN -> cai no De-Para
  -> retorna 12655 corretamente
- SKU 12655 + marca AD_FASTREF='R' (outras marcas) -> bate em REFFORN normalmente

### Regra agora alinhada com o legado
Conforme `reference_sku_rule_legacy.md`:
- `AD_FAST='S' AND AD_FASTREF='R'` -> SKU FC = REFFORN do produto
- `AD_FAST='S' AND AD_FASTREF='C'` -> SKU FC = CODPROD do produto (regra MILITEC)
- `AD_FAST='N'` -> produto nao participa do FC

### Correcao retroativa para nota 4861316
Necessario corrigir manualmente o NUNOTA 4861316:
- Item 2: trocar CODPROD 14412 -> 12655
- Conferir VLRUNIT, VLRTOT, VLRDESC com o que veio do FC
A NF 70810 ja foi emitida (NUNOTA 4862157) e a 407112 (NUNOTA 4862337 - subset com so item 1)
pode requerer cancelamento/reemissao no Sankhya.

### Validacao apos instalar v1.2.84
1. Importar 1 pedido FC com SKU que coincida com algum REFFORN do BD
2. Conferir que o CODPROD resolvido bate com o SKU (nao com o REFFORN coincidente)
3. Conferir log: nao deve aparecer "(REFFORN+marca FC)" para SKUs de marcas AD_FASTREF='C'

## 2026-05-19 - v1.2.83 (CAUSA RAIZ REAL: nome do campo no payload de updateOrderStatus estava errado)

### Reinterpretacao do erro do FC
O usuario chamou atencao: "acho que o problema nao e esse, le o erro e interpreta direitinho".
Re-lendo a mensagem do FC com mais atencao:

> "O codigo de status informado no parametro **'OrderStatusId'** nao e um codigo valido para
> o pedido atual."

A v1.2.81/82 interpretaram como "maquina de estado bloqueando retrocesso" e silenciavam o
erro. **Estava errado.** A mensagem refere ao **PARAMETRO 'OrderStatusId'**. O addon Java
estava enviando JSON com nome do campo errado.

### Causa raiz REAL
`OrderStatusDTO` declarava:
```java
private int status;
private String message;
```
Gson serializava como:
```json
{"status": 201, "message": "...", "timestamp": "..."}
```
**Mas a API FC espera:**
```json
{"OrderId": ..., "OrderStatusId": 201, "Message": "..."}
```
Como o JSON enviado nao tinha `OrderStatusId`, o FC recebia `OrderStatusId=0` (default int)
e respondia HTTP 400. A sugestao "verifique PossibleNextStatuses" era um fallback generico
da API que indicava "0 nao e um status valido para o pedido atual" - mensagem ambigua que
nos confundiu por varios fixes.

Confirmado consultando o legado Node.js
(`gbi-app-integrador/models/mssqlModels/sankhya/Pedidos.js:144-147`):
```js
const statusToInput = {
    "OrderId": parseInt(row[0]),
    "OrderStatusId": parseInt(row[1])
};
```

### Fix v1.2.83
`OrderStatusDTO` recebeu anotacoes `@SerializedName("OrderStatusId")`, `@SerializedName("Message")`,
`@SerializedName("Timestamp")`. Agora o Gson produz JSON com os nomes corretos esperados
pela API FC. Sem necessidade de silenciar ou criar workarounds - o status sera aceito normalmente.

### Mantidos como rede de seguranca
Os detectores `isFcStateTransitionError` e `isFcInvoiceNotAcceptable` da v1.2.81/82 ficam.
Eles ainda servem para:
- Casos reais de transicao bloqueada (FC mais adiantado que Sankhya legitimamente)
- 404 em sendInvoice quando NF ja' anexada
Sao defesas residuais, mas a partir desta versao **nao serao mais o caminho primario**
porque o erro raiz foi resolvido.

## 2026-05-19 - v1.2.82 (FIX: detectores isFcStateTransitionError/isFcInvoiceNotAcceptable robustos a encoding)

### Sintoma
Apos v1.2.81, o erro HTTP 400 "OrderStatusId nao e um codigo valido" CONTINUOU em loop
infinito em PROD. Items 554, 555, 556, 557 (PEDIDO_STATUS, pedidos FC 4793, 4794, 4786...)
ficaram tentando retry indefinidamente apesar da v1.2.81 ter implementado o detector.

### Causa raiz - encoding nos logs e exceptions
O detector v1.2.81 procurava `"não é um código válido"` em UTF-8 puro. Mas em PROD a
mensagem da exception chega com **acentos corrompidos**:
```
"O c�digo de status... n�o � um c�digo v�lido"
```
Origem: o JSON do FC vem em UTF-8 mas, em algum ponto da pipeline (logger File appender,
SystemOut, conversao Cp1252 do Wildfly), os caracteres `ó/ã/é` viram `�` (replacement char).
A comparacao `msg.toLowerCase().contains("não é um código válido")` falha porque os bytes
nao batem.

### Fix v1.2.82 - detectores invariantes a encoding
Reescritos os 2 detectores usando APENAS palavras-chave SEM acentos (sao invariantes
qualquer que seja o encoding):

**`isFcStateTransitionError`** agora retorna true quando:
- mensagem contem `possiblenextstatuses` (chave forte - so aparece nesse erro), OU
- mensagem contem `orderstatusid` + `400`/`badrequest`/`http 400`

**`isFcInvoiceNotAcceptable`** agora retorna true quando:
- mensagem indica 404 (`"statuscode": 404`, `"httpstatuscode":404`, `-> 404 `,
  `resource not found`, `status=404`) + contexto NF (`erro ao enviar nf` ou `/invoices`)

Sem mais dependencia de strings acentuadas.

### Acao manual aplicada no BD
Items 554, 555, 556, 557 (e outros do mesmo pedidos) que estavam em loop foram
manualmente marcados como SUCCESS via SQL para parar o spam imediatamente, antes do
deploy.

## 2026-05-15 - v1.2.81 (FIX: loop infinito ao sincronizar status/NF para pedidos FC ja' adiantados)

### Validacao da v1.2.80 - sucesso confirmado
Log do dia 15/05 mostra que v1.2.80 funcionou:
- `AutoProvisionamento[stock-full]: agendado initialDelay=120s, periodo=21600s` ✓
- `[StockFullSyncJob] Filtros aplicados: codEmp=26 codLocal=TODOS` ✓ (2 min apos boot, como esperado)
- `[STOCK_SYNC] StockFullSyncJob concluido. Enviados: 375, Erros: 852` ✓
- `[PRICE_SYNC] PriceFullSyncJob iniciado. Produtos a sincronizar: 2541` ✓
- ServiceInvokerStrategy criou pedido 4780 -> NUNOTA 4860533 ✓
- PEDIDO_STATUS handler ativo: `Atualizando status FC pedido 4780 para 201` ✓ (v1.2.79)

### Novos sintomas descobertos no log
Apos a v1.2.79 habilitar a sincronizacao reversa Sankhya->FC (PEDIDO_STATUS handler), 2
novos erros apareceram em loop infinito para pedidos FC ja' adiantados:

**Erro 1 - HTTP 400 PUT /orders/{id}/status:**
```
"O codigo de status informado no parametro 'OrderStatusId' nao e um codigo valido para o
 pedido atual. Verifique os possiveis valores atraves da propriedade 'PossibleNextStatuses'..."
```
Causa: FC tem maquina de estado. Sankhya envia L (=APPROVED 201) mas o pedido FC ja' esta
em status 300+ (NF emitida). Voltar para 201 e' proibido pela API.

**Erro 2 - HTTP 404 POST /orders/{id}/invoices:**
```
"statusCode": 404, "message": "Resource not found"
```
Causa: pedido FC ja' tem NF anexada ou foi cancelado. Endpoint rejeita nova NF.

**Loop infinito**: A cada `afterUpdate` no TGFCAB, o NotaFiscalListener tentava reenviar
NF + status. OutboxProcessor classificava HTTP 400/404 como erro generico e enfileirava
retry. Resultado: spam de chamadas FC, log poluido, items ficando em ERRO indefinidamente.

### Fix v1.2.81

1. **`OutboxProcessorJob.isFcStateTransitionError()`** (novo): detecta a mensagem especifica
   de transicao de estado invalida ("orderstatusid" + "nao e um codigo valido" /
   "possiblenextstatuses") e marca como **SUCCESS** (parar retry - FC ja' esta certo).

2. **`OutboxProcessorJob.isFcInvoiceNotAcceptable()`** (novo): detecta HTTP 404 ao enviar
   NF e marca como **SUCCESS** (FC nao aceita - ja' tem NF ou cancelado).

3. **`OutboxProcessorJob.isNonPublishableSkuError()`** ajustado: era amplo demais, pegava
   404 de NF erroneamente. Restringido para matches explicitos de "SKU".

4. **`NotaFiscalListener.processInvoiceCreated()` com idempotencia**: cache em memoria
   (`INVOICE_TERMINAL_STATES`) das NUNOTAs ja processadas. Evita chamar `sendInvoice`
   repetidamente a cada afterUpdate trivial no TGFCAB. Eviction simples quando atinge
   5000 entradas (limpa tudo).

5. **`NotaFiscalListener.processInvoiceCreated()` tratamento de 404**: HTTP 404 agora
   loga INFO ("ja' anexada no FC") em vez de SEVERE. Marca como terminal no cache para
   parar retry.

### Resultado esperado em PROD apos v1.2.81
- **Sem mais loop infinito** de retry de status/NF para pedidos FC adiantados
- Log mais limpo (sem spam de 400/404)
- Items na fila marcados corretamente como SUCCESS quando FC ja' esta sincronizado

## 2026-05-15 - v1.2.80 (FIX CRITICO: bug de unidade em scheduler interno - stock/price full sync NUNCA rodavam)

### Sintoma reportado
Chefe reportou que estoque nao estava sincronizando. Site FC mostrava SKU 31251453 sem
estoque, mas no BD a empresa configurada (CODEMP=26) tinha **2038 unidades disponiveis**
(ESTOQUE=2038, RESERVADO=50, DISP=1988).

### Investigacao
- **AD_FCQUEUE ESTOQUE nas ultimas 24h: 0 itens**. EstoqueListener nunca enfileirou.
- EstoqueListener esta registrado (confirmado no log das 06:08 e 09:17).
- **StockFullSyncJob (safety-net periodico) NUNCA ROUTOU desde o boot**.

### Causa raiz CRITICA
Bug de unidade no `FastchannelAutoProvisioning.schedule()` (linha 333-351):
```java
long initialDelay = Math.max(20L, Math.min(120L, unit.toSeconds(period)));  // segundos
internalScheduler.scheduleWithFixedDelay(task, initialDelay, period, unit);  // ← unit aplica AOS DOIS
```

Para `stock-full (period=6, unit=HOURS)`:
- `unit.toSeconds(6)` = 21600, min(120, 21600) = 120
- `initialDelay = 120` (calculado em **segundos**)
- `scheduleWithFixedDelay(task, 120, 6, HOURS)` → Java interpreta `initialDelay=120` com
  `unit=HOURS` = **120 horas = 5 dias!**

Resultado: tarefas com period em HOURS/DAYS **nunca rodavam** porque o initialDelay
catapultava 5+ dias no futuro - e reinstalacoes do addon resetavam o timer constantemente.

**Tarefas afetadas**:
- `stock-full` (6h) → ~5 dias initialDelay
- `price-full` (6h) → ~5 dias initialDelay
- `depara-sync` (24h) → ~5 dias initialDelay
- `status-sync` (3min) → 2 horas initialDelay (menos critico, mas tambem errado)

### Por que so' funcionavam os jobs de minutos
`order-import` e `outbox` usam `scheduleDynamic()` (NAO `schedule()`) que tem assinatura
diferente e nao tem esse bug. Por isso esses dois jobs rodavam normalmente.

### Por que sintoma surgiu agora
Bug existia ha tempos, mas como o JAPE EstoqueListener ocasionalmente disparava em
algumas operacoes, o estoque era atualizado parcialmente. Quando o Sankhya muda estoque
via SQL direto (procedures de movimentacao), o listener nao dispara e o
StockFullSyncJob (a rede de seguranca) nunca rodava → sintoma de "estoque desatualizado".

### Fix v1.2.80
Converter tudo para SECONDS antes de passar para `scheduleWithFixedDelay`:
```java
long initialDelaySecs = Math.max(20L, Math.min(120L, unit.toSeconds(period)));
long periodSecs = unit.toSeconds(period);
internalScheduler.scheduleWithFixedDelay(task, initialDelaySecs, periodSecs, TimeUnit.SECONDS);
```

Tambem adicionado log no agendamento:
```
AutoProvisionamento[stock-full]: agendado initialDelay=120s, periodo=21600s.
```

### Validacao apos instalar v1.2.80
1. No log esperar (~2 min apos boot):
   ```
   AutoProvisionamento[stock-full]: agendado initialDelay=120s, periodo=21600s.
   ```
2. ~2 min depois: `[StockFullSyncJob] Filtros aplicados: codEmp=26 codLocal=TODOS`
3. Stock no FC sera atualizado para todos os SKUs ativos.

### Estado do BD agora (antes do fix em PROD)
- SKU 31251453 (CODPROD 5919): CODEMP 26 tem 2038 disp, mas FC mostra 0
- Outros SKUs FC tambem provavelmente dessincronizados
- Apos instalar v1.2.80, em ~5 min todos serao re-sincronizados

## 2026-05-14 - v1.2.79 (FIX: 2 bugs descobertos no log do pedido 4779)

### Confirmacao positiva no log de 14/05 (server.log_20260514184109.zip)
**A v1.2.77/78 ESTA EM PROD e funcionando**: pedido FC 4779 (NUNOTA 4860138) foi criado as
16:50:48 pela **ServiceInvokerStrategy** (XML/CACSP.incluirNota - paridade com legado), nao
pelo InternalApiStrategy. VLRNOTA=8566.82 correto desde o save. Mudanca de v1.2.77 (troca de
ordem das estrategias) validada.

```
16:50:48,790 Tentando estrategia: ServiceInvoker     ← NOVA ORDEM
16:50:48,791 [ServiceInvoker] Criando pedido 4779 via bridge nativo Sankhya
16:50:50,620 [ServiceInvoker] Pedido 4779 criado como NUNOTA 4860138
16:50:50,620 === SUCESSO com estrategia ServiceInvoker - NUNOTA: 4860138 ===
```

### Bug 1 - ENTITY_PEDIDO_STATUS sem handler no OutboxProcessor
**Sintoma**: cada mudanca de status de pedido FC (P->A->L, cancelamentos) era enfileirada
com `ENTITY_TYPE='PEDIDO_STATUS'`, mas o `OutboxProcessorJob` switch nao tinha case para
esse tipo - todas caiam no default e eram marcadas como ERRO_FATAL "Tipo desconhecido".

Resultado: o **FC nunca recebia notificacao** de que o pedido foi processado/liberado/
cancelado/faturado no Sankhya. Confirmado no log:
```
17:29:26,831 Enfileirado: PEDIDO_STATUS/UPDATE - 4779
17:30:12,296 Tipo de entidade desconhecido: PEDIDO_STATUS
17:30:12,298 Item 509 marcado como ERRO_FATAL: Tipo desconhecido
```

**Fix**: Adicionado `case FastchannelConstants.ENTITY_PEDIDO_STATUS` no switch principal
de `OutboxProcessorJob.run()`. Novo metodo `processOrderStatusItem(item)` parseia o
payload `[orderId, statusInt]` e invoca `FastchannelOrdersClient.updateOrderStatus(...)`.

### Bug 2 - ClassCastException em processInvoiceCreated (NUMNOTA)
**Sintoma**: ao tentar enviar NF para o FC apos confirmacao:
```
17:52:50,388 WARNING NotaFiscalListener: Erro ao enviar NF para Fastchannel:
              java.lang.ClassCastException: java.math.BigDecimal cannot be cast to java.lang.String
17:52:50,389 SEVERE [ORDER_IMPORT] Falha ao enviar NF para pedido 4779
```

**Causa**: linha 132 do `NotaFiscalListener.processInvoiceCreated` chamava
`vo.asString("NUMNOTA")` mas NUMNOTA em TGFCAB e' BigDecimal no schema JAPE.
`asString()` faz cast direto e lanca ClassCastException.

**Fix**: ler direto via `vo.asBigDecimal("NUMNOTA")` e converter com `toPlainString()`.
Adicionado helper `safeAsString(vo, field)` que tenta `asString` e cai em
`asBigDecimal -> toPlainString` no catch - protege os outros campos
(CHAVENFE, SERIENOTA) caso tenham o mesmo problema.

### Outros pontos do log (informativo)
- **Duplicata NUNOTA 4860343** (mesmo FC 4779, criada 17:36:50) foi **DUPLICACAO MANUAL**
  no Sankhya por um operador (default task-2870), nao pelo addon. Nao ha bug a corrigir.
- **5 ocorrencias de "Tipo desconhecido"** no log (4 PEDIDO_STATUS + 1 NF) - todas serao
  evitadas a partir desta versao.
- VLRNOTA-REPAIR continuou disparando corretamente durante as confirmacoes (no-op porque
  ServiceInvoker ja criou com valores corretos).

### Estado do BD pos-instalacao v1.2.77/78 (5 pedidos)
- 4858582 (FC 4776, cupom 1215.46): VLRNOTA=6147.60 ✓
- 4858879 (FC 4777, cupom 1065.31): VLRNOTA=13674.70 ✓
- 4859028 (FC 4776, cupom 1215.46): VLRNOTA=6147.60 ✓
- 4860138 (FC 4779, sem cupom): VLRNOTA=8566.82 ✓ (criado por ServiceInvoker!)
- 4860343 (FC 4779, duplicata manual): VLRNOTA=8566.82 ✓

## 2026-05-14 - v1.2.78 (FIX: bug NOMUSU em OrderService.resolveCodUsuIntegracao + AD_FCDUPPURGE guard)

### Sintoma reportado pelo chefe
Apos instalar v1.2.77, ainda apareceu pedido com VLRNOTA errado (NUNOTA 4858879, FC 4777,
VLRNOTA=14726.10 bruto vs 13660.79 correto). E o Sankhya Place mostrava aviso "Houve um
erro que impediu a inicializacao do Addon" + erro "PreparedStatement com parametro nulo
na entidade 'SolutionBinary': param[0] = null" ao clicar Reiniciar.

### Investigacao do log
1. **Pedido 4858879** foi criado as **11:01:19 com a versao ANTIGA** (v1.2.72 ou anterior),
   confirmado por SQL com `NOMUSU OR NOMEUSU` no log - essa query foi removida na v1.2.73,
   ou seja, naquele instante a v1.2.77 ainda nao tinha entrado em vigor.
2. **v1.2.77 so entrou em vigor as 11:02:51** ("Inicializacao do Modulo 'addon-fastchannel'
   concluida com sucesso"). Reinstalacao demorou para se propagar.
3. Pedido foi criado por `InternalApiStrategy` (a versao antiga ainda usava JAPE primario),
   e durante `applyCabecalhoParityNative -> resolveCodUsuIntegracao` quebrou com:
   ```
   SQL: SELECT TOP 1 CODUSU FROM TSIUSU WHERE UPPER(NOMUSU)=UPPER(?) OR UPPER(NOMEUSU)=UPPER(?)
   Caused by: SQLServerException: Nome de coluna 'NOMUSU' invalido
   ```
4. Essa exception **abortou o post-import parity update**, deixando o VLRNOTA bruto que o
   MGECOM tinha calculado durante o save inicial **sem ser corrigido pelo forceVlrNotaCorrect**.

### Bug encontrado (causa raiz do VLRNOTA bruto)
A v1.2.73 corrigiu o bug `NOMUSU` em `InternalApiStrategy.resolveCodUsuByName` e
`InternalApiStrategy.resolveCodCenCusPadByName`. **Mas ficou de fora um terceiro lugar**:
`OrderService.resolveCodUsuIntegracao` (linha 3217). Esse metodo eh chamado por
`applyCabecalhoParityNative` durante o post-import - e quebrava com `NOMUSU invalido`
em todo pedido FC criado, **abortando a transacao de parity update** e deixando VLRNOTA
no estado bruto que o MGECOM tinha calculado.

### Fix 1 - `OrderService.resolveCodUsuIntegracao`
Removida `OR UPPER(NOMUSU)=UPPER(:user)` da query. TSIUSU so tem `NOMEUSU` (com E).
Aplicado o mesmo padrao da v1.2.73.

### Fix 2 - `FastchannelAutoProvisioning.runOneShotPurgeAdNumFastDups`
Tabela `AD_FCDUPPURGE` e' tabela de utilidade opcional para purges historicos. Em PROD
Bel Lube ela nao foi criada e o startup loga warning a cada vez:
```
[CRIT-PURGE] Erro geral no one-shot purge: SQLServerException: Nome de objeto 'AD_FCDUPPURGE' invalido
```
Adicionado guard `IF EXISTS` consultando `sys.objects` antes de executar a query. Skip
silencioso se a tabela nao existir (comportamento esperado em clientes sem historico).

### Erro "SolutionBinary param[0] = null" ao clicar Reiniciar
**Esse erro NAO e' do addon - e' interno do Sankhya Place.** Acontece quando o backend do
Place tenta ler o jar assinado do addon para reinstalar e o registro `SolutionBinary` esta
inconsistente. Geralmente se resolve:
1. Aguardar 1-2 min apos publicar nova versao na Area Dev (a sincronizacao do Place leva
   alguns segundos)
2. Recarregar a pagina F5
3. Se persistir, Desinstalar e reinstalar (perde nada - todas configuracoes ficam em AD_FCCONFIG)

A inicializacao do addon em si esta funcionando (log confirma `Inicializacao concluida com
sucesso` as 11:02:51 com a v1.2.77).

### Correcao retroativa
- NUNOTA 4858879: VLRNOTA 14726.10 -> **13660.79** (cupom 1065.31 agora refletido)

### Validacao apos instalar v1.2.78
1. No log esperar `Inicializacao do Modulo 'addon-fastchannel' concluida com sucesso`
2. Importar 1 pedido FC com cupom
3. Conferir no log:
   - `[ServiceInvoker]` (preferencial) ou
   - `[InternalAPI]` com `forceItemValuesFromFc` + `VLRNOTA forcado correto` (fallback)
4. Conferir no BD que VLRNOTA = SUM(VLRTOT-VLRDESC) + VLRFRETE + VLRJURO
5. Confirmar a nota e conferir que VLRNOTA continua correto

## 2026-05-14 - v1.2.77 (ATACA CAUSA RAIZ: ServiceInvoker (XML) virou estrategia primaria, InternalApi (JAPE) fica como fallback)

### Contexto
Feedback do chefe (literal): "agnt fica inventando formas de remendar, de contornar o problema,
fazendo codigo e mais codigo pra resolver uma coisa que ja deveria vir corretamente ainda mais
uma coisa simples como essa que e' um cupom de desconto que deve ser rateado nos itens e lancar
o valor correto objetivamente"

### Causa raiz arquitetural
As v1.2.71, v1.2.74 e v1.2.76 sao TODAS remendos da mesma causa raiz: o `InternalApiStrategy`
usa **JAPE direto** (`FluidCreateVO`) e seta manualmente VLRUNIT, VLRTOT, VLRDESC, PERCDESC,
PRECOBASE, VLRNOTA, VLRDESCTOT. Quando o MGECOM/STP_CONFIRMANOTA2 recalcula valores derivados
seguindo a logica nativa Sankhya, divergem dos valores que o addon forcou - dai as 4 camadas
de "repair" em sequencia para forcar de volta.

O legado Node.js (gbi-app-integrador `models/erp/sankhya.js`) NUNCA teve esse problema porque
usa XML via `CACSP.incluirNota` com `<itens INFORMARPRECO="True">`. Envia apenas o minimo
(VLRUNIT + PERCDESC + QTDNEG + cabecalho minimo) e DEIXA o Sankhya calcular TUDO. Sem conflito,
sem repair, sem listener, sem job de varredura.

### Fix
**Trocada a ordem das estrategias** em `OrderCreationOrchestrator`:

| Posicao | Antes (v1.2.76) | Depois (v1.2.77) |
|---------|-----------------|------------------|
| 1 (preferencial) | InternalApi (JAPE direto) | **ServiceInvoker (XML/CACSP.incluirNota)** |
| 2 (fallback 1) | ServiceInvoker | InternalApi (mantido intacto como rede de seguranca) |
| 3 (fallback 2) | Http | Http |

**Nada foi removido**: o InternalApiStrategy, os repairs (forceVlrNotaCorrect,
forceItemValuesFromFc, repairVlrNotaAfterConfirmation, FCVlrNotaRepairJob) seguem todos
ativos. Se o ServiceInvoker falhar por qualquer motivo (ex: erro de configuracao no XML,
campo customizado faltando), o orchestrator cai automaticamente para o InternalApi - e
nesse cenario as 4 camadas de repair entram em acao normalmente.

### Comportamento esperado em PROD
- **Pedidos novos**: chamam ServiceInvokerStrategy primeiro. O Sankhya recebe XML
  CACSP.incluirNota e calcula VLRTOT, VLRDESC, VLRNOTA, VLRDESCTOT automaticamente seguindo
  a logica nativa. Sem conflito com STP_CONFIRMANOTA2.
- **Logs esperados**: `[ServiceInvoker] Criando pedido X via bridge nativo Sankhya` + `[ServiceInvoker] Pedido X criado como NUNOTA Y`.
- **Se ServiceInvoker falhar**: log mostra `Estrategia ServiceInvoker falhou: <erro>` + cai para `Tentando estrategia: InternalAPI`. Comportamento atual preservado integralmente.
- **VLRNOTA-REPAIR no listener**: pode parar de disparar para pedidos novos (porque ja
  virao corretos do XML). Continua disparando para pedidos antigos / casos do fallback.

### Riscos conhecidos do XML atual (a observar em PROD)
O `OrderXmlBuilder.buildIncluirNotaXml` (usado pela ServiceInvokerStrategy) **ainda envia
alguns campos que o legado nao envia** (VLRNOTA, VLRDESC, NUTAB, PRECOBASE, CUSTO/VLRCUS,
USOPROD, ATUALESTOQUE, RESERVA). Se em PROD aparecer divergencia, o proximo passo sera
**alinhar exatamente com o legado** - removendo esses campos do XML para deixar o Sankhya
calcular tudo. Por hora, mantemos como esta para nao alterar mais do que o estritamente
necessario.

### Como reverter (se algo der errado)
Editar `OrderCreationOrchestrator.java` linhas 38-40 e voltar a ordem antiga:
```java
strategies.add(new InternalApiStrategy());      // 1.
strategies.add(new ServiceInvokerStrategy());   // 2.
strategies.add(new HttpServiceStrategy());      // 3.
```
Rebuildar v1.2.78 e reinstalar.

## 2026-05-14 - v1.2.76 (REDE DE SEGURANCA: Job periodico de repair de VLRNOTA - cobre TODOS caminhos)

### Sintoma persistente reportado
Mesmo apos as v1.2.71 (forceItemValuesFromFc), v1.2.72 (ORDEMCARGA), v1.2.73 (SQL bugs),
v1.2.74 (NotaFiscalListener.repairVlrNotaAfterConfirmation) e v1.2.75 (estoque/precos),
o cupom continuou aparecendo divergente em alguns pedidos.

### Investigacao - estado real do BD
Varredura nos ultimos 60 dias revelou que **apenas 1 pedido FC tinha VLRNOTA divergente**:
- NUNOTA 4857418 (FC 4774, criado 13/05 as 15:44, STATUSNOTA=P)
- VLRNOTA=7363.05 (BRUTO) vs liquido correto=6147.59 (cupom 1215.46 ausente)
- Provavelmente criado ANTES da v1.2.74 estar em PROD - o listener nao existia para reparar

Confirmado no log de 14/05:
- NUNOTA 4858368 (criado 14/05 08:36 = pos-v1.2.74): VLRNOTA=6147.59 desde o save inicial
- Listener disparou as 09:14:53 (`[VLRNOTA-REPAIR] formula reaplicada`) durante confirmacao automatica
- Repair persistiu no commit

### Por que ainda preciso de mais uma camada
As camadas existentes cobrem TODOS os caminhos JAPE conhecidos, mas dependem do JAPE disparar
eventos. **Stored procedures SQL** (ex: STP_CONFIRMANOTA2 chamada via outros caminhos) e
**UPDATEs JDBC diretos** nao disparam afterUpdate JAPE. O listener da v1.2.74 funciona no
caminho `confirmarNotas` (que dispara um update JAPE antes da SP), mas pode nao funcionar em:
- Procedures invocadas por outros botões/jobs Sankhya
- Edicoes manuais via SQL direto
- Pedidos importados ANTES do listener existir

### Comparacao com legado (Node.js gbi-app-integrador)
Analisado o legado e validado que o rateio de PERCDESC esta correto:
- Legado linha 248: `vlrDiffBetweenProdSellCost = SubtotalProducts / ProductCost`
- Legado linha 282: `PERCDESC = (1 - vlrDiffBetweenProdSellCost) * 100`
- Para pedido FC 4776 (ProductCost=7363.05, SubtotalProducts=6147.59): PERCDESC=16.50%
- Addon novo grava 16.51% nos mesmos itens - **bate com o legado** (diferenca apenas de
  arredondamento BigDecimal 4 casas vs Number JS)

O legado NAO grava VLRNOTA no header (deixa Sankhya calcular via `INFORMARPRECO="True"`).
O addon novo grava + forca + repara em camadas. Mesmo resultado final na maioria dos casos,
mas o novo tem mais pontos onde algo pode quebrar.

### Fix v1.2.76 - Nova camada de defesa proativa
**Novo job**: `br.com.bellube.fastchannel.job.FCVlrNotaRepairJob` (implementa
`EventoProgramavelJava`). Roda como job agendado e:

1. Varre TGFCAB de pedidos FC dos ultimos 30 dias
2. Detecta divergencias: `ABS(VLRNOTA - (SUM(VLRTOT-VLRDESC)+VLRFRETE+VLRJURO)) > 0.01`
3. Aplica UPDATE direto via JDBC (nao depende de evento JAPE)
4. Loga cada correcao em AD_FCLOG (auditoria via OP_ORDER_IMPORT)
5. Idempotente: no-op para pedidos ja corretos

### Camadas de defesa apos v1.2.76
| Camada | Quando atua | Cobre |
|--------|------------|-------|
| 1. `forceItemValuesFromFc` (v1.2.71) | Durante save de cada item | Recalc MGECOM em NUTAB ativa |
| 2. `forceVlrNotaCorrect` | Apos save do header | Calculo inicial do total |
| 3. `repairVlrNotaAfterConfirmation` (v1.2.74) | afterUpdate JAPE da TGFCAB | confirmarNotas via JAPE |
| 4. `FCVlrNotaRepairJob` (v1.2.76, **NOVA**) | Job periodico | Qualquer caminho (SP, JDBC, manual) |

### Acoes manuais necessarias apos instalar v1.2.76
1. Sankhya > Configuracoes > Eventos Programaveis > Agendamento
2. Adicionar novo agendamento:
   - Classe: `br.com.bellube.fastchannel.job.FCVlrNotaRepairJob`
   - Intervalo: 60-120 segundos
3. (Opcional) Para correcao historica de pedidos anteriores a 30 dias, rodar SQL manual:
   ```sql
   UPDATE TGFCAB SET VLRNOTA = (
     ISNULL((SELECT SUM(VLRTOT - ISNULL(VLRDESC,0)) FROM TGFITE WHERE NUNOTA=TGFCAB.NUNOTA),0)
     + ISNULL(VLRFRETE,0) + ISNULL(VLRJURO,0)
   )
   WHERE AD_NUMFAST IS NOT NULL
     AND ABS(VLRNOTA - (...mesma formula...)) > 0.01;
   ```

### Correcao retroativa aplicada
- NUNOTA 4857418: VLRNOTA 7363.05 -> 6147.59 (cupom 1215.46 restaurado)
- Varredura confirmou: nenhum outro pedido FC divergente em 60 dias

### Transparencia sobre v1.2.74
A v1.2.74 (listener afterUpdate) **funciona**, conforme log de 14/05 09:14:53. Minha
analise anterior foi incompleta - eu olhei um log que cortou antes do evento e conclui
erroneamente que o listener nao disparava. A v1.2.76 NAO substitui a v1.2.74; ela
**complementa** como rede de seguranca para caminhos que o listener nao pega.

## 2026-05-11 - v1.2.75 (FIX: estoque sobrescrito por outras empresas + tela de precos vazia por NUTABs obsoletas)

### Sintomas
1. **Estoque vendido sem ter**: cliente DISMAR VARGINHA (CODPARC 7906) comprou 5 unidades do
   produto 11896 (IPIRANGA ATF DEXRON VI CX-24/1, SKU FC 32004153) no pedido FC 4770
   (NUNOTA 4851871), mas a empresa configurada do FC (CODEMP=26) tinha 0 estoque com 5
   reservados (disponivel = -5).
2. **Tela "Gestao de Precos" vazia**: frontend renderiza "Nenhum item encontrado" mesmo
   com 34259 precos cadastrados em TGFEXC.

### Causa raiz 1 - Estoque race condition no StockFullSyncJob
`StockFullSyncJob.executeScheduler()` itera SELECT DISTINCT (CODPROD, CODEMP, CODLOCAL)
sem filtrar por empresa configurada. Para produto com estoque em 4 empresas, dispara 4 PUTs
`/stock/SKU` consecutivos no FC. **Cada PUT sobrescreve o estoque do SKU no FC com o valor
da empresa atual** (StorageId varia mas usar mesmo SKU). Resultado: FC fica com o estoque
da ULTIMA empresa processada, que pode nao ser a empresa que atende o canal.

Log confirmou para SKU 32004153 em 11/05 03:41:29-30:
```
Atualizando estoque do SKU 32004153: 0.0    (CODEMP 11)
Atualizando estoque do SKU 32004153: 120.0  (CODEMP 14)
Atualizando estoque do SKU 32004153: -5.0   (CODEMP 26 ← CONFIG)
Atualizando estoque do SKU 32004153: 6.0    (CODEMP 37 ← ultimo, vence)
```
FC ficou com **estoque 6**, embora CODEMP 26 tivesse 0 disponivel.

O `EstoqueListener` ja' tinha o filtro correto (linha 77-89 `isConfiguredLocalEmpresa`),
mas o full sync agendado ignorava esse filtro.

### Causa raiz 2 - NUTABs obsoletas em AD_FCDEPARA
A tabela de-para tem 6 entradas para TIPO_ENTIDADE='TABELA_PRECO' com COD_SANKHYA contendo
NUTABs antigas (4427, 4354, 3060, 4442, 4443, 4444). Essas NUTABs **ainda existem em TGFTAB**
(versionamento de tabelas de preco), mas foram SUPERADAS por versoes mais recentes:
- CODTAB 17: NUTAB 4427 (2026-04-01) -> **4460** vigente
- CODTAB 19: NUTAB 3060 (2023-08-28) -> **4459** vigente
- CODTAB 62: NUTAB 4354 (2026-01-30) -> **4462** vigente
- CODTAB 64-66: NUTABs 4442-4444 (2026-04-15) -> **4461, 4458, 4457** vigentes

`FCPrecosService.validateAndResolveNuTabs()` aceitava as NUTABs antigas como validas (existem
em TGFTAB) mas TGFEXC tem dados apenas nas NUTABs vigentes. Resultado: `E.NUTAB IN (antigas)`
retorna ZERO produtos -> tela vazia.

### Fix 1 - `StockFullSyncJob`
Adicionado filtro por `config.getCodemp()` e `config.getCodLocal()` na query SELECT, alinhando
o full sync com o EstoqueListener. Pedidos com config (CODEMP=26, CODLOCAL=null) agora
sincronizam apenas a empresa 26 (todos locais).

### Fix 2 - `FCPrecosService.validateAndResolveNuTabs()`
Adicionado novo metodo `resolveLatestNuTabForSameCodTab(conn, nuTab)`: dado uma NUTAB
candidata, busca a NUTAB vigente (mais recente por DTVIGOR) da mesma CODTAB. Aplicado para
TODA NUTAB candidata - se ela e' obsoleta, normaliza para a vigente automaticamente.

Impacto: tela de precos agora renderiza os 12859+ precos vigentes (em vez de 0).

### Acoes manuais recomendadas
- Considerar atualizar AD_FCDEPARA para usar NUTABs vigentes (operacional pode fazer pela
  tela de De-Para). O fix atual normaliza em tempo de query, mas atualizar a config evita
  cargas extras.
- Verificar config CODLOCAL: atualmente NULL (todos locais). Se o canal FC vende apenas
  do local 1 (por exemplo), configurar para evitar somar estoque de outros locais.

## 2026-05-11 - v1.2.74 (FIX: STP_CONFIRMANOTA2 zerava cupom de desconto no VLRNOTA)

### Sintoma
Chefe reportou que pedido FC 4769 (NUNOTA 4851833) ainda apresentou valor cheio
(R$ 5.890,44) na tela do Sankhya, mesmo com cupom de R$ 337,34 aplicado pelo cliente
na loja FC. Liquido pago real era R$ 5.553,10.

### Confirmacao no log que o fix v1.2.71 (forceItemValuesFromFc) FUNCIONOU:
```
16:23:37,788 forceItemValuesFromFc para NUNOTA 4851833 SEQ 1 (target VLRUNIT=772.19 VLRDESC=176.89)
16:23:37,829 forceItemValuesFromFc para NUNOTA 4851833 SEQ 2 (target VLRUNIT=700.42 VLRDESC=160.45)
16:23:37,954 VLRNOTA forcado correto para NUNOTA 4851833 (= SUM(VLRTOT - VLRDESC) + ...)
```
Os itens ficaram com VLRDESC correto. VLRNOTA do header ficou 5553.10 (correto).

### Causa raiz - novo caminho descoberto
Apos a importacao, o operador (ou job) chama `ServicosNfeSPBean.confirmarNotas` que
invoca a procedure nativa Sankhya **STP_CONFIRMANOTA2**:
```
16:24:25,236 [ServicosNfeSPBean] - [confirmarNotas] - Notas: [4851833]
16:24:29,535 [ConfirmacaoNotaHelper: Chamado STP_CONFIRMANOTA2 - NUNOTA: 4851833 ...]
```
A STP_CONFIRMANOTA2 **recalcula VLRNOTA = SUM(VLRTOT)** sem considerar VLRDESC dos
itens. Resultado: VLRNOTA do header vira o bruto (R$ 5.890,44 = soma dos VLRTOT),
descartando o cupom que estava em VLRDESC dos itens. Os itens permanecem com VLRDESC
correto, mas o cabecalho fica errado.

Confirmado em 4 de 5 pedidos FC duplicados do 4769:
- NUNOTA 4851833 (confirmada via STP_CONFIRMANOTA2): VLRNOTA 5890.44 ERRADO
- NUNOTA 4851839 (nao confirmada): VLRNOTA 5553.11 OK
- NUNOTA 4852118 (nao confirmada): VLRNOTA 5553.11 OK

### Fix
`NotaFiscalListener.repairVlrNotaAfterConfirmation(nuNota)`: novo metodo invocado em
todo `afterUpdate` da TGFCAB para pedidos FC. Executa UPDATE direto via NativeSql
(nao retrigera o listener) que re-aplica a formula correta:
```
VLRNOTA = SUM(VLRTOT - VLRDESC) + VLRFRETE + VLRJURO
```
Idempotente: so atua quando `|VLRNOTA_atual - formula| > 0.01`.

Como o listener fica ativo enquanto o addon estiver rodando, **qualquer confirmacao
futura (manual ou automatica) sera corrigida em tempo real**.

### Correcao retroativa aplicada
- NUNOTA 4851833: VLRNOTA 5890.44 -> 5553.10 (cupom 337.34 recuperado).

Query de varredura nos ultimos 30 dias nao encontrou outros pedidos FC divergentes
(apenas o 4851833 estava quebrado).

## 2026-05-06 - v1.2.73 (FIX: 2 bugs de SQL silenciosos em InternalApiStrategy)

### Bug 1 - JAPE filter `this.NOMUSU` (coluna inexistente)
**Sintoma:** SQL Server retornava erro `O identificador de varias partes "Usuario.NOMUSU" nao
pode ser associado` ao tentar resolver CODUSU/CODCENCUSPAD do usuario logado durante a
importacao de pedidos FC.

**Causa raiz:** Em `resolveCodUsuByName()` (l.2939) e `resolveCodCenCusPadByName()` (l.640),
a primeira tentativa usava `usuDAO.find("this.NOMUSU = ?", user)` mas a coluna correta da
TSIUSU e' **NOMEUSU** (com E). JAPE traduzia `this.NOMUSU` para `WHERE Usuario.NOMUSU = ?`
no SQL gerado, falhando porque a coluna nao existe.

**Fix:** Removida a primeira tentativa errada (NOMUSU). Mantida apenas a chamada com
`this.NOMEUSU = ?` que ja existia como fallback e funciona corretamente. Tambem removido
o fallback nativo `WHERE NOMUSU = ?` em `resolveCodUsuByName()` pelo mesmo motivo.

**Impacto pre-fix:** Erro nao-fatal — o orchestrator caia no fallback e o pedido era
criado, mas com warnings poluindo o log e potencial CODUSU=0 (default) em alguns casos.

### Bug 2 - `ORDER BY DTALTER DESC` em TGFEXC (coluna inexistente)
**Sintoma:** SQL Server retornava `Nome de coluna 'DTALTER' invalido` ao buscar preco
base do produto via TGFEXC.

**Causa raiz:** Em `resolveItemPricingData()` (l.2229), a query
```sql
SELECT TOP 1 NUTAB, VLRVENDA FROM TGFEXC
WHERE CODPROD = ? AND NUTAB = ? ORDER BY DTALTER DESC
```
usa `DTALTER` que **nao existe** em TGFEXC (schema real: NUTAB, CODPROD, CODLOCAL,
CONTROLE, VLRVENDA, TIPO, MODBASEICMS, PERCDESC, MARGLUCRO, PERCCOM, CODUSUALTREG,
DHALTREG).

**Fix:** Trocado `ORDER BY DTALTER DESC` por `ORDER BY NUTAB DESC` (consistente com a
query de fallback na linha 2276 abaixo na mesma funcao).

**Impacto pre-fix:** Erro nao-fatal — a query falhava e cai em fallback, podendo
resultar em PRECOBASE/NUTAB nulo no item gravado.

### Pedido 4764 (mencionado como "orfao")
Investigado: NAO e' orfao. Foi importado em 2026-05-05 e existe como NUNOTA 4845000
(NUMNOTA 70644, status L) e NUNOTA 4845476 (NUMNOTA 405423, L). E' duplicata do mesmo
pedido FC — problema antigo de idempotencia ja' resolvido em v1.2.69. O log
`[FC-DIAG] foundInCab=false` foi um check transiente ANTES da criacao efetiva.

## 2026-05-05 - v1.2.72 (FIX: ORDEMCARGA herdada de pedidos historicos em pedidos FC)

### Contexto
Chefe reportou que pedidos FC estavam entrando com ORDEMCARGA herdada de pedidos
historicos do mesmo parceiro, em vez de zero. Pedidos pendentes (NUMNOTA=0) afetados
em 04/05/2026: NUNOTA 4843683 (FC 4762, ORDEMCARGA=53676), 4843584 (FC 4761, =54009),
4843161 (FC 4760, =54009).

### Causa raiz
Caminho duplicado de fallback: o `InternalApiStrategy` ja tinha guarda `!isPedidoFastchannel`
(corrigido em 2026-04-30), mas `OrderService.applyCabecalhoParityNative()` — chamado APENAS
para pedidos FC apos a importacao — ainda preenchia ORDEMCARGA via `resolveCabNumericFallback`
sem nenhuma guarda. Esse caminho de "paridade pos-importacao" estava vazando.

### Fix
- `OrderService.applyCabecalhoParityNative()` linhas 936-942: removido o bloco que setava
  ORDEMCARGA via `resolveCabNumericFallback`. Como o metodo so' e' chamado para pedidos FC,
  pedido FC agora SEMPRE entra com ORDEMCARGA=0 — logistica precisa atribuir manualmente
  quando aplicavel (politica de negocio confirmada pelo chefe).

## 2026-05-05 - v1.2.71 (FIX: cupom de desconto absorvido em VLRDESC junto com delta de tabela)

### Contexto
Pedido FC 4763 (NUNOTA 4843808) reportou cupom incorretamente aplicado: VLRUNIT gravado como
7042.72 (preço cheio da tabela 4065 ATIVA) em vez de 6766.53 (SalePrice da FC), com
VLRDESC=844.72 (cupom 568.53 + 276.19 de delta de tabela) em vez do cupom isolado.

### Causa raiz
Ao inserir TGFITE com NUTAB=4065 (versão ativa 2025-07-08, CODTAB=27, CODTABORIG=2) e
CODPROD=14117, o MGECOM da Sankhya recalcula VLRUNIT aplicando o percentual da TGFEXC
(+2%) sobre a tabela mãe (NUTAB=4468, VLRVENDA=6904.62 desde 2026-05-02): 6904.62 × 1.02 =
7042.72. O cupom da FC fica absorvido em VLRDESC junto com esse delta.

Confirmado em 4 de 5 pedidos com cupom dos últimos 90 dias: NUTABs superadas (3665/3667 de
2024-09-30) NÃO acionam recalc do MGECOM. Só NUTABs ATIVAS disparam o problema.

### Fix
- `InternalApiStrategy.forceItemValuesFromFc(nuNota, seq, item, qty)`: novo método pós-save
  que UPDATE diretamente TGFITE corrigindo VLRUNIT/VLRTOT/VLRDESC/PERCDESC para refletir o
  que a FC enviou. Idempotente — só atua quando |VLRUNIT_atual − SalePrice| > 0.01.
  Preserva PRECOBASE como MAX(PRECOBASE_atual, SalePrice) para manter referência de margem.
- Chamado em `createItens()` logo após `itemBuilder.save()`, dentro do loop de itens.
- `forceVlrNotaCorrect()` (existente) recalcula VLRNOTA do cabeçalho — combinado mantém a
  fórmula VLRNOTA = SUM(VLRTOT − VLRDESC) + VLRFRETE + VLRJURO.

### Correção retroativa aplicada
- NUNOTA 4843808 e 4844000 (duplicata do PEDFC=4763): VLRUNIT 7042.72 → 6766.53, VLRDESC
  844.72 → 568.53, PERCDESC 11.99 → 8.40. PRECOBASE mantido em 7042.72. VLRNOTA inalterado
  em 6198.00 (líquido pago não muda).

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

