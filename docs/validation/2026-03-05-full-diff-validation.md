# Validacao Completa de Divergencias (2026-03-05)

## Escopo
- Comparativo pedido legado x novo (cabecalho `TGFCAB` e itens `TGFITE`) informado pelo usuario.
- Evidencias usadas:
  - Log: `X:\tmp_serverlog_20260305_181024\server.log`
  - Codigo: `InternalApiStrategy`, `OrderService`, `FastchannelHeaderMappingService`.

## Status dos pontos exigidos pelo usuario

| Regra/Campo | Status | Evidencia |
|---|---|---|
| TOP 403 | Validado | `InternalApiStrategy.preferTop403()` |
| `TGFCAB.AD_CODVENDEXEC = 167` | Validado | Set em criacao (`InternalApiStrategy`) e reforco na paridade (`OrderService`) |
| `TGFTPV.AD_IDFAST` para resolver `CODTIPVENDA` | Validado | `FastchannelHeaderMappingService.resolveTipNeg()` + query em `TGFTPV.AD_IDFAST` |
| Nao confirmar pedido se validacao falhar | Parcialmente validado | Pedidos do lote 4473/4474/4475 ficaram `STATUSNOTA=P` e `PENDENTE=S` (nao confirmados) |
| `TGFITE.NUTAB` via `TGFEXC` | Validado | Set na criacao (`InternalApiStrategy.resolveItemPricingData`) + reforco pos-importacao (`OrderService`) |
| `TGFITE.PRECOBASE` | Validado | Criacao + reforco pos-importacao (incluindo quando valor 0) |
| `TGFITE.CUSTO` | Validado | Criacao + reforco pos-importacao (incluindo quando valor 0) |
| `TGFITE.VLRCUS` | Validado | Criacao + reforco pos-importacao (incluindo quando valor 0) |
| `TGFITE.USOPROD` | Validado | Criacao + reforco pos-importacao (corrige divergencia com cadastro produto) |
| `TGFITE.ATUALESTTERC` | Validado | Criacao + reforco pos-importacao |
| `TGFITE.TERCEIROS` | Validado | Criacao prioriza `TERCEIROS`; fallback `TERCEIRO`; paridade idem |
| `TGFCAB.CIF_FOB = C` | Validado | Criacao + reforco pos-importacao |

## Divergencias ainda observadas no comparativo (nao cobertas totalmente)

| Campo | Situacao atual | Observacao |
|---|---|---|
| `TGFCAB.STATUSNOTA` (`P` vs `L`) | Nao fechado | Depende de regras nativas de liberacao/confirmacao (processo posterior) |
| `TGFCAB.PENDENTE` (`S` vs `N`) | Nao fechado | Consequencia do status do pedido e entrega/faturamento |
| `TGFCAB.CODUSU`/`CODUSUINC` | Nao fechado | Ainda aparecem `0` em casos novos; nao ha set explicito no fluxo |
| `TGFITE.QTDENTREGUE` (`0` vs `1`) | Nao fechado | Nao ha preenchimento explicito no fluxo de criacao |
| `TGFITE.RESERVA` (`N` vs `S`) | Nao fechado | Nao ha set explicito; comportamento depende de regras da TOP/estoque |
| `TGFITE.CODTRIB` | Nao fechado | Nao ha set explicito; depende de tributacao padrao do ambiente |
| `TGFITE.CODVEND`/`CODUSU` | Nao fechado | Nao ha set explicito em item |

## Falha raiz identificada e corrigida
- Antes: `Falha ao aplicar paridade pos-importacao ... NUNOTA ... Nao existe uma sessao JAPE aberta`.
- Agora: paridade pos-importacao roda dentro de `JapeSession.open() + execWithTX` em `OrderService`.

## Validacao de abertura/fechamento de sessoes
- Varredura estatica em `model/src/main/java`:
  - Sem vazamento evidente de `JdbcWrapper.openSession()/closeSession()`.
  - `OrderService` usa fechamento de Jape via helper `closeJapeSession(hnd)` para ambas as aberturas.

## Limitacao desta validacao
- Nao foi possivel executar comparativo SQL direto no banco deste ambiente (host SQL inacessivel a partir da sessao atual).
- Portanto, a validacao final de dados em massa (todos os campos da tabela) depende de execucao no ambiente com acesso ao SQL.
