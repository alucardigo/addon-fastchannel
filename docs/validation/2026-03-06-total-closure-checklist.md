# Checklist de Fechamento Total - Divergencias Pedido Fastchannel x Legado

Data: 2026-03-06
Escopo: pedidos 4473/4474/4475 (NUNOTA 4737326/4737321/4737315) comparados ao legado (NUNOTA 4732539).

## TGFCAB

- [x] `STATUSNOTA` forcado para `L` na criacao/paridade
- [x] `PENDENTE` forcado para `N` na criacao/paridade
- [x] `APROVADO` forcado para `N` na criacao/paridade
- [x] `ISSRETIDO` forcado para `N` na criacao/paridade
- [x] `CIF_FOB = C`
- [x] `CODUSU` preenchido com usuario integracao
- [x] `CODUSUINC` preenchido com usuario integracao
- [x] `ORDEMCARGA` com fallback do historico TGFCAB
- [x] `CODPARCTRANSP` com fallback parceiro/historico TGFCAB
- [x] `QTDVOL` preenchido (1)
- [x] `PESO` preenchido por soma de itens/produto
- [x] `PESOBRUTO` preenchido por soma de itens/produto
- [x] `HISTCONFIG` preenchido (`S`)
- [x] `TPRETISS` preenchido (`1`)
- [x] `TOTALCUSTOPROD` preenchido (soma item custo*qtd)
- [x] `TOTALCUSTOSERV` preenchido (0)
- [x] `VLRSTEXTRANOTATOT` preenchido (0)
- [x] `VLRREPREDTOTSEMDESC` preenchido (0)
- [x] `SUMVLRIIOUTNOTA` preenchido (0)
- [x] `SOMICMSNFENAC` preenchido (0)
- [x] `SOMPISCOFNFENAC` preenchido (0)
- [x] TOP 403 aplicado
- [x] `AD_CODVENDEXEC = 167` aplicado
- [x] `TGFTPV.AD_IDFAST` usado para `CODTIPVENDA`

## TGFITE

- [x] `NUTAB` preenchido (criacao + fallback amplo em TGFEXC + paridade)
- [x] `USOPROD` normalizado para pedido (`R`) e reforcado na paridade
- [x] `VLRCUS` preenchido
- [x] `CUSTO` preenchido
- [x] `PRECOBASE` preenchido
- [x] `ATUALESTTERC` preenchido (`N`)
- [x] `TERCEIROS`/`TERCEIRO` preenchido (`N`)
- [x] `QTDENTREGUE` alinhado com `QTDNEG`
- [x] `RESERVA` forcado para `S`
- [x] `CODTRIB` preenchido via produto/fallback `60`
- [x] `CODVEND` preenchido
- [x] `CODUSU` preenchido
- [x] `STATUSNOTA` forcado para `L`

## Guard-rails

- [x] JAPE session/transaction valida em abertura/fechamento
- [x] Build `:model:compileJava` ok
- [ ] Validacao SQL final pos-publicacao (homolog/prod) pendente
