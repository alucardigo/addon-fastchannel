# Validacao SQL em Homologacao (2026-02-12)

Ambiente validado:
- Host: `172.16.127.12`
- Porta: `1433`
- Banco: `SANKHYA_TESTE`
- Usuario: `sankhya`
- Driver: `pymssql` (SQL Server 2019)

## Resultado geral

- Consultas validadas (execucao real): **22**
- Sucesso: **22**
- Falhas: **0** (considerando as queries efetivamente usadas pelo codigo atual com fallback dinamico)

## Observacoes importantes de schema

- `TGFEXC` **nao possui** as colunas: `ATIVO`, `DTINIC`, `DTFIM`, `CODEMP`.
- O codigo atual da tela de precos usa fallback por metadados (`supportsColumn`) para esse caso.
- `AD_FCLOG` possui `DH_LOG`, `DH_REGISTRO` e `DETALHES`.
- `AD_FCPEDIDO` possui `STATUS_IMPORT`, `DH_IMPORTACAO` e `DH_PEDIDO`.
- `TGFTAB` possui `NUTAB`, `CODTAB`, `DTVIGOR`, mas nao possui `DESCRTAB` (o codigo atual ja usa descoberta dinamica da coluna de descricao).

## Pontos validados por modulo

- Estoque: contagem/listagem com `OUTER APPLY`, fila e logs.
- Precos:
  - fonte Sankhya (com fallback de colunas ausentes),
  - fonte API (base SQL de SKUs),
  - fonte fila/logs.
- Pedidos: listagem, detalhe e reprocessamento.
- Fila: estatisticas, listagem, reset de retry.
- Logs: listagem e limpeza por data.
- Dashboard: contadores de fila/pedidos/sync/logs.
- Configuracao: leitura de `AD_FCCONFIG`.
- De-Para: empresas, locais, tabelas de preco, mapeamentos.
- Funcao de preco: `[sankhya].SNK_GET_PRECO(...)` executando com sucesso.

## Nota sobre o documento `sql-consultas-fastchannel.md`

O arquivo possui:
- consultas do **add-on atual** (validadas),
- um bloco grande de consultas do **legado** (contracts/models), que nao representa o runtime do add-on atual.

Para o objetivo de producao deste addon, foi validado o conjunto realmente utilizado pelos services Java atuais.
