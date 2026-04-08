# Change: Update Fastchannel order import to use Sankhya service flow

## Why
Pedidos estão sendo inseridos diretamente no banco sem passar pelas regras do core do Sankhya. Isso gera inconsistências e viola a premissa de validação de regras de negócio.

## What Changes
- Importação de pedidos passa a usar o serviço nativo `CACSP.incluirNota` via ServiceInvoker (com fallback HTTP).
- Proibição de criação de produtos a partir da Fastchannel.
- Correção de gaps de mapeamento para equivaler ao legado (CODTIPVENDA, CODVEND, CODNAT, CODCENCUS, VLRFRETE, AD_NUMFAST, descontos).
- Nova configuração em `AD_FCCONFIG` para bloquear atualização de status/NF/tracking/sync no Fastchannel.
- Resiliência: falha de um pedido não interrompe o processamento dos demais.

## Impact
- Affected specs: fastchannel-orders (novo)
- Affected code: OrderService, FastchannelOrdersClient, NotaFiscalListener, ReenviarStatusPedidoAction, FastchannelConfig, dbscripts
