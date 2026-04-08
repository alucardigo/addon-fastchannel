# Fastchannel Homolog Fixes Plan

**Data:** 2026-03-23

## Objetivo

Corrigir os problemas reportados em homologacao:

- data/hora exibidas incorretamente nas telas;
- cursor de importacao de pedidos avancando indevidamente;
- falhas de `Data truncation` ao persistir `AD_FCPEDIDO`;
- telas de estoque e precos exibindo conjunto artificialmente limitado de registros;
- texto de paginacao inconsistente nas telas.

## Etapas

1. Ajustar `OrderService` para nao avancar `LAST_ORDER_SYNC` para o horario atual quando nao houver importacao real.
2. Tornar o `upsert` de `AD_FCPEDIDO` resiliente a divergencias reais de tamanho de coluna no banco via metadata JDBC.
3. Remover o filtro oculto por de-para de produto das listagens de estoque/precos, preservando apenas filtros explicitamente configurados pelo usuario.
4. Padronizar renderizacao de datas no frontend para timestamps sem timezone e corrigir informacao de paginacao.
5. Executar validacao local com build/testes direcionados.
