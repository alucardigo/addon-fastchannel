# Guia de Migração - Importação de Pedidos Fastchannel v2.0

## Visão Geral

Esta migração atualiza a importação de pedidos do Fastchannel para um sistema de **Três Camadas de Resiliência**, garantindo que o pedido seja inserido no Sankhya respeitando as regras de negócio (listeners e validações do core).

## ⚡ Estratégia de "Três Camadas" (Orquestrador)

O sistema agora tenta criar o pedido seguindo esta ordem de preferência:

1.  **InternalApiStrategy (JAPE)**: Usa o framework JAPE do Sankhya. É a preferencial pois dispara todos os *listeners* de entidade e validações internas, sendo a mais performática.
2.  **ServiceInvokerStrategy (XML/API)**: Caso o contexto JAPE não esteja disponível ou falhe, invoca o serviço `CACSP.incluirNota` internamente via reflexão.
3.  **HttpServiceStrategy (HTTP/XML)**: Último recurso. Realiza uma chamada externa via HTTP para a API de serviços do Sankhya, garantindo a integração mesmo em contextos isolados.

## Mudanças no Banco de Dados (Checklist de Deploy)

Para esta versão, novos campos de configuração e logs foram adicionados. **É obrigatório executar os scripts na pasta `dbscripts/`**:

1.  `dbscripts/migration_add_fc_config_defaults.sql`: Adiciona campos `CODNAT`, `CODCENCUS` e `CODVEND_PADRAO` na `AD_FCCONFIG`.
2.  `dbscripts/migration_add_fc_pedido_fields.sql`: Adiciona campos de rastreio e status (`STATUS_IMPORT`, `VALOR_TOTAL`, etc) na `AD_FCPEDIDO`.
3.  `dbscripts/migration_add_fc_log_fields.sql`: Ajusta a `AD_FCLOG` para o novo padrão de métricas e detalhes.
4.  `dbscripts/migration_add_sync_status_flag.sql`: Adiciona a flag `SYNC_STATUS_ENABLED`.

## Configuração de Estoque (SQL Server)

Para o envio correto de estoque ao Fastchannel, é necessário configurar o de-para em `AD_FCDEPARA`:

- **STOCK_STORAGE**: `CODLOCAL` -> `StorageId`
- **STOCK_RESELLER**: `CODEMP` -> `ResellerId`

Sem esses mapeamentos, o estoque será **ignorado** e um aviso será registrado no log.

## Melhorias de Resiliência

- **Independência de Pedidos**: A falha na importação de um pedido não trava mais o processamento dos demais na fila.
- **Métricas no Dashboard**: O dashboard agora exibe a taxa de sucesso das estratégias e o tempo médio de integração nas últimas 24h.
- **Normalização de Valores**: Valores vindos do Fastchannel (centavos) são automaticamente normalizados para reais (casas decimais) antes da inserção.
- **Fallback de Vendedor**: Caso o vendedor não seja encontrado, o sistema utiliza o fallback configurado (ou `198` como padrão legado) e emite um alerta no log.

## Arquivos Chave da Nova Arquitetura

- `model/.../service/strategy/OrderCreationOrchestrator.java`: O cérebro que gerencia os fallbacks.
- `model/.../service/strategy/InternalApiStrategy.java`: Implementação via JAPE (Primária).
- `model/.../service/OrderXmlBuilder.java`: Construtor do XML para as estratégias de serviço.
- `model/.../service/metrics/StrategyMetricDetails.java`: Modelo para persistência de métricas de performance.

## Como Validar o Deploy

1.  Acesse a tela de **Configuração Fastchannel**.
2.  Verifique se os novos campos (`Natureza`, `Centro de Custo`, `Vendedor Padrão`) aparecem e podem ser salvos.
3.  No **Dashboard**, verifique se o card "Métricas de Estratégia" aparece (pode levar 24h para popular totalmente).
4.  Consulte os **Logs** e verifique se a coluna "Detalhes" exibe o JSON das métricas para a operação `ORDER_METRIC`.

---
*Atualizado em 2026-02-02*
