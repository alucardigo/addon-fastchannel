# Change: Add Estoque/Precos UI with configurable sources

## Why
Clientes precisam de telas de Estoque e Precos no addon, alinhadas ao padrao visual existente, com informacoes mais completas e configuracao centralizada no banco.

## What Changes
- Adiciona telas de Estoque e Precos com filtros, comparacao e acoes em lote
- Inclui configuracao global no AD_FCCONFIG para habilitar fontes 1/2/3 e padrao da opcao
- Backend exposto para listar, comparar, forcar sync e reprocessar por fonte

## Impact
- Affected specs: fastchannel-ui, fastchannel-config
- Affected code: web services, HTML5 UI, datadictionary/AD_FCCONFIG
