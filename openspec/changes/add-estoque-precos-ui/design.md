## Context
Adicionar telas de Estoque e Precos no addon seguindo o padrao visual existente. Deve permitir consultar dados do Sankhya (opcao 1), API Fastchannel (opcao 2) e fila/logs (opcao 3), com configuracao global persistida no banco.

## Goals / Non-Goals
- Goals: telas completas com filtros, comparacao, acoes em lote; configuracao centralizada; compatibilidade com legado
- Non-Goals: refatorar arquitetura geral do addon; mudar contrato de integracoes existentes

## Decisions
- Usar AD_FCCONFIG para armazenar flags globais de habilitar opcao 2/3 e opcao padrao
- UI usa as mesmas classes e componentes do addon; nao replica layout do legado
- Back-end fornece listagem base (opcao 1/3) e comparacao com API (opcao 2)

## Risks / Trade-offs
- Dependencia da disponibilidade da API Fastchannel para comparacao
- Campos novos exigem ajuste no datadictionary e scripts de criacao

## Migration Plan
- Criar colunas novas no AD_FCCONFIG via datadictionary (autoDDL) e script DDL de referencia
- Atualizar telas para ler configuracao ao carregar

## Open Questions
- Nenhuma
