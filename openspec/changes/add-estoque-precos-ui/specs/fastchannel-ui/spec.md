## ADDED Requirements
### Requirement: Estoque UI
O sistema MUST fornecer uma tela de Estoque com filtros, listagem e acoes de sincronizacao e reprocessamento.

#### Scenario: Listagem com filtros
- **WHEN** o usuario aplica filtros de SKU, produto ou datas
- **THEN** a tela mostra apenas registros correspondentes

#### Scenario: Acoes em lote
- **WHEN** o usuario seleciona varios itens
- **THEN** o sistema permite forcar sync ou reprocessar em lote

### Requirement: Precos UI
O sistema MUST fornecer uma tela de Precos com filtros, listagem e comparacao com API.

#### Scenario: Comparacao com API
- **WHEN** o usuario solicita comparacao
- **THEN** o sistema mostra preco Sankhya e preco Fastchannel com delta

## MODIFIED Requirements
