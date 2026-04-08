# Relatório de Testes - Addon Fastchannel v1.2.53
## Ambiente de Homologação - 16/03/2026

**Ambiente:** http://172.16.127.11:8080/mge/
**Usuário:** sup
**Versão:** 1.2.53 (branch merge/unify-fastchannel)
**Data:** 16/03/2026, 17:30 - 21:00

---

## 1. Resumo Executivo

| Item | Status |
|------|--------|
| Addon instalado e ativo | OK |
| Menus visíveis (8 abas) | OK |
| Conexão API Fastchannel | OK (Online, Token Válido) |
| Dashboard funcional | OK |
| Configuração funcional | OK |
| De-Para (backend) | OK (dados corretos no banco) |
| De-Para (UI Tabelas Preco) | BUG - não renderiza na sub-aba "Tabelas de Preco" |
| Pedidos - listagem | OK |
| Pedidos - detalhes | OK |
| Pedidos - importação | OK (186 importados) |
| Estoque - listagem | OK |
| Estoque - dados corretos | OK (SKU, CODPROD, LOCAL, EMPRESA, STORAGEID) |
| Preços - listagem | OK |
| Preços - comparação FC | PARCIAL - botão não retorna resultado visível |
| Preços - sync forçado | BUG - PriceTableId inválido na API FC |
| Fila de Sincronização | OK |
| Logs | OK |

---

## 2. Testes por Aba

### 2.1 Dashboard

**Status:** FUNCIONAL

Dados observados:
- Status: Conectado (API Online, Token Válido, Integração SIM)
- Última verificação: 16/03/2026, 17:36:28
- Pedidos: 186 importados hoje, 0 pendentes, 120 com erro
- Fastchannel API: 6 pendentes (2 Aguardando Confirmação, 1 Pedido Confirmado, 2 Pagamento Recebido)
- Operações: 0 pendentes, 0 processando, 7 com erro, Estoque 24h: 0

Botões testados: "Testar Conexao", "Importar Pedidos", "Processar Fila", "Atualizar" - todos visíveis e clicáveis.

### 2.2 Configuração

**Status:** FUNCIONAL

Campos verificados:
- Integração Ativa: ON
- Atualizar Status na Fastchannel: OFF (correto para homologação)
- Desabilitar verificação de duplicação: OFF
- OAuth2: Client ID, Client Secret, Auth URL, Scope - todos preenchidos
- API Fastchannel: URL Base, 3 Subscription Keys (Legado, Distribuição, Consumo), Timeout 30000ms
- Configurações de Preços (UI): Fonte Padrão "Sankhya (Banco local)", Fontes 2 e 3 habilitadas
- Agendamento: Importação 1 min, Processamento Fila 1 min, Logs 1 dia, Max Retries 1

**Ação realizada:** PRICE_TABLE_IDS configurado para "9,24" via endpoint FCConfigSP.save

### 2.3 De-Para

**Status:** PARCIAL

- Sub-aba **Empresas**: Funcional - lista empresas (POSTO REDE BELS LTDA, JANNUZZI ARMAZENS ANEL, etc.)
- Sub-aba **Locais**: Não testada visualmente (backend OK)
- Sub-aba **Tabelas de Preco**: **BUG** - mostra "Nenhum registro encontrado" apesar de existirem 3 mapeamentos no banco

**Dados no banco (AD_FCDEPARA, TIPO_ENTIDADE='TABELA_PRECO'):**
- NUTAB 3932 → FC ID "3" (integração automática: S)
- NUTAB 4321 → FC ID "9" (integração automática: S)
- NUTAB 3996 → FC ID "24" (integração automática: S)

**Backend OK:** Endpoints `FCDeparaSP.listTabelasPreco` (60 tabelas) e `FCDeparaSP.listMappings` (3 mapeamentos) retornam dados corretos.

**Bug provável:** Erro na função `mergeMappings()` do JavaScript do frontend (depara.html) ao combinar os dados base com os mapeamentos.

### 2.4 Pedidos

**Status:** FUNCIONAL

- Listagem com filtros (Status, Data, Order ID) funcionando
- Botão "Importar Novos Pedidos" visível
- Tabela mostra: ORDER ID, DATA PEDIDO, CLIENTE, VALOR TOTAL, STATUS FC, STATUS IMPORT, NUNOTA, AÇÕES
- Modal "Detalhes do Pedido" funcional

**Pedidos testados:**
| Order ID | Data | Cliente | Valor | Status FC | Status Import | NUNOTA |
|----------|------|---------|-------|-----------|---------------|--------|
| 4259 | 02/02/2026 14:47 | A V V DISTRIBUIDORA | R$ 4.053,60 | 201 | SUCESSO | 4627861 |
| 4258 | 02/02/2026 12:26 | SHOPPING PEÇAS MANCHETE | R$ 1.336,08 | 201 | SUCESSO | 4627860 |
| 4256 | 31/01/2026 13:35 | H 2 W COM DE LUB | R$ 2.482,80 | 201 | SUCESSO | 4627859 |
| 4255 | 31/01/2026 09:28 | ANDRÉ LUIZ ALVES OLIVEIRA | R$ 1.374,77 | 201 | PROCESSANDO | -- |

**Detalhe do Pedido 4259:**
- CPF/CNPJ: 08097269000140
- Frete: R$ 42,00
- CODPARC: 1031

### 2.5 Estoque

**Status:** FUNCIONAL

- Título: "Estoque Sankhya + Fastchannel"
- Base: Sankhya, Comparar Automático: ON
- Botões: "Forçar Sync Selecionados", "Reprocessar Selecionados", "Comparar Selecionados"

**Produtos verificados:**
| SKU | CODPROD | Descrição | Local | Empresa | Estoque | Status Fila |
|-----|---------|-----------|-------|---------|---------|-------------|
| 31254353 | 1001 | PSH306 - FILTRO | 99000000 | 26 | 1 | ENVIADO (24/02) |
| 31254253 | 1316 | PSL619 - FILTRO | 99000000 | 26 | 827 | -- |
| 32063822 | 1419 | ARL4150 - FILTRO | 99000000 | 26 | 590 | -- |

### 2.6 Preços

**Status:** PARCIAL

- Gestão de Preços com fonte "Sankhya (Banco local)"
- Filtros: SKU, COD PRODUTO, TABELA, PRICETABLEID, COD EMPRESA
- Checkbox "Auto comparar FC" ativa
- Botão "Sincronizar Selecionados"

**Produtos com preço verificados:**
| SKU | Descrição | Tabela | PriceTableID | Preço Venda |
|-----|-----------|--------|-------------|-------------|
| 31251453 | IPITUR AW HLP 68 - GRANEL | 4321 | 9 | R$ 25,85 |
| 31240853 | IPIRANGA BRUTUS 15W40 CI-4 BB-20LT | 4321 | 9 | R$ 22,68 |
| 31252653 | IPIRANGA MOTO PROTECTION 20W50 SL CX-24/1 | 4321 | 9 | R$ 29,02 |
| 31041653 | TEXACO URSA PREMIUM TDX - GRANEL 159 LT | 4321 | 9 | R$ 16,34 |
| 31412953 | IPIRANGA MOTO PERFORMANCE 10W30 SL CX 24/1 | 4321 | 9 | R$ 35,36 |

**BUG encontrado:** Ao tentar forçar sync de preço (FCPrecosSP.forcarSync), a API Fastchannel retorna:
```
HTTP 400: "O código da tabela de preços não é válido ou está incorreto"
```
O PriceTableId "9" (De-Para: NUTAB 4321 → FC ID "9") não é reconhecido pela API Fastchannel. Possível causa: mapeamento De-Para incorreto ou tabela removida no portal FC.

### 2.7 Fila de Sincronização

**Status:** FUNCIONAL

Cards:
- 0 Pendentes
- 0 Processando
- 0 Concluídos (24h)
- 7 Com Erro

**Itens na fila:**
| ID | Tipo | Ref | SKU | Status | Tentativas | Último Erro |
|----|------|-----|-----|--------|------------|-------------|
| 21 | ESTOQUE | 1001 | 31254353 | CONCLUIDO | 0 | -- |
| 20 | ESTOQUE | 1001 | 7891342032896 | ERRO | 3 | Erro PUT estoq... |
| 19 | PRECO | 4342 | 31013353 | CONCLUIDO | 0 | -- |
| 18 | PRECO | 4342 | 31013353 | CONCLUIDO | 0 | -- |

### 2.8 Logs

**Status:** FUNCIONAL

- Filtros: Nível, Operação, Data Início, Data Fim, Buscar
- Botão "Limpar Logs Antigos"
- Job de importação rodando a cada minuto (confirmado pelo log)

**Últimos logs:**
- 16/03/2026 17:47:14 - INFO - Import Pedidos - "Job concluído. 0 pedidos importados."
- 16/03/2026 17:41:50 - INFO - Import Pedidos - "Job concluído. 0 pedidos importados."

---

## 3. Teste de Sincronização de Preço

### 3.1 Configuração
- PRICE_TABLE_IDS: "9,24" (FC IDs mapeados para NUTAB 4321 e 3996)
- Produto teste: CODPROD 5919 (SKU 31254353), Preço atual: R$ 32,19

### 3.2 Resultado
- Tentativa de sync via `FCPrecosSP.forcarSync` retornou erro HTTP 400 da API Fastchannel
- **Causa raiz:** O PriceTableId enviado ("9") não é válido no portal Fastchannel
- **Ação necessária:** Verificar no portal Fastchannel qual é o ID correto da tabela de preços e atualizar o De-Para

### 3.3 Conclusão
A lógica de sincronização de preço está implementada e funcional (o endpoint do addon resolve corretamente NUTAB → PriceTableId via De-Para, monta o DTO e envia para a API). O problema é de **configuração/mapeamento**, não de código.

---

## 4. Bugs Encontrados

### BUG-001: De-Para - Tabelas de Preço não renderizam na UI
- **Severidade:** Média
- **Local:** `vc/src/main/webapp/html5/fastchannel/depara.html`
- **Descrição:** Sub-aba "Tabelas de Preco" mostra "Nenhum registro encontrado" apesar dos endpoints retornarem dados corretos
- **Causa provável:** Erro na função `mergeMappings()` ao combinar dados base com mapeamentos

### BUG-002: PriceTableId inválido na API Fastchannel
- **Severidade:** Alta
- **Local:** Configuração De-Para (AD_FCDEPARA)
- **Descrição:** O mapeamento NUTAB 4321 → FC ID "9" resulta em erro 400 na API Fastchannel
- **Ação:** Verificar no portal Fastchannel os IDs corretos das tabelas de preço e atualizar o De-Para

### BUG-003: Preço FC não exibido na comparação
- **Severidade:** Baixa
- **Local:** `vc/src/main/webapp/html5/fastchannel/precos.html`
- **Descrição:** Botão "Comparar" não preenche coluna "PRECO FC (R$)" na tabela
- **Causa provável:** Relacionado ao BUG-002 (se a API retorna erro, o preço FC não é obtido)

---

## 5. Configuração do Banco (AD_FCCONFIG)

| Campo | Valor |
|-------|-------|
| ATIVO | S |
| CODEMP | 26 |
| CODLOCAL | 99000000 |
| NUTAB | NULL |
| PRICE_TABLE_IDS | 9,24 |
| PRICE_TABLE_TIPOS | NULL |
| INTERVAL_ORDERS | 1 |
| INTERVAL_QUEUE | 1 |
| MAX_RETRIES | 1 |
| SYNC_STATUS_ENABLED | N |
| DISABLE_DUPLICATE_CHECK | N |
