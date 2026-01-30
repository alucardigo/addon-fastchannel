# Guia de Migração - Importação de Pedidos via Serviço

## Visão Geral

Esta migração atualiza a importação de pedidos do Fastchannel para usar o serviço nativo do Sankhya (`CACSP.incluirNota`) ao invés de inserção direta via JAPE. Isso garante que todas as regras de negócio do core sejam aplicadas aos pedidos importados.

## Data da Implementação

2026-01-30

## Mudanças Principais

### 1. Nova Flag de Sincronização

**Campo adicionado**: `AD_FCCONFIG.SYNC_STATUS_ENABLED`
- **Tipo**: VARCHAR(1)
- **Valores**: 'S' (Sim) ou 'N' (Não)
- **Default**: 'S' (habilitado)
- **Propósito**: Permite desabilitar sincronização de status/NF/tracking com Fastchannel

**Script de migração**: `dbscripts/migration_add_sync_status_flag.sql`

### 2. Fluxo de Importação Baseado em Serviço

**Antes**:
- Inserção direta em TGFCAB e TGFITE via JAPE
- Regras de negócio não eram aplicadas
- Criação automática de produtos

**Depois**:
- Uso do serviço `CACSP.incluirNota` via ServiceInvoker
- Fallback HTTP para `/mge/service.sbr` se ServiceInvoker não disponível
- Todas as regras de negócio do core são aplicadas
- Criação de produtos BLOQUEADA - pedido falha se produto não existir

### 3. Validação de Produtos

**Nova regra**: Produtos devem existir ANTES da importação
- Sistema valida todos os SKUs antes de criar o pedido
- Pedido falha completamente se algum produto não for encontrado
- Mensagem de erro clara: "Produto não encontrado para SKU: XXX. Criação de produto a partir do Fastchannel não é permitida."

### 4. Campos Mapeados (Paridade com Legado)

**Cabeçalho**:
- CODTIPVENDA - Tipo de venda
- CODVEND - Vendedor
- CODNAT - Natureza de operação
- CODCENCUS - Centro de custo
- VLRFRETE - Valor do frete
- AD_NUMFAST - Número do pedido Fastchannel

**Itens**:
- CODPROD - Código do produto
- CODVOL - Unidade de medida
- ORIGPROD - Origem do produto ('F' = Fastchannel)
- VLRUNIT - Valor unitário
- PERCDESC - Percentual de desconto
- QTDNEG - Quantidade negociada

### 5. Resiliência no Processamento

**Novo comportamento**: Falha em um pedido não interrompe os demais
- Cada pedido é processado independentemente
- Erros são logados individualmente
- Processamento continua para pedidos subsequentes

### 6. Controle de Sincronização

**Quando `SYNC_STATUS_ENABLED = 'N'`**:
- `OrderService.importPendingOrders()` - Não chama `markAsSynced()`
- `NotaFiscalListener.processNotaUpdate()` - Não processa atualizações
- `ReenviarStatusPedidoAction.doAction()` - Retorna erro se tentado manualmente

## Arquivos Modificados

### Novos Arquivos

1. `model/src/main/java/br/com/bellube/fastchannel/service/SankhyaServiceInvoker.java`
   - Encapsula chamada de serviço com fallback HTTP

2. `model/src/main/java/br/com/bellube/fastchannel/service/OrderXmlBuilder.java`
   - Constrói XML para `CACSP.incluirNota`

3. `dbscripts/migration_add_sync_status_flag.sql`
   - Script de migração para nova coluna

### Arquivos Modificados

1. `datadictionary/AD_FCCONFIG.xml`
   - Adicionado campo `SYNC_STATUS_ENABLED`

2. `model/src/main/java/br/com/bellube/fastchannel/config/FastchannelConfig.java`
   - Adicionado `syncStatusEnabled` e getter `isSyncStatusEnabled()`

3. `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
   - Método `importOrder()` refatorado para usar serviço
   - Adicionado `validateAllProductsExist()`
   - Adicionado `importOrderViaService()`
   - Adicionado verificação de `syncStatusEnabled` antes de `markAsSynced()`

4. `model/src/main/java/br/com/bellube/fastchannel/listener/NotaFiscalListener.java`
   - Adicionado verificação de `syncStatusEnabled` em `processNotaUpdate()`

5. `model/src/main/java/br/com/bellube/fastchannel/action/ReenviarStatusPedidoAction.java`
   - Adicionado verificação de `syncStatusEnabled` em `doAction()`

## Passos de Migração

### 1. Backup

```sql
-- Backup da tabela de configuração
SELECT * INTO AD_FCCONFIG_BACKUP FROM AD_FCCONFIG;
```

### 2. Executar Script de Migração

```bash
# SQL Server
sqlcmd -S <servidor> -d <database> -i dbscripts/migration_add_sync_status_flag.sql

# Oracle
sqlplus <usuario>/<senha>@<sid> @dbscripts/migration_add_sync_status_flag.sql
```

### 3. Build e Deploy

```bash
# Build
./gradlew.bat clean build

# Deploy
copy build\libs\Addon-FastChannel.ear X:\Wildfly_Clean\wildfly_producao\standalone\deployments\
```

### 4. Verificar Configuração

Acessar `/addon-fastchannel/html5/fastchannel/config.html` e verificar:
- Campo "Sincronizar Status com Fastchannel" deve estar visível
- Valor default deve ser "Sim"

### 5. Teste de Importação

1. Marcar um pedido como pendente no Fastchannel
2. Executar "Importar Pedidos Agora" na tela de configuração
3. Verificar logs para confirmar uso do serviço:
   - "Invocando servico via ServiceInvoker: CACSP.incluirNota"
   - Ou "Invocando servico via HTTP: CACSP.incluirNota" (fallback)
4. Verificar que pedido foi criado corretamente no Sankhya

### 6. Teste de Bloqueio de Produto

1. Criar pedido no Fastchannel com SKU não cadastrado
2. Tentar importar
3. Verificar erro: "Produto não encontrado para SKU: XXX"
4. Confirmar que pedido NÃO foi criado

### 7. Teste de Flag de Sincronização

1. Configurar `SYNC_STATUS_ENABLED = 'N'` no AD_FCCONFIG
2. Importar pedido
3. Verificar que `markAsSynced()` não foi chamado
4. Faturar pedido
5. Verificar que Fastchannel NÃO foi notificado
6. Restaurar `SYNC_STATUS_ENABLED = 'S'`

## Rollback

Se necessário reverter:

```sql
-- 1. Restaurar configuração
DELETE FROM AD_FCCONFIG;
INSERT INTO AD_FCCONFIG SELECT * FROM AD_FCCONFIG_BACKUP;

-- 2. Fazer deploy da versão anterior do addon
```

## Pontos de Atenção

### Produtos Não Cadastrados

**IMPORTANTE**: A partir desta versão, produtos devem estar cadastrados no Sankhya ANTES de serem vendidos no Fastchannel.

**Processo recomendado**:
1. Cadastrar produtos no Sankhya
2. Sincronizar com Fastchannel (se implementado)
3. Pedidos com produtos não cadastrados serão rejeitados

### Performance

O uso do serviço `CACSP.incluirNota` pode ser mais lento que inserção direta JAPE, mas garante consistência de dados.

**Recomendações**:
- Ajustar `BATCH_SIZE` se necessário (default: 50)
- Monitorar tempo de importação
- Ajustar `INTERVAL_ORDERS` se filas crescerem

### Sincronização Desabilitada

Quando `SYNC_STATUS_ENABLED = 'N'`:
- Fastchannel não será notificado de mudanças
- Status no Fastchannel pode ficar dessincronizado
- Usar apenas em casos específicos (testes, manutenção)

## Suporte

Para dúvidas ou problemas, verificar:
1. Logs em `AD_FCLOGS`
2. Logs do WildFly: `wildfly_producao/standalone/log/server.log`
3. Verificar conectividade com Fastchannel

## Changelog

### v1.1.0 - 2026-01-30

**Adicionado**:
- Importação via serviço `CACSP.incluirNota`
- Flag `SYNC_STATUS_ENABLED` para controle de sincronização
- Validação obrigatória de produtos existentes
- Mapeamento de campos legados
- Resiliência no processamento de pedidos

**Modificado**:
- Fluxo de importação de pedidos
- Validação de produtos (agora obrigatória)

**Removido**:
- Criação automática de produtos a partir do Fastchannel
- Inserção direta via JAPE (substituída por serviço)

**Segurança**:
- Regras de negócio do core agora são aplicadas obrigatoriamente
