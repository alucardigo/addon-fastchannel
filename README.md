# Addon Fastchannel - Integração Sankhya

Addon para integração do ERP Sankhya com o Fastchannel Data Lake.

## Descrição

Este addon fornece integração completa entre o sistema Sankhya ERP e o Fastchannel, permitindo:

- Sincronização de pedidos
- Gerenciamento de fila de sincronização
- Monitoramento em tempo real
- Configuração centralizada
- Logs detalhados de operações

## Tecnologias

- Java EE
- WildFly Application Server
- JAPE Framework (Sankhya)
- SQL Server
- REST API

## Estrutura do Projeto

- `model/` - Camada de modelo e lógica de negócio
- `vc/` - Camada de visualização e controle (webapp)
- `Addon-FastChannel.ear/` - Estrutura EAR para deployment

## Endpoints

- **Dashboard**: `/addon-fastchannel/html5/fastchannel/dashboard.html`
- **Configuração**: `/addon-fastchannel/html5/fastchannel/config.html`
- **Pedidos**: `/addon-fastchannel/html5/fastchannel/pedidos.html`
- **Fila**: `/addon-fastchannel/html5/fastchannel/fila.html`
- **Logs**: `/addon-fastchannel/html5/fastchannel/logs.html`
- **API REST**: `/addon-fastchannel/fc-direct`

## Configuração de Banco de Dados

O addon usa acesso JNDI direto ao datasource `java:/MGEDS` configurado no WildFly.

### Tabelas Principais

- `AD_FCCONFIG` - Configurações da integração
- `AD_FCQUEUE` - Fila de sincronização
- `AD_FCPEDIDO` - Pedidos importados
- `AD_FCLOGS` - Logs de operações
- `AD_FCDEPARA` - Mapeamento de/para

## Build

```bash
./gradlew.bat clean build
```

## Deploy

Copiar o arquivo `Addon-FastChannel.ear` para o diretório de deployments do WildFly:

```
X:\Wildfly_Clean\wildfly_producao\standalone\deployments\
```

## Autor

Desenvolvido por BEL DISTRIBUIDOR DE LUBRIFICANTES LTDA

## Versão

1.0.1

## Licença

Proprietário
