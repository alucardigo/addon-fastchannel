# Addon Fastchannel - Integração Sankhya

Addon para integração do ERP Sankhya com o Fastchannel.

## 📚 Documentação (leia primeiro)

Para continuar o projeto, fazer alterações, manutenção ou dar suporte, comece por:

| Documento | Para quê |
|---|---|
| **[docs/DEVELOPER_GUIDE.md](docs/DEVELOPER_GUIDE.md)** | Guia do desenvolvedor: arquitetura, mapa de pacotes, ciclo de vida, build & deploy/publicação, referência da API FastChannel (OAuth, preço, escalonado/batches, pedido, estoque, status), configuração e modelo de dados (AD_FC*, tabelas Sankhya, mapa NUTAB↔tabela FC). |
| **[docs/OPERATIONS_RUNBOOK.md](docs/OPERATIONS_RUNBOOK.md)** | Runbook de operação/suporte: jobs de sync, troubleshooting (índice de incidentes v1.2.85→v1.2.91), acesso a PROD para diagnóstico, scripts de reconciliação/force-sync, gotchas conhecidos (bloqueio de crédito, cache da vitrine). |
| **[CHANGELOG.md](CHANGELOG.md)** | Histórico detalhado versão a versão (sintoma → causa raiz → correção). Fonte real de versão (o `extension.xml` pode estar defasado). |

> ⚠️ **Pré-requisitos de build que NÃO podem ser removidos** (sob pena do addon não carregar): a task `stripJarsFromWar` no `build.gradle` (evita `ClassCastException` de classloader JAPE) e o `rootProject.name` kebab-case lowercase no `settings.gradle`. Detalhes em DEVELOPER_GUIDE.md → Build & Deploy.

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
