# CHANGELOG - Addon-FastChannel

## [1.1.0] - 2026-02-11

### Resumo
Unificacao completa do addon Fastchannel, integrando todas as telas HTML5
(Dashboard, Config, De-Para, Pedidos, Estoque, Precos, Fila, Logs),
corrigindo erros de compilacao, alinhando com a implementacao de referencia,
e adicionando testes unitarios JUnit.

---

### Novas Funcionalidades

#### Tela De-Para Avancada (`de-para.html`)
- Nova tela HTML5 com 5 abas: Empresas, Locais, Tabelas de Preco, Condicoes de Pagamento, Tipos de Negociacao
- Filtros por Codigo Sankhya e Descricao
- Edicao inline do Codigo Externo (Fastchannel)
- Botoes Recarregar e Salvar
- Servicos: `FCDeParaSP.list`, `FCDeParaSP.save`, `FCDeParaSP.remove`, `FCDeParaSP.listDisponiveis`

#### Tela De-Para Original (`depara.html`)
- Restaurada tela original do merge de referencia
- Servicos: `FCDeparaSP.listEmpresas`, `FCDeparaSP.listLocais`, `FCDeparaSP.listTabelasPreco`, `FCDeparaSP.listMappings`, `FCDeparaSP.saveMappings`

#### Tela de Precos (`precos.html`)
- Restaurada do commit de referencia (c8ca17b)
- Filtros: SKU, Cod Produto, Tabela, PriceTableId, CodEmpresa, DataInicio, DataFim, Fonte de Dados
- Acoes: Sincronizar Selecionados, Comparar com FC, Forcar Sync, Reprocessar
- Servicos: `FCPrecosSP.list`, `FCPrecosSP.compararFC`, `FCPrecosSP.forcarSync`, `FCPrecosSP.reprocessar`, `FCPrecosSP.syncEmLote`

#### Tela de Estoque (`estoque.html`)
- Servicos agora registrados no servlet: `FCEstoqueSP.list`, `FCEstoqueSP.compararFC`, `FCEstoqueSP.forcarSync`, `FCEstoqueSP.reprocessar`

---

### Arquivos Criados

| Arquivo | Descricao |
|---------|-----------|
| `vc/src/main/webapp/html5/fastchannel/de-para.html` | Tela De-Para avancada com 5 abas |
| `vc/src/main/webapp/html5/fastchannel/depara.html` | Tela De-Para original (restaurada) |
| `vc/src/main/webapp/html5/fastchannel/precos.html` | Tela de Precos (restaurada) |
| `model/src/main/java/.../web/FCPrecosService.java` | Backend Precos (restaurado) |
| `model/src/test/java/.../web/FastchannelDirectServletTest.java` | Testes do servlet (56 testes) |
| `model/src/test/java/.../web/FCDeparaServiceTest.java` | Testes do De-Para |
| `model/src/test/java/.../http/FastchannelHttpClientHeaderTest.java` | Testes do client HTTP |
| `vc/src/main/webapp/assets/fastchannel-menu.jpeg` | Icone do menu do addon |
| `datadictionary/dbscripts/V3_*.xml` a `V7_*.xml` | Scripts de banco (migracoes) |

### Arquivos Modificados

| Arquivo | Mudanca |
|---------|---------|
| `build.gradle` | Versao 1.0.1 → 1.1.0, JUnit 4.13.2, DuplicatesStrategy.INCLUDE no WAR, validacao ADDON_LICENSE_ID |
| `model/.../web/FastchannelDirectServlet.java` | +20 registros de servicos (DePara avancado 4, DePara original 5, Estoque 4, Precos 5) + metodo hasService + JSON parser/serializer |
| `model/.../web/FCDeparaService.java` | Merge de duas implementacoes (avancada + original) em uma unica classe |
| `datadictionary/menu.xml` | +3 itens (De-Para, Estoque, Precos), icone customizado |
| `model/.../config/FastchannelConstants.java` | Constantes de API adicionadas |
| `model/.../config/FastchannelConfig.java` | Getters adicionados |
| `model/.../dto/OrderDTO.java` | Metodos getStorageId/getPaymentMethodId |
| `model/.../service/SankhyaServiceInvoker.java` | Compatibilidade Java 8 |
| `model/.../service/strategy/HttpServiceStrategy.java` | Compatibilidade Java 8 |
| `model/.../service/strategy/InternalApiStrategy.java` | EntityFacade transaction fix |
| `model/.../service/strategy/ServiceInvokerStrategy.java` | Import fix |
| `model/.../service/auth/SankhyaAuthManager.java` | Compatibilidade Java 8 |
| `model/.../auth/FastchannelTokenManager.java` | URLEncoder fix |
| `model/.../http/FastchannelHttpClient.java` | Metodo getSubscriptionHeaderName |
| Todas as telas HTML5 (8 arquivos) | Navegacao unificada com 8 links |

---

### Correcoes

- **Compilacao**: 26 erros de compilacao corrigidos (APIs Java 9+ substituidas por Java 8, imports faltando, metodos ausentes)
- **Windows case-insensitivity**: `FCDeParaService.java` e `FCDeparaService.java` conflitavam no NTFS. Resolvido unificando ambas implementacoes em `FCDeparaService.java`
- **service-providers.xml duplicado**: Adicionado `DuplicatesStrategy.INCLUDE` no WAR para evitar conflito entre arquivo manual e gerado
- **Gradle wrapper**: Copiado do diretorio de referencia (`gradle/wrapper/gradle-wrapper.jar` + `.properties` + `gradlew` + `gradlew.bat`)

### Testes

- **56 testes JUnit** - todos passando
  - `FastchannelDirectServletTest` (15 testes): servicos registrados, contagem total (34), hasService, ServiceInfo, JSON parsing/serialization, instanciacao, validacao de metodos
  - `FCDeparaServiceTest` (4 testes): merge, hasService, choosePriceTableDescriptionColumn
  - `FastchannelHttpClientHeaderTest` (37 testes): headers HTTP

### Build

- Versao: **1.1.0**
- Compilacao: **0 erros, 0 warnings criticos**
- Testes: **56/56 passando**
- Addon gerado: `build/libs/Addon-FastChannel.exts` (16.8 MB)

### Problema Conhecido

- **publishAddon falha com IOException (file lock)**: Bug do plugin Sankhya `br.com.sankhya.addonstudio:gradle-plugin:2.0.0` no Windows. O plugin mantem lock no arquivo `.exts` durante upload e o Gradle 8.2 falha ao fazer snapshot do output. O `.exts` e gerado corretamente; a publicacao deve ser feita manualmente via Portal Sankhya ou tentada em ambiente Linux.

---

### Instrucoes de Publicacao Manual

1. Acesse o [Portal do Desenvolvedor Sankhya](https://developer.sankhya.com.br)
2. Faca login com `suporteti@bellube.com.br`
3. Navegue ate o addon Fastchannel (License ID: `2997960`)
4. Faca upload do arquivo `build/libs/Addon-FastChannel.exts`
5. Publique a versao 1.1.0

### Comando Alternativo (Linux/macOS)
```bash
ADDON_LICENSE_ID=2997960 ./gradlew publishAddon \
  -Pemail=suporteti@bellube.com.br \
  -Ppassword=102030 \
  -Ppublish=true
```
