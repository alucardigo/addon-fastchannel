# Sankhya Native Engineering Reverse

## Objetivo
Catalogar APIs/helpers/classes nativas do Sankhya identificadas por engenharia reversa dos jars locais para orientar a convergência do addon Fastchannel.

## Fontes analisadas
- `model/libs/mge-modelcore.jar`
- `model/libs/jape.jar`
- `api_sankhya/sanws.jar` (workspace auxiliar)
- `api_sankhya/mge-modelcore-sources.jar` (decompilado)

## Principais achados

### 1) Invocação de serviço interna (oficial)
Classe: `br.com.sankhya.modelcore.servicecaller.ServiceCaller`

Assinaturas relevantes:
- `new ServiceCaller(MGEFrontFacade)`
- `callAsXml(serviceName, module, body, ServiceResult)`
- `ServiceResult.onSuccess(String)`
- `ServiceResult.onFailure(int, String)`

Comportamento interno (decompilado):
- Monta `serviceRequest` XML no padrão Sankhya (`ISO-8859-1`)
- Usa `HttpServiceBroker` internamente (`directCall`)
- Suporta módulo `mge` e `mgecom`
- Em falha XML, decodifica `statusMessage` base64

### 2) Numeração oficial de nota
Classe: `br.com.sankhya.modelcore.util.NumeracaoNotaHelper`

Fluxo principal:
- Monta `ParamNota` com dados da TOP (`BASENUMERACAO`, `TIPONUMERACAO`, etc.)
- Busca `ControleNumeracao` (`TGFNUM`) com lock nativo
- Pode usar procedure interna (`STP_NUMERAR_NOTA2`) conforme contexto
- Respeita regras de série/empresa/top/validação de data

Implicação:
- Evitar `MAX(NUMNOTA)+1` e usar helper oficial para aderir 100% ao padrão Sankhya.

### 3) Contexto e autenticação interna
Classes:
- `br.com.sankhya.modelcore.auth.AuthenticationInfo`
- `br.com.sankhya.modelcore.auth.AuthenticationServiceContext`
- `br.com.sankhya.ws.ServiceContext`

Uso prático no addon:
- Priorizar `MGEFrontFacade` já presente em `ServiceContext`/`JapeSessionContext`
- Fallback para criação via JNDI (`MGEFrontFacadeHome`) com usuário técnico quando necessário

### 4) Broker webservice interno
Classes (sanws):
- `br.com.sankhya.ws.HttpServiceBroker`
- `br.com.sankhya.ws.ServiceContext`

Ponto crítico:
- `ServiceCaller` já encapsula esse broker; não há ganho em reproduzir manualmente a lógica HTTP quando o caller nativo está disponível.

## Aplicações já implementadas no addon
1. `InternalApiStrategy` agora usa `NumeracaoNotaHelper` para `NUMNOTA` obrigatório.
2. `ServiceInvokerStrategy` agora usa bridge nativa unificada (`SankhyaNativeServiceCaller`).
3. `SankhyaServiceInvoker` foi alinhado para usar a mesma bridge nativa antes de HTTP fallback.

## Gaps restantes para próxima onda
- Reduzir pós-processamento SQL em `OrderService.enforcePostImportParity` migrando para APIs nativas/Jape onde possível.
- Mapear helpers nativos equivalentes para preço/custo/observações de itens.
- Introduzir testes de regressão focados em parity com pedido criado via interface padrão Sankhya.

## Critério de convergência nativa
Uma lógica só deve permanecer manual/SQL quando **todas** condições forem verdadeiras:
1. Não existe helper/classe/serviço nativo equivalente no runtime.
2. Não existe método público utilizável via reflexão.
3. Não existe caminho seguro via Jape para a operação.
4. Foi documentado no roadmap com justificativa técnica.
