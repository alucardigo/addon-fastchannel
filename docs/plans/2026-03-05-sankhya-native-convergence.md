# Sankhya Native Convergence Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Convergir a integração Fastchannel para APIs, helpers e fluxos nativos do Sankhya, reduzindo SQL/manualidade e maximizando aderência ao comportamento interno do ERP.

**Architecture:** Introduzir uma camada de bridge nativa para chamadas de serviço (`ServiceCaller`/`ServiceInvoker`), adotar numeração oficial (`NumeracaoNotaHelper`) e migrar gradualmente pós-processamentos de pedido para serviços/helpers nativos. Executar por ondas para manter segurança operacional.

**Tech Stack:** Java 8+, Jape, mge-modelcore, sanws, Add-on Studio/Gradle.

### Task 1: Baseline de Inventário Nativo

**Files:**
- Create: `docs/sankhya/NATIVE_ENGINEERING_REVERSE.md`
- Create: `docs/sankhya/NATIVE_CONVERGENCE_ROADMAP.md`

**Step 1: Catalogar classes nativas úteis por domínio**
- Serviço: `br.com.sankhya.modelcore.servicecaller.ServiceCaller`
- Numeração: `br.com.sankhya.modelcore.util.NumeracaoNotaHelper`
- Facade/autenticação: `MGEFrontFacade`, `AuthenticationServiceContext`
- Contexto webservice: `br.com.sankhya.ws.ServiceContext`, `HttpServiceBroker`

**Step 2: Documentar padrão de uso com limites/riscos**
- Contexto de sessão (ServiceContext/Jape)
- Diferenças `mge` vs `mgecom`
- Fallback controlado (HTTP só último recurso)

### Task 2: Bridge Nativa Unificada para Serviço

**Files:**
- Create: `model/src/main/java/br/com/bellube/fastchannel/service/nativeapi/SankhyaNativeServiceCaller.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/ServiceInvokerStrategy.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/SankhyaServiceInvoker.java`

**Step 1: Implementar invocação nativa em ordem**
1. ServiceInvoker legado (quando presente)
2. ServiceCaller modelcore (oficial)

**Step 2: Resolver `MGEFrontFacade` por contexto nativo**
1. `ServiceContext.getCurrent().getHttpRequest().getAttribute(...)`
2. `JapeSessionContext` (`MGEFrontFacade`/`mgeFrontFacade`)
3. JNDI (`MGEFrontFacadeHome`) com credencial técnica

**Step 3: Padronizar erros e diagnóstico por módulo (`mgecom`, `mge`)**

### Task 3: Numeração Oficial de Nota

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java`

**Step 1: Remover heurística de geração e usar `NumeracaoNotaHelper`**
- Montar `ParamNota` com dados da TOP vigente
- Gerar `NUMNOTA` só quando coluna for obrigatória

**Step 2: Garantir logs de troubleshooting com TOP/base/tipo de numeração**

### Task 4: Onda 2 (próxima) - Pós-processamento e validações nativas

**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Create: `model/src/main/java/br/com/bellube/fastchannel/service/nativeapi/OrderNativePostProcessor.java`

**Step 1: Substituir updates SQL de paridade por operações nativas (Jape/helper/service) onde possível**

**Step 2: Manter SQL apenas para casos sem API/helper nativo comprovado**

### Task 5: Verificação e publicação

**Files:**
- Modify: `docs/sankhya/NATIVE_ENGINEERING_REVERSE.md`

**Step 1: Compilar**
Run: `./gradlew.bat :model:compileJava --no-daemon`
Expected: `BUILD SUCCESSFUL`

**Step 2: Publicar**
Run: `./gradlew.bat --no-daemon publishAddon "-PADDON_LICENSE_ID=2997960" "-Pemail=suporteti@bellube.com.br" "-Ppassword=102030" "-Ppublish=true"`
Expected: `O addon foi atualizado na área do desenvolvedor com sucesso!`
