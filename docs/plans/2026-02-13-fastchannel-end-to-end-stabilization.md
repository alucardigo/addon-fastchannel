# Fastchannel End-to-End Stabilization Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Restaurar funcionamento real da integração Fastchannel (preço, estoque, comparação e importação de pedidos) em dev e homolog, eliminando falhas de pool/conexão e garantindo publicação do addon sem dependência de `fastchannel.key`.

**Architecture:** O trabalho será dividido em três trilhas coordenadas: (1) correção de código para estabilidade de conexões e fallback de integração (`CACSP.incluirNota` via Service Invoker ou API interna), (2) recuperação operacional do ambiente local com sincronização de configuração/de-para de homolog, (3) validação e publicação em homolog com credenciais/licença.

**Tech Stack:** Java (Sankhya Add-on), Gradle, WildFly/JBoss, SQL (Sankhya DB), scripts PowerShell, serviços Web Sankhya.

### Task 1: Baseline técnico e compilação limpa

**Files:**
- Read/Modify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Read/Modify: `model/src/main/java/br/com/bellube/fastchannel/util/DBUtil.java`
- Read/Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCAdminService.java`

1. Ajustar inconsistências pendentes de escopo/fechamento de sessão no `OrderService`.
2. Executar `./gradlew :model:compileJava`.
3. Corrigir qualquer erro de compilação até obter sucesso.

### Task 2: Importação de pedido resiliente (ServiceInvoker + fallback)

**Files:**
- Read/Modify: `model/src/main/java/br/com/bellube/fastchannel/service/SankhyaServiceInvoker.java`
- Read/Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/ServiceInvokerStrategy.java`
- Read/Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java`
- Read/Modify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`

1. Garantir que `CACSP.incluirNota` seja primeira tentativa quando disponível.
2. Garantir fallback para API interna quando Service Invoker indisponível/falhar por capability.
3. Adicionar logs diagnósticos claros de decisão de estratégia.
4. Compilar novamente.

### Task 3: Estabilização de pool e sessão no fluxo core

**Files:**
- Read/Modify: `model/src/main/java/br/com/bellube/fastchannel/service/QueueService.java`
- Read/Modify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Read/Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCAdminService.java`

1. Revisar e padronizar abertura/fechamento de `JdbcWrapper` em blocos críticos.
2. Eliminar possíveis vazamentos em loops e fluxos de exceção.
3. Validar via execução dos serviços sem crescimento de erro `IJ000655/IJ000453`.

### Task 4: Reinício controlado do ambiente local + redeploy

**Operational Steps:**
1. Encerrar processos WildFly/JBoss residuais.
2. Limpar marcadores de deploy problemáticos (`.failed/.isdeploying`) quando aplicável.
3. Subir servidor local limpo.
4. Rodar `deployAddon` e aguardar `.deployed`.

### Task 5: Sincronização de configuração real (homolog -> dev)

**Operational Steps:**
1. Extrair dados de configuração/de-para relevantes de homolog.
2. Replicar no banco local (somente entidades da integração Fastchannel).
3. Validar tela/serviço de configuração retornando dados equivalentes aos de homolog.

### Task 6: Smoke test funcional completo (dev)

**Operational Steps:**
1. Testar `FCConfigSP.get`.
2. Testar `FCEstoqueSP.list`.
3. Testar `FCPrecosSP.list`.
4. Testar comparação/admin (`FCAdminSP.processarFila`).
5. Testar `FCAdminSP.importarPedidos` com evidência de processamento.

### Task 7: Publicação homolog sem `fastchannel.key`

**Operational Steps:**
1. Remover referência/uso de `fastchannel.key` no fluxo local.
2. Publicar via credenciais + licença informadas (`email`, `senha`, `codigo da licença`, `app key`).
3. Validar no host `172.16.127.11:8180` com `sup/tecsis`.

### Task 8: Evidências e relatório final

**Files:**
- Create: `docs/validation/2026-02-13-end-to-end-stabilization.md`

1. Registrar comandos executados, resultados de cada endpoint e erros restantes (se houver).
2. Consolidar pendências bloqueantes objetivas (se existirem).
3. Entregar status por fluxo: preço, estoque, comparação, importação de pedidos.

## Orchestration Evidence
- Skills carregados: `writing-plans`, `multi-cli-agent-orchestrator`, `loki-mode`.
- Execução via orquestração: `scripts/orchestrate-router.ps1` + delegações MCP quando aplicável.
