# Fastchannel Production Go-Live Finalization Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Publicar/instalar a versão final do addon e validar em produção o ciclo completo de pedidos, preços e estoque com evidência cruzada Fastchannel x Addon x Sankhya/BD.

**Architecture:** Fluxo em três blocos: (1) publicação e disponibilização no Place, (2) instalação/ativação em Produção via Minhas Soluções, (3) execução E2E operacional + cruzamento de dados e correções até sucesso.

**Tech Stack:** Gradle Addon Studio, Sankhya UI (skw), Fastchannel Portal/API, SQL Server, logs WildFly.

### Task 1: Publicação e disponibilidade no Place
**Files:**
- Modify: `build.gradle`
- Modify: `META-INF/addon-fastchannel-extension.xml`
- Modify: `Addon-FastChannel.ear/extension.xml`
- Modify: `vc/src/main/webapp/META-INF/addon-fastchannel-extension.xml`

1. Validar versão alvo e consistência nos manifests.
2. Executar `publishAddon` com credenciais.
3. Confirmar versão em estado RELEASED no Area Dev.

### Task 2: Instalação e ativação em Produção
**Files:**
- N/A (execução operacional UI)

1. Acessar `skw.bellube.com.br` com usuário informado.
2. Instalar/atualizar `Integração_Fast` em Minhas Soluções.
3. Validar estado final `Ativo`.

### Task 3: E2E produtivo e correções
**Files:**
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/OrderCreationOrchestrator.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/ServiceInvokerStrategy.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java`
- Modify: `model/src/main/java/br/com/bellube/fastchannel/web/FCEstoqueService.java`

1. Executar importação de pedidos pela UI.
2. Executar sincronização de preços com alteração de centavos e reversão.
3. Executar sincronização de estoque (CD ID 2).
4. Cruzar resultados em UI Fastchannel, UI Sankhya e tabelas `AD_FCPEDIDO/AD_FCQUEUE/AD_FCLOG`.
5. Corrigir e repetir até ciclo completo sem falha.
