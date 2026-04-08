# Fastchannel 1.0.6 Production Publish/Install/E2E Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Publicar e instalar a versão 1.0.6 do addon em produção, validar pedidos/preços/estoque ponta a ponta e corrigir bloqueios críticos.

**Architecture:** Fluxo único: build/publicação em Areadev, instalação via Minhas Soluções em produção, execução dos serviços na UI do addon, conferência cruzada com Fastchannel UI e logs. Correções de código só quando o erro for reproduzido no ambiente produtivo.

**Tech Stack:** Gradle Addon Studio, Sankhya Addon, Fastchannel API/portal, UI browser automation, SQL/Logs.

### Task 1: Build e publish do addon 1.0.6
- Confirmar tasks Gradle válidas (`publishAddon`).
- Rodar publish com versão e licença.
- Validar artefato/extensão 1.0.6 gerado/publicado.

### Task 2: Instalação em produção
- Acessar `skw.bellube.com.br`.
- Abrir Minhas Soluções > Integração_Fast.
- Instalar/atualizar para 1.0.6 e garantir estado `Ativo`.

### Task 3: Validação E2E em produção
- Rodar importação de pedidos.
- Rodar exportação/sincronização de preços.
- Rodar exportação/sincronização de estoque.
- Conferir logs com identificação de cliente/CNPJ.

### Task 4: Cruzamento Fastchannel x Addon x Sankhya
- Conferir tabela de preço padrão/site e de-para (ID Fast x campo adicional Sankhya).
- Alterar centavos em preço, validar ida e volta e restaurar valor original.
- Conferir estoque (CD ID 2) refletindo na Fastchannel.

### Task 5: Correções finais
- Corrigir erros remanescentes reproduzidos em produção.
- Repetir ciclo completo até sem erro bloqueante.
