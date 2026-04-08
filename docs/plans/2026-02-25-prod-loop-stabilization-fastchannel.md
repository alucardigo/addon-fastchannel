# Fastchannel Prod Loop Stabilization Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Publicar nova versão do addon, instalar em produção e iterar correções até estabilizar preço/estoque/pedidos.

**Architecture:** Iteração fechada: (1) ajustar código, (2) compilar/deploy local, (3) publicar versão na Área Dev, (4) instalar no prod, (5) testar fluxos e logs, (6) corrigir e repetir. Priorização no erro real de produção em cada ciclo.

**Tech Stack:** Java (Add-on Studio Sankhya), Gradle, WildFly, MSSQL, Fastchannel API, UI Sankhya/Fastchannel.

### Task 1: Stabilize critical order path
- Modify: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java`
- Step: manter fallback robusto de CODLOCAL e impedir bloqueio desnecessário.
- Step: compilar e validar sem regressão.

### Task 2: Publish versioned build
- Modify: `build.gradle` (somente para não bloquear publicação por automação opcional)
- Step: subir versão incremental com `publishAddon`.
- Step: confirmar release disponível para instalação.

### Task 3: Install in production and validate
- Step: instalar em `skw.bellube.com.br` via UI Minhas Soluções.
- Step: testar UI de pedidos/preços/estoque e cruzar com Fastchannel/BD.
- Step: coletar erro objetivo, corrigir, republicar e repetir.
