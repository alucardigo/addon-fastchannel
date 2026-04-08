## [LRN-20260323-001] correction

**Logged**: 2026-03-23T21:10:00Z
**Priority**: high
**Status**: pending
**Area**: infra

### Summary
Não confundir `deployAddon` com `publishAddon` neste repositório Fastchannel.

### Details
O usuário corrigiu que a tarefa pedida era publish, não deploy. Neste projeto, `deployAddon` copia o EAR para o WildFly local definido em `snk.serverFolder`, enquanto `publishAddon` é o fluxo de publicação para a Área Dev/portal Sankhya. A promoção automática para `RELEASED` depende adicionalmente de `AREADEV_BEARER_TOKEN` quando `-Ppublish=true` está habilitado.

### Suggested Action
Antes de qualquer operação de release, confirmar no `build.gradle` se a intenção é deploy local (`deployAddon`) ou publicação externa (`publishAddon`) e validar os parâmetros/credenciais exigidos.

### Metadata
- Source: user_feedback
- Related Files: build.gradle, run_build.bat, AGENTS.md
- Tags: deploy, publish, areadev, gradle

---
