# Legacy vs Merge-Unify Order Debug Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Reproduzir e comparar o fluxo de importação de pedidos do legado em homologação com a merge-unify e fechar gaps funcionais restantes (incluindo sync de preço/estoque).

**Architecture:** O legado (Node) será usado como referência comportamental de montagem/envio do pedido (CACSP.incluirNota), enquanto a merge-unify (addon Java) será validada por endpoint direto no WildFly homolog. A comparação será feita por evidências de código e logs, com ajustes mínimos e verificáveis.

**Tech Stack:** Node.js legado, Java Add-on Studio (Sankhya), WildFly, MSSQL, Fastchannel API, SSH/Tailscale.

### Task 1: Capturar baseline de fluxo no legado (homolog)

**Files:**
- Read: `/home/bellube.rodrigo.faria/gbi-app-integrador-main/gbi-app-integrador-main/controllers/api/data/integration/pedidos.js`
- Read: `/home/bellube.rodrigo.faria/gbi-app-integrador-main/gbi-app-integrador-main/models/erp/sankhya.js`

**Step 1: Identificar entrada e transformação de pedido**
Run: `grep -RinE 'syncNewOrdersFastChannel|_afterGetOrderDetail|sendSale|CACSP\.incluirNota' controllers models`
Expected: linhas-chave do fluxo de importação e chamada ao ERP.

**Step 2: Registrar campos obrigatórios do XML legado**
Run: `sed -n '1,360p' models/erp/sankhya.js`
Expected: bloco XML com cabeçalho/itens, regras de CODVOL/ORIGPROD/PERCDESC.

**Step 3: Registrar limitações do ambiente legado em homolog**
Run: `sed -n '1,220p' routes/api/data/integration/pedidos.js`
Expected: confirmação se rotas estão mockadas ou com fluxo real.

### Task 2: Comparar baseline com merge-unify

**Files:**
- Read: `model/src/main/java/br/com/bellube/fastchannel/service/OrderXmlBuilder.java`
- Read: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/ServiceInvokerStrategy.java`
- Read: `model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java`

**Step 1: Mapear divergências de campos/regras**
Run: comparação manual por blocos (cabecalho, itens, desconto, volume, local, controle).
Expected: lista objetiva de diferenças com impacto funcional.

**Step 2: Definir hipóteses e validação por log**
Run: leitura de log do WildFly antes/depois (`tail -n 200 server.log`).
Expected: hipóteses de causa-raiz e critérios de aceite para cada ajuste.

### Task 3: Validar importação real na merge-unify (homolog)

**Files:**
- Read/Observe: `/home/mgeweb/wildfly_teste/standalone/log/server.log`

**Step 1: Executar importarPedidos**
Run: `curl -sS -X POST 'http://127.0.0.1:8080/addon-fastchannel/fc-direct?serviceName=FCAdminSP.importarPedidos'`
Expected: JSON com `success=true` e `count >= 0` sem erro estrutural.

**Step 2: Verificar criação de nota e erros residuais**
Run: `sudo -n grep -nE 'SUCESSO com estrategia|erro|ESTOQUE|volume|NUNOTA|importado' server.log | tail -n 120`
Expected: evidência de criação (NUNOTA) e/ou erro acionável.

### Task 4: Validar sync Sankhya -> Fastchannel (preço e estoque)

**Files:**
- Read: `model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java`
- Read: `model/src/main/java/br/com/bellube/fastchannel/web/FCEstoqueService.java`

**Step 1: Listar candidatos e enfileirar sync**
Run: chamadas `FCPrecosSP.list`/`FCEstoqueSP.list` e `forcarSync`/`syncEmLote`.
Expected: itens elegíveis para envio à Fastchannel (incluindo tabela 9 quando aplicável).

**Step 2: Processar fila e confirmar envio**
Run: `FCAdminSP.processarFila` + `compararFC`.
Expected: diferença reduzida e respostas de sucesso nos itens processados.

### Task 5: Encerramento com evidências

**Files:**
- Create/Update: `docs/` (registro de evidências desta rodada)

**Step 1: Consolidar resultados**
Run: sumarizar comandos, respostas e logs relevantes.
Expected: relatório curto com o que foi validado, o que foi corrigido e o que falta.

**Step 2: Commit (apenas se houver alterações de código/documentação úteis)**
Run: `git add ... && git commit -m "fix: align order import behavior with legacy baseline"`
Expected: commit atômico ou justificativa explícita para não commitar.
