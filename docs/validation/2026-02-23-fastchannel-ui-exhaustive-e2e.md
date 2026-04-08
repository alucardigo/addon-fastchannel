# Fastchannel UI Exhaustive E2E - Homolog

## Script
- Arquivo: `scripts/e2e_fastchannel_ui_homolog.mjs`
- Objetivo: validar ponta a ponta a UI do Addon Fastchannel no homolog, com fluxo de clique humano.

## Pré-requisitos
1. Node.js 18+
2. Dependência Playwright instalada no diretório de execução:

```bash
npm i -D playwright
```

3. Navegador do Playwright instalado (primeira execução):

```bash
npx playwright install chromium
```

## Execução rápida (homolog)

```bash
node scripts/e2e_fastchannel_ui_homolog.mjs \
  --baseUrl "http://100.72.97.11:8080/mge/" \
  --user "sup" \
  --password "Azsxdc" \
  --headless true
```

## Execução com browser visível (debug)

```bash
node scripts/e2e_fastchannel_ui_homolog.mjs \
  --baseUrl "http://100.72.97.11:8080/mge/" \
  --user "sup" \
  --password "Azsxdc" \
  --headless false \
  --slowMoMs 120
```

## Variáveis/flags suportadas
- `--baseUrl` ou `FC_BASE_URL`
- `--user` ou `FC_USER`
- `--password` ou `FC_PASSWORD`
- `--headless` ou `FC_HEADLESS`
- `--slowMoMs` ou `FC_SLOWMO_MS`
- `--actionTimeoutMs` ou `FC_ACTION_TIMEOUT_MS`
- `--longTimeoutMs` ou `FC_LONG_TIMEOUT_MS`
- `--outDir` ou `FC_OUT_DIR`

## Cobertura do script
1. Login no MGE.
2. Abertura do Addon pelo menu de pesquisa (`Addon-FastChannel`).
3. Pedidos: importar + reprocessar pedido.
4. Preços: sincronização em lote de item selecionado.
5. Estoque: forçar sync de item selecionado.
6. Fila: processar fila agora.
7. Dashboard: testar conexão.
8. Logs: validação de registros recentes.

## Artefatos gerados
- `report.json`: status de cada etapa, mensagens de erro e diálogos (`confirm`/`alert`) capturados.
- `screenshots/*.png`: screenshot full-page por etapa.

Por padrão os artefatos ficam em:
- `reports/e2e/fastchannel-ui-homolog-<timestamp>/`
