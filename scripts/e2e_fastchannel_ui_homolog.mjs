#!/usr/bin/env node
import fs from 'fs/promises';
import path from 'path';

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      args[key] = 'true';
      continue;
    }
    args[key] = next;
    i += 1;
  }
  return args;
}

function asBoolean(value, fallback) {
  if (value == null) return fallback;
  const normalized = String(value).toLowerCase().trim();
  if (['1', 'true', 'yes', 'y', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'n', 'off'].includes(normalized)) return false;
  return fallback;
}

function sanitizeName(input) {
  return String(input)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);
}

async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const baseUrl = args.baseUrl || process.env.FC_BASE_URL;
  const user = args.user || process.env.FC_USER;
  const password = args.password || process.env.FC_PASSWORD;

  if (!baseUrl || !user || !password) {
    console.error(
      [
        'Parametros obrigatorios ausentes.',
        'Informe via argumentos ou variaveis de ambiente:',
        '  --baseUrl / FC_BASE_URL',
        '  --user / FC_USER',
        '  --password / FC_PASSWORD',
      ].join('\n')
    );
    process.exit(2);
  }

  const config = {
    baseUrl,
    user,
    password,
    headless: asBoolean(args.headless ?? process.env.FC_HEADLESS, true),
    slowMoMs: Number(args.slowMoMs ?? process.env.FC_SLOWMO_MS ?? 0),
    actionTimeoutMs: Number(args.actionTimeoutMs ?? process.env.FC_ACTION_TIMEOUT_MS ?? 30000),
    longTimeoutMs: Number(args.longTimeoutMs ?? process.env.FC_LONG_TIMEOUT_MS ?? 360000),
    outDir:
      args.outDir ||
      process.env.FC_OUT_DIR ||
      path.join(process.cwd(), 'reports', 'e2e', `fastchannel-ui-homolog-${ts}`),
  };

  let playwright;
  try {
    playwright = await import('playwright');
  } catch (e) {
    console.error('Playwright nao encontrado. Instale com: npm i -D playwright');
    process.exit(2);
  }

  await ensureDir(config.outDir);
  const screenshotsDir = path.join(config.outDir, 'screenshots');
  await ensureDir(screenshotsDir);

  const report = {
    startedAt: new Date().toISOString(),
    status: 'running',
    config: {
      baseUrl: config.baseUrl,
      user: config.user,
      headless: config.headless,
      slowMoMs: config.slowMoMs,
      actionTimeoutMs: config.actionTimeoutMs,
      longTimeoutMs: config.longTimeoutMs,
      outDir: config.outDir,
    },
    steps: [],
    dialogs: [],
  };

  const { chromium } = playwright;
  const browser = await chromium.launch({ headless: config.headless, slowMo: config.slowMoMs });
  const context = await browser.newContext({ viewport: { width: 1600, height: 1000 } });
  const page = await context.newPage();
  page.setDefaultTimeout(config.actionTimeoutMs);

  const dialogEvents = [];
  page.on('dialog', async (dialog) => {
    const item = {
      timestamp: new Date().toISOString(),
      type: dialog.type(),
      message: dialog.message(),
      url: page.url(),
    };
    dialogEvents.push(item);
    report.dialogs.push(item);
    try {
      await dialog.accept();
      item.accepted = true;
    } catch (e) {
      item.accepted = false;
      item.acceptError = e.message;
    }
  });

  let stepIndex = 0;

  async function saveScreenshot(stepName) {
    stepIndex += 1;
    const file = path.join(
      screenshotsDir,
      `${String(stepIndex).padStart(2, '0')}-${sanitizeName(stepName)}.png`
    );
    await page.screenshot({ path: file, fullPage: true });
    return file;
  }

  async function waitForCondition(predicate, timeoutMs, errorMessage) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const result = await predicate();
      if (result) return result;
      await page.waitForTimeout(250);
    }
    throw new Error(errorMessage);
  }

  async function waitForDialog(startIndex, predicate, timeoutMs, errorMessage) {
    return waitForCondition(
      async () => dialogEvents.slice(startIndex).find(predicate) || null,
      timeoutMs,
      errorMessage
    );
  }

  async function isWorkspaceBlockingModalVisible() {
    return page.evaluate(() => {
      const visible = (el) => {
        if (!el) return false;
        const style = window.getComputedStyle(el);
        if (
          style.display === 'none' ||
          style.visibility === 'hidden' ||
          style.opacity === '0' ||
          style.pointerEvents === 'none'
        ) {
          return false;
        }
        const rect = el.getBoundingClientRect();
        return rect.width > 30 && rect.height > 30;
      };

      const rx = /navegador sankhya.*ciclo de vida/i;
      const candidates = Array.from(document.querySelectorAll('body *')).filter((el) =>
        rx.test((el.textContent || '').replace(/\s+/g, ' '))
      );
      return candidates.some(visible);
    });
  }

  async function neutralizeBlockingWorkspaceOverlay() {
    return page
      .evaluate(() => {
        const normalized = (txt) => (txt || '').replace(/\s+/g, ' ').trim().toLowerCase();
        const visible = (el) => {
          if (!el) return false;
          const style = window.getComputedStyle(el);
          if (
            style.display === 'none' ||
            style.visibility === 'hidden' ||
            style.opacity === '0' ||
            style.pointerEvents === 'none'
          ) {
            return false;
          }
          const rect = el.getBoundingClientRect();
          return rect.width > 16 && rect.height > 16;
        };

        let changed = false;
        const blockers = [];
        const rx = /navegador sankhya.*ciclo de vida/i;

        for (const el of Array.from(document.querySelectorAll('body *'))) {
          const text = normalized(el.textContent);
          if (!rx.test(text)) continue;

          let container = el;
          for (let i = 0; i < 8 && container && container.parentElement; i += 1) {
            const rect = container.getBoundingClientRect();
            if (rect.width >= 300 && rect.height >= 200) break;
            container = container.parentElement;
          }
          if (container) blockers.push(container);
        }

        for (const root of blockers) {
          const closers = Array.from(root.querySelectorAll('button, a, [role="button"], span, i'));
          const closeCandidate = closers.find((el) => {
            const txt = normalized(el.textContent);
            const label = normalized(el.getAttribute('aria-label'));
            const title = normalized(el.getAttribute('title'));
            return (
              visible(el) &&
              (txt === 'x' ||
                txt === 'fechar' ||
                label.includes('fechar') ||
                title.includes('fechar') ||
                /close|x-tool-close|icon-close|fa-times/.test((el.className || '').toString()))
            );
          });
          if (closeCandidate) {
            closeCandidate.click();
            changed = true;
          }

          const wrapper = root.closest('[role="dialog"], .modal, .x-window, .x-panel, .x-component') || root;
          if (wrapper && visible(wrapper)) {
            wrapper.style.setProperty('display', 'none', 'important');
            wrapper.style.setProperty('visibility', 'hidden', 'important');
            wrapper.style.setProperty('pointer-events', 'none', 'important');
            changed = true;
          }
        }

        const manager = document.querySelector('manager-webcomponent');
        if (manager) {
          manager.style.setProperty('pointer-events', 'none', 'important');
          manager.style.setProperty('display', 'none', 'important');
          changed = true;
        }

        return changed;
      })
      .catch(() => false);
  }

  async function getBlockingModalBounds() {
    return page.evaluate(() => {
      const rx = /navegador sankhya.*ciclo de vida/i;
      const visible = (el) => {
        if (!el) return false;
        const style = window.getComputedStyle(el);
        if (
          style.display === 'none' ||
          style.visibility === 'hidden' ||
          style.opacity === '0' ||
          style.pointerEvents === 'none'
        ) {
          return false;
        }
        const rect = el.getBoundingClientRect();
        return rect.width > 80 && rect.height > 40;
      };

      for (const el of Array.from(document.querySelectorAll('body *'))) {
        if (!rx.test((el.textContent || '').replace(/\s+/g, ' '))) continue;
        let container = el;
        for (let i = 0; i < 12 && container.parentElement; i += 1) {
          const rect = container.getBoundingClientRect();
          if (rect.width >= 350 && rect.height >= 350) break;
          container = container.parentElement;
        }
        if (!visible(container)) continue;
        const rect = container.getBoundingClientRect();
        return {
          x: rect.x,
          y: rect.y,
          width: rect.width,
          height: rect.height,
        };
      }
      return null;
    });
  }

  async function waitForAddonFrame(timeoutMs = 60000) {
    const frame = await waitForCondition(
      async () =>
        page
          .frames()
          .find((f) => /\/addon-fastchannel\/html5\/fastchannel\/.+\.html/i.test(f.url())) || null,
      timeoutMs,
      'Frame do Addon Fastchannel nao encontrado'
    );
    return frame;
  }

  async function gotoAddonTab(tabName, headingRegex) {
    await dismissBlockingWorkspaceModal();
    const frame = await waitForAddonFrame();
    const currentHeading = ((await frame.getByRole('heading').first().textContent().catch(() => '')) || '').trim();
    if (headingRegex.test(currentHeading)) {
      return frame;
    }

    await frame.getByRole('link', { name: tabName, exact: true }).click({ force: true });
    const updated = await waitForAddonFrame();
    await updated.getByRole('heading', { name: headingRegex }).waitFor({ timeout: 25000 });
    return updated;
  }

  async function runStep(name, fn, options = {}) {
    const started = Date.now();
    const step = { name, startedAt: new Date(started).toISOString(), status: 'running' };
    report.steps.push(step);
    try {
      const data = await fn();
      step.status = 'passed';
      step.data = data ?? null;
    } catch (e) {
      step.status = 'failed';
      step.error = e && e.message ? e.message : String(e);
      if (!options.continueOnError) {
        step.finishedAt = new Date().toISOString();
        step.durationMs = Date.now() - started;
        try {
          step.screenshot = await saveScreenshot(`${name}-failed`);
        } catch (_) {
          // ignore screenshot failures
        }
        throw e;
      }
    } finally {
      step.finishedAt = new Date().toISOString();
      step.durationMs = Date.now() - started;
      if (!step.screenshot) {
        try {
          step.screenshot = await saveScreenshot(name);
        } catch (_) {
          // ignore screenshot failures
        }
      }
    }
  }

  async function dismissBlockingWorkspaceModal() {
    const candidates = [
      page.getByRole('button', { name: 'Cancelar' }).first(),
      page.locator('button:has-text("Cancelar")').first(),
      page.locator('a:has-text("Cancelar")').first(),
      page.getByText('Cancelar', { exact: true }).first(),
      page.getByRole('button', { name: /Fechar/i }).first(),
      page.locator('button[aria-label*="fechar" i]').first(),
      page.locator('[title*="fechar" i]').first(),
      page.locator('.x-tool-close').first(),
      page.locator('[class*="close"]').first(),
      page.getByText('x', { exact: true }).first(),
      page.locator('[aria-label="close"]').first(),
      page.locator('button:has-text("×")').first(),
    ];

    async function clickLikelyWorkspaceModalHotspots() {
      const viewport = page.viewportSize() || { width: 1600, height: 1000 };
      const cx = Math.round(viewport.width / 2);
      const cy = Math.round(viewport.height / 2);
      const hotspots = [
        // close "X" in top-right corner of centered modal
        { x: cx + 220, y: cy - 250 },
        // cancel button region
        { x: cx - 20, y: cy + 210 },
      ];
      for (const point of hotspots) {
        await page.mouse.click(point.x, point.y).catch(() => {});
      }
    }

    for (let attempt = 0; attempt < 8; attempt += 1) {
      await clickLikelyWorkspaceModalHotspots();

      const bounds = await getBlockingModalBounds();
      if (bounds) {
        // Close icon (top-right) and Cancel button (bottom area) by coordinates.
        const closeX = Math.round(bounds.x + bounds.width - 24);
        const closeY = Math.round(bounds.y + 24);
        await page.mouse.click(closeX, closeY).catch(() => {});

        const cancelX = Math.round(bounds.x + bounds.width * 0.45);
        const cancelY = Math.round(bounds.y + bounds.height - 38);
        await page.mouse.click(cancelX, cancelY).catch(() => {});
      }

      for (const candidate of candidates) {
        const visible = await candidate.isVisible().catch(() => false);
        if (!visible) continue;
        await candidate.click({ force: true }).catch(() => {});
      }

      await page.keyboard.press('Escape').catch(() => {});
      await neutralizeBlockingWorkspaceOverlay();
      await page.waitForTimeout(350);

      const visibleAfter = await isWorkspaceBlockingModalVisible();
      if (!visibleAfter && attempt >= 1) return true;
    }
    return !(await isWorkspaceBlockingModalVisible());
  }

  async function finish(exitCode) {
    report.finishedAt = new Date().toISOString();
    if (report.status === 'running') report.status = exitCode === 0 ? 'passed' : 'failed';
    const reportFile = path.join(config.outDir, 'report.json');
    await fs.writeFile(reportFile, JSON.stringify(report, null, 2), 'utf8');
    console.log(`Report: ${reportFile}`);
    await context.close();
    await browser.close();
    process.exit(exitCode);
  }

  try {
    await runStep('login-mge', async () => {
      await page.goto(config.baseUrl, { waitUntil: 'domcontentloaded' });

      const passwordCandidates = [
        page.locator('input[type="password"]').first(),
        page.locator('input[placeholder*="senha" i]').first(),
        page.locator('input[name*="senha" i]').first(),
      ];

      const loginButtons = [
        page.getByRole('button', { name: /Prosseguir/i }).first(),
        page.getByRole('button', { name: /Entrar/i }).first(),
        page.locator('button[type="submit"]').first(),
      ];

      async function clickLoginButton() {
        for (const button of loginButtons) {
          const visible = await button.isVisible().catch(() => false);
          if (!visible) continue;
          await button.click();
          return true;
        }
        return false;
      }

      async function tryFillPassword() {
        for (const candidate of passwordCandidates) {
          const visible = await candidate
            .waitFor({ state: 'visible', timeout: 1500 })
            .then(() => true)
            .catch(() => false);
          if (!visible) continue;
          await candidate.fill(config.password);
          return true;
        }
        return false;
      }

      const hasPasswordOnFirstScreen = await tryFillPassword();
      if (!hasPasswordOnFirstScreen) {
        const userCandidates = [
          page.locator('input[placeholder*="usu" i]').first(),
          page.locator('input[placeholder*="Usuário"]').first(),
          page.locator('input[placeholder*="Usuario"]').first(),
          page.locator('input[name*="user" i]').first(),
          page.locator('input[name*="login" i]').first(),
          page.locator('input[id*="user" i]').first(),
          page.locator('input[id*="login" i]').first(),
          page.locator('input[type="text"]').first(),
        ];
        let userFilled = false;
        for (const candidate of userCandidates) {
          const visible = await candidate
            .waitFor({ state: 'visible', timeout: 2500 })
            .then(() => true)
            .catch(() => false);
          if (!visible) continue;
          const readonly = await candidate.getAttribute('readonly');
          if (readonly != null) continue;
          await candidate.fill(config.user);
          userFilled = true;
          break;
        }
        if (!userFilled) {
          throw new Error('Campo de usuario nao encontrado na tela de login');
        }

        const clickedUserStep = await clickLoginButton();
        if (!clickedUserStep) {
          throw new Error('Botao para avancar etapa de usuario nao encontrado');
        }

        await waitForCondition(
          async () => tryFillPassword(),
          30000,
          'Campo de senha nao encontrado apos etapa de usuario'
        );
      }

      const clickedPasswordStep = await clickLoginButton();
      if (!clickedPasswordStep) {
        throw new Error('Botao para enviar senha nao encontrado');
      }

      await page.waitForURL(/\/system\.jsp/i, { timeout: config.longTimeoutMs });
      await page.waitForTimeout(2500);

      const noRestoreBtn = page.getByRole('button', { name: 'Não' }).first();
      if (await noRestoreBtn.isVisible().catch(() => false)) {
        await noRestoreBtn.click();
      }

      return { url: page.url() };
    });

    await runStep('open-addon-through-menu-search', async () => {
      await waitForCondition(async () => dismissBlockingWorkspaceModal(), 30000, 'Modal bloqueante do workspace permaneceu aberto');

      await waitForCondition(
        async () => {
          const visibleLoadingPanels = await page.locator('.loading-panel:visible').count().catch(() => 0);
          return visibleLoadingPanels === 0;
        },
        90000,
        'Tela principal do MGE nao finalizou carregamento (loading-panel ativo)'
      );

      const searchBox = page.getByRole('textbox', { name: 'Pesquisar' }).first();
      await searchBox.waitFor({ state: 'visible', timeout: 30000 });
      await searchBox.click({ force: true });
      await page.keyboard.press('Control+A');
      await page.keyboard.type('Addon-FastChannel', { delay: 20 });
      await page.waitForTimeout(500);

      const candidates = [
        /Pedidos Addon-FastChannel/i,
        /Dashboard Addon-FastChannel/i,
        /Estoque Addon-FastChannel/i,
      ];
      let openedBy = null;

      for (const pattern of candidates) {
        const item = page.getByRole('menuitem', { name: pattern }).first();
        const visible = await item
          .waitFor({ state: 'visible', timeout: 4000 })
          .then(() => true)
          .catch(() => false);
        if (!visible) continue;
        await item.click();
        openedBy = pattern.toString();
        break;
      }

      if (!openedBy) {
        for (const pattern of candidates) {
          const item = page.getByText(pattern).first();
          const visible = await item
            .waitFor({ state: 'visible', timeout: 3000 })
            .then(() => true)
            .catch(() => false);
          if (!visible) continue;
          await item.click({ force: true });
          openedBy = `${pattern.toString()}(text)`;
          break;
        }
      }

      if (!openedBy) {
        await page.keyboard.press('Enter');
        const frame = await waitForAddonFrame(15000).catch(() => null);
        if (frame) {
          const title = await frame.getByRole('heading').first().textContent().catch(() => null);
          return { openedBy: 'keyboard-enter', title: (title || '').trim() };
        }
      }

      if (!openedBy) {
        throw new Error('Nao foi possivel abrir o Addon pelo menu de pesquisa');
      }

      const frame = await waitForAddonFrame(90000);
      const title = await frame.getByRole('heading').first().textContent();
      return { openedBy, title: (title || '').trim() };
    });

    await runStep('pedidos-importar', async () => {
      const frame = await gotoAddonTab('Pedidos', /Fastchannel - Pedidos/i);
      await frame.locator('#pedidosTable tr').first().waitFor({ timeout: 30000 });

      const firstBefore = await frame
        .locator('#pedidosTable tr td strong')
        .first()
        .textContent()
        .catch(() => null);

      const startDialog = dialogEvents.length;
      await frame.getByRole('button', { name: 'Importar Novos Pedidos' }).click({ force: true });

      await waitForDialog(
        startDialog,
        (d) => d.type === 'confirm' && /importar novos pedidos/i.test(d.message),
        20000,
        'Confirmacao da importacao nao apareceu'
      );

      const finalAlert = await waitForDialog(
        startDialog,
        (d) => d.type === 'alert' && /importacao/i.test(d.message),
        config.longTimeoutMs,
        'Alerta final da importacao nao apareceu'
      );

      await frame.locator('#pedidosTable tr').first().waitFor({ timeout: 30000 });
      const firstAfter = await frame
        .locator('#pedidosTable tr td strong')
        .first()
        .textContent()
        .catch(() => null);

      return {
        firstOrderBefore: firstBefore ? firstBefore.trim() : null,
        firstOrderAfter: firstAfter ? firstAfter.trim() : null,
        finalAlert: finalAlert.message,
      };
    });

    await runStep('pedidos-reprocessar-primeiro', async () => {
      const frame = await waitForAddonFrame();
      await frame.getByRole('button', { name: 'Detalhes' }).first().click({ force: true });
      await frame.locator('#modalDetails').waitFor({ state: 'visible', timeout: 15000 });

      const orderId = await frame
        .locator('#modalBody .detail-row .detail-value')
        .first()
        .textContent()
        .catch(() => null);

      const startDialog = dialogEvents.length;
      await frame.locator('#modalDetails').getByRole('button', { name: 'Reprocessar' }).click({ force: true });

      await waitForDialog(
        startDialog,
        (d) => d.type === 'confirm' && /reprocessar pedido/i.test(d.message),
        20000,
        'Confirmacao de reprocessamento nao apareceu'
      );

      const reprocessAlert = await waitForDialog(
        startDialog,
        (d) => d.type === 'alert' && /pedido marcado para reprocessamento/i.test(d.message),
        20000,
        'Alerta de reprocessamento nao apareceu'
      );

      const closeBtn = frame.locator('#modalDetails').getByRole('button', { name: 'Fechar' }).first();
      if (await closeBtn.isVisible().catch(() => false)) {
        await closeBtn.click({ force: true });
      }

      const hasPending = await frame
        .locator('#pedidosTable')
        .getByText('PENDENTE')
        .first()
        .isVisible()
        .catch(() => false);

      return {
        orderId: orderId ? orderId.trim() : null,
        reprocessAlert: reprocessAlert.message,
        pendingVisibleAfterReprocess: hasPending,
      };
    });

    await runStep('precos-sync-selecionados', async () => {
      const frame = await gotoAddonTab('Precos', /Fastchannel - Precos/i);
      await frame.locator('#precosTable tr').first().waitFor({ timeout: 30000 });

      const firstCheckbox = frame.locator('#precosTable input[type="checkbox"]').first();
      await firstCheckbox.check({ force: true });

      await waitForCondition(
        async () => !(await frame.locator('#btnSyncLote').isDisabled()),
        10000,
        'Botao de sincronizacao em lote permaneceu desabilitado'
      );

      const startDialog = dialogEvents.length;
      await frame.locator('#btnSyncLote').click({ force: true });

      await waitForDialog(
        startDialog,
        (d) => d.type === 'confirm' && /sincronizar/i.test(d.message),
        20000,
        'Confirmacao de sincronizacao de preco nao apareceu'
      );

      const syncAlert = await waitForDialog(
        startDialog,
        (d) => d.type === 'alert' && /sincronizacao/i.test(d.message),
        config.longTimeoutMs,
        'Alerta de sincronizacao de preco nao apareceu'
      );

      return { syncAlert: syncAlert.message };
    });

    await runStep('estoque-forcar-sync-selecionados', async () => {
      const frame = await gotoAddonTab('Estoque', /Fastchannel - Estoque/i);
      await frame.getByRole('button', { name: 'Filtrar' }).click({ force: true });
      await frame.locator('#estoqueTable tr').first().waitFor({ timeout: 30000 });

      const checkbox = frame.locator('#estoqueTable input[type="checkbox"][data-key]').first();
      await checkbox.check({ force: true });

      const startDialog = dialogEvents.length;
      await frame.getByRole('button', { name: 'Forcar Sync Selecionados' }).click({ force: true });

      const stockAlert = await waitForDialog(
        startDialog,
        (d) =>
          d.type === 'alert' &&
          /(enfileirado|processado|erro|falha|sucesso)/i.test(d.message),
        config.longTimeoutMs,
        'Alerta de forcar sync de estoque nao apareceu'
      );

      return { stockAlert: stockAlert.message };
    });

    await runStep('fila-processar-agora', async () => {
      const frame = await gotoAddonTab('Fila de Sincronizacao', /Fila de Sincronizacao/i);
      await frame.locator('#filaTable tr').first().waitFor({ timeout: 30000 });

      const startDialog = dialogEvents.length;
      await frame.getByRole('button', { name: 'Processar Fila Agora' }).click({ force: true });

      await waitForDialog(
        startDialog,
        (d) => d.type === 'confirm' && /processar todos os itens pendentes/i.test(d.message),
        20000,
        'Confirmacao para processar fila nao apareceu'
      );

      const queueAlert = await waitForDialog(
        startDialog,
        (d) => d.type === 'alert' && /(fila|processamento)/i.test(d.message),
        config.longTimeoutMs,
        'Alerta de processamento da fila nao apareceu'
      );

      return { queueAlert: queueAlert.message };
    });

    await runStep('dashboard-testar-conexao', async () => {
      const frame = await gotoAddonTab('Dashboard', /Fastchannel - Dashboard/i);
      const btn = frame.locator('#btnTestarConexao');
      await btn.waitFor({ state: 'visible', timeout: 20000 });

      const startDialog = dialogEvents.length;
      await btn.click({ force: true });

      const connectionAlert = await waitForDialog(
        startDialog,
        (d) => d.type === 'alert' && /(conexao|teste concluido|erro)/i.test(d.message),
        config.longTimeoutMs,
        'Alerta do teste de conexao nao apareceu'
      );

      return { connectionAlert: connectionAlert.message };
    });

    await runStep('logs-validate-recent-rows', async () => {
      const frame = await gotoAddonTab('Logs', /Fastchannel - Logs/i);
      await frame.locator('#logsTable tr').first().waitFor({ timeout: 30000 });

      const rows = frame.locator('#logsTable tr');
      const rowCount = await rows.count();
      const firstRowText = rowCount > 0 ? await rows.first().innerText() : '';

      if (rowCount === 0 || /nenhum log encontrado/i.test(firstRowText)) {
        throw new Error('Tela de logs sem evidencias para validacao');
      }

      return { rowCount, firstRowPreview: firstRowText.trim().slice(0, 240) };
    });

    report.status = 'passed';
    await finish(0);
  } catch (e) {
    report.status = 'failed';
    report.fatalError = e && e.message ? e.message : String(e);
    await finish(1);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
