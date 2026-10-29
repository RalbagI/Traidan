import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { chromium } from 'playwright-core';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const frontendRoot = resolve(scriptDir, '..');
const repoRoot = resolve(frontendRoot, '..');
const artifactDir = resolve(repoRoot, 'output', 'playwright');
const port = Number(process.env.TRAIDAN_DASHBOARD_SMOKE_PORT ?? 5187);
const baseUrl = `http://127.0.0.1:${port}`;

const viewports = [
  { name: 'fully-invested-desktop', context: { viewport: { width: 1440, height: 900 } }, mode: 'fully_invested' },
  { name: 'fully-invested-mobile', context: { viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true }, mode: 'fully_invested' },
  { name: 'desktop', context: { viewport: { width: 1440, height: 900 } }, mode: 'connected', interactions: true },
  { name: 'tablet', context: { viewport: { width: 1024, height: 768 }, hasTouch: true }, mode: 'connected' },
  { name: 'mobile', context: { viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true }, mode: 'connected', interactions: true, tokenFocus: true },
  { name: 'minimum', context: { viewport: { width: 320, height: 568 }, isMobile: true, hasTouch: true }, mode: 'connected' },
  { name: 'waiting', context: { viewport: { width: 1440, height: 900 } }, mode: 'waiting' },
  { name: 'stale', context: { viewport: { width: 1440, height: 900 } }, mode: 'stale', interactions: true },
  { name: 'persistence-failure', context: { viewport: { width: 1440, height: 900 } }, mode: 'state_persistence_failed' },
  { name: 'disconnected', context: { viewport: { width: 1440, height: 900 } }, mode: 'disconnected' },
  { name: 'unauthorized', context: { viewport: { width: 1440, height: 900 } }, mode: 'unauthorized' },
];

const viteServer = startVite();

try {
  await waitForVite(viteServer);
  await mkdir(artifactDir, { recursive: true });
  const browser = await chromium.launch({
    args: ['--disable-dev-shm-usage', '--no-sandbox'],
    executablePath: await findChrome(),
    headless: true,
  });

  try {
    for (const scenario of viewports) {
      await verifyDashboardViewport(browser, scenario);
    }
  } finally {
    await browser.close();
  }

  console.log(`dashboard_screenshot_smoke=ok output=${artifactDir}`);
} finally {
  stopVite(viteServer);
}

function startVite() {
  const viteBin = resolve(frontendRoot, 'node_modules', '.bin', 'vite');
  const server = spawn(
    viteBin,
    ['--host', '127.0.0.1', '--port', String(port), '--strictPort'],
    {
      cwd: frontendRoot,
      env: {
        ...process.env,
        CHOKIDAR_USEPOLLING: process.env.CHOKIDAR_USEPOLLING ?? '1',
        VITE_TRAIDAN_WS_URL: 'ws://127.0.0.1:8080/stream',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  );
  server.output = '';
  server.stdout.on('data', (chunk) => {
    server.output += chunk.toString();
  });
  server.stderr.on('data', (chunk) => {
    server.output += chunk.toString();
  });
  return server;
}

async function waitForVite(server) {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    if (server.exitCode !== null) {
      throw new Error(`Vite exited before serving ${baseUrl}\n${server.output}`);
    }
    try {
      const response = await fetch(baseUrl);
      if (response.ok) return;
    } catch {
      await sleep(100);
    }
  }
  throw new Error(`Timed out waiting for ${baseUrl}\n${server.output}`);
}

async function verifyDashboardViewport(browser, scenario) {
  const {
    name,
    context,
    mode,
    interactions = false,
    tokenFocus = false,
  } = scenario;
  const page = await browser.newPage(context);
  const consoleErrors = [];
  page.on('console', (message) => {
    if (message.type() === 'error' && !message.text().includes('status of 404')) {
      consoleErrors.push(message.text());
    }
  });
  page.on('pageerror', (error) => {
    consoleErrors.push(error.message);
  });

  await page.addInitScript({
    content: [
      installMockWebSocket.toString(),
      equityFrames.toString(),
      installMockAdminApi.toString(),
      'Date.now = () => Date.parse("2026-05-28T14:31:00Z");',
      `installMockWebSocket(${JSON.stringify(mode)});`,
      `installMockAdminApi(${JSON.stringify(mode)});`,
      mode === 'unauthorized'
        ? 'sessionStorage.setItem("traidan.operatorToken.v1", "x".repeat(32));'
        : '',
    ].join('\n'),
  });
  await page.goto(baseUrl, { waitUntil: 'networkidle' });
  await page.getByRole('heading', { name: 'דסק מניות LLM' }).waitFor({ state: 'visible' });
  assert.equal(await page.locator('html').getAttribute('lang'), 'he');
  assert.equal(await page.locator('html').getAttribute('dir'), 'rtl');
  const populated = !['disconnected', 'waiting'].includes(mode);
  if (populated) {
    await page.waitForFunction(
      () => document.body.innerText.includes('ראיות טריות'),
      undefined,
      { timeout: 10_000 },
    );
  } else if (mode === 'waiting') {
    await page.waitForFunction(
      () => document.body.innerText.includes('ממתין לנתונים מאומתים'),
      undefined,
      { timeout: 10_000 },
    );
  } else {
    await page.waitForFunction(
      () => document.body.innerText.includes('זרם הנתונים מהשרת מנותק'),
      undefined,
      { timeout: 10_000 },
    );
  }

  const bodyText = await page.locator('body').innerText();
  assert.match(bodyText, /תיק/);
  assert.match(bodyText, /החלטה אחרונה/);
  assert.match(bodyText, /GPT-6 Astra xhigh/);
  if (populated) {
    if (mode === 'connected') assert.match(bodyText, /זמין/);
    assert.match(bodyText, /ראיות טריות/);
    const cycleTimes = await page.locator('.cycle-list-time > bdi').allTextContents();
    assert.ok(cycleTimes.some((label) => label.startsWith('27 במאי, ')));
    assert.ok(cycleTimes.some((label) => label.startsWith('28 במאי, ')));
    assert.ok(cycleTimes.every((label) => /^\d+ במאי, \d{2}:\d{2}$/.test(label)));
    assert.match(bodyText, /SPY/);
    assert.match(bodyText, /\$10,064\.00/);
    assert.match(bodyText, /התחלה \$10,000/);
    assert.match(bodyText, /S&P500/);
    assert.match(bodyText, /להעדיף ראיות טריות/);
    if (mode !== 'stale') assert.doesNotMatch(bodyText, /לא זמין/);
    if (interactions) {
      await verifyPortfolioNavChartTooltip(page);
      await verifyTraceDrawerFocus(page, name);
    }
    if (mode === 'fully_invested') {
      assert.match(bodyText, /מושקע במלואו/);
      assert.doesNotMatch(bodyText, /ה-LLM לא סיפק נימוק|עדיין אין פקודה שעברה/);
      assert.equal(await page.locator('.hero-panel [aria-label="הקצאת תיק מלאה"] .ref-chip').count(), 20);
      await page.locator('.hero-panel .link-button').click();
      assert.equal(await page.locator('.drawer [aria-label="הקצאת תיק מלאה"] .ref-chip').count(), 20);
      await page.keyboard.press('Escape');
      assert.match(bodyText, /100\.0%/);
      assert.equal(await page.locator('.positions-table tbody tr').count(), 20);
    }
    if (mode === 'state_persistence_failed') {
      const servicePill = page.locator('.status-pill').filter({ hasText: 'שירות' });
      assert.equal(await servicePill.getAttribute('class'), 'status-pill bad');
      assert.match(await servicePill.innerText(), /נכשל/);
    }
    if (mode === 'stale') {
      assert.match(bodyText, /הנתונים נשמרו מהעדכון האחרון/);
      assert.match(await page.locator('.portfolio-panel').getAttribute('class'), /data-stale/);
      assert.match(await page.locator('.benchmark-status').innerText(), /לא זמין/);
      assert.doesNotMatch(await page.locator('.benchmark-status').innerText(), /unavailable/);
    }
    if (mode === 'unauthorized') {
      await page.locator('#incident-title').filter({ hasText: 'גישת Admin API נדחתה' }).waitFor({ state: 'visible' });
      assert.match(await page.locator('body').innerText(), /הטוקן נדחה/);
    }
  } else if (mode === 'waiting') {
    assert.match(bodyText, /ממתין לנתונים מאומתים/);
    assert.doesNotMatch(bodyText, /\$10,000\.00/);
    assert.equal(await page.locator('.memory-item').count(), 0);
  } else {
    assert.match(bodyText, /לא זמין/);
    assert.match(bodyText, /זרם הנתונים מהשרת מנותק/);
    assert.doesNotMatch(bodyText, /\$10,000\.00/);
  }
  await verifyAccessibilityStructure(page, context.hasTouch === true);
  await verifyPanelReadingAndFocusOrder(page);
  await verifyAuthorityFlow(page);
  if (context.viewport.width <= 390 && populated) {
    await assertElementsWithinViewport(page, '.flow-node', `${name} authority gates should remain visible`);
  }
  await assertNoHorizontalOverflow(page, '.app-shell', `${name} app should fit viewport`);
  assert.deepEqual(consoleErrors, []);
  await page.screenshot({
    fullPage: true,
    path: resolve(artifactDir, `dashboard-${name}.png`),
  });
  if (tokenFocus) {
    const tokenInput = page.getByLabel('טוקן מפעיל מקומי');
    await page.setViewportSize({ width: context.viewport.width, height: 480 });
    await tokenInput.focus();
    await tokenInput.scrollIntoViewIfNeeded();
    await page.screenshot({
      path: resolve(artifactDir, `dashboard-${name}-token-focused.png`),
    });
  }
  await page.close();
}

function installMockWebSocket(mode) {
  const NativeWebSocket = window.WebSocket;
  class MockWebSocket {
    static CONNECTING = 0;
    static OPEN = 1;
    static CLOSING = 2;
    static CLOSED = 3;

    constructor(url) {
      if (!String(url).includes('127.0.0.1:8080/stream')) {
        return new NativeWebSocket(url);
      }
      this.url = url;
      this.readyState = MockWebSocket.CONNECTING;
      setTimeout(() => {
        if (mode === 'disconnected') {
          this.readyState = MockWebSocket.CLOSED;
          this.onclose?.({ code: 1006, reason: 'mock disconnect' });
          return;
        }
        this.readyState = MockWebSocket.OPEN;
        this.onopen?.({});
        for (const frame of equityFrames(mode)) {
          this.onmessage?.({ data: JSON.stringify(frame) });
        }
        if (mode === 'stale') {
          this.readyState = MockWebSocket.CLOSED;
          this.onclose?.({ code: 1006, reason: 'mock disconnect after populated frames' });
        }
      }, 20);
    }

    send() {}

    close() {
      this.readyState = MockWebSocket.CLOSED;
      this.onclose?.({ code: 1000, reason: 'closed' });
    }
  }
  window.WebSocket = MockWebSocket;
}

function equityFrames(mode) {
  const timestampNs = '1700000000000000000';
  const asOf = '2026-05-28T14:30:00Z';
  const portfolioFrames = [0, 1, 2, 3].map((index) => ({
    type: 'equity_portfolio_state',
    data: {
      initial_equity_usd: 10000,
      cash_usd: 9000 - index * 75,
      nav_usd: [10000, 10028, 9992, 10064][index],
      gross_exposure_usd: 1000 + index * 120,
      daily_pnl_usd: [0, 28, -8, 64][index],
      total_pnl_usd: [0, 28, -8, 64][index],
      total_pnl_pct: [0, 0.0028, -0.0008, 0.0064][index],
      realized_pnl_usd: [0, 2, 2, 4][index],
      unrealized_pnl_usd: [0, 26, -10, 60][index],
      max_drawdown_pct: index === 2 ? 0.004 : 0,
      benchmark_symbol: 'SPY',
      benchmark_label: 'S&P500',
      benchmark_status: mode === 'stale' && index === 3 ? 'unavailable' : 'live',
      ...(mode === 'stale' && index === 3 ? {} : { benchmark_price_usd: [500, 502, 501, 507][index] }),
      positions: [{
        symbol: 'SPY',
        quantity: 2,
        avg_price: 500,
        current_price_usd: 500 + index * 12,
        cost_basis_usd: 1000,
        market_value_usd: 1000 + index * 120,
        unrealized_pnl_usd: index * 24,
        unrealized_pnl_pct: index * 0.024,
        weight_pct: 0.1 + index * 0.01,
      }],
      timestamp_ns: String(BigInt(timestampNs) + BigInt(index) * 86_400_000_000_000n),
    },
  }));
  const cycleSummaryFrames = Array.from({ length: 20 }, (_, index) => ({
    type: 'llm_equity_cycle_summary',
    data: {
      cycle_id: `cycle-summary-${index}`,
      cycle_kind: 'news_scan',
      as_of: `2026-05-${27 + Math.floor(index / 10)}T${String(14 + Math.floor((index % 10) / 4)).padStart(2, '0')}:${String(30 + (index % 4) * 5).padStart(2, '0')}:00Z`,
      market_session: 'rth',
      status: index % 5 === 0 ? 'blocked' : 'accepted',
      mandatory_trade_status: index % 5 === 0 ? 'blocked' : 'accepted',
      required_notional_usd: 25,
      requested_notional_usd: 30 + index,
      accepted_notional_usd: index % 5 === 0 ? 0 : 25 + index,
      actions: [index % 2 === 0 ? 'buy' : 'sell'],
      symbols: [index % 2 === 0 ? 'SPY' : 'QQQ'],
      rationale: `ראיות טריות תומכות במחזור מסחר ${index}; נשמר מינימום $25 כש-Rust מאשר.`,
      reason: index % 5 === 0 ? 'mandatory_order_notional_required' : 'simulated_fill',
      timestamp_ns: String(BigInt(timestampNs) + 5_000_000_000n + BigInt(index) * 1_000_000n),
    },
  })).reverse();
  if (mode === 'waiting') {
    return [{
      type: 'llm_equity_service_status',
      data: {
        status: 'waiting_for_hourly_cycle',
        detail: 'הזרם מחובר וממתין למחזור השעתי הראשון.',
        last_cycle_id: null,
        last_error: null,
        timestamp_ns: timestampNs,
      },
    }];
  }
  const frames = [
    {
      type: 'llm_equity_service_status',
      data: {
        status: mode === 'state_persistence_failed' ? mode : 'cycle_applied',
        detail: mode === 'state_persistence_failed'
          ? 'שמירת מצב הסמכות נכשלה; הפקודה לא אושרה.'
          : 'הסריקה השעתית הושלמה; פקודת מניות בסימולציה יושמה.',
        last_cycle_id: 'cycle-1',
        last_error: null,
        timestamp_ns: timestampNs,
      },
    },
    ...portfolioFrames,
    ...cycleSummaryFrames,
    {
      type: 'market_research_snapshot',
      data: {
        snapshot_id: 'snap-1',
        as_of: asOf,
        market_session: 'rth',
        symbols: ['SPY'],
        latest_bars: {
          SPY: {
            timestamp: asOf,
            open: 499,
            high: 501,
            low: 498,
            close: 500,
            volume: 1000000,
            source: 'alpaca_market_data',
          },
        },
        latest_quotes: {
          SPY: {
            timestamp: asOf,
            bid: 499.95,
            ask: 500.05,
            source: 'alpaca_market_data',
          },
        },
        market_calendar_state: {
          source: 'alpaca_market_clock',
          retrieved_at: asOf,
          is_open: true,
          next_open: '2026-05-29T13:30:00Z',
          next_close: '2026-05-28T20:00:00Z',
          freshness: 'fresh',
        },
        trend_summaries: { SPY: 'מגמה קונסטרוקטיבית.' },
        source_freshness: [{ source: 'alpaca_market_data', freshness: 'fresh' }],
        macro_news_summaries: [{
          source: 'macro_calendar',
          url: 'https://example.com/calendar',
          retrieved_at: asOf,
          summary: 'אין אירוע משמעותי בטווח הקרוב.',
          freshness: 'fresh',
        }],
        unavailable_data_flags: [],
        timestamp_ns: timestampNs,
      },
    },
    {
      type: 'llm_equity_cycle',
      data: {
        cycle_id: 'cycle-1',
        sequence: 1,
        as_of: asOf,
        market_session: 'rth',
        portfolio_nav_usd: 10000,
        commands: [{
          action: 'buy_notional',
          symbol: 'SPY',
          rationale: 'ראיות טריות מהשוק, המאקרו והזיכרון תומכות בכניסת סימולציה קטנה.',
          confidence: 0.74,
          max_notional_usd: 1000,
          horizon: '60m',
          evidence_refs: ['ev-1'],
        }],
        evidence: [{
          id: 'ev-1',
          source: 'market_research_snapshot',
          summary: 'bars, ציטוטים והקשר מאקרו טריים.',
          retrieved_at: asOf,
          freshness: 'fresh',
        }],
        risk_assessment: {
          summary: 'בתוך מגבלות הפקודה והחשיפה.',
          max_loss_scenario: 'כניסה קטנה בחשבון מזומן.',
          risk_limit_verdict: mode === 'stale' ? 'reduce' : 'pass',
        },
        memory_updates: [{
          conclusion: 'להעדיף ראיות טריות וגודל קטן עד שסיגנלים חוזרים מתבססים.',
        }],
        timestamp_ns: timestampNs,
      },
    },
    {
      type: 'equity_order_simulated',
      data: {
        order_id: 'cycle-1-1',
        cycle_id: 'cycle-1',
        symbol: 'SPY',
        side: 'buy',
        status: 'accepted',
        qty: 2,
        avg_price: 500,
        notional_usd: 1000,
        reason: 'simulated_fill',
        timestamp_ns: timestampNs,
      },
    },
    {
      type: 'llm_memory_update',
      data: {
        event_id: 'memory-1',
        cycle_id: 'cycle-1',
        thesis: 'כניסה קטנה.',
        outcome: 'בהמתנה.',
        conclusion: 'להעדיף ראיות טריות וגודל קטן עד שסיגנלים חוזרים מתבססים.',
        next_cycle_watch_items: ['רוחב שוק'],
        timestamp_ns: timestampNs,
      },
    },
  ];
  if (mode === 'fully_invested') {
    frames[0].data.status = mode;
    for (const frame of portfolioFrames) {
      frame.data.cash_usd = 0.00000001;
      frame.data.gross_exposure_usd = frame.data.nav_usd - frame.data.cash_usd;
      frame.data.positions = Array.from({ length: 20 }, (_, i) => ({
        symbol: `STK${i}`, quantity: 5, avg_price: 100, current_price_usd: frame.data.nav_usd / 100,
        cost_basis_usd: 500, market_value_usd: frame.data.nav_usd / 20, unrealized_pnl_usd: frame.data.total_pnl_usd / 20,
        unrealized_pnl_pct: frame.data.total_pnl_pct, weight_pct: 0.05,
      }));
    }
    const cycle = frames.find((frame) => frame.type === 'llm_equity_cycle').data;
    cycle.commands = [];
    cycle.execution_policy = 'fully_invested_v1';
    cycle.target_portfolio = { schema_version: 1, holdings: Array.from({ length: 20 }, (_, i) => ({ symbol: `STK${i}`, weight_bps: 500 })) };
    cycleSummaryFrames[0].data.actions = Array.from({ length: 40 }, (_, i) => i < 20 ? 'sell' : 'buy');
    cycleSummaryFrames[0].data.symbols = Array.from({ length: 40 }, (_, i) => `STK${i}`);
  }
  return frames;
}

function installMockAdminApi(mode) {
  const originalFetch = window.fetch?.bind(window);
  window.fetch = async (resource, options = {}) => {
    const url = new URL(String(resource), window.location.href);
    if (url.origin === 'http://127.0.0.1:8090') {
      const authorized = mode !== 'unauthorized'
        && String(options.headers?.Authorization ?? '').startsWith('Bearer ');
      return new Response(JSON.stringify({
        status: authorized ? 'ok' : 'failed',
        reason: authorized ? undefined : 'unauthorized',
      }), {
        status: authorized ? 200 : 401,
        headers: { 'Content-Type': 'application/json' },
      });
    }
    return originalFetch(resource, options);
  };
}

async function assertNoHorizontalOverflow(page, selector, message) {
  const overflow = await page.locator(selector).evaluate((element) => {
    const rect = element.getBoundingClientRect();
    return Math.ceil(element.scrollWidth) > Math.ceil(rect.width) + 1;
  });
  assert.equal(overflow, false, message);
}

async function assertElementsWithinViewport(page, selector, message) {
  const result = await page.locator(selector).evaluateAll((elements) => elements.map((element) => {
    const rect = element.getBoundingClientRect();
    return {
      left: rect.left,
      right: rect.right,
      visible: rect.left >= -1 && rect.right <= window.innerWidth + 1,
    };
  }));
  assert.equal(result.every((item) => item.visible), true, `${message}: ${JSON.stringify(result)}`);
}

async function verifyAccessibilityStructure(page, touchViewport) {
  const unnamed = await page.locator('button, input, a[href]').evaluateAll((elements) => elements
    .filter((element) => {
      const style = window.getComputedStyle(element);
      return style.display !== 'none' && style.visibility !== 'hidden';
    })
    .filter((element) => {
      const labels = element.labels ? [...element.labels].map((label) => label.textContent).join(' ') : '';
      return !String(element.getAttribute('aria-label') ?? '').trim()
        && !String(element.getAttribute('aria-labelledby') ?? '').trim()
        && !String(element.textContent ?? '').trim()
        && !String(labels).trim();
    })
    .map((element) => element.outerHTML));
  assert.deepEqual(unnamed, []);

  const tokenInput = page.getByLabel('טוקן מפעיל מקומי');
  assert.equal(await tokenInput.evaluate((element) => element.closest('form') !== null), true);
  assert.equal(await page.locator('[aria-live="polite"]').count(), 1);
  assert.equal(await page.locator('main').count(), 1);

  if (await page.locator('.positions-table').count()) {
    assert.equal(await page.locator('.positions-table caption').count(), 1);
    assert.ok(await page.locator('.positions-table th[scope]').count() >= 4);
  }

  if (touchViewport) {
    const undersized = await page.locator('button, input, a[href]').evaluateAll((elements) => elements
      .filter((element) => {
        const rect = element.getBoundingClientRect();
        const style = window.getComputedStyle(element);
        return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
      })
      .map((element) => {
        const rect = element.getBoundingClientRect();
        return { tag: element.tagName, text: element.textContent?.trim(), width: rect.width, height: rect.height };
      })
      .filter((item) => item.width < 43.5 || item.height < 43.5));
    assert.deepEqual(undersized, []);
  }
}

async function verifyPanelReadingAndFocusOrder(page) {
  const result = await page.locator('.desk-grid > .panel').evaluateAll((panels) => {
    const panelKey = (panel) => [...panel.classList].find((name) => name.endsWith('-panel') && name !== 'panel');
    const domOrder = panels.map(panelKey);
    const visualOrder = panels
      .map((panel) => {
        const rect = panel.getBoundingClientRect();
        return { key: panelKey(panel), top: rect.top, right: rect.right };
      })
      .sort((left, right) => (Math.abs(left.top - right.top) > 4
        ? left.top - right.top
        : right.right - left.right))
      .map((panel) => panel.key);
    const focusOrder = [...document.querySelectorAll(
      '.desk-grid button:not([disabled]), .desk-grid input:not([disabled]), .desk-grid a[href], .desk-grid [tabindex]:not([tabindex="-1"])',
    )]
      .map((element) => element.closest('.panel'))
      .filter(Boolean)
      .map(panelKey)
      .filter((key, index, keys) => keys.indexOf(key) === index);
    return { domOrder, visualOrder, focusOrder };
  });

  assert.deepEqual(result.visualOrder, result.domOrder, JSON.stringify(result));
  assert.deepEqual(
    result.focusOrder,
    result.domOrder.filter((key) => result.focusOrder.includes(key)),
    JSON.stringify(result),
  );
}

async function verifyAuthorityFlow(page) {
  const labels = await page.locator('.flow-node .gate-name').allTextContents();
  assert.deepEqual(labels, [
    'כוונת LLM',
    'סכמה',
    'טריות',
    'מסחר',
    'תמחור',
    'בדיקת Replay',
    'יומן נייר',
  ]);
}

async function verifyTraceDrawerFocus(page, artifactName) {
  const trigger = page.locator('.command-card').first();
  await trigger.click();

  const dialog = page.getByRole('dialog', { name: /מעקב/ });
  await dialog.waitFor({ state: 'visible' });
  const dialogText = await dialog.innerText();
  assert.match(dialogText, /נשלף 28 במאי, 17:30/);
  assert.match(await dialog.locator('.cycle-id').innerText(), /28 במאי, 17:30/);
  assert.doesNotMatch(dialogText, /retrieved 2026-05-28T14:30:00Z/);

  const activeLabel = await page.evaluate(() => document.activeElement?.getAttribute('aria-label'));
  assert.equal(activeLabel, 'סגירה');
  assert.equal(await page.locator('main').evaluate((element) => element.inert), true);
  if (artifactName === 'stale') {
    const reducedVerdict = dialog.locator('.risk-verdict.reduced');
    await reducedVerdict.waitFor({ state: 'visible' });
    assert.equal(
      await reducedVerdict.getAttribute('data-icon'),
      'alert',
      'a reduced risk verdict should retain its warning affordance',
    );
  }

  await page.waitForTimeout(300);

  await page.screenshot({
    path: resolve(artifactDir, `dashboard-${artifactName}-trace.png`),
  });

  await page.keyboard.press('Tab');
  const activeInsideDialog = await dialog.evaluate((element) => element.contains(document.activeElement));
  assert.equal(activeInsideDialog, true);
  await page.keyboard.press('Shift+Tab');
  const reverseFocusInsideDialog = await dialog.evaluate((element) => element.contains(document.activeElement));
  assert.equal(reverseFocusInsideDialog, true);

  await page.keyboard.press('Escape');
  assert.match(
    await page.locator('.drawer').innerText(),
    /market_research_snapshot/,
    'the trace content should remain visible while the drawer closes',
  );
  await page.waitForTimeout(300);
  await dialog.waitFor({ state: 'hidden' });
  const focusRestored = await trigger.evaluate((element) => document.activeElement === element);
  assert.equal(focusRestored, true);
  assert.equal(await page.locator('main').evaluate((element) => element.inert), false);
  assert.equal(
    await page.evaluate(() => window.scrollX),
    0,
    'closing the RTL trace drawer should not leave the dashboard horizontally shifted',
  );
}

async function verifyPortfolioNavChartTooltip(page) {
  const chart = page.locator('.portfolio-nav-chart').first();
  await chart.waitFor({ state: 'visible' });
  await chart.scrollIntoViewIfNeeded();
  const box = await chart.boundingBox();
  assert.ok(box);
  assert.ok(box.height >= 100);
  const timelineTickLabels = await page.locator('.portfolio-nav-tick text').allTextContents();
  assert.ok(timelineTickLabels.length >= 3);
  assert.ok(timelineTickLabels.filter((label) => /\d{1,2}\.\d{1,2}/.test(label)).length >= 2);

  await page.mouse.move(box.x + box.width * 0.7, box.y + box.height * 0.45);
  const tooltip = page.locator('.portfolio-nav-tooltip');
  await tooltip.waitFor({ state: 'visible' });
  const tooltipText = await tooltip.innerText();
  assert.match(tooltipText, /NAV/);
  assert.match(tooltipText, /S&P500/);
  assert.match(tooltipText, /מול S&P500/);
  assert.match(tooltipText, /חשיפה ברוטו/);
  assert.match(tooltipText, /פוזיציות/);
  await assertTooltipContained(page);

  const tradeMarkers = await page.locator('.portfolio-trade-marker').count();
  assert.ok(tradeMarkers >= 1);
  await page.mouse.move(box.x + box.width * 0.27, box.y + box.height * 0.45);
  await tooltip.waitFor({ state: 'visible' });
  const tradeTooltipText = await tooltip.innerText();
  assert.match(tradeTooltipText, /עסקת LLM/);
  assert.match(tradeTooltipText, /קניית לונג SPY/);
  await assertTooltipContained(page);

  await chart.focus();
  await page.keyboard.press('ArrowLeft');
  await tooltip.waitFor({ state: 'visible' });
  const keyboardTooltipText = await tooltip.innerText();
  assert.match(keyboardTooltipText, /NAV/);
  await assertTooltipContained(page);
}

async function assertTooltipContained(page) {
  const result = await page.evaluate(() => {
    const chartElement = document.querySelector('.portfolio-nav-chart');
    const tooltipElement = document.querySelector('.portfolio-nav-tooltip');
    if (!chartElement || !tooltipElement) return { contained: false, reason: 'missing-elements' };
    const chartRect = chartElement.getBoundingClientRect();
    const tooltipRect = tooltipElement.getBoundingClientRect();
    return {
      contained: tooltipRect.left >= chartRect.left - 1
      && tooltipRect.right <= chartRect.right + 1
      && tooltipRect.top >= chartRect.top - 1
      && tooltipRect.bottom <= chartRect.bottom + 1,
      chart: {
        left: chartRect.left,
        right: chartRect.right,
        top: chartRect.top,
        bottom: chartRect.bottom,
      },
      tooltip: {
        left: tooltipRect.left,
        right: tooltipRect.right,
        top: tooltipRect.top,
        bottom: tooltipRect.bottom,
      },
    };
  });
  assert.equal(result.contained, true, JSON.stringify(result));
}

async function findChrome() {
  const candidates = [
    process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE,
    '/usr/bin/google-chrome',
    '/usr/bin/google-chrome-stable',
    '/usr/bin/chromium',
    '/usr/bin/chromium-browser',
  ].filter(Boolean);
  for (const candidate of candidates) {
    try {
      await import('node:fs/promises').then((fs) => fs.access(candidate));
      return candidate;
    } catch {
      // Try next candidate.
    }
  }
  throw new Error('No Chrome or Chromium executable found');
}

function stopVite(server) {
  if (server.exitCode === null) {
    server.kill('SIGTERM');
  }
}

function sleep(ms) {
  return new Promise((resolveSleep) => {
    setTimeout(resolveSleep, ms);
  });
}
