import { stat } from 'node:fs/promises';
import { isIP } from 'node:net';

import { chromium } from 'playwright-core';

const DEFAULT_TIMEOUT_MS = 20_000;
const DEFAULT_HOSTING_URL = process.env.FIREBASE_HOSTING_URL || '';
const DEFAULT_WSS_URL = process.env.VITE_TRAIDAN_WS_URL || '';
const DEFAULT_WSS_HOST_RESOLVER_IP = process.env.TRAIDAN_HOSTED_SMOKE_WSS_HOST_RESOLVER_IP || '';
const KNOWN_MISSING_RESOURCE_PATHS = new Set(['/favicon.ico']);

const args = parseArgs(process.argv.slice(2));
const hostingUrl = validateHostingUrl(args.url);
const wssUrl = validateWssUrl(args.wssUrl);

if (args.dryRun) {
  console.log(`hosted_dashboard_url=${hostingUrl.href}`);
  console.log(`hosted_dashboard_wss_url=${wssUrl.href}`);
  console.log('hosted_dashboard_smoke=dry_run');
  process.exit(0);
}

const executablePath = await findChrome();
const browser = await chromium.launch({
  args: buildBrowserArgs(wssUrl, args.wssHostResolverIp),
  executablePath,
  headless: true,
});

try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  const consoleErrors = [];
  const failedSameOriginAssets = [];

  page.on('console', (message) => {
    if (message.type() === 'error') {
      consoleErrors.push(message.text());
    }
  });
  page.on('pageerror', (error) => {
    consoleErrors.push(error.message);
  });
  page.on('response', (response) => {
    const responseUrl = parseUrl(response.url());
    if (!responseUrl || responseUrl.origin !== hostingUrl.origin || response.status() < 400) return;
    if (isKnownMissingResource(responseUrl)) return;
    failedSameOriginAssets.push(`${response.status()} ${response.url()}`);
  });

  const websocketSeen = page.waitForEvent('websocket', {
    predicate: (websocket) => websocket.url() === wssUrl.href,
    timeout: args.timeoutMs,
  });

  await page.goto(hostingUrl.href, { waitUntil: 'domcontentloaded', timeout: args.timeoutMs });
  await websocketSeen;
  await page.getByText('דסק מניות LLM', { exact: true }).waitFor({ timeout: args.timeoutMs });
  if (await page.locator('html').getAttribute('dir') !== 'rtl') {
    throw new Error('hosted dashboard is not rendering RTL');
  }
  await page.waitForFunction(
    () => document.body.textContent.includes('מחובר'),
    undefined,
    { timeout: args.timeoutMs },
  );
  await page.waitForFunction(
    () => {
      const text = document.body.textContent ?? '';
      const removedLabels = [
        ['B', 'TC'].join(''),
        ['ETF', ' paper strategy'].join(''),
        ['Order', ' book'].join(''),
        ['Phase', ' 8'].join(''),
      ];
      return removedLabels.every((label) => !text.includes(label));
    },
    undefined,
    { timeout: args.timeoutMs },
  );

  if (consoleErrors.length > 0) {
    throw new Error(`browser console errors: ${consoleErrors.join(' | ')}`);
  }
  if (failedSameOriginAssets.length > 0) {
    throw new Error(`hosted dashboard asset errors: ${failedSameOriginAssets.join(' | ')}`);
  }

  console.log(`hosted_dashboard_smoke=ok url=${hostingUrl.href} wss=${wssUrl.href}`);
  console.log('dashboard_surface=llm_equity_desk');
} finally {
  await browser.close();
}

function parseArgs(argv) {
  const parsed = {
    dryRun: false,
    timeoutMs: DEFAULT_TIMEOUT_MS,
    url: DEFAULT_HOSTING_URL,
    wssHostResolverIp: DEFAULT_WSS_HOST_RESOLVER_IP,
    wssUrl: DEFAULT_WSS_URL,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--dry-run') {
      parsed.dryRun = true;
    } else if (arg === '--url') {
      index += 1;
      parsed.url = argv[index] || '';
    } else if (arg === '--wss-url') {
      index += 1;
      parsed.wssUrl = argv[index] || '';
    } else if (arg === '--wss-host-resolver-ip') {
      index += 1;
      parsed.wssHostResolverIp = argv[index] || '';
    } else if (arg === '--timeout-ms') {
      index += 1;
      const timeoutMs = Number.parseInt(argv[index] || '', 10);
      if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) {
        throw new Error('--timeout-ms must be a positive integer');
      }
      parsed.timeoutMs = timeoutMs;
    } else if (arg === '--help' || arg === '-h') {
      usage();
      process.exit(0);
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  return parsed;
}

function usage() {
  console.log(
    [
      'Usage: node scripts/hosted-dashboard-smoke.mjs [--dry-run] [--url https://host] [--wss-url wss://host/stream]',
      '       [--wss-host-resolver-ip 127.0.0.1]',
      '',
      'Defaults come from FIREBASE_HOSTING_URL and VITE_TRAIDAN_WS_URL.',
      'The optional resolver IP defaults to TRAIDAN_HOSTED_SMOKE_WSS_HOST_RESOLVER_IP.',
    ].join('\n'),
  );
}

function validateHostingUrl(rawUrl) {
  if (!rawUrl) {
    throw new Error('missing hosted dashboard URL');
  }
  const url = new URL(rawUrl);
  if (url.protocol !== 'https:') {
    throw new Error('hosted dashboard URL must use https://');
  }
  if (url.username || url.password) {
    throw new Error('hosted dashboard URL must not contain credentials');
  }
  return url;
}

function validateWssUrl(rawUrl) {
  if (!rawUrl) {
    throw new Error('missing hosted WSS URL');
  }
  const url = new URL(rawUrl);
  if (url.protocol !== 'wss:') {
    throw new Error('hosted dashboard stream must use wss://');
  }
  if (url.pathname !== '/stream') {
    throw new Error('hosted dashboard stream path must be /stream');
  }
  if (url.username || url.password) {
    throw new Error('hosted dashboard stream URL must not contain credentials');
  }
  return url;
}

function buildBrowserArgs(streamUrl, resolverIp) {
  const args = ['--disable-dev-shm-usage', '--no-sandbox'];
  if (!resolverIp) return args;

  if (isIP(resolverIp) !== 4) {
    throw new Error('--wss-host-resolver-ip must be an IPv4 address');
  }

  args.push(`--host-resolver-rules=MAP ${streamUrl.hostname} ${resolverIp}`);
  args.push(
    '--disable-features=LocalNetworkAccessChecks,BlockInsecurePrivateNetworkRequests,PrivateNetworkAccessSendPreflights,PrivateNetworkAccessRespectPreflightResults',
  );
  return args;
}

function isKnownMissingResource(url) {
  return KNOWN_MISSING_RESOURCE_PATHS.has(url.pathname);
}

function parseUrl(rawUrl) {
  try {
    return new URL(rawUrl);
  } catch {
    return null;
  }
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
      await stat(candidate);
      return candidate;
    } catch {
      // Try the next common browser path.
    }
  }

  return undefined;
}
