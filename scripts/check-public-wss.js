#!/usr/bin/env node
'use strict';

const DEFAULT_TIMEOUT_MS = 10_000;
const EXPECTED_TYPES = new Set([
  'operator_event',
  'llm_equity_service_status',
  'equity_portfolio_state',
  'market_research_snapshot',
  'llm_equity_cycle',
  'equity_order_simulated',
  'llm_memory_update',
]);
const NON_STREAM_PATH = '/__traidan_wss_health_probe__';

function usage() {
  console.error(
    [
      'Usage: node scripts/check-public-wss.js [--dry-run] [--timeout-ms N] [wss://host/stream]',
      '',
      'URL defaults to VITE_TRAIDAN_WS_URL.',
    ].join('\n'),
  );
}

function parseArgs(argv) {
  const args = {
    dryRun: false,
    timeoutMs: DEFAULT_TIMEOUT_MS,
    url: process.env.VITE_TRAIDAN_WS_URL || '',
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--dry-run') {
      args.dryRun = true;
    } else if (arg === '--timeout-ms') {
      index += 1;
      const value = Number.parseInt(argv[index] || '', 10);
      if (!Number.isSafeInteger(value) || value <= 0) {
        throw new Error('--timeout-ms must be a positive integer');
      }
      args.timeoutMs = value;
    } else if (arg === '--help' || arg === '-h') {
      usage();
      process.exit(0);
    } else if (arg.startsWith('--')) {
      throw new Error(`unknown argument: ${arg}`);
    } else {
      args.url = arg;
    }
  }

  return args;
}

function validateStreamUrl(rawUrl) {
  if (!rawUrl) {
    throw new Error('missing WSS URL; pass an argument or set VITE_TRAIDAN_WS_URL');
  }

  const url = new URL(rawUrl);
  const { protocol, pathname } = url;
  if (protocol !== 'wss:') {
    throw new Error('public dry-run endpoint must use wss://');
  }
  if (pathname !== '/stream') {
    throw new Error('public dry-run endpoint path must be /stream');
  }
  if (url.username || url.password) {
    throw new Error('public dry-run endpoint URL must not contain credentials');
  }
  return url;
}

function timeoutHandle(ms, onTimeout) {
  const timer = setTimeout(onTimeout, ms);
  if (typeof timer.unref === 'function') {
    timer.unref();
  }
  return timer;
}

function closeQuietly(ws) {
  try {
    ws.close();
  } catch {
    // Best-effort cleanup only.
  }
}

function connectAndCollectTypes(url, timeoutMs) {
  return new Promise((resolve, reject) => {
    const seen = new Set();
    const ws = new WebSocket(url.href);
    let settled = false;

    const finish = (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      closeQuietly(ws);
      if (error) {
        reject(error);
      } else {
        resolve(seen);
      }
    };

    const timer = timeoutHandle(timeoutMs, () => {
      const missing = [...EXPECTED_TYPES].filter((type) => !seen.has(type));
      finish(new Error(`timed out waiting for dry-run stream types: ${missing.join(', ')}`));
    });

    ws.onopen = () => {
      console.log(`stream_upgrade=ok url=${url.href}`);
    };
    ws.onerror = () => {
      finish(new Error(`stream_upgrade=failed url=${url.href}`));
    };
    ws.onclose = () => {
      const missing = [...EXPECTED_TYPES].filter((type) => !seen.has(type));
      if (missing.length > 0) {
        finish(new Error(`stream closed before types arrived: ${missing.join(', ')}`));
      }
    };
    ws.onmessage = (event) => {
      let parsed;
      try {
        parsed = JSON.parse(String(event.data));
      } catch (error) {
        finish(new Error(`invalid JSON frame: ${error.message}`));
        return;
      }

      if (typeof parsed.type === 'string') {
        seen.add(parsed.type);
      }
      if ([...EXPECTED_TYPES].every((type) => seen.has(type))) {
        finish();
      }
    };
  });
}

function rejectedPathUrl(streamUrl) {
  const rejected = new URL(streamUrl.href);
  rejected.pathname = NON_STREAM_PATH;
  rejected.search = '';
  rejected.hash = '';
  return rejected;
}

function assertNonStreamPathRejected(url, timeoutMs) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url.href);
    let settled = false;

    const finish = (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      closeQuietly(ws);
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    };

    const timer = timeoutHandle(timeoutMs, () => {
      finish(new Error(`non-stream path did not reject before timeout: ${url.href}`));
    });

    ws.onopen = () => {
      finish(new Error(`non-stream path unexpectedly upgraded: ${url.href}`));
    };
    ws.onerror = () => {
      finish();
    };
    ws.onclose = () => {
      finish();
    };
  });
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const streamUrl = validateStreamUrl(args.url);

  if (args.dryRun) {
    console.log(`public_wss_url=${streamUrl.href}`);
    console.log(`non_stream_probe=${rejectedPathUrl(streamUrl).href}`);
    return;
  }

  if (typeof WebSocket === 'undefined') {
    throw new Error('Node.js with built-in WebSocket support is required');
  }

  const seen = await connectAndCollectTypes(streamUrl, args.timeoutMs);
  await assertNonStreamPathRejected(rejectedPathUrl(streamUrl), args.timeoutMs);
  console.log(`stream_types=ok types=${[...seen].sort().join(',')}`);
  console.log('non_stream_path=rejected');
}

main().catch((error) => {
  console.error(`ERROR: ${error.message}`);
  process.exit(1);
});
