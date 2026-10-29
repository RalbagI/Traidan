# Public Dry-Run WSS Endpoint

The hosted dashboard connects to the Rust dry-run stream through Caddy TLS:

```text
Firebase Hosting SPA -> wss://stream.example.com/stream -> Caddy :443 -> Rust 127.0.0.1:8080 /stream
```

The stream is read-only and sanitized. It carries LLM Equity Desk status,
portfolio state, market snapshots, decisions, simulated fills, and memory
updates. It does not expose prompts, credentials, raw logs, full article text,
metrics, or local ingestion endpoints.

## Endpoint Contract

Use these URLs for hosted builds:

```bash
VITE_TRAIDAN_WS_URL=wss://stream.example.com/stream
FIREBASE_PREVIEW_WS_URL=wss://stream.example.com/stream
```

The Rust process still binds locally:

```bash
TRAIDAN_WS_BIND_ADDR=127.0.0.1:8080
TRAIDAN_METRICS_BIND_ADDR=127.0.0.1:9100
```

Caddy is the only public listener and forwards only `/stream`. Keep metrics,
`/internal/*`, and `/admin/*` private.

## AWS Network Shape

Terraform defines `aws_security_group.dry_run_public_wss` for the public WSS
surface. Configure `dry_run_wss_allowed_cidr_blocks` with operator or reviewer
CIDRs that may load the hosted dashboard. The security group exposes only
`tcp/443`; SSH, SSM, private Prometheus scraping, and other operational access
belong in `additional_execution_security_group_ids`.

The default CI fixtures leave `dry_run_wss_allowed_cidr_blocks` empty so mock
plans keep the endpoint closed by default.

## Caddy Setup

Install Caddy on the dry-run host and copy the example site:

```bash
sudo install -m 0644 deployment/wss/Caddyfile.example /etc/caddy/Caddyfile
sudo systemctl enable caddy
sudo systemctl restart caddy
```

Set the public DNS name before starting Caddy:

```bash
sudo systemctl edit caddy
```

Use this environment override:

```ini
[Service]
Environment=TRAIDAN_WSS_HOST=stream.example.com
```

Caddy obtains and rotates the public TLS certificate automatically through ACME
when DNS points at the execution host and inbound `tcp/443` is allowed.

## Start And Stop

Start the dry-run engine on the host:

```bash
sudo systemctl start traidan
```

The service must run with:

```bash
TRAIDAN_RUN_MODE=dry_run
TRAIDAN_MARKET_DATA_SOURCE=alpaca_equity
TRAIDAN_MARKET_SYMBOL=SPY
TRAIDAN_WS_BIND_ADDR=127.0.0.1:8080
TRAIDAN_METRICS_BIND_ADDR=127.0.0.1:9100
```

Stop public access without changing Firebase Hosting:

```bash
sudo systemctl stop traidan
sudo systemctl stop caddy
```

To rotate the endpoint, update DNS and the Caddy `TRAIDAN_WSS_HOST` override,
then restart Caddy.

## Health Checks

Validate URL shape without network access:

```bash
node scripts/check-public-wss.js --dry-run wss://stream.example.com/stream
```

Validate the live endpoint:

```bash
node scripts/check-public-wss.js wss://stream.example.com/stream
```

The check requires `/stream` to accept a WebSocket upgrade and emit:

```text
llm_equity_service_status
equity_portfolio_state
market_research_snapshot
llm_equity_cycle
operator_event
```

After Firebase deploys, open `https://dashboard.example.com` and confirm the LLM
Equity Desk connects without mixed-content errors. The hosted browser smoke
performs the same check:

```bash
cd frontend
npm run smoke:hosted-dashboard -- \
  --url https://dashboard.example.com \
  --wss-url wss://stream.example.com/stream
```
