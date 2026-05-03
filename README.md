# Traderalbag

AI-Driven Cryptocurrency Algorithmic Trading Engine.

## Architecture

See [architecture.md](architecture.md) for the full technical specification.

## Layer Overview

```
┌─────────────────────────────────────────┐
│   INTELLIGENCE LAYER  (Python)          │
│   RL Agents · LLM Orchestrator · OFI/VPIN│
└────────────────┬────────────────────────┘
                 │ ZeroMQ + Apache Arrow (zero-copy)
┌────────────────▼────────────────────────┐
│   EXECUTION LAYER  (Rust)               │
│   OMS · LMAX Disruptor · VaR Engine     │
│   Binance SBE  ·  Coinbase FIX          │
└─────────────────────────────────────────┘
```

## Development

```bash
# Local stack (QuestDB + Prometheus + Grafana)
docker compose -f deployment/docker-compose.yml up -d

# Rust build
cargo build --release

# Python intelligence layer
pip install -e ".[dev]"
```

## Project Phases

| Phase | Scope |
|-------|-------|
| 1 | Infrastructure & Data Layer (AWS + QuestDB) |
| 2 | Execution Layer — Rust OMS, SBE, FIX |
| 3 | IPC — ZeroMQ + Apache Arrow |
| 4 | Intelligence Layer — RL, LLM, Feature Engineering |
| 5 | Telemetry & UI — Prometheus, Grafana, React |
