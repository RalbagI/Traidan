# Architecture: AI-Driven Cryptocurrency Algorithmic Trading Engine

> **Status:** Living specification — update this document when any architectural decision changes.

---

## 1. System Philosophy

The engine is built on a strict two-layer separation:

| Layer | Language | Role | Constraint |
|-------|----------|------|------------|
| **Thinking** | Python | ML inference, RL training, LLM orchestration, feature engineering | Async, latency-tolerant |
| **Doing** | Rust | Order execution, risk enforcement, market data ingestion | Deterministic, GC-free, hot-path only |

Python's Global Interpreter Lock and garbage collector make it structurally incompatible with sub-millisecond order routing. Rust's ownership model eliminates runtime garbage collection entirely, guaranteeing microsecond-level determinism from signal receipt to API dispatch. The two layers communicate exclusively through defined IPC contracts (§5) — the Thinking layer never touches a live socket; the Doing layer never runs a neural network.

---

## 2. Infrastructure & Colocation

Geographic proximity to exchange matching engines is the primary deterministic latency factor. The deployment targets exchange-specific AWS availability zones:

| Exchange | Region | Expected RTT |
|----------|--------|-------------|
| Binance | ap-northeast-1 (Tokyo) | 2–3 ms |
| Coinbase | us-east-1 (N. Virginia) | < 1 ms |
| Kraken | us-east-1 / eu-west-1 | < 1–5 ms |
| OKX / Bybit | ap-southeast-1 (Singapore) | 2–3 ms |

**Compute:** AWS EC2 bare-metal instances (`z1d.metal` class). Bare-metal eliminates hypervisor jitter, cutting P99.9 tail latency by ~29% versus virtualized instances.

**Network topology:**
- All execution instances in a single-zone **Cluster Placement Group** to minimize inter-node switch hops.
- **VPC Peering** directly to exchange order gateways where available, bypassing the public internet entirely.
- Trade-off: single-zone placement sacrifices multi-AZ redundancy. Failover is handled at the OMS level via secondary routing, not infrastructure replication.

**Infrastructure-as-code:** Terraform manages all AWS resources (see `deployment/terraform/`). Local development uses Docker Compose (see `deployment/docker-compose.yml`).

---

## 3. Data Layer

### QuestDB

All time-series data (ticks, L2/L3 order book snapshots, derived features) is stored in QuestDB. Selection rationale:

- WAL architecture bypasses MergeTree merge overhead — benchmarks show 4.1–4.8× faster ingestion than ClickHouse under high-cardinality loads.
- Peak ingestion: ~8.59 million rows/second.
- Native nanosecond timestamp precision — mandatory for cryptocurrency tick reconstruction.
- `LATEST ON` syntax uses hash maps to retrieve the most recent order book state up to 27× faster than `GROUP BY`-based approaches.

### Primary Schema

```sql
CREATE TABLE market_data (
    timestamp TIMESTAMP,
    symbol    SYMBOL,
    bids      DOUBLE[20][2],   -- [price_levels][volumes], top 20 ticks
    asks      DOUBLE[20][2]
) TIMESTAMP(timestamp)
  PARTITION BY HOUR
  DEDUP UPSERT KEYS(timestamp, symbol);
```

`bids` and `asks` store the full L2 snapshot as N-dimensional arrays in a single row. Contiguous memory layout enables SIMD-vectorized reads. `SYMBOL` type uses dictionary encoding (integer lookup, not string comparison). `PARTITION BY HOUR` keeps file sizes manageable under sub-millisecond update rates. `DEDUP UPSERT` prevents duplicate ticks from out-of-order WebSocket delivery.

### Cold Storage

Historical data older than the rolling hot window is exported to **Apache Parquet on Amazon S3** via QuestDB's native export. The Python training pipeline reads Parquet directly via Polars or PyArrow — no proprietary lock-in.

---

## 4. Execution Layer (Rust)

Source lives in `src/`. Built as a Cargo workspace with `maturin` providing the PyO3 FFI boundary for any cross-language bindings.

### Order Management System (`src/oms/`)

- `state.rs` — live portfolio state: open positions, pending orders, realized/unrealized PnL, margin utilization.
- `routing.rs` — smart order routing: selects venue and algorithm (aggressive IOC vs. passive limit) based on current spread and VaR headroom.

### Risk Engine (`src/risk/`)

- `var_limits.rs` — hard-coded Value-at-Risk thresholds. Breaching a drawdown limit triggers immediate fail-safe: cancel all open orders, flatten positions, halt signal consumption.
- `exposure.rs` — real-time gross/net exposure calculations per asset and portfolio-wide.

### Exchange Integrations (`src/exchange/`)

#### Binance SBE (`binance_sbe.rs`)

Binance is the primary venue. The engine connects via **Simple Binary Encoding (SBE) over WebSocket** using Binance's official `spot_2_0.xml` FIX SBE schema. SBE uses fixed-position binary layouts (little-endian), allowing the Rust allocator to cast the incoming byte stream directly into a pre-defined struct — zero string parsing, zero allocation on the hot path.

Subscription: **Diff Depth Stream** at 25 ms update intervals (Binance's current minimum). The decoder checks `messageHeader.templateId` to distinguish `DepthDiffStreamEvent` payloads and applies incremental updates to the local L2 book.

Binance rate limit note: successful order placements and cancellations carry zero weight against the request-weight limit, enabling near-unlimited execution throughput.

#### Coinbase FIX (`coinbase_fix.rs`)

Secondary venue for regulated fiat-crypto pairs and cross-venue arbitrage. Orders are submitted via **FIX 5.0 (FIXT.1.1) over persistent TCP+SSL**:

```
tcp+ssl://fix-ord.exchange.coinbase.com:6121
```

The Rust session layer manages sequence numbers (`MsgSeqNum`, tag 34), Logon handshake (`35=A`), and heartbeat synchronization. Trade entry uses `NewOrderSingle` (`35=D`):

| Tag | Field | Notes |
|-----|-------|-------|
| 8 | BeginString | `FIXT.1.1` |
| 11 | ClOrdID | Deterministic UUID v4 per order |
| 35 | MsgType | `D` (NewOrderSingle) |
| 38 | OrderQty | Base asset quantity |
| 40 | OrdType | `1` Market / `2` Limit / `4` Stop-Limit |
| 44 | Price | Required for limit orders |
| 54 | Side | `1` Buy / `2` Sell |
| 55 | Symbol | e.g. `BTC-USD` |
| 59 | TimeInForce | `3` IOC / `4` FOK for HFT strategies |

Cancellations use `OrderCancelRequest` (`35=F`) referencing the original `ClOrdID`.

### LMAX Disruptor (`src/disruptor/`)

The engine processes millions of events per second across multiple concurrent threads (SBE decoder, risk engine, FIX router). Rather than lock-based queues (mutexes introduce kernel context-switch overhead), the OMS implements the **LMAX Disruptor pattern**:

- Pre-allocated, bounded, **lock-free ring buffer** (`ring_buffer.rs`). Fixed size eliminates dynamic allocation on the hot path.
- Each event processor claims a sequence slot via atomic compare-and-swap.
- `padding.rs` applies **cache-line alignment** (`#[repr(align(64))]`) to all ring buffer entries, preventing false sharing between independent processor threads writing to adjacent memory addresses in the same 64-byte CPU cache line.

---

## 5. IPC Contracts

The Thinking and Doing layers run in separate processes. All communication crosses the IPC boundary through two mechanisms, selected by data shape:

### ZeroMQ — Discrete Signals (`src/ipc/zmq_router.rs`)

ZeroMQ provides brokerless, sub-20 µs messaging with automatic reconnection and async I/O. Two socket patterns:

| Pattern | Direction | Payload |
|---------|-----------|---------|
| **Push/Pull** | Python → Rust | Trade commands: symbol, side, price, size, confidence, algo params |
| **Pub/Sub** | Rust → Python | Portfolio state, VaR warnings, execution fills |

The Pub/Sub broadcast ensures the intelligence layer always has a consistent portfolio snapshot before generating the next action sequence.

### Apache Arrow C Data Interface — Matrix Streaming (`src/ipc/arrow_ffi.rs`)

ZeroMQ is insufficient for the continuous high-frequency order book matrices (20-level depth × 2 sides × 40 updates/second from Binance 25 ms streams). Serializing these via JSON or Protobuf would saturate the CPU.

Instead, the engine uses **shared memory via the Apache Arrow C Data Interface**:

1. Rust ingests SBE stream → updates local L2 array in memory.
2. Rust populates `FFI_ArrowArray` and `FFI_ArrowSchema` C structs pointing to that memory.
3. Rust sends the 64-bit memory pointer over a ZeroMQ control channel.
4. Python receives the pointer, wraps it in a `PyCapsule`, and constructs PyTorch tensors / Polars DataFrames backed by the **same physical memory** — zero copy, zero serialization.

---

## 6. Intelligence Layer (Python)

Source lives in `intelligence/`. The layer is async and latency-tolerant; it never touches order submission directly.

### Feature Engineering (`data_pipeline/`)

Raw order book data is insufficient. Two microstructure features capture informed-flow dynamics:

- **Order Flow Imbalance (OFI)** (`features_ofi.py`): net change in volume at the best bid/ask over microsecond windows. Rising OFI precedes short-term directional moves — institutional traders interact with limit books before prices move.
- **VPIN** (`features_vpin.py`): classifies tick volume as buyer- or seller-initiated. VPIN spikes signal toxic institutional flow; the RL agent widens spreads in response to avoid adverse selection.

### Reinforcement Learning (`rl_agents/`)

The engine uses **actor-critic RL** (PPO primary, SAC secondary) rather than supervised classification. Financial markets are non-stationary; a bull-market classifier fails during liquidity crashes. RL agents adapt to regime shifts via ongoing reward feedback.

- **Policy network (Actor):** maps state (order book depth, OFI, VPIN, portfolio exposure) → continuous action ∈ [-1.0, 1.0] (position sizing).
- **Value network (Critic):** estimates long-term expected return from current state; guides actor gradient updates.

**Market impact simulation** (`market_impact.py`): training assumes finite liquidity via the **Almgren-Chriss framework**. The simulator applies temporary and permanent price impact proportional to execution velocity and available resting liquidity, preventing agents from learning strategies that only work under infinite-liquidity assumptions.

**Reward function** (`rewards.py`): composite Market Impact Adjusted Reward:
- Raw PnL adjusted for simulated slippage (penalizes large orders that move the book).
- CARA utility term: `U = -exp(-λ * r)` — exponentially penalizes high-variance strategies, naturally discouraging excessive risk.
- Temporal coherence bonus: rewards consistency across consecutive timesteps, preventing capital-bleeding oscillations from maker/taker fees.

### LLM Orchestrator (`llm_orchestrator/`)

LLMs provide zero-shot macro-context reasoning over unstructured text. **Hard constraint: LLMs never touch order execution.** Their autoregressive decoding latency (100 ms–seconds) and non-deterministic outputs are incompatible with the execution hot-path.

Role is strictly:
- `sentiment_parser.py`: parse news/social streams → sentiment tensors fed as RL input features.
- `agent_manager.py`: multi-agent coordination, hyperparameter tuning proposals.
- `strategy_augmenter.py`: offline algorithm logic review and augmentation.

### Experiment Tracking (`backtester/tracking.py`)

MLflow or Weights & Biases tracks hyperparameters, feature sets, and episodic rewards across training runs. Models must pass a **simulation-to-reality parity check** — running in a paper-trading environment that replicates the Rust engine's queue-position modeling and network delays — before any live capital allocation.

---

## 7. Telemetry & UI

### Infrastructure Monitoring (Prometheus + Grafana)

Prometheus scrapes the Rust execution layer for:
- CPU context switches and memory allocation overhead
- LMAX Disruptor thread latency (P50/P99/P99.9)
- API rate limit consumption
- WebSocket reconnection events and packet drops

Grafana renders live dashboards and manages alerting. Critical alerts (WebSocket disconnect, execution latency spike beyond threshold) trigger fail-safe liquidation mode in the Rust OMS.

### Offline Research (Streamlit)

Quantitative researchers use Streamlit to explore Pandas DataFrames and PyTorch tensors offline. Not suitable for live trading dashboards due to server-side rendering limitations under high-frequency data.

### Live Operations UI (React + ECharts)

The live dashboard is a decoupled **React** SPA (`frontend/`). The Rust engine pushes normalized JSON via WebSocket directly to the browser. To avoid DOM overload when rendering 100k-point matrices updating tick-by-tick, all order book visualization uses **ECharts with the HTML5 Canvas API**:

- `OrderBookCanvas.jsx`: L2 order book depth heatmap, resting liquidity at each price level.
- `WebSocketClient.jsx`: maintains persistent WebSocket connection to the Rust engine.
- AI signal overlay and live execution markers rendered as Canvas layers on top of the order book.

---

## 8. Directory Structure

```
Traderalbag/
├── Cargo.toml                    # Rust workspace manifest
├── pyproject.toml                # Maturin build + Python dependencies
├── architecture.md               # This document
├── README.md
│
├── .github/
│   └── workflows/
│       └── ci.yml                # CI: Rust build + Python tests
│
├── deployment/
│   ├── terraform/                # AWS EC2, VPC, Placement Groups
│   ├── docker-compose.yml        # Local: QuestDB, Prometheus, Grafana
│   └── grafana_dashboards/       # Pre-built dashboard JSON exports
│
├── src/                          # EXECUTION LAYER — Rust ("The Doing")
│   ├── main.rs                   # Primary binary entry point
│   ├── lib.rs                    # PyO3 FFI boundary
│   ├── oms/
│   │   ├── state.rs              # Live portfolio + order state
│   │   └── routing.rs            # Smart order routing
│   ├── risk/
│   │   ├── var_limits.rs         # Hard drawdown enforcement
│   │   └── exposure.rs           # Real-time position exposure
│   ├── exchange/
│   │   ├── binance_sbe.rs        # SBE WebSocket decoder
│   │   └── coinbase_fix.rs       # FIX 5.0 order gateway
│   ├── disruptor/
│   │   ├── ring_buffer.rs        # LMAX lock-free ring buffer
│   │   └── padding.rs            # Cache-line alignment structs
│   └── ipc/
│       ├── zmq_router.rs         # ZeroMQ Push/Pull + Pub/Sub
│       └── arrow_ffi.rs          # Arrow C Data Interface binding
│
├── intelligence/                 # INTELLIGENCE LAYER — Python ("The Thinking")
│   ├── __init__.py
│   ├── llm_orchestrator/
│   │   ├── sentiment_parser.py   # News/social → sentiment tensors
│   │   ├── agent_manager.py      # Multi-agent coordination
│   │   └── strategy_augmenter.py # Offline algorithm tuning
│   ├── data_pipeline/
│   │   ├── questdb_client.py     # Async QuestDB queries (LATEST ON)
│   │   ├── features_ofi.py       # Order Flow Imbalance
│   │   └── features_vpin.py      # Volume-Synchronized PIN
│   ├── rl_agents/
│   │   ├── ppo_actor_critic.py   # PPO policy + value networks
│   │   ├── market_impact.py      # Almgren-Chriss simulation
│   │   └── rewards.py            # CARA + temporal coherence reward
│   └── backtester/
│       ├── engine.py             # Tick-by-tick latency-adjusted sim
│       └── tracking.py           # MLflow / W&B experiment tracking
│
└── frontend/                     # TELEMETRY & UI
    ├── package.json
    └── src/
        ├── App.jsx               # React application root
        ├── OrderBookCanvas.jsx   # ECharts + Canvas L2 heatmap
        └── WebSocketClient.jsx   # WebSocket connection to Rust engine
```

---

## 9. Technology Decision Log

| Decision | Choice | Rationale | Alternative Rejected |
|----------|--------|-----------|---------------------|
| Execution language | Rust | No GC pauses, ownership-enforced memory safety, C-level throughput | C++ (memory unsafety), Go (GC), Java (JVM pause) |
| Intelligence language | Python | PyTorch, NumPy, Pandas ecosystem; async-friendly | Julia (smaller ecosystem), C++ (tooling overhead) |
| Time-series DB | QuestDB | WAL ingestion (8.59M rows/s), native nanosecond timestamps, LATEST ON | ClickHouse (4× slower ingestion), InfluxDB (DevOps-focused), TimescaleDB (row-based degradation at scale) |
| Exchange protocol (Binance) | SBE over WebSocket | Fixed binary layout, zero string parsing, zero allocation on hot path | JSON REST (string parsing overhead), FIX (exchange doesn't offer for market data) |
| Exchange protocol (Coinbase) | FIX 5.0 over TCP+SSL | Industry-standard deterministic sequencing, persistent connection, no HTTP overhead | REST (connection setup per request), WebSocket JSON (parsing overhead) |
| Inter-thread comms | LMAX Disruptor | Lock-free ring buffer, cache-line aware, eliminates kernel context switches | std::Mutex queues (kernel intervention), channel-based (allocation overhead) |
| IPC — signals | ZeroMQ | Brokerless, sub-20 µs, Rust + Python bindings, auto-reconnect | gRPC (100+ µs overhead), HTTP/REST (connection overhead), raw sockets (no framing) |
| IPC — matrices | Arrow C Data Interface | True zero-copy shared memory, language-agnostic, no serialization | ZeroMQ frames (serialization cost), Protobuf (encode/decode cost), Pickle (Python-only) |
| RL algorithm | PPO (primary) / SAC (secondary) | Stable training, handles non-stationary markets, continuous action space | DQN (discrete actions only), supervised ML (non-adaptive to regime shifts) |
| Frontend rendering | ECharts + HTML5 Canvas | GPU-accelerated, handles 100k-point tick-by-tick updates without DOM bottleneck | SVG (DOM update cost), D3 (DOM-based), Grafana (insufficient custom interaction) |
