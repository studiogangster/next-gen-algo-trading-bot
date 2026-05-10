---
name: ngat-architecture
description: Analyze and evolve the overall architecture of next-gen-algo-trading-bot across ingestion, aggregation, strategy execution, storage, APIs, and UI layers. Use when making cross-module changes, debugging data flow breaks between modules, planning new features, or assessing tradeoffs and risk before refactors.
---

# Ngat Architecture

## Use This Workflow

1. Trace a request through layers in this order: `config -> cli -> core workers -> storage -> tradingview backend -> frontend`.
2. Verify timestamp conventions per boundary (UTC epoch seconds in Redis, timezone-aware pandas inside workers, IST formatting only at UI).
3. Check whether the module is write-heavy (Redis/parquet) or read-heavy (API/UI) before changing behavior.
4. Prefer small interface contracts over broad rewrites.

## Module Map

- `brokers/`: Zerodha login, session, historical pull, order/position reads.
- `feeds/`: live websocket tick adapter.
- `core/`: orchestration, aggregation, timeframe and indicator workers.
- `storage/`: RedisTimeSeries helpers and parquet persistence.
- `dashboard/`: local operator dashboards (Streamlit/Dash).
- `tradingview_dashboard/backend/`: FastAPI read APIs for candles/indicators.
- `tradingview_dashboard/frontend/`: Vue + lightweight-charts visualization.
- `config/` + `cli/`: runtime configuration and process entrypoints.

## Pros

- Keep boundaries clear between ingestion, compute, and presentation.
- Support horizontal scaling with Ray actors and Redis as shared state.
- Keep broker-specific logic mostly isolated from strategy logic.

## Cons

- Mix long-running loops and orchestration in actor methods without lifecycle controls.
- Duplicate candle aggregation logic in multiple places.
- Mix cache, compute, and API responsibilities with minimal schema centralization.

## Cross-Cutting Caveats

- Treat config consistency as high risk: `config/config.yaml` includes timeframes (`1d`, `1w`, `1M`, `1y`) that `Settings.validate_timeframes` currently rejects.
- Treat timestamp unit mismatches as high risk: comments say milliseconds while code often uses epoch seconds.
- Treat logging output as noisy and partially debug-only (`print` in hot paths), which can hide production signal.
- Avoid assuming polling loops are idempotent: some loops use `from_ts="-"` and `to_ts="+"`, forcing full-range scans.

## Forward Features

- Add a shared candle schema package with strict timestamp/unit typing.
- Add centralized feature flags for backfill mode vs incremental mode.
- Add structured logging/metrics (latency, dropped ticks, indicator lag, Redis write volume).
- Add graceful actor shutdown and health probes for Ray workers.
