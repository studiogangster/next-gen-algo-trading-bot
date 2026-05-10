---
name: ngat-core
description: Implement and maintain the core orchestration and compute pipeline in core/ including base interfaces, engine actors, aggregation, timeframe generation, and indicator generation. Use when changing runtime data flow, worker behavior, or strategy execution mechanics.
---

# Ngat Core

## Overview

Drive ingestion-to-signal execution in the Python compute layer. Use this skill to safely modify actor loops, aggregation semantics, and core interface contracts.

## Submodules

- `core/base.py`: abstract interfaces for feed, aggregator, strategy, broker, storage.
- `core/engine.py`: `Engine`, `SymbolWorker`, `OrderAndPositionWorker`, Ray orchestration.
- `core/aggregator.py`: in-memory tick/candle store and timeframe resampling with parquet dump.
- `core/timeframe_generator_worker.py`: aggregate 1m Redis candles into derived frames.
- `core/indicator_generator_worker.py`: compute indicators from candles and write signal series.

## Pros

- Keep modular boundaries through base interfaces.
- Keep symbol-level concurrency model explicit through dedicated workers.
- Keep caching path simple with Redis + optional parquet mirrors.

## Cons

- Keep multiple infinite loops with no cooperative shutdown.
- Keep repeated full-range Redis scans (`-` to `+`) that do not scale.
- Keep some dead/commented paths (`on_tick` flow mostly bypassed by sync workers).

## Caveats

- Treat `indicator_generator_worker` timestamp bookkeeping carefully: `last_polled` mixes millisecond comments with second-based fetches.
- Treat `timeframe_generator_worker` data-loop control carefully: `break` on missing data exits symbol loop early.
- Treat resample rule normalization carefully; timeframe token mapping is duplicated and inconsistent across modules.
- Keep actor construction lightweight; current workers instantiate broker/session paths repeatedly.

## Forward Features

- Add incremental window fetches keyed by last successful per-timeframe watermark.
- Add strict actor lifecycle management (`start`, `pause`, `shutdown`, heartbeats).
- Add deterministic replay mode for backtests using stored candles/signals.
- Add unified timeframe utility module used by aggregator, workers, API, and frontend.
