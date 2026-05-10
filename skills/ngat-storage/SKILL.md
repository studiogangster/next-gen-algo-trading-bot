---
name: ngat-storage
description: Maintain storage abstractions and backends in storage/ including RedisTimeSeries access patterns and parquet persistence. Use when changing candle/signal write semantics, optimizing Redis queries, or evolving data retention behavior.
---

# Ngat Storage

## Overview

Manage durable and cached market data paths for candles and signals.

## Submodules

- `storage/redis_client.py`: Redis client singleton and TS helper wrappers (`TS.ADD`, `TS.RANGE`, `TS.MREVRANGE`).
- `storage/parquet.py`: `ParquetStorage` implementing `BaseStorage`.
- `core/base.py::BaseStorage`: write-contract for candles/signals/pnl.

## Pros

- Keep time-series writes efficient with pipelines.
- Keep redis helpers reusable across workers and API.
- Keep parquet snapshots simple for local inspection/dashboarding.

## Cons

- Keep no explicit retention policy in Redis series creation.
- Keep schema implicit in key naming and labels instead of typed contracts.
- Keep parquet writes full-rewrite for candles, which is expensive at scale.

## Caveats

- Use `ON_DUPLICATE FIRST/LAST` intentionally; wrong mode can silently hide corrections.
- Avoid unbounded `TS.MREVRANGE - +` on large datasets in hot paths.
- Handle Redis module availability (`unknown command`) as startup health check.

## Forward Features

- Add retention and compaction policies per timeframe.
- Add typed serializer/deserializer for candle/signal payloads.
- Add partitioned parquet writer with append-safe semantics.
