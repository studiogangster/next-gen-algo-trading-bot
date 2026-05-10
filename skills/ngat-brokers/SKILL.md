---
name: ngat-brokers
description: Implement and maintain broker connectivity for Zerodha in brokers/ including auth, session retry behavior, order/position APIs, and historical data sync helpers. Use when touching login flow, order execution, broker resilience, or market data backfill behavior.
---

# Ngat Brokers

## Overview

Own the boundary between internal engine components and Zerodha endpoints. Use this skill for reliability and security improvements in broker-facing logic.

## Submodules

- `brokers/utils.py`: TOTP generation, login, cache read/write (`.keystore.json`), token validation.
- `brokers/kite_trade.py`: `ZerodhaBroker` REST wrapper and retry behavior.
- `brokers/zerodha.py`: historical backfill and realtime sync into RedisTimeSeries.

## Pros

- Keep provider-specific behavior isolated from core strategy code.
- Reuse cached `enctoken` and refresh on retriable auth failures.
- Backfill and realtime sync share one candle ingestion path.

## Cons

- Keep direct `os.system` installs in import path (`kite_trade.py`) which is unsafe in production.
- Keep secret token cache in project root without encryption.
- Keep historical loops broad and potentially expensive when partitions are unset.

## Caveats

- Never log secrets; `print` statements currently risk leaking operational internals.
- Treat retry login and request replay as non-idempotent for order endpoints.
- Treat timeframe mapping carefully (`day` vs intraday intervals) to avoid API errors.
- Handle Zerodha API limits explicitly; current code relies on chunking but not adaptive throttling.

## Forward Features

- Add secure credential management (OS keychain or vault) for token cache.
- Add idempotency keys and order state reconciliation layer.
- Add broker abstraction coverage for alternate providers beyond Zerodha.
