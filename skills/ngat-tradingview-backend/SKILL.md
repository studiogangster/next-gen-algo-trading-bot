---
name: ngat-tradingview-backend
description: Maintain the FastAPI service in tradingview_dashboard/backend for candles, indicators, and latest timestamp endpoints backed by RedisTimeSeries. Use when changing API contracts, performance, indicator response shape, or Redis query behavior for frontend consumers.
---

# Ngat Tradingview Backend

## Overview

Provide read APIs over Redis-stored OHLC and computed indicator data for chart consumers.

## Submodules

- `tradingview_dashboard/backend/main.py`: `/candles`, `/indicator`, `/indicators`, `/candles/latest`.
- `tradingview_dashboard/backend/pyproject.toml`: backend dependency constraints.

## Pros

- Keep API slim and data-focused.
- Use label-driven RedisTimeSeries filters for efficient field retrieval.
- Expose indicator endpoints directly for frontend overlays.

## Cons

- Keep duplicate endpoint families (`/candles` and `/_candles`) with overlapping purpose.
- Compute indicators on request without caching or batching.
- Include hardcoded `COUNT 500` in queries, not dynamically tied to `limit`.

## Caveats

- Keep indicator output schemas stable; frontend assumes specific lowercased column names.
- Guard against missing field series; endpoint currently hard-fails when any OHLCV field is missing.
- Verify pandas-ta column names for KC; upstream naming may differ from `KC_Upper/Middle/Lower`.

## Forward Features

- Add pagination/windowing contracts with cursor or explicit candle index.
- Add server-side indicator caching keyed by symbol/timeframe/window/config hash.
- Add pydantic response models and OpenAPI examples for frontend contract safety.
