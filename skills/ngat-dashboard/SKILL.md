---
name: ngat-dashboard
description: Maintain local operator dashboards in dashboard/ using Streamlit and Dash for candle inspection and indicator visualization. Use when improving manual observability, chart behavior, or live diagnostics from parquet-backed data.
---

# Ngat Dashboard

## Overview

Support quick local introspection tools for candles and indicators separate from the production-facing Vue dashboard.

## Submodules

- `dashboard/visualize.py`: Streamlit dashboard with candle/Keltner rendering.
- `dashboard/dash_app.py`: Dash dashboard with symbol/timeframe selection and overlays.

## Pros

- Keep immediate visual feedback while debugging aggregation paths.
- Keep no dependency on frontend build chain for basic diagnostics.

## Cons

- Maintain two dashboard stacks (Streamlit + Dash) with duplicated logic.
- Tie to local parquet files instead of Redis/API source of truth.

## Caveats

- Guard timezone conversions; some files assume timezone-aware timestamps.
- Keep timeframe support consistent; `day` and higher frames are partially handled.
- Watch performance for full-file parquet reads on each refresh.

## Forward Features

- Add shared chart utility module reused by Dash and Streamlit.
- Add websocket/live push updates instead of polling and manual refresh.
- Add operator panels for worker health, Redis lag, and signal throughput.
