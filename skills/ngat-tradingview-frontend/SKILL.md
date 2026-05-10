---
name: ngat-tradingview-frontend
description: Maintain the Vue + lightweight-charts UI in tradingview_dashboard/frontend including multi-timeframe chart rendering, indicator overlays, and API polling. Use when improving chart UX, syncing behavior, performance, or frontend/backend data contracts.
---

# Ngat Tradingview Frontend

## Overview

Render interactive multi-timeframe market charts and overlays using backend candle/indicator APIs.

## Submodules

- `src/App.vue`: main chart orchestration, API calls, synchronized multi-chart rendering.
- `src/components/TradingViewChart.vue`: reusable chart wrapper with crosshair and pagination hooks.
- `src/main.js`: app bootstrap.
- `package.json`: frontend runtime/tooling versions.

## Pros

- Support multi-timeframe comparison in one screen.
- Keep indicator overlays extensible via backend-provided metadata.
- Keep interactive tooling (range sync, crosshair sync, tooltip enrichment).

## Cons

- Mix substantial logic in single-file components.
- Recreate charts on resize/data-load paths, increasing CPU churn.
- Keep hardcoded API base URL and instrument options.

## Caveats

- Ensure timestamp normalization remains consistent (`number` vs ISO string branch paths).
- Check lightweight-charts API compatibility; methods differ across major versions.
- Preserve scroll/range state carefully while prepending historical candles.

## Forward Features

- Add composable stores/services for API and chart sync state.
- Add websocket stream mode to reduce polling latency and API load.
- Add virtualized indicator rendering and performance instrumentation.
