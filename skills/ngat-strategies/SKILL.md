---
name: ngat-strategies
description: Implement and refine strategy logic in strategies/ based on BaseStrategy contracts, currently centered on Supertrend + RSI signal generation. Use when adding new strategies, modifying signal criteria, or improving risk controls and dedup behavior.
---

# Ngat Strategies

## Overview

Maintain reusable strategy modules that receive candles and emit trade signals.

## Submodules

- `strategies/supertrend_rsi.py`: `SupertrendRSIStrategy`.
- `core/base.py::BaseStrategy`: `on_candle` and `reset` contract.

## Pros

- Keep strategy self-contained and simple to reason about.
- Deduplicate repeated signals with `last_signal` memory.

## Cons

- Keep no explicit stop-loss/target/risk model in signal payload.
- Keep single-strategy example with limited portfolio context.
- Depend on pandas-ta naming conventions that can shift by version.

## Caveats

- Validate indicator column keys from pandas-ta before direct indexing.
- Avoid making strategy state non-serializable if moved across Ray workers.
- Ensure minimum warmup rows before signal generation.

## Forward Features

- Add risk module fields (`stop_loss`, `take_profit`, `position_sizing`).
- Add strategy registry and dynamic loading from config.
- Add portfolio-aware filters (exposure, correlation, daily loss cap).
