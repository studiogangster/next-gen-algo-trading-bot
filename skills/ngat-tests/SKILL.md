---
name: ngat-tests
description: Maintain and expand test coverage under tests/ with focus on indicator correctness, aggregation behavior, and reliable separation of unit vs integration test paths. Use when adding modules, fixing regressions, or improving confidence in data/strategy pipelines.
---

# Ngat Tests

## Overview

Strengthen correctness checks for indicators and timeframe aggregation while making tests deterministic and CI-friendly.

## Submodules

- `tests/test_indicator_generator.py`: pandas-ta parity checks, Redis-backed fetch/aggregate smoke coverage.
- indirect dependency on `core/timeframe_generator_worker.py` fetch and aggregate helpers.

## Pros

- Validate indicator outputs against pandas-ta directly.
- Cover single-value and dataframe-style indicator results.

## Cons

- Blend unit and environment-dependent integration concerns in one file.
- Depend on live Redis data for one test path.
- Miss coverage for broker/auth, API contracts, and frontend transformations.

## Caveats

- Mark Redis-dependent tests as integration and skip when infrastructure is absent.
- Avoid asserting against unstable vendor indicator column naming without normalization.
- Ensure timezone-aware fixtures for aggregation tests.

## Forward Features

- Split tests into `unit/`, `integration/`, and `contract/` groups.
- Add API response contract tests for `/candles`, `/indicators`, `/candles/latest`.
- Add worker-loop tests with bounded iterations and fake clocks.
