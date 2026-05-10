---
name: ngat-config
description: Manage and validate runtime configuration schemas and YAML files under config/ plus root config.yaml for the trading engine. Use when adding config fields, fixing env interpolation, or resolving mismatches between configured and supported timeframes/indicators.
---

# Ngat Config

## Overview

Control how runtime settings are modeled and loaded. Use this skill when schema evolution or config correctness impacts startup and worker behavior.

## Submodules

- `config/settings.py`: Pydantic models for feed/broker/storage/strategy/settings.
- `config/config.yaml`: runtime config (symbols, base/derived timeframes, indicators).
- `config/sample_config.yaml` and root `config.yaml`: alternates/examples.

## Pros

- Keep typed config contract through Pydantic.
- Keep feed/broker/storage abstraction explicit at configuration level.

## Cons

- Keep timeframe validator too narrow for current YAML (`1d/1w/1M/1y` unsupported by validator).
- Keep duplicate config files with drift risk.

## Caveats

- Treat env interpolation as global and invasive in CLI loader.
- Keep naming consistent between `day` vs `1d`; mixed vocabulary causes runtime failures.
- Assume indicators list may be consumed by multiple services; avoid shape changes without fallback.

## Forward Features

- Add schema versioning and migration helpers.
- Add strict timeframe enum shared by config, aggregator, API, and frontend.
- Add startup config lint command with actionable error messages.
