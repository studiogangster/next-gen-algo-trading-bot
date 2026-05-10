---
name: ngat-cli
description: Operate and extend the Typer CLI entrypoints in cli/main.py for engine start/stop and timeframe worker execution. Use when adding commands, wiring new modules into runtime startup, or debugging startup/config/env failures.
---

# Ngat Cli

## Overview

Control process startup and command semantics for the trading engine. Use this skill when command UX, startup order, and runtime wiring need edits.

## Submodules

- `start`: load config, login, build feed/broker/storage/strategies, create `Engine`, run forever.
- `stop`: placeholder command; does not terminate remote workers.
- `generate-timeframe`: start `TimeframeGeneratorWorker` through Ray.

## Pros

- Keep runtime composition in one entrypoint.
- Keep environment substitution logic close to command parsing.

## Cons

- Mix command setup and domain wiring in one file.
- Duplicate login calls and partial unused variables.

## Caveats

- `yaml.SafeLoader` hook overrides all strings to run env substitution; treat as fragile.
- `start` enters infinite loop after `engine.start()`; Ctrl+C is only local exit path.
- `stop` is informational only; do not rely on it for process control.

## Forward Features

- Add explicit lifecycle commands (`status`, `shutdown`, `restart-worker`).
- Add config validation command before startup.
- Add dry-run smoke test command that checks Redis and broker connectivity only.
