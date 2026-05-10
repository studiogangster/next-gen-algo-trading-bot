---
name: ngat-feeds
description: Maintain live market data feed adapters in feeds/ with focus on Zerodha websocket integration and normalized tick payloads. Use when adding new feed providers, normalizing tick schema, or debugging live tick ingestion issues.
---

# Ngat Feeds

## Overview

Own realtime data ingress semantics from broker websocket streams into internal candle-ready events.

## Submodules

- `feeds/zerodha_ws.py`: `ZerodhaWebSocketFeed` implementing `BaseFeed`.
- `core/base.py::BaseFeed`: contract for subscribe/unsubscribe/close behavior.

## Pros

- Keep adapter small and focused.
- Normalize tick payload shape for downstream aggregator use.

## Cons

- Keep reconnection behavior implicit in vendor client defaults; no explicit heartbeat handling.
- Keep subscription mapping coupled to assumption that symbols are instrument token strings.

## Caveats

- Guard against `exchange_timestamp` missing/null in ticks.
- Keep timezone normalization explicit (`astimezone(pytz.utc)`).
- Avoid pushing raw verbose tick logs in production path.

## Forward Features

- Add feed health metrics and reconnect event callbacks.
- Add multiplexed feed support for multiple brokers.
- Add optional buffering/coalescing before aggregator for high tick rates.
