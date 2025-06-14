# Modular Algo Trading Framework

A production-ready, scalable, and extensible algorithmic trading framework built with Python 3.10+, Ray, pandas-ta, and modern best practices.

## Features

- **Real-time data ingestion** from custom feeds (Zerodha WebSocket, Kafka, REST APIs)
- **Dynamic multi-timeframe aggregation** (1m, 5m, 30m, 1h, etc.)
- **Pluggable technical indicator engine** (pandas-ta, ta, NumPy, vectorbt)
- **Parallel processing** of multiple symbols, timeframes, and strategies (Ray)
- **Real-time signal execution** and broker order dispatch (Zerodha, extendable)
- **Plugin architecture** for feeds, strategies, brokers, and storage
- **Unified live trading & backtesting** logic
- **Flexible storage**: PostgreSQL, TimescaleDB, or Parquet (configurable)
- **Config & logging**: pydantic, loguru
- **CLI** to start/stop trading per symbol/strategy
- **Optional**: Streamlit dashboard for real-time PnL and signals
- **Optional**: Dry-run simulation mode

## Project Structure

```
/feeds         # Data feed adapters (WebSocket, REST, Kafka, etc.)
/strategies    # Trading strategies (Supertrend, RSI, etc.)
/core          # Core engine, base classes, time aggregation, signal engine
/brokers       # Broker adapters (Zerodha, etc.)
/storage       # Storage backends (PostgreSQL, TimescaleDB, Parquet)
/config        # Config models (pydantic)
/cli           # CLI and entrypoints
/dashboard     # (Optional) Streamlit dashboard
```

## Quick Start

1. **Clone the repo**
2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```
3. **Configure your settings** in `config/`
4. **Run the CLI**
   ```bash
   python -m cli.main --help
   ```

## Extending

- **Add a new feed:** Implement `BaseFeed` in `/feeds`
- **Add a new strategy:** Implement `BaseStrategy` in `/strategies`
- **Add a new broker:** Implement `BaseBroker` in `/brokers`
- **Add a new storage backend:** Implement `BaseStorage` in `/storage`

## Requirements

- Python 3.10+
- Ray
- pandas-ta, ta, numpy, vectorbt
- kiteconnect (for Zerodha)
- pydantic, loguru
- async libraries: asyncio, uvloop
- PostgreSQL/TimescaleDB (optional)
- Streamlit (optional)

## License

MIT
