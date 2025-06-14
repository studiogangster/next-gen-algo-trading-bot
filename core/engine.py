import ray
from typing import List, Dict, Any, Optional, Callable
from core.base import BaseFeed, BaseTimeframeAggregator, BaseStrategy, BaseBroker, BaseStorage

class EngineConfig:
    def __init__(
        self,
        symbols: List[str],
        timeframes: List[str],
        strategies: List[BaseStrategy],
        feed: BaseFeed,
        aggregator: BaseTimeframeAggregator,
        broker: BaseBroker,
        storage: BaseStorage,
        dry_run: bool = False,
        max_workers: int = 4,
    ):
        self.symbols = symbols
        self.timeframes = timeframes
        self.strategies = strategies
        self.feed = feed
        self.aggregator = aggregator
        self.broker = broker
        self.storage = storage
        self.dry_run = dry_run
        self.max_workers = max_workers

@ray.remote
class SymbolWorker:
    def __init__(
        self,
        symbol: str,
        timeframes: List[str],
        strategies: List[BaseStrategy],
        aggregator: BaseTimeframeAggregator,
        broker: BaseBroker,
        storage: BaseStorage,
        dry_run: bool = False,
    ):
        self.symbol = symbol
        self.timeframes = timeframes
        self.strategies = strategies
        self.aggregator = aggregator
        self.broker = broker
        self.storage = storage
        self.dry_run = dry_run

    def on_tick(self, tick: dict):
        self.aggregator.add_tick(self.symbol, tick)
        for tf in self.timeframes:
            candles = self.aggregator.get_candles(self.symbol, tf)
            if candles is not None and not candles.empty:
                for strategy in self.strategies:
                    signal = strategy.on_candle(self.symbol, tf, candles)
                    if signal:
                        self.storage.save_signal(self.symbol, tf, signal)
                        if not self.dry_run:
                            self.broker.place_order(
                                symbol=self.symbol,
                                action=signal.get("action"),
                                quantity=signal.get("quantity", 1),
                                **signal.get("order_kwargs", {})
                            )

    def close(self):
        self.storage.close()
        self.broker.close()
        for strategy in self.strategies:
            strategy.reset()

class Engine:
    def __init__(self, config: EngineConfig):
        self.config = config
        self.symbol_workers = {}

    def start(self):
        ray.init(ignore_reinit_error=True, num_cpus=self.config.max_workers)
        for symbol in self.config.symbols:
            worker = SymbolWorker.remote(
                symbol,
                self.config.timeframes,
                self.config.strategies,
                self.config.aggregator,
                self.config.broker,
                self.config.storage,
                self.config.dry_run,
            )
            self.symbol_workers[symbol] = worker

        def on_data(symbol: str, tick: dict):
            if symbol in self.symbol_workers:
                self.symbol_workers[symbol].on_tick.remote(tick)

        self.config.feed.subscribe(self.config.symbols, on_data)

    def stop(self):
        self.config.feed.close()
        for worker in self.symbol_workers.values():
            worker.close.remote()
        ray.shutdown()
