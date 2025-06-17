import ray
from typing import List, Dict, Any, Optional, Callable
from core.base import BaseFeed, BaseTimeframeAggregator, BaseStrategy, BaseBroker, BaseStorage
from core.aggregator import TimeframeAggregator
from brokers.zerodha import sync_zerodha_historical_realtime, fetch_zerodha_historical
import ray

class EngineConfig:
    def __init__(
        self,
        symbols: List[str],
        timeframes: List[str],
        strategies: List[BaseStrategy],
        feed: BaseFeed,
        broker: BaseBroker,
        storage: BaseStorage,
        dry_run: bool = False,
        max_workers: int = 4,
    ):
        self.symbols = symbols
        self.timeframes = timeframes
        self.strategies = strategies
        self.feed = feed
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
        broker: BaseBroker,
        storage: BaseStorage,
        dry_run: bool = False,
    ):
        
        print("symbol:", symbol)
        
        self.symbol = symbol
        self.timeframes = timeframes
        self.strategies = strategies
        self.broker = broker
        self.storage = storage
        self.dry_run = dry_run
        # Get Ray actor ID for logging
        
        # Define per-worker loader functions
        def historical_loader(symbol, timeframe):
            return fetch_zerodha_historical(
                enctoken=broker.enctoken,
                symbol=symbol,
                timeframe=timeframe,
                interval_days=60
            )
            
        def realtime_loader(symbol, timeframe):
            return sync_zerodha_historical_realtime(
                enctoken=broker.enctoken,
                symbol=symbol,
                timeframe=timeframe,
                sync_interval=0.5,
                interval_days=60,
                partition_timestamp=None
            )
        
        self.historical_loader =historical_loader
        self.realtime_loader =realtime_loader
        
        # self.start()

        


    def start(self):
        self.actor_id = getattr(ray.get_runtime_context(), "get_actor_id", lambda: None)()
        print("self.actor_id", self.actor_id)
        # Each worker gets its own aggregator
        self.aggregator = TimeframeAggregator(
            self.timeframes,
            symbols=[self.symbol],
            historical_loader=self.historical_loader,
            realtime_loader=self.realtime_loader
        )
        self.aggregator.start()
        

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
        print("Engine.start")
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=self.config.max_workers)
        
        
        workers = []
        print("self.act", self.config.symbols , self.config.max_workers)
        for symbol in self.config.symbols:
            worker = SymbolWorker.remote(
                symbol,
                self.config.timeframes,
                self.config.strategies,
                self.config.broker,
                self.config.storage,
                self.config.dry_run,
            )
            workers.append(worker.start.remote())
            self.symbol_workers[symbol] = worker
            
            
        # workers = [
         
        #  worker.start.remote() for worker in self.symbol_workers.values()   
        # ]
            

        ray.get(workers)
            
            

        # def on_data(symbol: str, tick: dict):
        #     if symbol in self.symbol_workers:
        #         self.symbol_workers[symbol].on_tick.remote(tick)

        # self.config.feed.subscribe(self.config.symbols, on_data)

    def stop(self):
        self.config.feed.close()
        for worker in self.symbol_workers.values():
            worker.close.remote()
        ray.shutdown()
