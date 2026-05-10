import json
import os
from time import sleep
import traceback
import ray
from types import SimpleNamespace
from typing import List, Dict, Any, Optional, Callable
from brokers.kite_trade import ZerodhaBroker
from brokers.utils import login
from core.base import BaseFeed, BaseTimeframeAggregator, BaseStrategy, BaseBroker, BaseStorage
from core.aggregator import TimeframeAggregator
from brokers.zerodha import sync_zerodha_historical_realtime, fetch_zerodha_historical
import ray
from brokers.instrument_master import ensure_universe_catalog
from core.indicator_sync_worker import IndicatorSyncWorker
from core.timeframe_generator_worker import TimeframeGeneratorWorker

from storage.redis_client import get_redis_client


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
        bootstrap_universe: bool = True,
        universe_instruments_csv: str = "data/reference/instruments.csv.gz",
        universe_index_dir: str = "data/reference/indexes",
        universe_namespace: str = "universe:v1",
        universe_refresh_hours: int = 20,
        universe_sync_default_indexes: bool = True,
        indicators: Optional[List[Dict[str, Any]]] = None,
        derived_timeframes: Optional[List[str]] = None,
        indicator_timeframes: Optional[List[str]] = None,
        indicator_poll_interval: float = 1.0,
        timeframe_generator_poll_interval: float = 5.0,
        timeframe_max_1m_points_per_cycle: int = 3000,
        enable_order_sync_worker: bool = True,
        enable_symbol_workers: bool = True,
        enable_indicator_worker: bool = True,
        enable_timeframe_worker: bool = True,
    ):
        self.symbols = symbols
        self.timeframes = timeframes
        self.strategies = strategies
        self.feed = feed
        self.broker = broker
        self.storage = storage
        self.dry_run = dry_run
        self.max_workers = max_workers
        self.bootstrap_universe = bootstrap_universe
        self.universe_instruments_csv = universe_instruments_csv
        self.universe_index_dir = universe_index_dir
        self.universe_namespace = universe_namespace
        self.universe_refresh_hours = universe_refresh_hours
        self.universe_sync_default_indexes = universe_sync_default_indexes
        self.indicators = indicators or []
        self.derived_timeframes = derived_timeframes or []
        self.indicator_timeframes = indicator_timeframes or []
        self.indicator_poll_interval = indicator_poll_interval
        self.timeframe_generator_poll_interval = timeframe_generator_poll_interval
        self.timeframe_max_1m_points_per_cycle = int(timeframe_max_1m_points_per_cycle)
        self.enable_order_sync_worker = enable_order_sync_worker
        self.enable_symbol_workers = enable_symbol_workers
        self.enable_indicator_worker = enable_indicator_worker
        self.enable_timeframe_worker = enable_timeframe_worker


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

        self.historical_loader = historical_loader
        self.realtime_loader = realtime_loader

    def start_historical_sync(self):
        self.actor_id = getattr(ray.get_runtime_context(),
                                "get_actor_id", lambda: None)()
        print("self.actor_id", self.actor_id)
        aggregator = TimeframeAggregator(
            self.timeframes,
            symbols=[self.symbol],
            historical_loader=self.historical_loader,
        )

        aggregator.start()
        return True

    def start_realtime_sync(self):
        self.actor_id = getattr(ray.get_runtime_context(),
                                "get_actor_id", lambda: None)()
        print("self.actor_id", self.actor_id)

        aggregator = TimeframeAggregator(
            self.timeframes,
            symbols=[self.symbol],
            realtime_loader=self.realtime_loader
        )

        aggregator.start()
        return True

    def start(self):
        self.actor_id = getattr(ray.get_runtime_context(),
                                "get_actor_id", lambda: None)()
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


@ray.remote
class OrderAndPositionWorker:
    def __init__(
        self,
        broker: ZerodhaBroker,
        dry_run: bool = False,
    ):
        self.broker = broker
        self.dry_run = dry_run
        self.redis_client = get_redis_client()

    def start(self):

        response = login()
        user_id = response["user_id"]
        self.broker = self.broker()

        while True:
            try:
                positions = self.broker.positions()
                orders = json.dumps( self.broker.orders() )
                
                redis_key = f"user:{user_id}:orders"
                self.redis_client.execute_command(
                        "SET", f"{redis_key}", orders)


                redis_key = f"user:{user_id}:position"
                if "net" in positions:
                    net_positions = json.dumps(positions["net"])

                    self.redis_client.execute_command(
                        "SET", f"{redis_key}:net", net_positions)

                if "day" in positions:
                    day_positions = json.dumps( positions["day"] )
                    self.redis_client.execute_command(
                        "SET", f"{redis_key}:day", day_positions)

            except:
                traceback.print_exc()
                pass
            finally:
                sleep(0.5)
                
        return True

    def close(self):
        self.broker.close()


class Engine:
    def __init__(self, config: EngineConfig,


                 ):
        self.config = config
        self.symbol_workers = {}

    def start(self):
        print("Engine.start")
        if self.config.bootstrap_universe:
            summary = ensure_universe_catalog(
                instruments_csv_path=self.config.universe_instruments_csv,
                index_dir=self.config.universe_index_dir,
                namespace=self.config.universe_namespace,
                refresh_if_older_than_hours=self.config.universe_refresh_hours,
                sync_default_indexes=self.config.universe_sync_default_indexes,
            )
            print("Engine.universe", summary)

        if not ray.is_initialized():
            ray_kwargs: Dict[str, Any] = {
                "ignore_reinit_error": True,
                "num_cpus": self.config.max_workers,
                "include_dashboard": False,
            }
            object_store_mem = os.getenv("RAY_OBJECT_STORE_MEMORY_BYTES")
            if object_store_mem:
                try:
                    ray_kwargs["object_store_memory"] = int(object_store_mem)
                except Exception:
                    pass
            ray.init(**ray_kwargs)

        workers: List[Any] = []
        worker_labels: List[str] = []

        if self.config.enable_order_sync_worker:
            orderEngine = OrderAndPositionWorker.remote(broker=ZerodhaBroker)
            workers.append(orderEngine.start.remote())
            worker_labels.append("order-sync")
        else:
            print("Engine.order_sync_worker disabled (expected external order-sync service).")

        if self.config.enable_indicator_worker and self.config.indicators and self.config.indicator_timeframes:
            indicator_worker = IndicatorSyncWorker.remote(
                symbols=self.config.symbols,
                timeframes=self.config.indicator_timeframes,
                indicators=self.config.indicators,
                poll_interval=self.config.indicator_poll_interval,
                warmup_bars=400,
                max_points_per_cycle=2000,
                historical_chunk_points=2500,
            )
            workers.append(indicator_worker.start.remote())
            worker_labels.append("indicator-sync")

        if self.config.enable_timeframe_worker and self.config.derived_timeframes:
            timeframe_cfg = SimpleNamespace(
                symbols=self.config.symbols,
                derived_timeframes=self.config.derived_timeframes,
                max_1m_points_per_cycle=self.config.timeframe_max_1m_points_per_cycle,
            )
            timeframe_worker = TimeframeGeneratorWorker.remote(
                config=timeframe_cfg,
                poll_interval=self.config.timeframe_generator_poll_interval,
            )
            workers.append(timeframe_worker.run.remote())
            worker_labels.append("timeframe-sync")

        if self.config.enable_symbol_workers:
            for symbol in self.config.symbols:
                worker = SymbolWorker.remote(
                    symbol,
                    self.config.timeframes,
                    self.config.strategies,
                    self.config.broker,
                    self.config.storage,
                    self.config.dry_run,
                )
                # Start one per-symbol worker flow to avoid duplicating
                # historical and realtime pipelines in parallel.
                workers.append(worker.start.remote())
                worker_labels.append(f"symbol-sync:{symbol}")
        else:
            print("Engine.symbol_workers disabled (expected dedicated candle-sync service elsewhere).")

        # workers = [

        #  worker.start.remote() for worker in self.symbol_workers.values()
        # ]

        print("Engine.start", len(workers))
        if not workers:
            raise RuntimeError("No workers enabled. Set at least one of symbol/indicator/order workers.")

        # Resolve workers one-by-one so crashes identify the exact failing worker role.
        for label, ref in zip(worker_labels, workers):
            try:
                ray.get(ref)
            except Exception as exc:
                print(f"[Engine] worker failed: {label} :: {exc}")
                traceback.print_exc()
                raise

        # def on_data(symbol: str, tick: dict):
        #     if symbol in self.symbol_workers:
        #         self.symbol_workers[symbol].on_tick.remote(tick)

        # self.config.feed.subscribe(self.config.symbols, on_data)

    def stop(self):
        self.config.feed.close()
        for worker in self.symbol_workers.values():
            worker.close.remote()
        ray.shutdown()
