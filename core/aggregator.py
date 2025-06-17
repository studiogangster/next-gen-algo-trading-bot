import pandas as pd
from typing import Dict, Optional
from core.base import BaseTimeframeAggregator

class TimeframeAggregator(BaseTimeframeAggregator):
    """
    Aggregates tick or 1m data into higher timeframes (5m, 30m, 1h, etc.) per symbol.
    """

    def __init__(self, timeframes: list, symbols: list = None, historical_loader=None , realtime_loader = None ):
        """
        timeframes: list of timeframes (e.g. ["1m", "5m"])
        symbols: list of symbols to preload historical data for
        historical_loader: function(symbol, timeframe) -> pd.DataFrame
        realtime_loader: function(symbol, timeframe) -> pd.DataFrame
        """
        self.timeframes = timeframes
        self.data: Dict[str, pd.DataFrame] = {}  # symbol -> 1m candles DataFrame
        self.agg_cache: Dict[str, Dict[str, pd.DataFrame]] = {}  # symbol -> timeframe -> DataFrame
        
        self.symbols = symbols
        self.historical_loader = historical_loader
        self.realtime_loader = realtime_loader

        # Load historical data if loader and symbols provided
        
    def start(self):
        if  self.symbols:
            
            if self.historical_loader:
                for symbol in self.symbols:
                    for tf in self.timeframes:
                        candles_gen = self.historical_loader(symbol, tf)
                        if candles_gen is not None:
                            for candles in candles_gen:
                                if candles is not None and not candles.empty:
                                    self.load_historical(symbol, tf, candles)
                                    print(f"[Aggregator] Loaded historical for {symbol} {tf}: {len(candles)} rows")

            if self.realtime_loader:
                for symbol in self.symbols:
                    for tf in self.timeframes:
                        candles_gen = self.realtime_loader(symbol, tf)
                        if candles_gen is not None:
                            for candles in candles_gen:
                                if candles is not None and not candles.empty:
                                    self.load_historical(symbol, tf, candles)
                                    print(f"[Aggregator] Realtime historical loader for {symbol} {tf}: {len(candles)} rows")

                
            



    def load_historical(self, symbol: str, timeframe: str, candles: pd.DataFrame) -> None:
        """
        Load historical candles for a symbol and timeframe.
        """
        # Ensure columns: timestamp, open, high, low, close, volume
        required = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        candles = candles[required].copy()

        candles['timestamp'] = pd.to_datetime(candles['timestamp'])#.dt.tz_convert('Asia/Kolkata')
        candles = candles.drop_duplicates(subset=['timestamp'], keep='last')
        candles = candles.sort_values('timestamp')
        # Store as if it was 1m data for aggregation
        if symbol not in self.data:
            self.data[symbol] = candles
        else:
            df = self.data[symbol]
            df = pd.concat([df, candles], ignore_index=True)
            df = df.drop_duplicates(subset=['timestamp'], keep='last')
            df = df.sort_values('timestamp')
            self.data[symbol] = df
        self.agg_cache[symbol] = {}

    def add_tick(self, symbol: str, tick: dict) -> None:
        # Debug: print tick received
        print(f"[Aggregator] add_tick: symbol={symbol}, tick={tick}")
        # Assume tick is a dict with keys: 'timestamp', 'open', 'high', 'low', 'close', 'volume'
        df = self.data.setdefault(symbol, pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']))
        tick_df = pd.DataFrame([tick])
        tick_df['timestamp'] = pd.to_datetime(tick_df['timestamp']) # .dt.tz_localize('Asia/Kolkata') # .dt.tz_convert('UTC')

        df = pd.concat([df, tick_df], ignore_index=True)
        df = df.drop_duplicates(subset=['timestamp'], keep='last')
        # Drop rows with missing or invalid timestamp before sorting
        df = df[df['timestamp'].notna()]
        df = df.sort_values('timestamp')
        self.data[symbol] = df

        # Persist to parquet for dashboard visualization
        try:
            import os
            out_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, f"aggregator_{symbol}.parquet")
            df.to_parquet(out_path, index=False)
        except Exception as e:
            print(f"[Aggregator] Failed to write parquet for {symbol}: {e}")

        # Invalidate cache for this symbol
        self.agg_cache[symbol] = {}

    def get_candles(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        if symbol not in self.data or self.data[symbol].empty:
            return None
        if symbol in self.agg_cache and timeframe in self.agg_cache[symbol]:
            return self.agg_cache[symbol][timeframe]

        df = self.data[symbol].copy()
        # Ensure 'timestamp' is datetime and set as index
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df = df.set_index('timestamp')
        df = df[~df.index.isna()]  # Drop rows with invalid timestamps

        rule = timeframe
        if timeframe.endswith('m'):
            rule = f"{int(timeframe[:-1])}min"
        elif timeframe.endswith('h'):
            rule = f"{int(timeframe[:-1])}H"
        else:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        if not isinstance(df.index, (pd.DatetimeIndex, pd.TimedeltaIndex, pd.PeriodIndex)):
            return None  # Can't resample without a valid datetime index

        agg = df.resample(rule).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        agg = agg.reset_index()
        self.agg_cache.setdefault(symbol, {})[timeframe] = agg
        return agg
