import logging
import time
import traceback
import yaml
import pandas as pd
import ray
import pandas_ta as ta
from storage.redis_client import get_redis_client, ts_add
from datetime import datetime, timedelta
import pytz
import os

def load_config(path="config/config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def fetch_candles(symbol, timeframe, from_ts, to_ts):
    # This function should fetch candles from Redis or storage, similar to fetch_1m_candles
    # For simplicity, assume 1m candles are available and aggregate as needed
    from core.timeframe_generator_worker import fetch_1m_candles, parse_redis_candles, aggregate_candles
    if timeframe == "1m":
        data = fetch_1m_candles(symbol, from_ts, to_ts)
        df = parse_redis_candles(data)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        return df
    else:
        data = fetch_1m_candles(symbol, from_ts, to_ts)
        df = parse_redis_candles(data)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df = aggregate_candles(df, timeframe, market_open_time="09:15")
        return df

def indicator_key(symbol, timeframe, indicator_name):
    return f"ts:signal:{symbol}:{timeframe}:{indicator_name}"

@ray.remote
class IndicatorGeneratorWorker:
    def __init__(self, config_path="config/config.yaml", poll_interval=10):
        self.poll_interval = poll_interval
        self.config = load_config(config_path)
        self.symbols = self.config.get("symbols", [])
        self.timeframes = self.config.get("derived_timeframes", []) + self.config.get("timeframes", [])
        self.indicators = self.config.get("indicators", [])
        self.redis_client = get_redis_client()
        self.last_polled = {}  # symbol -> last timestamp polled (ms)

    def run(self):
        print("[IndicatorGeneratorWorker] Starting worker", self.symbols)
        while True:
            for symbol in self.symbols:
                now = int(time.time())
                from_ts = self.last_polled.get(symbol, now - (60 * 60 * 24 * 7))  # default: last 1 week
                to_ts = now
                to_ts = "+"
                from_ts = "-"
                for tf in self.timeframes:
                    try:
                        df = fetch_candles(symbol, tf, from_ts, to_ts)
                        if df.empty:
                            continue
                        df = df.sort_values("timestamp")
                        df = df.set_index("timestamp")
                        for ind in self.indicators:
                            ind_type = ind.get("type")
                            params = ind.get("params", {})
                            # Compute indicator using pandas_ta
                            try:
                                if hasattr(ta, ind_type):
                                    func = getattr(ta, ind_type)
                                    result = func(df["close"], **params)
                                    if isinstance(result, pd.DataFrame):
                                        # For multi-column indicators (e.g., KC)
                                        for col in result.columns:
                                            signal_name = f"{ind_type}_{col}"
                                            values = result[col].dropna()
                                            for ts, val in values.items():
                                                epoch = int(pd.Timestamp(ts).timestamp())
                                                key = indicator_key(symbol, tf, signal_name)
                                                ts_add(
                                                    key, epoch, float(val),
                                                    labels={
                                                        "type": "signal",
                                                        "instrument_token": symbol,
                                                        "timeframe": tf,
                                                        "signal_name": signal_name
                                                    },
                                                    upsert=False
                                                )
                                    else:
                                        # Single column indicator
                                        signal_name = ind_type
                                        values = result.dropna()
                                        for ts, val in values.items():
                                            epoch = int(pd.Timestamp(ts).timestamp())
                                            key = indicator_key(symbol, tf, signal_name)
                                            ts_add(
                                                key, epoch, float(val),
                                                labels={
                                                    "type": "signal",
                                                    "instrument_token": symbol,
                                                    "timeframe": tf,
                                                    "signal_name": signal_name
                                                },
                                                upsert=False
                                            )
                                else:
                                    print(f"[IndicatorGeneratorWorker] Unknown indicator: {ind_type}")
                            except Exception as e:
                                print(f"[IndicatorGeneratorWorker] Error computing {ind_type} for {symbol} {tf}: {e}")
                                traceback.print_exc()
                    except Exception as e:
                        print(f"[IndicatorGeneratorWorker] Error for {symbol} {tf}: {e}")
                        traceback.print_exc()
                self.last_polled[symbol] = int(time.time() * 1000)
            time.sleep(self.poll_interval)
