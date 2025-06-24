import logging
import time
import traceback
from typing import Dict
import yaml
import pandas as pd
import ray
from storage.redis_client import get_redis_client, ts_range, ts_add
from datetime import datetime, timedelta, timezone
import pytz
import os



def get_derived_timeframes_from_config(config):
    # Exclude "1m" from aggregation targets
    return [tf for tf in config.get("derived_timeframes", []) if tf != "1m"]

def get_symbols_from_config(config):
    return config.get("symbols", [])

def redis_key(symbol, timeframe, field):
    return f"ts:candle:{symbol}:{timeframe}:{field}"

def fetch_1m_candles(symbol, from_ts, to_ts):
    from storage.redis_client import ts_mrange
    fields = ["open", "high", "low", "close"]
    filters = [
        "timeframe=1m",
        "type=ohlc",
        f"instrument_token={symbol}",
    ]
    # Some RedisTimeSeries setups may use type=open/high/low/close/volume as a label
    # If not, adjust the filter accordingly
    try:
        result = ts_mrange(from_ts, to_ts, filters)
        
    except Exception as e:
        # print(f"[TimeframeGeneratorWorker] MRANGE error for {symbol}: {e}")
        return pd.DataFrame([])

    # print(f"Timeframe generator {symbol} { len( result )} ")
    field_data: Dict[str, Dict[int, float]] = {}
    
    for entry in result:
        key, labels, data = entry
        # print(key, labels, data)
        key = key.split(":")[-1]
        # label_dict = {k.decode(): v.decode() for k, v in labels}
        # field = label_dict.get("field")
        if key:
            field_data[key] = {int(ts): float(val) for ts, val in data}

    # Use 'open' field as reference for aligned timestamps
    result = [
        {
            "timestamp": ts,
            "epoch": ts,
            "open": field_data["open"][ts],
            "high": field_data["high"][ts],
            "low": field_data["low"][ts],
            "close": field_data["close"][ts],
            "volume": field_data["volume"][ts],
        }
        for ts in field_data["open"].keys()
    ]
    return result

def parse_redis_candles(data):
    return pd.DataFrame(data)

def aggregate_candles(df, timeframe):
    # df: DataFrame with 1m candles, must have 'timestamp' as datetime
    if df.empty:
        return df
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"]) #.dt.tz_convert("Asia/Kolkata")
    df = df.set_index("timestamp")
    if timeframe.endswith("m"):
        rule = f"{int(timeframe[:-1])}min"
    elif timeframe.endswith("h"):
        rule = f"{int(timeframe[:-1])}H"
    elif timeframe.endswith("d"):
        rule = f"{int(timeframe[:-1])}D"
    elif timeframe.endswith("w"):
        rule = f"{int(timeframe[:-1])}W"
    elif timeframe.endswith("M"):
        rule = f"{int(timeframe[:-1])}M"  # M = month in Pandas DateOffset
    elif timeframe.endswith("y"):
        rule = f"{int(timeframe[:-1])}Y"
    else:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    
    print("resampling")
    agg = df.resample(rule, label="left", closed="left").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum"
    }).dropna().reset_index()
    return agg

@ray.remote
class TimeframeGeneratorWorker:
    def __init__(self, config, poll_interval=5):

        self.poll_interval = poll_interval
        self.config = config
        self.symbols = self.config.symbols
        self.target_timeframes = self.config.derived_timeframes
        self.redis_client = get_redis_client()
        self.last_polled = {}  # symbol -> last timestamp polled (ms)

    def run(self):
        print("[TimeframeGeneratorWorker] Starting worker", self.symbols)
        while True:
            for symbol in self.symbols:
                now = int(time.time() )
                from_ts = self.last_polled.get(symbol, now - ( 60 * 60  * 24 * 7 ) )  # default: last 1 hour
                to_ts = now
                to_ts = "+"
                from_ts = "-"
                redis_data = fetch_1m_candles(symbol, from_ts, to_ts)
                
                
                if redis_data == None or len(redis_data) < 1:
                    break

                df = parse_redis_candles(redis_data)
                # print("time_diff_head", df.head())
                # print("time_diff_tail", df.tail())
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
                
                print(f"Timeframe generator {symbol} {self.target_timeframes} {len(redis_data)} ")
                if not df.empty:
                    for tf in self.target_timeframes:
                        print(f"Timeframe generator for  {symbol} ")
                        
                        agg = aggregate_candles(df, tf)
                        
                        print("time_diff_head", agg.head())
                        print("time_diff_tail", agg.tail())
                
                        # print( "original")
                        # print( df.head())
                        
                        # print("aggregate")
                        # print( agg.head())
                        
                        # Store aggregated candles back to Redis
                        pipe = get_redis_client().pipeline(transaction=False)
                        
                        for _, row in agg.iterrows():

                            ts = row.timestamp
                       
                            epoch = int(ts.timestamp())
                            
                            for field in ["open", "high", "low", "close", "volume"]:
                                ts_field_key = f"ts:candle:{symbol}:{tf}:{field}"
                                value = getattr(row, field)
                                ts_add(ts_field_key, epoch, float(value), pipe=pipe, labels={"type": "ohlc", "instrument_token": symbol, "timeframe": tf,"sub_type":  field }   , upsert=False )
                        
                            
                            
                            
                        print("pipe", "execute" , ts , epoch)
                        
                        pipe.execute()
                        
                    # Update last polled timestamp
                    self.last_polled[symbol] = int(df["timestamp"].max().timestamp() * 1000)
            time.sleep(self.poll_interval)
