from fastapi import FastAPI, HTTPException, Query
from typing import Dict, List, Optional
from datetime import datetime

import pandas as pd
import pandas_ta as ta

from redis import Redis
from storage.redis_client import get_redis_client, ts_range

app = FastAPI()

@app.get("/candles")
def get_candles(
    instrument_token: int,
    timeframe: str,
    start: int = Query(..., description="Start timestamp (epoch seconds)"),
    end: int = Query(..., description="End timestamp (epoch seconds)"),
    limit: int = Query(100, description="Max number of candles to return")
):
    """
    Fetch OHLC candles from RedisTimeSeries for a given instrument and timeframe.
    Uses TS.MRANGE with labels.
    """
    client: Redis = get_redis_client()

    from_ts = str(start) if start >= 0 else "-"
    to_ts = str(end) if end >= 0 else "+"

    # Compose label filter
    label_filter = [
        f"type=ohlc",
        f"instrument_token={instrument_token}",
        f"timeframe={timeframe}"
    ]

    try:
        query =["TS.MREVRANGE", from_ts, to_ts ,  "COUNT", 500, "FILTER", *label_filter]
        print("query", *query)
        result = client.execute_command( *query )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis MRANGE error: {str(e)}")

    # Parse MRANGE result
    field_data: Dict[str, Dict[int, float]] = {}

    for entry in result:
        key, labels, data = entry
        # print(key, labels, data)
        key = key.split(":")[-1]
        # label_dict = {k.decode(): v.decode() for k, v in labels}
        # field = label_dict.get("field")
        if key:
            field_data[key] = {int(ts): float(val) for ts, val in data}

    # Ensure all four OHLC fields are present
    expected_fields = {"open", "high", "low", "close", "volume"}
    if not expected_fields.issubset(field_data.keys()):
        raise HTTPException(status_code=404, detail=f"Missing one or more OHLC fields  ")

    # Intersect timestamps to build complete candles
    common_ts = set.intersection(*(set(fd.keys()) for fd in field_data.values()))
    sorted_ts = sorted(common_ts)[-limit:] if limit > 0 else sorted(common_ts)

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
        for ts in sorted_ts
    ]

    return {"candles": result}

@app.get("/indicator")
def get_indicator(
    instrument_token: int,
    timeframe: str,
    start: int = Query(..., description="Start timestamp (epoch seconds)"),
    end: int = Query(..., description="End timestamp (epoch seconds)"),
    indicator: str = Query(..., description="Indicator type, e.g. rsi, kc"),
    length: int = Query(14, description="Indicator period/length (default 14)"),
    limit: int = Query(100, description="Max number of candles to use")
):
    """
    Compute indicator (e.g., RSI, KC) on-the-fly for given instrument and timeframe.
    """
    client: Redis = get_redis_client()

    from_ts = str(start) if start >= 0 else "-"
    to_ts = str(end) if end >= 0 else "+"

    # Compose label filter
    label_filter = [
        f"type=ohlc",
        f"instrument_token={instrument_token}",
        f"timeframe={timeframe}"
    ]

    try:
        query = ["TS.MREVRANGE", from_ts, to_ts, "COUNT", 500, "FILTER", *label_filter]
        result = client.execute_command(*query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis MRANGE error: {str(e)}")

    # Parse MRANGE result
    field_data: Dict[str, Dict[int, float]] = {}
    for entry in result:
        key, labels, data = entry
        key = key.split(":")[-1]
        if key:
            field_data[key] = {int(ts): float(val) for ts, val in data}

    expected_fields = {"open", "high", "low", "close", "volume"}
    if not expected_fields.issubset(field_data.keys()):
        raise HTTPException(status_code=404, detail=f"Missing one or more OHLC fields")

    # Intersect timestamps to build complete candles
    common_ts = set.intersection(*(set(fd.keys()) for fd in field_data.values()))
    sorted_ts = sorted(common_ts)[-limit:] if limit > 0 else sorted(common_ts)

    # Build DataFrame
    df = pd.DataFrame({
        "timestamp": sorted_ts,
        "open": [field_data["open"][ts] for ts in sorted_ts],
        "high": [field_data["high"][ts] for ts in sorted_ts],
        "low": [field_data["low"][ts] for ts in sorted_ts],
        "close": [field_data["close"][ts] for ts in sorted_ts],
        "volume": [field_data["volume"][ts] for ts in sorted_ts],
    }).set_index("timestamp")

    # Compute indicator
    indicator = indicator.lower()
    if indicator == "rsi":
        df["rsi"] = ta.rsi(df["close"], length=length)
        values = [
            {"timestamp": int(ts), "value": float(val) if pd.notna(val) else None}
            for ts, val in df["rsi"].items()
        ]
        return {"indicator": "rsi", "values": values}
    elif indicator == "kc":
        kc = ta.kc(df["high"], df["low"], df["close"], length=length)
        values = []
        for ts, row in kc.iterrows():
            values.append({
                "timestamp": int(ts),
                "kc_upper": float(row["KC_Upper"]) if pd.notna(row["KC_Upper"]) else None,
                "kc_middle": float(row["KC_Middle"]) if pd.notna(row["KC_Middle"]) else None,
                "kc_lower": float(row["KC_Lower"]) if pd.notna(row["KC_Lower"]) else None,
            })
        return {"indicator": "kc", "values": values}
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported indicator: {indicator}")

@app.get("/_candles")
def _get_candles(
    instrument_token: int,
    timeframe: str,
    start: int = Query(..., description="Start timestamp (epoch seconds)"),
    end: int = Query(..., description="End timestamp (epoch seconds)"),
    limit: int = Query(1000, description="Max number of candles to return")
):
    """
    Fetch OHLC candles from RedisTimeSeries for a given instrument and timeframe.
    """
    key_filter = [
    f"type=ohlc",
    f"instrument_token={instrument_token}",
    f"interval={timeframe}"
    ]
    
    fields = ["open", "high", "low", "close", "volume"]
    base_key = f"ts:candle:{instrument_token}:{timeframe}"

    from_ts = str(start) if start >= 0 else "-"
    to_ts = str(end) if end >= 0 else "+"

    # Fetch all field data
    field_data: Dict[str, Dict[int, float]] = {}

    for field in fields:
        key = f"{base_key}:{field}"
        try:
            data = ts_range(key, from_ts, to_ts)
            field_data[field] = {int(ts): float(val) for ts, val in data}
        except Exception as e:
            if "TSDB: the key does not exist" in str(e):
                field_data[field] = {}
            else:
                raise HTTPException(status_code=500, detail=f"Redis error: {str(e)}")

    # Intersect timestamps that exist in all fields (ensure complete candles)
    common_ts = set.intersection(*(set(fd.keys()) for fd in field_data.values()))
    sorted_ts = sorted(common_ts)[-limit:] if limit > 0 else sorted(common_ts)

    result = [
        {
            "timestamp": ts,
            "open": field_data["open"][ts],
            "high": field_data["high"][ts],
            "low": field_data["low"][ts],
            "close": field_data["close"][ts],
            "volume": field_data["volume"][ts],
            "epoch": ts,
        }
        for ts in sorted_ts
    ]

    return {"candles": result}


@app.get("/indicators")
def get_indicators(
    instrument_token: int,
    timeframe: str,
    start: int = Query(..., description="Start timestamp (epoch seconds)"),
    end: int = Query(..., description="End timestamp (epoch seconds)"),
    limit: int = Query(100, description="Max number of candles to use"),
):
    """
    Compute all supported indicators for a given instrument and timeframe, on-the-fly.
    Returns a generic, extensible array of indicator results for UI visualization.
    """
    import yaml
    import os

    # Load supported indicators from config/config.yaml if available
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config", "config.yaml")
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        indicators = config.get("indicators", [
            {"type": "ema", "params": {"length": 20}},
            {"type": "rsi", "params": {"length": 14}},
            {"type": "kc", "params": {"length": 20, "multiplier": 2}},
        ])
    else:
        indicators = [
            {"type": "ema", "params": {"length": 20}},
            {"type": "rsi", "params": {"length": 14}},
            {"type": "kc", "params": {"length": 20, "multiplier": 2}},
        ]

    client: Redis = get_redis_client()
    from_ts = str(start) if start >= 0 else "-"
    to_ts = str(end) if end >= 0 else "+"

    # Compose label filter
    label_filter = [
        f"type=ohlc",
        f"instrument_token={instrument_token}",
        f"timeframe={timeframe}"
    ]

    try:
        query = ["TS.MREVRANGE", from_ts, to_ts, "COUNT", 500, "FILTER", *label_filter]
        result = client.execute_command(*query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis MRANGE error: {str(e)}")

    # Parse MRANGE result
    field_data: Dict[str, Dict[int, float]] = {}
    for entry in result:
        key, labels, data = entry
        key = key.split(":")[-1]
        if key:
            field_data[key] = {int(ts): float(val) for ts, val in data}

    expected_fields = {"open", "high", "low", "close", "volume"}
    if not expected_fields.issubset(field_data.keys()):
        raise HTTPException(status_code=404, detail=f"Missing one or more OHLC fields")

    # Intersect timestamps to build complete candles
    common_ts = set.intersection(*(set(fd.keys()) for fd in field_data.values()))
    sorted_ts = sorted(common_ts)[-limit:] if limit > 0 else sorted(common_ts)

    # Build DataFrame
    df = pd.DataFrame({
        "timestamp": sorted_ts,
        "open": [field_data["open"][ts] for ts in sorted_ts],
        "high": [field_data["high"][ts] for ts in sorted_ts],
        "low": [field_data["low"][ts] for ts in sorted_ts],
        "close": [field_data["close"][ts] for ts in sorted_ts],
        "volume": [field_data["volume"][ts] for ts in sorted_ts],
    }).set_index("timestamp")

    indicator_results = []
    for ind in indicators:
        ind_type = ind.get("type")
        params = ind.get("params", {})
        try:
            if ind_type == "kc":
                kc = ta.kc(df["high"], df["low"], df["close"], **params)
                if kc is not None and not kc.empty:
                    columns = list(kc.columns)
                    values = []
                    for ts, row in kc.iterrows():
                        entry = {"timestamp": int(ts)}
                        for col in columns:
                            entry[col.lower()] = float(row[col]) if pd.notna(row[col]) else None
                        values.append(entry)
                    indicator_results.append({
                        "name": ind_type,
                        "params": params,
                        "columns": [c.lower() for c in columns],
                        "values": values,
                    })
            elif hasattr(ta, ind_type):
                func = getattr(ta, ind_type)
                result = func(df["close"], **params)
                if result is not None:
                    if isinstance(result, pd.DataFrame):
                        columns = list(result.columns)
                        values = []
                        for ts, row in result.iterrows():
                            entry = {"timestamp": int(ts)}
                            for col in columns:
                                entry[col.lower()] = float(row[col]) if pd.notna(row[col]) else None
                            values.append(entry)
                        indicator_results.append({
                            "name": ind_type,
                            "params": params,
                            "columns": [c.lower() for c in columns],
                            "values": values,
                        })
                    else:
                        # Series
                        values = [
                            {"timestamp": int(ts), "value": float(val) if pd.notna(val) else None}
                            for ts, val in result.items()
                        ]
                        indicator_results.append({
                            "name": ind_type,
                            "params": params,
                            "columns": ["value"],
                            "values": values,
                        })
        except Exception as e:
            indicator_results.append({
                "name": ind_type,
                "params": params,
                "columns": [],
                "values": [],
                "error": str(e),
            })

    return {"indicators": indicator_results}

@app.get("/candles/latest")
def get_latest_common_timestamp(instrument_token: int, timeframe: str):
    """
    Get the latest common timestamp across all OHLC series for an instrument and timeframe.
    """


    client = get_redis_client()

    try:
        result = client.execute_command("TS.MGET", "FILTER",f"type=ohlc",   f"instrument_token={instrument_token}" , f"timeframe={timeframe}" )
        _, _, [ts, _] = result[0]
        return {"timestamp": ts , "epoch" : ts}

    except Exception:
        # One of the fields might not exist
        return {"timestamp": None, "epoch": None}
