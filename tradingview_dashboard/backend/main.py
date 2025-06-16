from fastapi import FastAPI, HTTPException, Query
from typing import Dict, List, Optional
from datetime import datetime
from storage.redis_client import get_redis_client, ts_range

app = FastAPI()

@app.get("/candles")
def get_candles(
    instrument_token: int,
    timeframe: str,
    start: int = Query(..., description="Start timestamp (epoch seconds)"),
    end: int = Query(..., description="End timestamp (epoch seconds)"),
    limit: int = Query(1000, description="Max number of candles to return")
):
    """
    Fetch OHLC candles from RedisTimeSeries for a given instrument and timeframe.
    """
    fields = ["open", "high", "low", "close"]
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
            "epoch": ts,
        }
        for ts in sorted_ts
    ]

    return {"candles": result}


@app.get("/candles/latest")
def get_latest_common_timestamp(instrument_token: int, timeframe: str):
    """
    Get the latest common timestamp across all OHLC series for an instrument and timeframe.
    """
    fields = ["open", "high", "low", "close"]
    base_key = f"ts:candle:{instrument_token}:{timeframe}"
    keys = {field: f"{base_key}:{field}" for field in fields}

    client = get_redis_client()
    timestamps = []

    for field, key in keys.items():
        try:
            val = client.execute_command("TS.GET", key)
            if val:
                timestamps.append(int(val[0]))
        except Exception:
            # One of the fields might not exist
            return {"timestamp": None}

    if len(timestamps) < 4:
        return {"timestamp": None}

    # Return the latest timestamp that is common to all series
    if all(ts == timestamps[0] for ts in timestamps):
        return {"timestamp": timestamps[0] , "epoch" : timestamps[0]}
    else:
        return {"timestamp": None}
    """
    Get the latest available candle (OHLC) for a given instrument and timeframe from RedisTimeSeries.
    """
    base_key = f"ts:candle:{instrument_token}:{timeframe}"
    fields = ["open", "high", "low", "close"]
    keys = {field: f"{base_key}:{field}" for field in fields}

    try:
        open_data = ts_range(keys["open"], "-", "+")
        if not open_data:
            return {"timestamp": None, "epoch": None}
        latest_ts = int(open_data[-1][0])
    except Exception as e:
        if "TSDB: the key does not exist" in str(e):
            return {"timestamp": None, "epoch": None}
        raise

    result = {"timestamp": latest_ts, "epoch": latest_ts}
    for field in fields:
        try:
            val = ts_range(keys[field], latest_ts, latest_ts)
            result[field] = float(val[0][1]) if val else None
        except Exception:
            result[field] = None

    return result