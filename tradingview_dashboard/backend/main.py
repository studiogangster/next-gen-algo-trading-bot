from fastapi import FastAPI, HTTPException, Query
from typing import Dict, List, Optional
from datetime import datetime

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
        f"instrument_token={instrument_token}"
    ]

    try:
        query =["TS.MREVRANGE", from_ts, to_ts ,  "COUNT", 100, "FILTER", *label_filter]
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


@app.get("/candles/latest")
def get_latest_common_timestamp(instrument_token: int, timeframe: str):
    """
    Get the latest common timestamp across all OHLC series for an instrument and timeframe.
    """


    client = get_redis_client()

    try:
        result = client.execute_command("TS.MGET", "FILTER",f"type=ohlc",   f"instrument_token={instrument_token}")
        _, _, [ts, _] = result[0]
        return {"timestamp": ts , "epoch" : ts}

    except Exception:
        # One of the fields might not exist
        return {"timestamp": None, "epoch": None}
