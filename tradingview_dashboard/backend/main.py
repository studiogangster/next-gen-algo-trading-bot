from fastapi import FastAPI, Query
from typing import List, Optional
from datetime import datetime
import redis
import os

app = FastAPI()


def get_redis_client():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return redis.Redis.from_url(redis_url, decode_responses=True)


@app.get("/candles")
def get_candles(
    instrument_token: int,
    timeframe: str,
    start: int = Query(..., description="Start timestamp (epoch seconds)"),
    end: int = Query(..., description="End timestamp (epoch seconds)"),
    limit: int = Query(1000, description="Max number of candles to return")
):
    """
    Fetch candle data for a given instrument and timeframe from Redis.
    """
    # timeframe = "1m"
    redis_client = get_redis_client()
    redis_key = f"candle:{instrument_token}:{timeframe}"
    # ZRANGEBYSCORE returns values in [min, max]
    if end == -1:
        candles = redis_client.zrevrange(
        redis_key,
        start=0,
        end=limit,
        withscores=True
                                        #  end='+inf'
                                            #  max=end,
                                            # min='-inf',
                                            #  start=0,
                                            # num=limit,
                                            #  withscores=True
                                            
                                            )
    else:
        candles = redis_client.zrevrangebyscore(
        redis_key,
        max=end,
        min='-inf',
        start=0,
        num=limit,
        withscores=True
        )
    result = []
    print("candles", candles[:-2])
    for value, score in candles:
        # value is CSV: timestamp,open,high,low,close,volume
        parts = value.split(",")
        if len(parts) == 5:
            result.append({
                "timestamp": parts[0],
                "open": float(parts[1]),
                "high": float(parts[2]),
                "low": float(parts[3]),
                "close": float(parts[4]),
                # "volume": float(parts[5]),
                "epoch": int(score)
            })
    
    result = list(reversed(result))
    return {"candles": result}


@app.get("/candles/latest")
def get_latest_candle(
    instrument_token: int,
    timeframe: str
):
    """
    Get the latest available candle timestamp for a given instrument and timeframe.
    """
    timeframe = "1m"
    redis_client = get_redis_client()
    redis_key = f"candle:{instrument_token}:{timeframe}"
    latest = redis_client.zrevrange(redis_key, 0, 0, withscores=True)
    if latest:
        value, score = latest[0]
        return {
            "timestamp": value.split(",")[0],
            "epoch": int(score)
        }
    else:
        return {
            "timestamp": None,
            "epoch": None
        }
