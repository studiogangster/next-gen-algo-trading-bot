
import pandas as pd
from datetime import datetime, timedelta

from brokers.kite_trade import ZerodhaBroker
from storage.redis_client import get_redis_client

def fetch_zerodha_historical(enctoken, symbol, timeframe, from_date=None, to_date=None, previous_days = 350 * 20, interval_days=60):
    """
    Yield historical candles for a symbol and timeframe from Zerodha, in chunks of interval_days.
    Each yield is a DataFrame with columns: timestamp, open, high, low, close, volume
    """
    
    kite = ZerodhaBroker(enctoken=enctoken)

    # Map timeframe to Zerodha interval
    interval_map = {
        "1m": "minute",
        "5m": "5minute",
        "30m": "30minute",
        "1h": "60minute",
        "day": "day"
    }
    interval = interval_map.get(timeframe)
    if not interval:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    # Default to last 5 days if not provided
    if to_date is None:
        to_date = datetime.now()
    if from_date is None:
        from_date = to_date - timedelta(days=previous_days)

    instrument_token = int(symbol)

    from storage.redis_client import ts_add, ts_range, ts_get

    ts_key = f"ts:candle:{instrument_token}:{timeframe}"

    # Find the lowest and highest timestamp in RedisTimeSeries for the requested range
    try:
        ts_data = ts_range(ts_key, "-", "+")
    except Exception as e:
        if "TSDB: the key does not exist" in str(e):
            ts_data = []
        else:
            raise
    if ts_data:
        redis_min = int(ts_data[0][0])
        redis_max = int(ts_data[-1][0])
    else:
        redis_min = None
        redis_max = None

    # Convert from_date and to_date to epoch
    req_min = int(from_date.timestamp())
    req_max = int(to_date.timestamp())

    print(f"[fetch_zerodha_historical] Requested range: {from_date} ({req_min}) to {to_date} ({req_max})")
    if redis_min is not None and redis_max is not None:
        print(f"[fetch_zerodha_historical] Redis covers: {datetime.fromtimestamp(redis_min)} ({redis_min}) to {datetime.fromtimestamp(redis_max)} ({redis_max})")
    else:
        print("[fetch_zerodha_historical] Redis has no data for this key.")

    # Determine which ranges to fetch from Zerodha
    fetch_ranges = []
    if redis_min is None or req_min < redis_min:
        # Need to fetch from req_min to (redis_min - 1)
        fetch_start = from_date
        fetch_end = datetime.fromtimestamp(redis_min) if redis_min else to_date
        if fetch_start < fetch_end:
            fetch_ranges.append((fetch_start, fetch_end))
    if redis_max is None or req_max > redis_max:
        # Need to fetch from (redis_max + 1) to req_max
        fetch_start = datetime.fromtimestamp(redis_max) if redis_max else from_date
        fetch_end = to_date
        if fetch_start < fetch_end:
            fetch_ranges.append((fetch_start, fetch_end))

    print(f"[fetch_zerodha_historical] Will fetch {len(fetch_ranges)} missing range(s):")
    # for i, (start, end) in enumerate(fetch_ranges):
    #     print(f"  Range {i+1}: {start} to {end}")

    # If Redis fully covers the range, nothing to fetch
    if not fetch_ranges:
        print("[fetch_zerodha_historical] All requested candles are already in Redis.")
        return

    for fetch_from, fetch_to in fetch_ranges:
        current_from = fetch_from
        while current_from < fetch_to:
            current_to = min(current_from + timedelta(days=interval_days), fetch_to)
            print(f"[fetch_zerodha_historical] Fetching: {current_from} to {current_to} ...")
            candles = kite.historical_data(
                instrument_token=instrument_token,
                from_date=current_from,
                to_date=current_to,
                interval=interval,
                continuous=False,
                oi=False
            )
            if candles:
                df = pd.DataFrame(candles)
                df.rename(columns={"date": "timestamp", "volume": "volume"}, inplace=True)
                df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
                print(f"[fetch_zerodha_historical] Fetched {len(df)} rows for {current_from.date()} to {current_to.date()}")

 
                # Cache only missing candles to RedisTimeSeries (one series per field)
                pipe = get_redis_client().pipeline(transaction=False)
                for row in df.itertuples(index=False):
                    ts = row.timestamp
                    if isinstance(ts, str):
                        ts = pd.to_datetime(ts)
                    epoch = int(ts.timestamp())
                    # Insert each field into its own time series
                    for field in ["open", "high", "low", "close"]:
                        ts_field_key = f"ts:candle:{instrument_token}:{timeframe}:{field}"
                        value = getattr(row, field)
                        ts_add(ts_field_key, epoch, float(value), pipe=pipe)
                        continue
                        
                        try:
                            existing = ts_range(ts_field_key, epoch, epoch)
                        except Exception as e:
                            if "TSDB: the key does not exist" in str(e):
                                existing = []
                            else:
                                raise
                        if not existing:
                            ts_add(ts_field_key, epoch, float(value))
                
                pipe.execute()
                print(f"[fetch_zerodha_historical] Ingested new candles into RedisTimeSeries for {current_from} to {current_to}.")

                yield df
            else:
                print(f"[fetch_zerodha_historical] No data for {current_from.date()} to {current_to.date()}")
            current_from = current_to
