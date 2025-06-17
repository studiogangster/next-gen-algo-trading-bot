
import pandas as pd
from datetime import datetime, timedelta

from brokers.kite_trade import ZerodhaBroker
from storage.redis_client import get_redis_client
import time

def sync_zerodha_historical_realtime(enctoken, symbol, timeframe, sync_interval=60, interval_days=60, partition_timestamp=None):
    """
    Periodically syncs the latest historical candles from the last known timestamp in Redis up to now.
    Runs every `sync_interval` seconds.
    If partition_timestamp is provided, it will be used as the starting point for fetching new data.
    """
    from storage.redis_client import ts_range
    instrument_token = int(symbol)
    ts_field_key = f"ts:candle:{instrument_token}:{timeframe}:open"

    while True:
        # Get the latest timestamp in Redis
        try:
            ts_data = ts_range(ts_field_key, "-", "+")
        except Exception as e:
            if "TSDB: the key does not exist" in str(e):
                ts_data = []
            else:
                raise
        if partition_timestamp is not None:
            from_date = partition_timestamp if isinstance(partition_timestamp, datetime) else datetime.fromtimestamp(partition_timestamp)
        elif ts_data:
            redis_max = int(ts_data[-1][0])
            from_date = datetime.fromtimestamp(redis_max)
        else:
            from_date = None  # Will default to previous_days in fetch_zerodha_historical
            
        
        # Today's 00:00 timestamp
        from datetime import time as _time
        today_midnight = datetime.combine(datetime.today(),  _time.min)
        from_date = min(from_date, today_midnight)
        to_date = datetime.now()
        print(f"[sync_zerodha_historical_realtime] Syncing from {from_date} to {to_date}")

        # Fetch and ingest new data
        for _ in fetch_zerodha_historical(
            enctoken, symbol, timeframe, from_date=from_date, to_date=to_date, interval_days=interval_days, upsert=True
        ):
            pass  # fetch_zerodha_historical already ingests and prints

        print(f"[sync_zerodha_historical_realtime] Sleeping for {sync_interval} seconds...")
        time.sleep(sync_interval)

def fetch_zerodha_historical(enctoken, symbol, timeframe, from_date=None, to_date=None, previous_days = 350 * 20, interval_days=60 , upsert = False):
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

    # Use the 'open' field timeseries for Redis coverage check
    ts_field_key = f"ts:candle:{instrument_token}:{timeframe}:open"

    # Find the lowest and highest timestamp in RedisTimeSeries for the requested range
    try:
        ts_data = ts_range(ts_field_key, "-", "+")
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
        
        
    if upsert:
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

    # Helper: get timedelta for timeframe
    def get_timeframe_delta(tf):
        if tf == "1m":
            return timedelta(minutes=1)
        elif tf == "5m":
            return timedelta(minutes=5)
        elif tf == "30m":
            return timedelta(minutes=30)
        elif tf == "1h":
            return timedelta(hours=1)
        elif tf == "day":
            return timedelta(days=1)
        else:
            raise ValueError(f"Unsupported timeframe: {tf}")

    tf_delta = get_timeframe_delta(timeframe)

    # Determine which ranges to fetch from Zerodha
    fetch_ranges = []
    if redis_min is None or req_min < redis_min:
        # Need to fetch from req_min up to (but not including) redis_min
        fetch_start = from_date
        fetch_end = datetime.fromtimestamp(redis_min) - tf_delta if redis_min else to_date
        if fetch_start <= fetch_end:
            fetch_ranges.append((fetch_start, fetch_end))
    if redis_max is None or req_max > redis_max:
        # Need to fetch from (redis_max + 1 interval) up to req_max
        fetch_start = datetime.fromtimestamp(redis_max) + tf_delta if redis_max else from_date
        fetch_end = to_date
        if fetch_start <= fetch_end:
            fetch_ranges.append((fetch_start, fetch_end))

    print(f"[fetch_zerodha_historical] Will fetch {len(fetch_ranges)} missing range(s):")
    for i, (start, end) in enumerate(fetch_ranges):
        print(f"  Range {i+1}: {start} to {end}")

    # If Redis fully covers the range, nothing to fetch
    if not fetch_ranges:
        print("[fetch_zerodha_historical] All requested candles are already in Redis.")
        return

    # Process fetch_ranges from latest to oldest
    for fetch_from, fetch_to in reversed(fetch_ranges):
        current_to = fetch_to
        while current_to > fetch_from:
            current_from = max(fetch_from, current_to - timedelta(days=interval_days))
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

                # Reverse DataFrame to newest-to-oldest order
                df = df.iloc[::-1].reset_index(drop=True)
 
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
                        ts_add(ts_field_key, epoch, float(value), pipe=pipe, labels={"type": "ohlc", "instrument_token": instrument_token, "timeframe": timeframe,"sub_type":  field }   , upsert=upsert )
                pipe.execute()
                print(f"[fetch_zerodha_historical] Ingested new candles into RedisTimeSeries for {current_from} to {current_to}.")

                yield df
            else:
                print(f"[fetch_zerodha_historical] No data for {current_from.date()} to {current_to.date()}")
                break
            current_to = current_from
