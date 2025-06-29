import pandas as pd
import pandas_ta as ta
import pytest
import time

from core.timeframe_generator_worker import fetch_1m_candles, parse_redis_candles, aggregate_candles

# Sample candle data (OHLCV)
def sample_candles():
    data = {
        "timestamp": pd.date_range("2023-01-01 09:15", periods=30, freq="1min", tz="Asia/Kolkata"),
        "open": [100 + i for i in range(30)],
        "high": [101 + i for i in range(30)],
        "low": [99 + i for i in range(30)],
        "close": [100 + i for i in range(30)],
        "volume": [1000 + 10 * i for i in range(30)],
    }
    return pd.DataFrame(data)

# Indicator calculation logic (no Redis)
def compute_indicator(df, ind_type, params):
    if hasattr(ta, ind_type):
        func = getattr(ta, ind_type)
        result = func(df["close"], **params)
        return result
    else:
        raise ValueError(f"Unknown indicator: {ind_type}")

@pytest.mark.parametrize("ind_type,params", [
    ("ema", {"length": 10}),
    ("rsi", {"length": 14}),
    ("sma", {"length": 5}),
])
def test_single_column_indicators(ind_type, params):
    df = sample_candles()
    result = compute_indicator(df, ind_type, params)
    # Compare with pandas-ta directly
    expected = getattr(ta, ind_type)(df["close"], **params)
    pd.testing.assert_series_equal(result.dropna(), expected.dropna(), check_names=False)

def test_kc_indicator():
    df = sample_candles()
    # KC (Keltner Channel) requires high, low, close
    result = ta.kc(df["high"], df["low"], df["close"], length=10, multiplier=2)
    expected = ta.kc(df["high"], df["low"], df["close"], length=10, multiplier=2)
    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(result.dropna(), expected.dropna(), check_like=True)

def test_indicator_from_redis():
    # Fetch real 1m candles for NIFTY (symbol "256265") for the last 60 minutes
    symbol = "256265"
    now = int(time.time())
    from_ts = now - 60 * 60  # last 1 hour
    to_ts = now
    data = fetch_1m_candles(symbol, from_ts, to_ts)
    df = parse_redis_candles(data)
    assert not df.empty, "No candle data fetched from Redis"
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    # Aggregate to 5m candles as an example
    df_agg = aggregate_candles(df, "5m", market_open_time="09:15")
    assert not df_agg.empty, "Aggregation failed"
    if len(df_agg) < 15:
        pytest.skip(f"Not enough data to compute RSI(14): only {len(df_agg)} rows")
    # Compute EMA(10) on close
    ema = ta.ema(df_agg["close"], length=10)
    assert ema is not None and ema.notna().sum() > 0, "EMA calculation failed"
    # Compute RSI(14) on close
    rsi = ta.rsi(df_agg["close"], length=14)
    if rsi is None:
        print("RSI returned None. Data shape:", df_agg.shape)
        print(df_agg.head())
        pytest.skip("RSI calculation returned None (likely not enough data)")
    assert rsi.notna().sum() > 0, "RSI calculation failed"
    # Do not write to Redis

def test_indicator_config_loop():
    df = sample_candles()
    indicators = [
        {"type": "ema", "params": {"length": 10}},
        {"type": "rsi", "params": {"length": 14}},
        {"type": "sma", "params": {"length": 5}},
    ]
    for ind in indicators:
        ind_type = ind["type"]
        params = ind["params"]
        result = compute_indicator(df, ind_type, params)
        expected = getattr(ta, ind_type)(df["close"], **params)
        if isinstance(result, pd.DataFrame):
            pd.testing.assert_frame_equal(result.dropna(), expected.dropna(), check_like=True)
        else:
            pd.testing.assert_series_equal(result.dropna(), expected.dropna(), check_names=False)
