
import pandas as pd
from datetime import datetime, timedelta

from brokers.kite_trade import ZerodhaBroker

def fetch_zerodha_historical(enctoken, symbol, timeframe, days=5):
    """
    Fetch historical candles for a symbol and timeframe from Zerodha.
    Returns a DataFrame with columns: timestamp, open, high, low, close, volume
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

    # For demo, fetch last N days
    to_date = datetime.now()
    from_date = to_date - timedelta(days=days)
    # symbol is instrument_token (int or str)
    instrument_token = int(symbol)


    candles = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_date,
        to_date=to_date,
        interval=interval,
        continuous=False,
        oi=False
    )
    if not candles:
        return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df = pd.DataFrame(candles)
    df.rename(columns={"date": "timestamp", "volume": "volume"}, inplace=True)
    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
    return df
