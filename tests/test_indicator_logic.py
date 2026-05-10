import pandas as pd
import pytest

try:
    import pandas_ta as ta
except ImportError:
    import pandas_ta_classic as ta

from core.indicator_logic import compute_indicator
from core.indicator_logic import timeframe_seconds


def sample_df():
    return pd.DataFrame(
        {
            "timestamp": pd.RangeIndex(0, 120),
            "open": [100 + (i * 0.1) for i in range(120)],
            "high": [101 + (i * 0.1) for i in range(120)],
            "low": [99 + (i * 0.1) for i in range(120)],
            "close": [100 + (i * 0.1) for i in range(120)],
            "volume": [1000 + i for i in range(120)],
        }
    ).set_index("timestamp")


def test_compute_indicator_close_based():
    df = sample_df()
    ema = compute_indicator(df, "ema", {"length": 20})
    expected = ta.ema(df["close"], length=20)
    pd.testing.assert_series_equal(ema.dropna(), expected.dropna(), check_names=False)


def test_compute_indicator_kc_hlc():
    df = sample_df()
    kc = compute_indicator(df, "kc", {"length": 20, "multiplier": 2})
    expected = ta.kc(df["high"], df["low"], df["close"], length=20, multiplier=2)
    assert isinstance(kc, pd.DataFrame)
    pd.testing.assert_frame_equal(kc.dropna(), expected.dropna(), check_like=True)


def test_compute_indicator_unsupported_raises():
    df = sample_df()
    with pytest.raises(ValueError):
        compute_indicator(df, "adx", {"length": 14})


@pytest.mark.parametrize(
    "timeframe,seconds",
    [
        ("1m", 60),
        ("5m", 300),
        ("1h", 3600),
        ("1d", 86400),
    ],
)
def test_tf_seconds(timeframe, seconds):
    assert timeframe_seconds(timeframe) == seconds
