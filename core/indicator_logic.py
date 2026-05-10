from typing import Any, Dict

import pandas as pd

try:
    import pandas_ta as ta
except ImportError:
    import pandas_ta_classic as ta


# Keep this explicit to avoid silently wrong input assumptions.
SUPPORTED_INDICATOR_INPUTS = {
    "ema": "close",
    "rsi": "close",
    "sma": "close",
    "kc": "hlc",
}


def timeframe_seconds(timeframe: str) -> int:
    tf = str(timeframe).strip()
    if tf.endswith("m"):
        return int(tf[:-1]) * 60
    if tf.endswith("h"):
        return int(tf[:-1]) * 3600
    if tf.endswith("d"):
        return int(tf[:-1]) * 86400
    if tf.endswith("w"):
        return int(tf[:-1]) * 7 * 86400
    if tf.endswith("M"):
        return int(tf[:-1]) * 30 * 86400
    if tf.endswith("y"):
        return int(tf[:-1]) * 365 * 86400
    return 60


def compute_indicator(df: pd.DataFrame, ind_type: str, params: Dict[str, Any]):
    indicator = str(ind_type or "").lower().strip()
    if not indicator:
        raise ValueError("Indicator type is required.")

    input_mode = SUPPORTED_INDICATOR_INPUTS.get(indicator)
    if input_mode is None:
        raise ValueError(f"Unsupported indicator '{indicator}'. Add explicit input mapping first.")

    if indicator == "kc":
        return ta.kc(df["high"], df["low"], df["close"], **(params or {}))

    fn = getattr(ta, indicator, None)
    if fn is None:
        raise ValueError(f"Indicator function '{indicator}' not available in pandas_ta.")

    return fn(df["close"], **(params or {}))
