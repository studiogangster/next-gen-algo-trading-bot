import pandas as pd
import pandas_ta as ta
from typing import Any, Dict, Optional
from core.base import BaseStrategy

class SupertrendRSIStrategy(BaseStrategy):
    """
    Example strategy: Supertrend + RSI crossover.
    Buy: Supertrend flips bullish and RSI > 50
    Sell: Supertrend flips bearish and RSI < 50
    """

    def __init__(self, rsi_length: int = 14, supertrend_length: int = 10, supertrend_multiplier: float = 3.0):
        self.rsi_length = rsi_length
        self.supertrend_length = supertrend_length
        self.supertrend_multiplier = supertrend_multiplier
        self.last_signal = None

    def on_candle(self, symbol: str, timeframe: str, candles: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(candles) < max(self.rsi_length, self.supertrend_length) + 2:
            return None

        df = candles.copy()
        df['rsi'] = ta.rsi(df['close'], length=self.rsi_length)
        st = ta.supertrend(df['high'], df['low'], df['close'],
                           length=self.supertrend_length, multiplier=self.supertrend_multiplier)
        df['supertrend'] = st['SUPERT_{}_{}'.format(self.supertrend_length, self.supertrend_multiplier)]
        df['supertrend_direction'] = st['SUPERTd_{}_{}'.format(self.supertrend_length, self.supertrend_multiplier)]

        # Signal logic: look at last two rows for crossover
        prev, curr = df.iloc[-2], df.iloc[-1]
        signal = None

        if prev['supertrend_direction'] == -1 and curr['supertrend_direction'] == 1 and curr['rsi'] > 50:
            signal = {'action': 'BUY', 'confidence': float(curr['rsi']) / 100, 'quantity': 1}
        elif prev['supertrend_direction'] == 1 and curr['supertrend_direction'] == -1 and curr['rsi'] < 50:
            signal = {'action': 'SELL', 'confidence': 1 - float(curr['rsi']) / 100, 'quantity': 1}

        if signal and signal != self.last_signal:
            self.last_signal = signal
            return signal
        return None

    def reset(self) -> None:
        self.last_signal = None
