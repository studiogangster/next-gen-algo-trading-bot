from typing import Any, Dict, Optional

import pandas as pd

from core.base import BaseStrategy
from core.yaml_signal_engine import latest_rule_signal


class YamlRuleStrategy(BaseStrategy):
    """
    Generic YAML-driven rule strategy.
    Supports nested logical expressions and indicator comparisons.
    """

    def __init__(self, **params: Any):
        self.params = params or {}
        self.last_emitted_key = None

    def on_candle(self, symbol: str, timeframe: str, candles: pd.DataFrame) -> Optional[Dict[str, Any]]:
        signal = latest_rule_signal(candles, self.params, symbol=symbol, timeframe=timeframe)
        if not signal:
            return None

        key = (signal.get("timestamp"), signal.get("action"))
        if key == self.last_emitted_key:
            return None
        self.last_emitted_key = key

        return {
            "strategy": signal.get("strategy"),
            "action": signal.get("action"),
            "confidence": 1.0,
            "quantity": signal.get("quantity", 1),
            "timestamp": signal.get("timestamp"),
            "price": signal.get("price"),
            "reason": signal.get("reason"),
            "indicators": signal.get("indicators", {}),
        }

    def reset(self) -> None:
        self.last_emitted_key = None
