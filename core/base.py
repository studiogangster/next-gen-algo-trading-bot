from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Callable
import pandas as pd

class BaseFeed(ABC):
    """Abstract base class for all data feeds."""

    @abstractmethod
    def subscribe(self, symbols: List[str], on_data: Callable[[str, dict], None]) -> None:
        """Subscribe to real-time data for given symbols. Calls on_data(symbol, data) on new tick."""
        pass

    @abstractmethod
    def unsubscribe(self, symbols: List[str]) -> None:
        """Unsubscribe from real-time data for given symbols."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Clean up resources, close connections."""
        pass

class BaseTimeframeAggregator(ABC):
    """Aggregates tick or lower timeframe data into higher timeframe candles."""

    @abstractmethod
    def add_tick(self, symbol: str, tick: dict) -> None:
        """Add a new tick or lower timeframe bar for aggregation."""
        pass

    @abstractmethod
    def get_candles(self, symbol: str, timeframe: str) -> pd.DataFrame:
        """Get the current DataFrame of candles for a symbol and timeframe."""
        pass

class BaseStrategy(ABC):
    """Abstract base class for all trading strategies."""

    @abstractmethod
    def on_candle(self, symbol: str, timeframe: str, candles: pd.DataFrame) -> Optional[Dict[str, Any]]:
        """
        Called on new candle. Returns signal dict or None.
        Example return: {'action': 'BUY', 'confidence': 0.9, ...}
        """
        pass

    @abstractmethod
    def reset(self) -> None:
        """Reset internal state (for backtesting/live switch)."""
        pass

class BaseBroker(ABC):
    """Abstract base class for broker adapters."""

    @abstractmethod
    def place_order(self, symbol: str, action: str, quantity: float, **kwargs) -> Any:
        """Place an order (BUY/SELL). Returns order id or result."""
        pass

    @abstractmethod
    def cancel_order(self, order_id: str) -> Any:
        """Cancel an order."""
        pass

    @abstractmethod
    def get_positions(self) -> List[Dict[str, Any]]:
        """Get current open positions."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Clean up resources, close connections."""
        pass

class BaseStorage(ABC):
    """Abstract base class for storage backends."""

    @abstractmethod
    def save_candles(self, symbol: str, timeframe: str, candles: pd.DataFrame) -> None:
        """Persist candles to storage."""
        pass

    @abstractmethod
    def save_signal(self, symbol: str, timeframe: str, signal: dict) -> None:
        """Persist signal to storage."""
        pass

    @abstractmethod
    def save_pnl(self, symbol: str, pnl: dict) -> None:
        """Persist PnL data to storage."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Clean up resources, close connections."""
        pass
