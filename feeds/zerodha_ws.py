from kiteconnect import KiteTicker
from typing import List, Callable

import pytz
from core.base import BaseFeed

class ZerodhaWebSocketFeed(BaseFeed):
    """
    Zerodha WebSocket feed adapter.
    """

    def __init__(self, api_key: str, access_token: str):
        self.api_key = api_key
        self.access_token = access_token
        self.kws = None
        self.on_data_callback = None
        self.subscribed_tokens = set()

    def subscribe(self, symbols: List[str], on_data: Callable[[str, dict], None]) -> None:
        # Map symbols to instrument tokens (user must provide mapping)
        # For demo, assume symbols are instrument tokens as strings
        tokens = [int(s) for s in symbols]
        self.kws = KiteTicker(self.api_key, self.access_token)
        self.on_data_callback = on_data
        self.subscribed_tokens.update(tokens)

        def on_ticks(ws, ticks):
            for tick in ticks:
                symbol = str(tick.get("instrument_token"))
                # Convert tick to standard format
                print("_tick_", tick)
                data = {
                    "timestamp": tick.get("exchange_timestamp").astimezone(pytz.utc),
                    "open": tick.get("ohlc", {}).get("open"),
                    "high": tick.get("ohlc", {}).get("high"),
                    "low": tick.get("ohlc", {}).get("low"),
                    "close": tick.get("last_price"),
                    "volume": tick.get("volume_traded"),
                }
                on_data(symbol, data)

        def on_connect(ws, response):
            ws.subscribe(tokens)
            ws.set_mode(ws.MODE_FULL, tokens)

        self.kws.on_ticks = on_ticks
        self.kws.on_connect = on_connect
        self.kws.connect(threaded=True)

    def unsubscribe(self, symbols: List[str]) -> None:
        if self.kws:
            tokens = [int(s) for s in symbols]
            self.kws.unsubscribe(tokens)
            for t in tokens:
                self.subscribed_tokens.discard(t)

    def close(self) -> None:
        if self.kws:
            self.kws.close()
            self.kws = None
