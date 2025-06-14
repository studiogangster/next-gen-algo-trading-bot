import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Dict
from core.base import BaseStorage

class ParquetStorage(BaseStorage):
    """
    Storage backend that saves candles, signals, and PnL data to Parquet files.
    """

    def __init__(self, base_dir: str = "data"):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

    def save_candles(self, symbol: str, timeframe: str, candles: pd.DataFrame) -> None:
        path = os.path.join(self.base_dir, f"{symbol}_{timeframe}_candles.parquet")
        print(f"[ParquetStorage] save_candles: symbol={symbol}, timeframe={timeframe}, rows={len(candles)} -> {path}")
        table = pa.Table.from_pandas(candles)
        pq.write_table(table, path)

    def save_signal(self, symbol: str, timeframe: str, signal: dict) -> None:
        path = os.path.join(self.base_dir, f"{symbol}_{timeframe}_signals.parquet")
        df = pd.DataFrame([signal])
        if os.path.exists(path):
            old = pd.read_parquet(path)
            df = pd.concat([old, df], ignore_index=True)
        df.to_parquet(path, index=False)

    def save_pnl(self, symbol: str, pnl: dict) -> None:
        path = os.path.join(self.base_dir, f"{symbol}_pnl.parquet")
        df = pd.DataFrame([pnl])
        if os.path.exists(path):
            old = pd.read_parquet(path)
            df = pd.concat([old, df], ignore_index=True)
        df.to_parquet(path, index=False)

    def close(self) -> None:
        pass
