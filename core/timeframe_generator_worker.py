import time
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import ray

from storage.redis_client import get_redis_client, ts_add

IST = ZoneInfo("Asia/Kolkata")
SERVICE_NS = "service:timeframe_sync"
OHLC_FIELDS = ("open", "high", "low", "close", "volume")


def timeframe_to_seconds(timeframe: str) -> int:
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
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def resample_rule(timeframe: str) -> str:
    tf = str(timeframe).strip()
    if tf.endswith("m"):
        return f"{int(tf[:-1])}min"
    if tf.endswith("h"):
        return f"{int(tf[:-1])}h"
    if tf.endswith("d"):
        return f"{int(tf[:-1])}D"
    if tf.endswith("w"):
        return f"{int(tf[:-1])}W"
    if tf.endswith("M"):
        return f"{int(tf[:-1])}ME"
    if tf.endswith("y"):
        return f"{int(tf[:-1])}YE"
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def aggregate_candles(df_1m: pd.DataFrame, timeframe: str, market_open_time: str = "09:15") -> pd.DataFrame:
    if df_1m.empty:
        return pd.DataFrame()

    df = df_1m.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_convert(IST)
    df = df.set_index("timestamp")

    hh, mm = map(int, market_open_time.split(":"))
    offset_td = pd.Timedelta(hours=hh, minutes=mm)
    rule = resample_rule(timeframe)

    out = (
        df.resample(
            rule,
            label="left",
            closed="left",
            origin="start_day",
            offset=offset_td,
        )
        .agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        .dropna()
        .reset_index()
    )
    return out


@ray.remote
class TimeframeGeneratorWorker:
    def __init__(self, config, poll_interval: float = 5.0):
        self.poll_interval = float(poll_interval)
        self.config = config
        self.symbols = [str(s) for s in getattr(self.config, "symbols", [])]
        self.target_timeframes = sorted(
            set(tf for tf in getattr(self.config, "derived_timeframes", []) if str(tf) != "1m")
        )
        self.redis_client = get_redis_client()
        # Cap per-cycle 1m fetch to prevent OOM on large historical backfills.
        self.max_1m_points_per_cycle = int(getattr(self.config, "max_1m_points_per_cycle", 3000))

    def _series_bounds(self, key: str) -> Tuple[Optional[int], Optional[int]]:
        try:
            info = self.redis_client.execute_command("TS.INFO", key)
        except Exception:
            return None, None
        first = None
        last = None
        for idx in range(0, len(info), 2):
            field = info[idx]
            if isinstance(field, bytes):
                field = field.decode("utf-8", errors="ignore")
            if field == "firstTimestamp":
                first = int(info[idx + 1])
            elif field == "lastTimestamp":
                last = int(info[idx + 1])
        return first, last

    def _fetch_1m_df(self, symbol: str, start_ts: int, end_ts: int) -> pd.DataFrame:
        if start_ts > end_ts:
            return pd.DataFrame()

        try:
            rows = self.redis_client.execute_command(
                "TS.MRANGE",
                str(int(start_ts)),
                str(int(end_ts)),
                "FILTER",
                "type=ohlc",
                f"instrument_token={symbol}",
                "timeframe=1m",
            )
        except Exception:
            return pd.DataFrame()

        field_data: Dict[str, Dict[int, float]] = {}
        for row in rows:
            key, _labels, points = row
            field = str(key).split(":")[-1]
            if field in OHLC_FIELDS:
                field_data[field] = {int(ts): float(val) for ts, val in points}

        if not all(field in field_data for field in OHLC_FIELDS):
            return pd.DataFrame()

        common_ts = set.intersection(*(set(field_data[f].keys()) for f in OHLC_FIELDS))
        if not common_ts:
            return pd.DataFrame()

        sorted_ts = sorted(common_ts)
        return pd.DataFrame(
            {
                "timestamp": sorted_ts,
                "open": [field_data["open"][ts] for ts in sorted_ts],
                "high": [field_data["high"][ts] for ts in sorted_ts],
                "low": [field_data["low"][ts] for ts in sorted_ts],
                "close": [field_data["close"][ts] for ts in sorted_ts],
                "volume": [field_data["volume"][ts] for ts in sorted_ts],
            }
        )

    def _write_derived(self, symbol: str, timeframe: str, agg_df: pd.DataFrame) -> int:
        if agg_df.empty:
            return 0
        pipe = self.redis_client.pipeline(transaction=False)
        writes = 0
        for _, row in agg_df.iterrows():
            ts = row["timestamp"]
            epoch = int(pd.Timestamp(ts).timestamp())
            for field in OHLC_FIELDS:
                key = f"ts:candle:{symbol}:{timeframe}:{field}"
                ts_add(
                    key,
                    epoch,
                    float(row[field]),
                    pipe=pipe,
                    labels={
                        "type": "ohlc",
                        "instrument_token": str(symbol),
                        "timeframe": str(timeframe),
                        "sub_type": str(field),
                    },
                    upsert=True,
                )
                writes += 1
        pipe.execute()
        return writes

    def _sync_symbol_timeframe(self, symbol: str, timeframe: str) -> Dict[str, int]:
        src_key = f"ts:candle:{symbol}:1m:open"
        dst_key = f"ts:candle:{symbol}:{timeframe}:open"

        src_first, src_last = self._series_bounds(src_key)
        if src_first is None or src_last is None:
            return {"rows": 0, "writes": 0}

        _dst_first, dst_last = self._series_bounds(dst_key)
        tf_sec = timeframe_to_seconds(timeframe)
        one_min_sec = 60

        if dst_last is None:
            start_ts = src_first
        else:
            # Recompute a small overlap window to fix late/updated source candles.
            start_ts = max(src_first, dst_last - (2 * tf_sec))
        max_window_sec = max(one_min_sec, self.max_1m_points_per_cycle * one_min_sec)
        end_ts = min(src_last, start_ts + max_window_sec)
        if start_ts > end_ts:
            return {"rows": 0, "writes": 0}

        df_1m = self._fetch_1m_df(symbol, start_ts, end_ts)
        if df_1m.empty:
            return {"rows": 0, "writes": 0}

        agg = aggregate_candles(df_1m, timeframe=timeframe, market_open_time="09:15")
        if agg.empty:
            return {"rows": 0, "writes": 0}

        writes = self._write_derived(symbol=symbol, timeframe=timeframe, agg_df=agg)
        return {"rows": int(len(agg)), "writes": int(writes)}

    def _set_service(self, key: str, value: str) -> None:
        try:
            self.redis_client.execute_command("SET", key, value)
        except Exception:
            pass

    def run(self):
        print("[TimeframeGeneratorWorker] starting", {"symbols": self.symbols, "timeframes": self.target_timeframes})
        while True:
            loop_start_ms = int(time.time() * 1000)
            self._set_service(f"{SERVICE_NS}:last_heartbeat_epoch_ms", str(loop_start_ms))
            try:
                for symbol in self.symbols:
                    for timeframe in self.target_timeframes:
                        summary = self._sync_symbol_timeframe(symbol=symbol, timeframe=timeframe)
                        self._set_service(
                            f"{SERVICE_NS}:symbol:{symbol}:{timeframe}:last_rows",
                            str(summary["rows"]),
                        )
                        self._set_service(
                            f"{SERVICE_NS}:symbol:{symbol}:{timeframe}:last_writes",
                            str(summary["writes"]),
                        )
                        self._set_service(
                            f"{SERVICE_NS}:symbol:{symbol}:{timeframe}:last_success_epoch_ms",
                            str(int(time.time() * 1000)),
                        )
                self._set_service(f"{SERVICE_NS}:last_success_epoch_ms", str(int(time.time() * 1000)))
                self._set_service(f"{SERVICE_NS}:last_error", "")
            except Exception as exc:
                traceback.print_exc()
                self._set_service(f"{SERVICE_NS}:last_error", str(exc))
            finally:
                time.sleep(self.poll_interval)
