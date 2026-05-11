import os
import time
import traceback
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import ray

from core.indicator_logic import compute_indicator, timeframe_seconds
from storage.redis_client import get_redis_client, ts_add


def _indicator_series_name(indicator: Dict[str, Any]) -> str:
    explicit = str(indicator.get("id") or indicator.get("name") or "").strip().lower()
    if explicit:
        return explicit
    ind_type = str(indicator.get("type") or "").strip().lower()
    params = indicator.get("params", {}) if isinstance(indicator.get("params", {}), dict) else {}
    length = params.get("length")
    if length is not None:
        try:
            return f"{ind_type}_{int(length)}"
        except Exception:
            return f"{ind_type}_{length}"
    return ind_type


def _indicator_length_hint(indicator: Dict[str, Any]) -> int:
    params = indicator.get("params", {}) if isinstance(indicator.get("params", {}), dict) else {}
    raw = params.get("length")
    try:
        n = int(raw)
        return max(1, n)
    except Exception:
        return 1


@ray.remote
class IndicatorSyncWorker:
    """
    Dedicated indicator pipeline:
      1) historical backfill (full candle range available in Redis)
      2) realtime incremental sync (with warmup overlap to keep values stable)
    """

    def __init__(
        self,
        symbols: List[str],
        timeframes: List[str],
        indicators: List[Dict[str, Any]],
        poll_interval: float = 1.0,
        warmup_bars: int = 600,
        max_points_per_cycle: int = 3000,
        historical_chunk_points: int = 5000,
    ):
        self.symbols = [str(s) for s in symbols]
        self.timeframes = sorted(set(timeframes))
        self.indicators = indicators or []
        self.poll_interval = poll_interval
        self.warmup_bars = warmup_bars
        self.max_points_per_cycle = max_points_per_cycle
        self.historical_chunk_points = historical_chunk_points
        self.client = get_redis_client()
        self.last_synced_ts: Dict[Tuple[str, str], int] = {}
        self.verbose = str(os.getenv("INDICATOR_SYNC_VERBOSE", "0")).strip().lower() in {"1", "true", "yes", "on"}
        self.skip_historical_if_present = str(os.getenv("INDICATOR_SKIP_HISTORICAL_IF_PRESENT", "1")).strip().lower() in {"1", "true", "yes", "on"}
        lookback_bars_raw = os.getenv("INDICATOR_HISTORICAL_LOOKBACK_BARS", "").strip()
        self.historical_lookback_bars: Optional[int] = None
        if lookback_bars_raw:
            try:
                parsed = int(lookback_bars_raw)
                if parsed > 0:
                    self.historical_lookback_bars = parsed
            except Exception:
                self.historical_lookback_bars = None

    def _configured_indicator_defs(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for ind in self.indicators:
            ind_type = str(ind.get("type", "")).lower().strip()
            ind_name = _indicator_series_name(ind)
            if not ind_type or not ind_name:
                continue
            out.append(
                {
                    "type": ind_type,
                    "name": ind_name,
                    "length_hint": _indicator_length_hint(ind),
                }
            )
        return out

    def _ts_info_bounds(self, key: str) -> Tuple[Optional[int], Optional[int]]:
        try:
            info = self.client.execute_command("TS.INFO", key)
        except Exception:
            return None, None
        first = None
        last = None
        for i in range(0, len(info), 2):
            field = info[i]
            if isinstance(field, bytes):
                field = field.decode("utf-8", errors="ignore")
            if field == "firstTimestamp":
                first = int(info[i + 1])
            elif field == "lastTimestamp":
                last = int(info[i + 1])
        return first, last

    def _has_precomputed_for_pair(self, symbol: str, timeframe: str) -> bool:
        """
        Returns True only when all configured indicator series are present for this symbol/timeframe.
        Also validates basic historical/realtime coverage against candle bounds to avoid partial series.
        """
        defs = self._configured_indicator_defs()
        if not defs:
            return False
        candle_first, candle_last = self._candle_bounds(symbol, timeframe)
        if candle_first is None or candle_last is None:
            return False

        tf_sec = timeframe_seconds(timeframe)
        # Allow last indicator point to lag by up to 2 bars.
        last_min_expected = candle_last - (2 * tf_sec)

        for item in defs:
            ind_name = str(item.get("name"))
            length_hint = int(item.get("length_hint") or 1)
            pattern = f"ts:indicator:{symbol}:{timeframe}:{ind_name}:*"
            keys = list(self.client.scan_iter(match=pattern, count=100))
            if not keys:
                return False
            min_first = None
            max_last = None
            for key in keys:
                first_ts, last_ts = self._ts_info_bounds(key)
                if first_ts is not None:
                    min_first = first_ts if min_first is None else min(min_first, first_ts)
                if last_ts is not None:
                    max_last = last_ts if max_last is None else max(max_last, last_ts)
            if min_first is None or max_last is None:
                return False

            # A long MA can start later; tolerate a length-based initial gap.
            first_max_expected = candle_first + (max(1, length_hint + 5) * tf_sec)
            if min_first > first_max_expected:
                return False
            if max_last < last_min_expected:
                return False
        return True

    def _fetch_ohlc_df(self, symbol: str, timeframe: str, start: str, end: str) -> pd.DataFrame:
        filters = [
            "type=ohlc",
            f"instrument_token={symbol}",
            f"timeframe={timeframe}",
        ]
        result = self.client.execute_command("TS.MRANGE", start, end, "FILTER", *filters)

        field_data: Dict[str, Dict[int, float]] = {}
        for entry in result:
            key, _labels, data = entry
            field = str(key).split(":")[-1]
            field_data[field] = {int(ts): float(val) for ts, val in data}

        required = {"open", "high", "low", "close", "volume"}
        if not required.issubset(field_data.keys()):
            return pd.DataFrame()

        common_ts = set.intersection(*(set(field_data[f].keys()) for f in required))
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
        ).set_index("timestamp")

    def _latest_candle_ts(self, symbol: str, timeframe: str) -> Optional[int]:
        try:
            rows = self.client.execute_command(
                "TS.MGET",
                "FILTER",
                "type=ohlc",
                f"instrument_token={symbol}",
                f"timeframe={timeframe}",
            )
        except Exception:
            return None

        latest = None
        for row in rows:
            if len(row) < 3:
                continue
            point = row[2]
            if not point:
                continue
            ts = int(point[0])
            if latest is None or ts > latest:
                latest = ts
        return latest

    def _candle_bounds(self, symbol: str, timeframe: str) -> Tuple[Optional[int], Optional[int]]:
        key = f"ts:candle:{symbol}:{timeframe}:open"
        try:
            info = self.client.execute_command("TS.INFO", key)
        except Exception:
            return None, None

        first = None
        last = None
        for i in range(0, len(info), 2):
            field = info[i]
            if field == "firstTimestamp":
                first = int(info[i + 1])
            elif field == "lastTimestamp":
                last = int(info[i + 1])
        return first, last

    def _compute_indicator(self, df: pd.DataFrame, ind_type: str, params: Dict[str, Any]):
        return compute_indicator(df, ind_type, params)

    def _write_indicator(self, symbol: str, timeframe: str, ind_type: str, indicator_name: str, result) -> int:
        if result is None:
            return 0

        if isinstance(result, pd.Series):
            frame = pd.DataFrame({"value": result})
        elif isinstance(result, pd.DataFrame):
            frame = result.copy()
            frame.columns = [str(c).lower() for c in frame.columns]
        else:
            return 0

        frame = frame.dropna(how="all")
        if frame.empty:
            return 0

        wrote = 0
        pipe = self.client.pipeline(transaction=False)
        for col in frame.columns:
            key = f"ts:indicator:{symbol}:{timeframe}:{indicator_name}:{col}"
            labels = {
                "type": "indicator",
                "instrument_token": str(symbol),
                "timeframe": str(timeframe),
                "indicator_name": str(indicator_name),
                "indicator": str(ind_type),
                "field": str(col),
            }
            for ts, val in frame[col].dropna().items():
                ts_add(key, int(ts), float(val), labels=labels, pipe=pipe, upsert=True)
                wrote += 1
        pipe.execute()
        return wrote

    def _sync_symbol_timeframe(self, symbol: str, timeframe: str, start: str, end: str) -> None:
        df = self._fetch_ohlc_df(symbol, timeframe, start, end)
        if df.empty:
            return

        for ind in self.indicators:
            ind_type = str(ind.get("type", "")).lower()
            params = ind.get("params", {}) or {}
            indicator_name = _indicator_series_name(ind)
            if not ind_type:
                continue
            try:
                result = self._compute_indicator(df, ind_type, params)
                wrote = self._write_indicator(symbol, timeframe, ind_type, indicator_name, result)
                if wrote > 0 and self.verbose:
                    print(
                        f"[IndicatorSync] {symbol} {timeframe} {indicator_name}({ind_type}) "
                        f"updated={wrote} start={start} end={end}"
                    )
            except Exception as exc:
                print(f"[IndicatorSync] compute/write error {symbol} {timeframe} {indicator_name}({ind_type}): {exc}")
                traceback.print_exc()

    def _historical_backfill(self) -> None:
        print("[IndicatorSync] historical backfill started")
        for symbol in self.symbols:
            for timeframe in self.timeframes:
                self._historical_backfill_pair(symbol, timeframe)
        print("[IndicatorSync] historical backfill completed")

    def _historical_backfill_pair(self, symbol: str, timeframe: str) -> None:
        try:
            first, latest = self._candle_bounds(symbol, timeframe)
            if first is None or latest is None or latest <= first:
                return

            sec = timeframe_seconds(timeframe)
            if self.historical_lookback_bars is not None:
                bounded_first = max(first, latest - (self.historical_lookback_bars * sec))
                if bounded_first > first and self.verbose:
                    print(
                        f"[IndicatorSync] {symbol} {timeframe}: limiting historical backfill "
                        f"to last {self.historical_lookback_bars} bars."
                    )
                first = bounded_first
            chunk_step = max(self.historical_chunk_points * sec, sec)
            overlap = self.warmup_bars * sec

            cursor = first
            while cursor <= latest:
                window_end = min(cursor + chunk_step, latest)
                window_start = max(first, cursor - overlap)
                self._sync_symbol_timeframe(
                    symbol,
                    timeframe,
                    str(window_start),
                    str(window_end),
                )
                cursor = window_end + sec

            self.last_synced_ts[(symbol, timeframe)] = latest
        except Exception as exc:
            print(f"[IndicatorSync] backfill error {symbol} {timeframe}: {exc}")
            traceback.print_exc()

    def _seed_last_synced_from_candles(self, symbol: str, timeframe: str) -> None:
        latest = self._latest_candle_ts(symbol, timeframe)
        if latest is not None:
            self.last_synced_ts[(symbol, timeframe)] = latest

    def start(self):
        if self.skip_historical_if_present:
            for symbol in self.symbols:
                for timeframe in self.timeframes:
                    if self._has_precomputed_for_pair(symbol, timeframe):
                        self._seed_last_synced_from_candles(symbol, timeframe)
                        if self.verbose:
                            print(
                                f"[IndicatorSync] {symbol} {timeframe}: all configured precomputed series detected; "
                                "skipping historical backfill for this pair."
                            )
                    else:
                        self._historical_backfill_pair(symbol, timeframe)
        else:
            self._historical_backfill()

        self._realtime_loop()

    def _realtime_loop(self) -> None:
        print("[IndicatorSync] realtime loop started")
        while True:
            for symbol in self.symbols:
                for timeframe in self.timeframes:
                    try:
                        latest = self._latest_candle_ts(symbol, timeframe)
                        if latest is None:
                            continue

                        prev = self.last_synced_ts.get((symbol, timeframe))
                        sec = timeframe_seconds(timeframe)
                        overlap_start = latest - (self.warmup_bars * sec)
                        if prev is not None:
                            overlap_start = min(overlap_start, prev - (self.warmup_bars * sec))

                        hard_window = latest - (self.max_points_per_cycle * sec)
                        start_ts = max(overlap_start, hard_window, 0)
                        self._sync_symbol_timeframe(symbol, timeframe, str(start_ts), str(latest))
                        self.last_synced_ts[(symbol, timeframe)] = latest
                    except Exception as exc:
                        print(f"[IndicatorSync] realtime error {symbol} {timeframe}: {exc}")
                        traceback.print_exc()
            time.sleep(self.poll_interval)
