from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Any, Dict, List, Optional
from datetime import datetime, date
import json
import math
import numbers

import pandas as pd

from redis import Redis
from core.indicator_logic import compute_indicator
from core.yaml_signal_engine import load_rule_strategies_from_config, generate_rule_signals, validate_strategy_params
from storage.redis_client import get_redis_client, ts_range

app = FastAPI()
UNIVERSE_NS = "universe:v1"


def _root_config_path() -> str:
    import os
    return os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config", "config.yaml")


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, numbers.Real):
        number = float(value)
        if not math.isfinite(number):
            return None
        return number
    return value


def _timeframe_seconds(timeframe: str) -> int:
    tf = str(timeframe).strip().lower()
    if tf.endswith("m"):
        return max(1, int(tf[:-1])) * 60
    if tf.endswith("h"):
        return max(1, int(tf[:-1])) * 3600
    if tf.endswith("d"):
        return max(1, int(tf[:-1])) * 86400
    return 60

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/candles")
def get_candles(
    instrument_token: int,
    timeframe: str,
    start: int = Query(..., description="Start timestamp (epoch seconds)"),
    end: int = Query(..., description="End timestamp (epoch seconds)"),
    limit: int = Query(100, description="Max number of candles to return")
):
    """
    Fetch OHLC candles from RedisTimeSeries for a given instrument and timeframe.
    Uses TS.MRANGE with labels.
    """
    client: Redis = get_redis_client()

    from_ts = str(start) if start >= 0 else "-"
    to_ts = str(end) if end >= 0 else "+"

    # Compose label filter
    label_filter = [
        f"type=ohlc",
        f"instrument_token={instrument_token}",
        f"timeframe={timeframe}"
    ]

    count = max(1, min(limit, 20000))

    try:
        query =["TS.MREVRANGE", from_ts, to_ts ,  "COUNT", count, "FILTER", *label_filter]
        print("query", *query)
        result = client.execute_command( *query )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis MRANGE error: {str(e)}")

    # Parse MRANGE result
    field_data: Dict[str, Dict[int, float]] = {}

    for entry in result:
        key, labels, data = entry
        # print(key, labels, data)
        key = key.split(":")[-1]
        # label_dict = {k.decode(): v.decode() for k, v in labels}
        # field = label_dict.get("field")
        if key:
            field_data[key] = {int(ts): float(val) for ts, val in data}

    # Ensure all four OHLC fields are present
    expected_fields = {"open", "high", "low", "close", "volume"}
    if not expected_fields.issubset(field_data.keys()):
        raise HTTPException(status_code=404, detail=f"Missing one or more OHLC fields  ")

    # Intersect timestamps to build complete candles
    common_ts = set.intersection(*(set(fd.keys()) for fd in field_data.values()))
    sorted_ts = sorted(common_ts)[-limit:] if limit > 0 else sorted(common_ts)

    result = [
        {
            "timestamp": ts,
            "epoch": ts,
            "open": field_data["open"][ts],
            "high": field_data["high"][ts],
            "low": field_data["low"][ts],
            "close": field_data["close"][ts],
            "volume": field_data["volume"][ts],
        }
        for ts in sorted_ts
    ]

    return {"candles": result}

@app.get("/indicator")
def get_indicator(
    instrument_token: int,
    timeframe: str,
    start: int = Query(..., description="Start timestamp (epoch seconds)"),
    end: int = Query(..., description="End timestamp (epoch seconds)"),
    indicator: str = Query(..., description="Indicator type, e.g. rsi, kc"),
    length: int = Query(14, description="Indicator period/length (default 14)"),
    limit: int = Query(100, description="Max number of candles to use")
):
    """
    Compute indicator (e.g., RSI, KC) on-the-fly for given instrument and timeframe.
    """
    client: Redis = get_redis_client()

    from_ts = str(start) if start >= 0 else "-"
    to_ts = str(end) if end >= 0 else "+"

    # Compose label filter
    label_filter = [
        f"type=ohlc",
        f"instrument_token={instrument_token}",
        f"timeframe={timeframe}"
    ]

    count = max(1, min(limit, 20000))

    try:
        query = ["TS.MREVRANGE", from_ts, to_ts, "COUNT", count, "FILTER", *label_filter]
        result = client.execute_command(*query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis MRANGE error: {str(e)}")

    # Parse MRANGE result
    field_data: Dict[str, Dict[int, float]] = {}
    for entry in result:
        key, labels, data = entry
        key = key.split(":")[-1]
        if key:
            field_data[key] = {int(ts): float(val) for ts, val in data}

    expected_fields = {"open", "high", "low", "close", "volume"}
    if not expected_fields.issubset(field_data.keys()):
        raise HTTPException(status_code=404, detail=f"Missing one or more OHLC fields")

    # Intersect timestamps to build complete candles
    common_ts = set.intersection(*(set(fd.keys()) for fd in field_data.values()))
    sorted_ts = sorted(common_ts)[-limit:] if limit > 0 else sorted(common_ts)

    # Build DataFrame
    df = pd.DataFrame({
        "timestamp": sorted_ts,
        "open": [field_data["open"][ts] for ts in sorted_ts],
        "high": [field_data["high"][ts] for ts in sorted_ts],
        "low": [field_data["low"][ts] for ts in sorted_ts],
        "close": [field_data["close"][ts] for ts in sorted_ts],
        "volume": [field_data["volume"][ts] for ts in sorted_ts],
    }).set_index("timestamp")

    indicator = indicator.lower()
    try:
        params = {"length": length}
        result = compute_indicator(df, indicator, params)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if isinstance(result, pd.Series):
        values = [
            {"timestamp": int(ts), "value": float(val) if pd.notna(val) else None}
            for ts, val in result.items()
        ]
        return {"indicator": indicator, "values": values}

    if isinstance(result, pd.DataFrame):
        columns = [str(c).lower() for c in result.columns]
        values = []
        for ts, row in result.iterrows():
            entry = {"timestamp": int(ts)}
            for col_raw, col in zip(result.columns, columns):
                entry[col] = float(row[col_raw]) if pd.notna(row[col_raw]) else None
            values.append(entry)
        return {"indicator": indicator, "columns": columns, "values": values}

    raise HTTPException(status_code=500, detail=f"Unexpected indicator output for '{indicator}'")

@app.get("/_candles")
def _get_candles(
    instrument_token: int,
    timeframe: str,
    start: int = Query(..., description="Start timestamp (epoch seconds)"),
    end: int = Query(..., description="End timestamp (epoch seconds)"),
    limit: int = Query(1000, description="Max number of candles to return")
):
    """
    Fetch OHLC candles from RedisTimeSeries for a given instrument and timeframe.
    """
    key_filter = [
    f"type=ohlc",
    f"instrument_token={instrument_token}",
    f"interval={timeframe}"
    ]
    
    fields = ["open", "high", "low", "close", "volume"]
    base_key = f"ts:candle:{instrument_token}:{timeframe}"

    from_ts = str(start) if start >= 0 else "-"
    to_ts = str(end) if end >= 0 else "+"

    # Fetch all field data
    field_data: Dict[str, Dict[int, float]] = {}

    for field in fields:
        key = f"{base_key}:{field}"
        try:
            data = ts_range(key, from_ts, to_ts)
            field_data[field] = {int(ts): float(val) for ts, val in data}
        except Exception as e:
            if "TSDB: the key does not exist" in str(e):
                field_data[field] = {}
            else:
                raise HTTPException(status_code=500, detail=f"Redis error: {str(e)}")

    # Intersect timestamps that exist in all fields (ensure complete candles)
    common_ts = set.intersection(*(set(fd.keys()) for fd in field_data.values()))
    sorted_ts = sorted(common_ts)[-limit:] if limit > 0 else sorted(common_ts)

    result = [
        {
            "timestamp": ts,
            "open": field_data["open"][ts],
            "high": field_data["high"][ts],
            "low": field_data["low"][ts],
            "close": field_data["close"][ts],
            "volume": field_data["volume"][ts],
            "epoch": ts,
        }
        for ts in sorted_ts
    ]

    return {"candles": result}


@app.get("/indicators")
def get_indicators(
    instrument_token: int,
    timeframe: str,
    start: int = Query(..., description="Start timestamp (epoch seconds)"),
    end: int = Query(..., description="End timestamp (epoch seconds)"),
    limit: int = Query(100, description="Max number of candles to use"),
):
    """
    Compute all supported indicators for a given instrument and timeframe, on-the-fly.
    Returns a generic, extensible array of indicator results for UI visualization.
    """
    import yaml
    import os
    
    def _env_bool(name: str, default: bool = False) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        return str(raw).strip().lower() in {"1", "true", "yes", "on", "y"}

    # Load supported indicators from config/config.yaml if available
    config_path = _root_config_path()
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        indicators = config.get("indicators", [
            {"type": "ema", "params": {"length": 20}},
            {"type": "rsi", "params": {"length": 14}},
            {"type": "kc", "params": {"length": 20, "multiplier": 2}},
        ])
    else:
        indicators = [
            {"type": "ema", "params": {"length": 20}},
            {"type": "rsi", "params": {"length": 14}},
            {"type": "kc", "params": {"length": 20, "multiplier": 2}},
        ]

    client: Redis = get_redis_client()
    from_ts = str(start) if start >= 0 else "-"
    to_ts = str(end) if end >= 0 else "+"

    tf_sec = _timeframe_seconds(timeframe)
    precomputed_only = _env_bool("INDICATORS_PRECOMPUTED_ONLY", False)
    # Compute with extra lookback so moving indicators (e.g. SMA200) don't
    # appear broken at the left edge of the visible range.
    warmup_bars = 600
    compute_start = max(0, int(start) - (warmup_bars * tf_sec)) if start >= 0 else -1
    compute_from_ts = str(compute_start) if compute_start >= 0 else "-"

    def latest_candle_ts() -> Optional[int]:
        try:
            rows = client.execute_command(
                "TS.MGET",
                "FILTER",
                "type=ohlc",
                f"instrument_token={instrument_token}",
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

    latest_candle = latest_candle_ts()

    def indicator_series_name(ind: Dict[str, Any]) -> str:
        explicit = str(ind.get("id") or ind.get("name") or "").strip().lower()
        if explicit:
            return explicit
        ind_type = str(ind.get("type", "")).strip().lower()
        params = ind.get("params", {}) if isinstance(ind.get("params", {}), dict) else {}
        length = params.get("length")
        if length is not None:
            try:
                return f"{ind_type}_{int(length)}"
            except Exception:
                return f"{ind_type}_{length}"
        return ind_type

    def fetch_precomputed(indicator_name: str):
        pattern = f"ts:indicator:{instrument_token}:{timeframe}:{indicator_name}:*"
        keys = sorted(list(client.scan_iter(match=pattern, count=2000)))
        if not keys:
            return None

        field_data: Dict[str, Dict[int, float]] = {}
        for key in keys:
            field = key.split(":")[-1].lower()
            data = ts_range(key, compute_from_ts, to_ts)
            field_data[field] = {int(ts): float(val) for ts, val in data}

        if not field_data:
            return None

        all_ts = set()
        for vals in field_data.values():
            all_ts.update(vals.keys())
        if not all_ts:
            return None

        visible_ts = [ts for ts in sorted(all_ts) if (start < 0 or ts >= int(start)) and (end < 0 or ts <= int(end))]
        sorted_ts = visible_ts[-limit:] if limit > 0 else visible_ts
        columns = sorted(field_data.keys())
        values = []
        for ts in sorted_ts:
            row = {"timestamp": int(ts)}
            for col in columns:
                row[col] = field_data[col].get(ts)
            values.append(row)
        latest_non_null_ts = None
        for row in values:
            if any((k != "timestamp" and row.get(k) is not None) for k in row.keys()):
                latest_non_null_ts = int(row["timestamp"])
        return columns, values, latest_non_null_ts

    # Try precomputed indicator timeseries first.
    precomputed_results = []
    missing_for_compute = []
    precomputed_cache: Dict[str, Dict[str, Any]] = {}
    for ind in indicators:
        ind_type = str(ind.get("type", "")).lower()
        params = ind.get("params", {})
        ind_name = indicator_series_name(ind)
        if not ind_type:
            continue
        pre = fetch_precomputed(ind_name)
        if pre is None:
            missing_for_compute.append(ind)
            if precomputed_only:
                precomputed_results.append(
                    {
                        "name": ind_name,
                        "params": params,
                        "columns": [],
                        "values": [],
                        "source": "missing_precomputed",
                        "freshness": "missing",
                    }
                )
            continue
        cols, vals, latest_non_null_ts = pre
        precomputed_cache[ind_name] = {
            "columns": cols,
            "values": vals,
            "latest_non_null_ts": latest_non_null_ts,
        }
        # If precomputed data is stale relative to actual latest candle, recompute on-the-fly for this request.
        freshness_target = latest_candle if latest_candle is not None else end
        is_stale = latest_non_null_ts is None or latest_non_null_ts < (freshness_target - tf_sec)
        if is_stale and not precomputed_only:
            missing_for_compute.append(ind)
            continue
        tagged_vals = [{**row, "_source": "precomputed"} for row in vals]
        precomputed_results.append(
            {
                "name": ind_name,
                "params": params,
                "columns": cols,
                "values": tagged_vals,
                "source": "precomputed_stale" if is_stale else "precomputed",
                "freshness": "stale" if is_stale else "fresh",
            }
        )

    if precomputed_only:
        return {
            "indicators": precomputed_results,
            "indicator_status": {
                "mode": "precomputed_only",
                "fresh_count": sum(1 for i in precomputed_results if i.get("freshness") == "fresh"),
                "stale_count": sum(1 for i in precomputed_results if i.get("freshness") == "stale"),
                "missing_count": sum(1 for i in precomputed_results if i.get("freshness") == "missing"),
            },
        }

    if precomputed_results and not missing_for_compute:
        return {"indicators": precomputed_results}

    # Compose label filter for on-the-fly fallback.
    label_filter = [
        f"type=ohlc",
        f"instrument_token={instrument_token}",
        f"timeframe={timeframe}"
    ]

    count = max(1, min(limit, 20000))

    try:
        query = ["TS.MREVRANGE", compute_from_ts, to_ts, "COUNT", count, "FILTER", *label_filter]
        result = client.execute_command(*query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis MRANGE error: {str(e)}")

    # Parse MRANGE result
    field_data: Dict[str, Dict[int, float]] = {}
    for entry in result:
        key, labels, data = entry
        key = key.split(":")[-1]
        if key:
            field_data[key] = {int(ts): float(val) for ts, val in data}

    expected_fields = {"open", "high", "low", "close", "volume"}
    if not expected_fields.issubset(field_data.keys()):
        raise HTTPException(status_code=404, detail=f"Missing one or more OHLC fields")

    # Intersect timestamps to build complete candles
    common_ts = set.intersection(*(set(fd.keys()) for fd in field_data.values()))
    sorted_all_ts = sorted(common_ts)
    sorted_ts = [ts for ts in sorted_all_ts if (start < 0 or ts >= int(start)) and (end < 0 or ts <= int(end))]
    sorted_ts = sorted_ts[-limit:] if limit > 0 else sorted_ts

    # Build DataFrame
    df = pd.DataFrame({
        "timestamp": sorted_all_ts,
        "open": [field_data["open"][ts] for ts in sorted_all_ts],
        "high": [field_data["high"][ts] for ts in sorted_all_ts],
        "low": [field_data["low"][ts] for ts in sorted_all_ts],
        "close": [field_data["close"][ts] for ts in sorted_all_ts],
        "volume": [field_data["volume"][ts] for ts in sorted_all_ts],
    }).set_index("timestamp")

    def _compute_indicator_values(ind_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
        result = compute_indicator(df, str(ind_type), params)
        if result is None:
            return {"columns": [], "values": []}

        if isinstance(result, pd.DataFrame):
            columns = [str(c).lower() for c in result.columns]
            values = []
            for ts, row in result.iterrows():
                if (start >= 0 and int(ts) < int(start)) or (end >= 0 and int(ts) > int(end)):
                    continue
                entry = {"timestamp": int(ts)}
                for col_raw, col in zip(result.columns, columns):
                    entry[col] = float(row[col_raw]) if pd.notna(row[col_raw]) else None
                values.append(entry)
            return {"columns": columns, "values": values}

        values = [
            {"timestamp": int(ts), "value": float(val) if pd.notna(val) else None}
            for ts, val in result.items()
            if not ((start >= 0 and int(ts) < int(start)) or (end >= 0 and int(ts) > int(end)))
        ]
        return {"columns": ["value"], "values": values}

    indicator_results = precomputed_results[:]
    for ind in missing_for_compute:
        ind_type = ind.get("type")
        params = ind.get("params", {})
        ind_name = indicator_series_name(ind)
        try:
            computed = _compute_indicator_values(str(ind_type), params)
            columns = computed["columns"]
            values = computed["values"]

            pre_meta = precomputed_cache.get(ind_name)
            if pre_meta and values:
                pre_last = pre_meta.get("latest_non_null_ts")
                pre_vals = pre_meta.get("values", [])
                merged_map: Dict[int, Dict[str, Any]] = {}
                if pre_last is not None:
                    for row in pre_vals:
                        ts = int(row.get("timestamp", 0))
                        if ts <= int(pre_last):
                            merged_map[ts] = {**row, "_source": "precomputed"}
                for row in values:
                    ts = int(row.get("timestamp", 0))
                    if pre_last is not None and ts <= int(pre_last):
                        continue
                    merged_map[ts] = {**row, "_source": "computed"}
                merged_values = [merged_map[ts] for ts in sorted(merged_map.keys())]
                indicator_results.append(
                    {
                        "name": ind_name,
                        "params": params,
                        "columns": columns,
                        "values": merged_values,
                        "source": "mixed",
                    }
                )
            else:
                tagged_values = [{**row, "_source": "computed"} for row in values]
                indicator_results.append(
                    {
                        "name": ind_name,
                        "params": params,
                        "columns": columns,
                        "values": tagged_values,
                        "source": "computed",
                    }
                )
        except Exception as e:
            indicator_results.append({
                "name": ind_name,
                "params": params,
                "columns": [],
                "values": [],
                "error": str(e),
            })

    return {"indicators": indicator_results}


@app.get("/chart-data")
def get_chart_data(
    instrument_token: int,
    timeframe: str,
    start: int = Query(..., description="Start timestamp (epoch seconds)"),
    end: int = Query(..., description="End timestamp (epoch seconds)"),
    limit: int = Query(500, description="Max number of candles/indicator points to return"),
    strategy_name: Optional[str] = Query(None, description="Optional active YAML signal strategy name"),
):
    """
    One-shot chart payload for frontend:
      - candles
      - indicators
    """
    candles_payload = get_candles(
        instrument_token=instrument_token,
        timeframe=timeframe,
        start=start,
        end=end,
        limit=limit,
    )

    try:
        indicators_payload = get_indicators(
            instrument_token=instrument_token,
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
        )
        indicators = indicators_payload.get("indicators", [])
    except HTTPException as exc:
        # If indicators are not yet available, still return candles.
        if exc.status_code in {404, 422}:
            indicators = []
        else:
            raise

    trade_contract = None
    try:
        signals_payload = get_signals(
            instrument_token=instrument_token,
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
            signal_limit=200,
            strategy_name=strategy_name,
        )
        signals = signals_payload.get("signals", [])
        trade_contract = signals_payload.get("trade_contract")
    except HTTPException as exc:
        if exc.status_code in {404, 422}:
            signals = []
            trade_contract = None
        else:
            raise

    return {
        "instrument_token": instrument_token,
        "timeframe": timeframe,
        "start": start,
        "end": end,
        "limit": limit,
        "candles": candles_payload.get("candles", []),
        "indicators": indicators,
        "signals": signals,
        "trade_contract": trade_contract,
    }


@app.get("/signals")
def get_signals(
    instrument_token: int,
    timeframe: str,
    start: int = Query(..., description="Start timestamp (epoch seconds)"),
    end: int = Query(..., description="End timestamp (epoch seconds)"),
    limit: int = Query(500, description="Max number of candles used for signal computation"),
    signal_limit: int = Query(200, description="Max number of generated signal events to return"),
    strategy_name: Optional[str] = Query(None, description="Optional strategy name filter"),
):
    client: Redis = get_redis_client()
    candles_payload = get_candles(
        instrument_token=instrument_token,
        timeframe=timeframe,
        start=start,
        end=end,
        limit=limit,
    )
    candles = candles_payload.get("candles", [])
    if not candles:
        return {"signals": [], "count": 0, "strategies": [], "trade_contract": None}

    df = pd.DataFrame(candles).set_index("timestamp")
    if df.empty:
        return {"signals": [], "count": 0, "strategies": [], "trade_contract": None}

    strategy_params_list = load_rule_strategies_from_config(_root_config_path())
    if strategy_name:
        strategy_params_list = [
            s for s in strategy_params_list if str(s.get("name", "")).strip().lower() == strategy_name.strip().lower()
        ]

    out: List[Dict[str, Any]] = []
    strategy_names: List[str] = []
    strategy_trade_contracts: List[Dict[str, Any]] = []
    for strategy_params in strategy_params_list:
        strategy_label = str(strategy_params.get("name") or "yaml_rule")
        strategy_names.append(strategy_label)
        trade_contract = _resolve_trade_contract_for_strategy(
            client=client,
            monitored_instrument_token=instrument_token,
            strategy_params=strategy_params,
        )
        strategy_trade_contracts.append({"strategy": strategy_label, "trade_contract": trade_contract})
        try:
            generated = generate_rule_signals(
                candles=df,
                strategy_params=strategy_params,
                symbol=str(instrument_token),
                timeframe=timeframe,
            )
            for event in generated:
                event["trade_contract"] = trade_contract
            out.extend(generated)
        except Exception as exc:
            out.append(
                {
                    "symbol": str(instrument_token),
                    "timeframe": timeframe,
                    "strategy": str(strategy_params.get("name") or "yaml_rule"),
                    "action": "ERROR",
                    "timestamp": None,
                    "reason": str(exc),
                    "trade_contract": trade_contract,
                }
            )

    out = [item for item in out if item.get("timestamp") is not None]
    out.sort(key=lambda item: int(item.get("timestamp", 0)))
    if signal_limit > 0:
        out = out[-signal_limit:]

    return {
        "signals": _json_safe(out),
        "count": len(out),
        "strategies": strategy_names,
        "trade_contract": _json_safe(strategy_trade_contracts[0]["trade_contract"]) if strategy_trade_contracts else None,
        "strategy_trade_contracts": _json_safe(strategy_trade_contracts),
    }


@app.get("/signals/validate")
def validate_signals_config(
    strategy_name: Optional[str] = Query(None, description="Optional strategy name filter"),
):
    strategy_params_list = load_rule_strategies_from_config(_root_config_path())
    report = []
    for params in strategy_params_list:
        name = str(params.get("name") or "yaml_rule")
        if strategy_name and name.strip().lower() != strategy_name.strip().lower():
            continue
        errors = validate_strategy_params(params)
        report.append(
            {
                "strategy": name,
                "valid": len(errors) == 0,
                "errors": errors,
            }
        )
    return {
        "count": len(report),
        "valid_count": sum(1 for item in report if item["valid"]),
        "items": report,
    }


@app.get("/signals/catalog")
def signals_catalog():
    strategy_params_list = load_rule_strategies_from_config(_root_config_path())
    items = []
    for params in strategy_params_list:
        name = str(params.get("name") or "yaml_rule")
        errors = validate_strategy_params(params)
        items.append(
            {
                "name": name,
                "valid": len(errors) == 0,
                "errors": errors,
                "trade_target": _strategy_trade_target(params),
            }
        )
    return {
        "count": len(items),
        "items": items,
    }

@app.get("/candles/latest")
def get_latest_common_timestamp(instrument_token: int, timeframe: str):
    """
    Get the latest common timestamp across all OHLC series for an instrument and timeframe.
    """


    client = get_redis_client()

    try:
        result = client.execute_command("TS.MGET", "FILTER",f"type=ohlc",   f"instrument_token={instrument_token}" , f"timeframe={timeframe}" )
        _, _, [ts, _] = result[0]
        return {"timestamp": ts , "epoch" : ts}

    except Exception:
        # One of the fields might not exist
        return {"timestamp": None, "epoch": None}


def _normalize_index_name(raw: str) -> str:
    return "".join(ch for ch in raw.upper().replace(" ", "_") if ch.isalnum() or ch == "_")


def _instrument_record(client: Redis, token: str) -> Dict[str, str]:
    key = f"{UNIVERSE_NS}:instrument:{token}"
    rec = client.hgetall(key)
    if not rec:
        return {}
    return rec


def _sanitize_symbol_key(raw: str) -> str:
    return "".join(ch for ch in str(raw or "").upper() if ch.isalnum())


def _parse_iso_date(raw: Any) -> Optional[date]:
    text = str(raw or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            pass
    return None


def _strategy_trade_target(strategy_params: Dict[str, Any]) -> Dict[str, Any]:
    # Backward/forward compatible strategy-level execution target.
    # Defaults keep existing behavior (trade the monitored/spot instrument).
    trade_target = strategy_params.get("trade_target")
    if not isinstance(trade_target, dict):
        trade_target = {}
    instrument = str(trade_target.get("instrument") or "SPOT").strip().upper()
    if instrument in {"FUTURE", "FUTURES"}:
        instrument = "FUT"
    if instrument not in {"SPOT", "FUT"}:
        instrument = "SPOT"

    preference = str(trade_target.get("future_preference") or trade_target.get("fut_preference") or "next").strip().lower()
    if preference not in {"next", "nearest"}:
        preference = "next"

    exchange = str(trade_target.get("exchange") or "NFO").strip().upper()
    if not exchange:
        exchange = "NFO"

    return {
        "instrument": instrument,
        "future_preference": preference,
        "exchange": exchange,
    }


def _pick_future_for_underlying(
    client: Redis,
    monitored_instrument_token: int,
    preferred_exchange: str = "NFO",
    future_preference: str = "next",
) -> Optional[Dict[str, Any]]:
    monitored = _instrument_record(client, str(monitored_instrument_token))
    if not monitored:
        return None

    underlying_symbol = str(monitored.get("tradingsymbol") or "").strip()
    underlying_name = str(monitored.get("name") or "").strip()
    symbol_key = _sanitize_symbol_key(underlying_symbol)
    name_key = _sanitize_symbol_key(underlying_name)
    candidate_prefixes = {k for k in {symbol_key, name_key, symbol_key.split("50")[0]} if k}
    if not candidate_prefixes:
        return None

    future_tokens: List[str] = []
    candidate_set_groups = [
        [f"{UNIVERSE_NS}:instruments:segment:{preferred_exchange}-FUT"],
        [f"{UNIVERSE_NS}:instruments:exchange:{preferred_exchange}", f"{UNIVERSE_NS}:instruments:type:FUT"],
        [f"{UNIVERSE_NS}:instruments:type:FUT"],
    ]
    for set_keys in candidate_set_groups:
        try:
            if len(set_keys) == 1:
                tokens = list(client.smembers(set_keys[0]))
            else:
                tokens = list(client.execute_command("SINTER", *set_keys))
        except Exception:
            tokens = []
        if tokens:
            future_tokens = tokens
            break
    if not future_tokens:
        return None

    pipe = client.pipeline(transaction=False)
    for tok in future_tokens:
        pipe.hgetall(f"{UNIVERSE_NS}:instrument:{tok}")
    all_futures = [rec for rec in pipe.execute() if rec]

    today = date.today()
    matches: List[Dict[str, Any]] = []
    for rec in all_futures:
        tradingsymbol = str(rec.get("tradingsymbol") or "")
        fut_key = _sanitize_symbol_key(tradingsymbol)
        if not fut_key:
            continue
        if not any(fut_key.startswith(prefix) for prefix in candidate_prefixes):
            continue

        expiry = _parse_iso_date(rec.get("expiry"))
        if expiry is None:
            continue
        if expiry < today:
            continue
        rec_copy = dict(rec)
        rec_copy["_expiry_date"] = expiry
        matches.append(rec_copy)

    if not matches:
        return None

    matches.sort(key=lambda row: (row["_expiry_date"], int(float(row.get("instrument_token", "0") or 0))))
    if future_preference == "next" and len(matches) > 1:
        chosen = matches[1]
    else:
        chosen = matches[0]

    return {
        "instrument_token": chosen.get("instrument_token"),
        "tradingsymbol": chosen.get("tradingsymbol"),
        "exchange": chosen.get("exchange"),
        "segment": chosen.get("segment"),
        "instrument_type": chosen.get("instrument_type"),
        "expiry": chosen.get("expiry"),
        "lot_size": chosen.get("lot_size"),
    }


def _resolve_trade_contract_for_strategy(
    client: Redis,
    monitored_instrument_token: int,
    strategy_params: Dict[str, Any],
) -> Dict[str, Any]:
    trade_target = _strategy_trade_target(strategy_params)
    if trade_target["instrument"] != "FUT":
        monitored = _instrument_record(client, str(monitored_instrument_token))
        return {
            "mode": "SPOT",
            "instrument_token": str(monitored_instrument_token),
            "tradingsymbol": monitored.get("tradingsymbol") if monitored else None,
            "exchange": monitored.get("exchange") if monitored else None,
            "source": "monitored_instrument",
        }

    fut = _pick_future_for_underlying(
        client=client,
        monitored_instrument_token=monitored_instrument_token,
        preferred_exchange=trade_target["exchange"],
        future_preference=trade_target["future_preference"],
    )
    if fut:
        return {
            "mode": "FUT",
            "future_preference": trade_target["future_preference"],
            "instrument_token": fut.get("instrument_token"),
            "tradingsymbol": fut.get("tradingsymbol"),
            "exchange": fut.get("exchange"),
            "segment": fut.get("segment"),
            "instrument_type": fut.get("instrument_type"),
            "expiry": fut.get("expiry"),
            "lot_size": fut.get("lot_size"),
            "source": "resolved_from_universe",
        }

    monitored = _instrument_record(client, str(monitored_instrument_token))
    return {
        "mode": "FUT",
        "future_preference": trade_target["future_preference"],
        "instrument_token": None,
        "tradingsymbol": None,
        "exchange": trade_target["exchange"],
        "source": "unresolved",
        "fallback_mode": "SPOT",
        "fallback_instrument_token": str(monitored_instrument_token),
        "fallback_tradingsymbol": monitored.get("tradingsymbol") if monitored else None,
    }


@app.get("/universe/meta")
def get_universe_meta():
    client: Redis = get_redis_client()
    meta = client.hgetall(f"{UNIVERSE_NS}:meta")
    return {"namespace": UNIVERSE_NS, "meta": meta}


@app.get("/universe/indexes")
def get_universe_indexes():
    client: Redis = get_redis_client()
    indexes = sorted(client.smembers(f"{UNIVERSE_NS}:indexes"))
    out = []
    for idx in indexes:
        meta = client.hgetall(f"{UNIVERSE_NS}:index:{idx}:meta")
        out.append(meta if meta else {"index_name": idx})
    return {"indexes": out}


@app.get("/universe/index/{index_name}/constituents")
def get_index_constituents(index_name: str, with_instruments: bool = True):
    client: Redis = get_redis_client()
    idx = _normalize_index_name(index_name)
    symbols = sorted(client.smembers(f"{UNIVERSE_NS}:index:{idx}:symbols"))
    tokens = sorted(client.smembers(f"{UNIVERSE_NS}:index:{idx}:tokens"), key=lambda x: int(float(x)))
    weight_pct = client.hgetall(f"{UNIVERSE_NS}:index:{idx}:weight_pct")
    ffmc = client.hgetall(f"{UNIVERSE_NS}:index:{idx}:ffmc")
    last_price = client.hgetall(f"{UNIVERSE_NS}:index:{idx}:last_price")
    pchange = client.hgetall(f"{UNIVERSE_NS}:index:{idx}:pchange")

    instruments = []
    instrument_by_token = {}
    if with_instruments:
        pipe = client.pipeline(transaction=False)
        for token in tokens:
            pipe.hgetall(f"{UNIVERSE_NS}:instrument:{token}")
        for rec in pipe.execute():
            if rec:
                instruments.append(rec)
                tok = str(rec.get("instrument_token", ""))
                if tok:
                    instrument_by_token[tok] = rec

    pipe = client.pipeline(transaction=False)
    for symbol in symbols:
        pipe.get(f"{UNIVERSE_NS}:symbol:NSE:{symbol}")
    symbol_tokens = pipe.execute()
    symbol_to_token = {s: (t or "") for s, t in zip(symbols, symbol_tokens)}

    constituents = []
    for symbol in symbols:
        token = symbol_to_token.get(symbol, "")
        w = weight_pct.get(symbol)
        rec = {
            "symbol": symbol,
            "instrument_token": token or None,
            "weight_pct": float(w) if w not in (None, "") else None,
            "ffmc": float(ffmc[symbol]) if symbol in ffmc and ffmc[symbol] not in ("", None) else None,
            "last_price": float(last_price[symbol]) if symbol in last_price and last_price[symbol] not in ("", None) else None,
            "pchange": float(pchange[symbol]) if symbol in pchange and pchange[symbol] not in ("", None) else None,
        }
        if with_instruments and token and token in instrument_by_token:
            rec["instrument"] = instrument_by_token[token]
        constituents.append(rec)

    meta = client.hgetall(f"{UNIVERSE_NS}:index:{idx}:meta")
    return {
        "index_name": idx,
        "meta": meta,
        "symbols": symbols,
        "instrument_tokens": tokens,
        "instruments": instruments,
        "constituents": constituents,
    }


@app.get("/universe/instruments")
def get_universe_instruments(
    exchange: Optional[str] = None,
    instrument_type: Optional[str] = None,
    segment: Optional[str] = None,
    index_name: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = Query(200, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    client: Redis = get_redis_client()
    set_keys = [f"{UNIVERSE_NS}:instruments:all"]

    if exchange:
        set_keys.append(f"{UNIVERSE_NS}:instruments:exchange:{exchange.upper()}")
    if instrument_type:
        set_keys.append(f"{UNIVERSE_NS}:instruments:type:{instrument_type.upper()}")
    if segment:
        set_keys.append(f"{UNIVERSE_NS}:instruments:segment:{segment.upper()}")
    if index_name:
        idx = _normalize_index_name(index_name)
        set_keys.append(f"{UNIVERSE_NS}:index:{idx}:tokens")

    if len(set_keys) == 1:
        tokens = list(client.smembers(set_keys[0]))
    else:
        tokens = list(client.execute_command("SINTER", *set_keys))

    tokens = sorted(tokens, key=lambda x: int(float(x)))
    total_before_search = len(tokens)

    records = []
    if search:
        q = search.strip().upper()
        pipe = client.pipeline(transaction=False)
        for token in tokens:
            pipe.hgetall(f"{UNIVERSE_NS}:instrument:{token}")
        for rec in pipe.execute():
            if not rec:
                continue
            sym = rec.get("tradingsymbol", "").upper()
            name = rec.get("name", "").upper()
            if q in sym or q in name:
                records.append(rec)
    else:
        paged = tokens[offset: offset + limit]
        pipe = client.pipeline(transaction=False)
        for token in paged:
            pipe.hgetall(f"{UNIVERSE_NS}:instrument:{token}")
        records = [r for r in pipe.execute() if r]
        return {
            "count": len(records),
            "total": total_before_search,
            "offset": offset,
            "limit": limit,
            "instruments": records,
        }

    total_after_search = len(records)
    paged_records = records[offset: offset + limit]
    return {
        "count": len(paged_records),
        "total": total_after_search,
        "offset": offset,
        "limit": limit,
        "instruments": paged_records,
    }


def _discover_user_ids(client: Redis) -> List[str]:
    user_ids = set()
    cursor = 0
    pattern = "user:*:*"
    while True:
        cursor, keys = client.scan(cursor=cursor, match=pattern, count=1000)
        for key in keys:
            parts = key.split(":")
            if len(parts) >= 2 and parts[0] == "user" and parts[1]:
                user_ids.add(parts[1])
        if cursor == 0:
            break
    return sorted(user_ids)


def _load_user_order_book(client: Redis, user_id: str):
    raw = client.get(f"user:{user_id}:orders")
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed
        return [parsed]
    except Exception:
        return []


def _load_user_positions(client: Redis, user_id: str):
    out = {"net": [], "day": []}
    raw_net = client.get(f"user:{user_id}:position:net")
    raw_day = client.get(f"user:{user_id}:position:day")

    for bucket, raw in (("net", raw_net), ("day", raw_day)):
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                out[bucket] = parsed
            elif isinstance(parsed, dict):
                out[bucket] = [parsed]
        except Exception:
            out[bucket] = []

    return out


def _service_order_sync_status(client: Redis):
    users = _discover_user_ids(client)
    per_user = []
    for user_id in users:
        per_user.append(
            {
                "user_id": user_id,
                "last_success_epoch_ms": client.get(f"service:order_sync:user:{user_id}:last_success_epoch_ms"),
                "last_orders_count": client.get(f"service:order_sync:user:{user_id}:last_orders_count"),
                "last_positions_net_count": client.get(f"service:order_sync:user:{user_id}:last_positions_net_count"),
                "last_positions_day_count": client.get(f"service:order_sync:user:{user_id}:last_positions_day_count"),
            }
        )

    return {
        "last_success_epoch_ms": client.get("service:order_sync:last_success_epoch_ms"),
        "last_error": client.get("service:order_sync:last_error"),
        "users": per_user,
    }


def _paginate(items: List[str], offset: int, limit: int) -> List[str]:
    return items[offset: offset + limit]


@app.get("/accounts/sync-status")
def get_accounts_sync_status():
    """
    Observability endpoint for broker->Redis order sync service.
    """
    client: Redis = get_redis_client()
    return _service_order_sync_status(client)


@app.get("/accounts/users")
def list_users(
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """
    List all user IDs discovered from Redis user:* keys (paginated).
    """
    client: Redis = get_redis_client()
    users = _discover_user_ids(client)
    page = _paginate(users, offset, limit)
    return {
        "count": len(page),
        "total": len(users),
        "offset": offset,
        "limit": limit,
        "users": page,
        "sync_status": _service_order_sync_status(client),
    }


@app.get("/accounts/order-books")
def list_order_books_all_users(
    limit: int = Query(100, ge=1, le=2000),
    offset: int = Query(0, ge=0),
):
    """
    List order books of all users at once (paginated by users).
    """
    client: Redis = get_redis_client()
    users = _discover_user_ids(client)
    page_users = _paginate(users, offset, limit)
    data = []
    for user_id in page_users:
        data.append(
            {
                "user_id": user_id,
                "order_book": _load_user_order_book(client, user_id),
                "positions": _load_user_positions(client, user_id),
            }
        )
    return {
        "count": len(data),
        "total": len(users),
        "offset": offset,
        "limit": limit,
        "items": data,
        "sync_status": _service_order_sync_status(client),
    }


@app.get("/accounts/users-with-order-books")
def list_users_with_order_books(
    limit: int = Query(100, ge=1, le=2000),
    offset: int = Query(0, ge=0),
):
    """
    List users and their order books together (paginated by users).
    """
    client: Redis = get_redis_client()
    users = _discover_user_ids(client)
    page_users = _paginate(users, offset, limit)
    data = []
    for user_id in page_users:
        data.append(
            {
                "user_id": user_id,
                "order_book": _load_user_order_book(client, user_id),
                "positions": _load_user_positions(client, user_id),
            }
        )
    return {
        "count": len(data),
        "total": len(users),
        "offset": offset,
        "limit": limit,
        "items": data,
        "sync_status": _service_order_sync_status(client),
    }


@app.get("/accounts/users-with-order-books/bulk")
def bulk_users_with_order_books():
    """
    Return all users and their order books in one response (non-paginated bulk).
    """
    client: Redis = get_redis_client()
    users = _discover_user_ids(client)
    data = []
    for user_id in users:
        data.append(
            {
                "user_id": user_id,
                "order_book": _load_user_order_book(client, user_id),
                "positions": _load_user_positions(client, user_id),
            }
        )
    return {
        "count": len(data),
        "items": data,
        "sync_status": _service_order_sync_status(client),
    }
