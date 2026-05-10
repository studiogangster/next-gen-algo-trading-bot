from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import numbers
import math
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import yaml

from core.indicator_logic import compute_indicator

IST = ZoneInfo("Asia/Kolkata")


def load_rule_strategies_from_config(config_path: str) -> List[Dict[str, Any]]:
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f) or {}
    except Exception:
        return []

    strategies = config.get("strategies", [])
    out: List[Dict[str, Any]] = []
    for strategy in strategies:
        if not isinstance(strategy, dict):
            continue
        strategy_type = str(strategy.get("type", "")).strip().lower()
        if strategy_type not in {"yaml_rule", "rule_based", "yaml_signal"}:
            continue
        params = strategy.get("params", {})
        if isinstance(params, dict):
            out.append(params)
    return out


def _to_epoch_seconds(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, numbers.Real):
        raw = int(value)
        return raw // 1000 if raw > 10**12 else raw
    try:
        ts = pd.Timestamp(value)
        return int(ts.timestamp())
    except Exception:
        return None


def _normalize_indicator_result(result: Any) -> Dict[str, pd.Series]:
    if result is None:
        return {}

    if isinstance(result, pd.Series):
        return {"value": result}

    if isinstance(result, pd.DataFrame):
        out: Dict[str, pd.Series] = {}
        for col in result.columns:
            out[str(col).lower()] = result[col]
        return out

    return {}


def _build_indicator_series(df: pd.DataFrame, indicator_specs: List[Dict[str, Any]]) -> Dict[str, Dict[str, pd.Series]]:
    out: Dict[str, Dict[str, pd.Series]] = {}
    for spec in indicator_specs:
        if not isinstance(spec, dict):
            continue
        indicator_id = str(spec.get("id") or spec.get("name") or spec.get("type") or "").strip().lower()
        indicator_type = str(spec.get("type") or indicator_id).strip().lower()
        params = spec.get("params", {})
        if not indicator_id or not indicator_type:
            continue
        if indicator_id in out:
            continue
        try:
            result = compute_indicator(df, indicator_type, params if isinstance(params, dict) else {})
            out[indicator_id] = _normalize_indicator_result(result)
        except Exception:
            out[indicator_id] = {}
    return out


def _set_context_value(context: Dict[str, Any], key: str, value: Any):
    context[key] = value


def _build_context(df: pd.DataFrame, idx: int, indicator_values: Dict[str, Dict[str, pd.Series]]) -> Dict[str, Any]:
    context: Dict[str, Any] = {}
    curr = df.iloc[idx]
    prev = df.iloc[idx - 1] if idx > 0 else None

    for field in ("open", "high", "low", "close", "volume"):
        _set_context_value(context, field, curr.get(field) if field in curr else None)
        _set_context_value(context, f"curr.{field}", curr.get(field) if field in curr else None)
        _set_context_value(context, f"prev.{field}", prev.get(field) if prev is not None and field in prev else None)

    for indicator_id, columns in indicator_values.items():
        for col_name, series in columns.items():
            current_value = series.iloc[idx] if len(series) > idx else None
            prev_value = series.iloc[idx - 1] if idx > 0 and len(series) > (idx - 1) else None
            _set_context_value(context, f"{indicator_id}.{col_name}", current_value)
            _set_context_value(context, f"curr.{indicator_id}.{col_name}", current_value)
            _set_context_value(context, f"prev.{indicator_id}.{col_name}", prev_value)
        if len(columns) == 1:
            only_col = next(iter(columns.keys()))
            _set_context_value(context, indicator_id, context.get(f"{indicator_id}.{only_col}"))
            _set_context_value(context, f"curr.{indicator_id}", context.get(f"curr.{indicator_id}.{only_col}"))
            _set_context_value(context, f"prev.{indicator_id}", context.get(f"prev.{indicator_id}.{only_col}"))
        if "value" in columns:
            _set_context_value(context, indicator_id, context.get(f"{indicator_id}.value"))
            _set_context_value(context, f"curr.{indicator_id}", context.get(f"curr.{indicator_id}.value"))
            _set_context_value(context, f"prev.{indicator_id}", context.get(f"prev.{indicator_id}.value"))

    return context


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"true", "yes", "1", "buy", "sell"}:
            return True
        if v in {"false", "no", "0", "", "none", "null"}:
            return False
    return bool(value)


def _resolve_operand(value: Any, context: Dict[str, Any]) -> Any:
    if isinstance(value, (int, float, bool)) or value is None:
        return value

    if isinstance(value, dict):
        if "ref" in value:
            return _resolve_operand(value.get("ref"), context)
        if "value" in value:
            return value.get("value")
        return None

    if isinstance(value, str):
        key = value.strip()
        if key in context:
            return context.get(key)
        low = key.lower()
        if low == "true":
            return True
        if low == "false":
            return False
        try:
            return float(key)
        except Exception:
            return None

    return None


def _resolve_current_prev(operand: Any, context: Dict[str, Any]) -> Tuple[Any, Any]:
    if isinstance(operand, str):
        key = operand.strip()
        if key.startswith("prev."):
            prev = _resolve_operand(key, context)
            curr_key = key.replace("prev.", "", 1)
            curr = _resolve_operand(curr_key, context)
            return curr, prev
        curr = _resolve_operand(key, context)
        prev = _resolve_operand(f"prev.{key}", context)
        return curr, prev
    curr = _resolve_operand(operand, context)
    return curr, curr


def _safe_num(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except Exception:
        return None
    if pd.isna(number):
        return None
    return number


def evaluate_rule_expr(expr: Any, context: Dict[str, Any]) -> bool:
    if expr is None:
        return False

    if isinstance(expr, list):
        return all(evaluate_rule_expr(item, context) for item in expr)

    if not isinstance(expr, dict):
        return _coerce_bool(_resolve_operand(expr, context))

    if "all" in expr:
        items = expr.get("all") or []
        return all(evaluate_rule_expr(item, context) for item in items)
    if "any" in expr:
        items = expr.get("any") or []
        return any(evaluate_rule_expr(item, context) for item in items)
    if "not" in expr:
        return not evaluate_rule_expr(expr.get("not"), context)

    if "between" in expr:
        parts = expr.get("between") or []
        if len(parts) != 3:
            return False
        value = _safe_num(_resolve_operand(parts[0], context))
        low = _safe_num(_resolve_operand(parts[1], context))
        high = _safe_num(_resolve_operand(parts[2], context))
        if value is None or low is None or high is None:
            return False
        return low <= value <= high

    if "cross_over" in expr or "cross_under" in expr:
        key = "cross_over" if "cross_over" in expr else "cross_under"
        parts = expr.get(key) or []
        if len(parts) != 2:
            return False
        left_curr, left_prev = _resolve_current_prev(parts[0], context)
        right_curr, right_prev = _resolve_current_prev(parts[1], context)
        lc, lp = _safe_num(left_curr), _safe_num(left_prev)
        rc, rp = _safe_num(right_curr), _safe_num(right_prev)
        if lc is None or lp is None or rc is None or rp is None:
            return False
        if key == "cross_over":
            return lp <= rp and lc > rc
        return lp >= rp and lc < rc

    comparator_map = {
        "gt": lambda a, b: a > b,
        "gte": lambda a, b: a >= b,
        "lt": lambda a, b: a < b,
        "lte": lambda a, b: a <= b,
        "eq": lambda a, b: a == b,
        "ne": lambda a, b: a != b,
    }

    for comp_key, op in comparator_map.items():
        if comp_key not in expr:
            continue
        parts = expr.get(comp_key) or []
        if len(parts) != 2:
            return False
        left_raw = _resolve_operand(parts[0], context)
        right_raw = _resolve_operand(parts[1], context)
        left_num = _safe_num(left_raw)
        right_num = _safe_num(right_raw)
        if left_num is not None and right_num is not None:
            return op(left_num, right_num)
        if left_raw is None or right_raw is None:
            return False
        return op(left_raw, right_raw)

    return False


def _extract_indicator_snapshot(context: Dict[str, Any], indicator_ids: List[str]) -> Dict[str, Any]:
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

    out: Dict[str, Any] = {}
    for indicator_id in indicator_ids:
        keys = sorted(k for k in context.keys() if k.startswith(f"{indicator_id}.") and not k.startswith("prev."))
        indicator_payload = {k.split(".", 1)[1]: _json_safe(context.get(k)) for k in keys}
        if indicator_payload:
            out[indicator_id] = indicator_payload
    return out


def generate_rule_signals(
    candles: pd.DataFrame,
    strategy_params: Dict[str, Any],
    symbol: Optional[str] = None,
    timeframe: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if candles is None or candles.empty:
        return []

    strategy_name = str(strategy_params.get("name") or strategy_params.get("strategy_name") or "yaml_rule").strip()
    indicator_specs = strategy_params.get("indicators", [])
    buy_expr = strategy_params.get("buy") or strategy_params.get("entry_buy")
    sell_expr = strategy_params.get("sell") or strategy_params.get("entry_sell")
    quantity = float(strategy_params.get("quantity", 1))
    dedupe = bool(strategy_params.get("dedupe_consecutive", True))
    cooldown_bars = max(0, int(strategy_params.get("cooldown_bars", 0) or 0))
    reset_on_new_day = bool(strategy_params.get("reset_on_new_day", True))
    emit_day_end_exit = bool(strategy_params.get("emit_day_end_exit", False))

    if not buy_expr and not sell_expr:
        return []

    indicators = _build_indicator_series(candles, indicator_specs if isinstance(indicator_specs, list) else [])
    indicator_ids = [str((spec or {}).get("id") or (spec or {}).get("type") or "").strip().lower() for spec in indicator_specs if isinstance(spec, dict)]
    indicator_ids = [x for x in indicator_ids if x]

    last_action = None
    last_signal_index = None
    last_day_key: Optional[str] = None
    events: List[Dict[str, Any]] = []
    for idx in range(len(candles)):
        epoch = _to_epoch_seconds(candles.index[idx])
        if epoch is not None:
            day_key = datetime.fromtimestamp(int(epoch), IST).strftime("%Y-%m-%d")
        else:
            day_key = None

        if reset_on_new_day and last_day_key is not None and day_key is not None and day_key != last_day_key:
            if emit_day_end_exit and last_action in {"BUY", "SELL"}:
                prev_idx = max(0, idx - 1)
                prev_epoch = _to_epoch_seconds(candles.index[prev_idx])
                prev_context = _build_context(candles, prev_idx, indicators)
                prev_price = _safe_num(prev_context.get("close"))
                if prev_epoch is not None:
                    events.append(
                        {
                            "symbol": str(symbol) if symbol is not None else None,
                            "timeframe": timeframe,
                            "strategy": strategy_name,
                            "action": "EXIT",
                            "timestamp": int(prev_epoch),
                            "epoch": int(prev_epoch),
                            "price": prev_price,
                            "quantity": quantity,
                            "reason": "session_day_reset",
                            "exit_of_action": last_action,
                            "indicators": _extract_indicator_snapshot(prev_context, indicator_ids),
                        }
                    )
            last_action = None
            last_signal_index = None

        if day_key is not None:
            last_day_key = day_key

        if cooldown_bars > 0 and last_signal_index is not None and (idx - last_signal_index) <= cooldown_bars:
            continue

        context = _build_context(candles, idx, indicators)

        buy_hit = evaluate_rule_expr(buy_expr, context) if buy_expr is not None else False
        sell_hit = evaluate_rule_expr(sell_expr, context) if sell_expr is not None else False

        if buy_hit and sell_hit:
            continue
        if not buy_hit and not sell_hit:
            continue

        action = "BUY" if buy_hit else "SELL"
        if dedupe and action == last_action:
            continue

        if epoch is None:
            continue
        price = _safe_num(context.get("close"))

        events.append(
            {
                "symbol": str(symbol) if symbol is not None else None,
                "timeframe": timeframe,
                "strategy": strategy_name,
                "action": action,
                "timestamp": int(epoch),
                "epoch": int(epoch),
                "price": price,
                "quantity": quantity,
                "reason": "buy_rule_match" if action == "BUY" else "sell_rule_match",
                "indicators": _extract_indicator_snapshot(context, indicator_ids),
            }
        )
        last_action = action
        last_signal_index = idx

    return events


def latest_rule_signal(candles: pd.DataFrame, strategy_params: Dict[str, Any], symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
    events = generate_rule_signals(candles, strategy_params, symbol=symbol, timeframe=timeframe)
    if not events:
        return None
    return events[-1]


def validate_strategy_params(strategy_params: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    if not isinstance(strategy_params, dict):
        return ["strategy params must be a mapping/object"]

    name = strategy_params.get("name")
    if name is None or str(name).strip() == "":
        errors.append("params.name is required")

    if strategy_params.get("buy") is None and strategy_params.get("entry_buy") is None:
        errors.append("buy rule is required (params.buy)")
    if strategy_params.get("sell") is None and strategy_params.get("entry_sell") is None:
        errors.append("sell rule is required (params.sell)")

    indicators = strategy_params.get("indicators", [])
    if indicators is not None and not isinstance(indicators, list):
        errors.append("params.indicators must be a list")
    elif isinstance(indicators, list):
        ids = set()
        for idx, spec in enumerate(indicators):
            if not isinstance(spec, dict):
                errors.append(f"params.indicators[{idx}] must be an object")
                continue
            indicator_id = str(spec.get("id") or spec.get("type") or "").strip().lower()
            indicator_type = str(spec.get("type") or "").strip().lower()
            if not indicator_id:
                errors.append(f"params.indicators[{idx}].id is required")
            if not indicator_type:
                errors.append(f"params.indicators[{idx}].type is required")
            if indicator_id in ids:
                errors.append(f"duplicate indicator id: {indicator_id}")
            ids.add(indicator_id)

    try:
        quantity = float(strategy_params.get("quantity", 1))
        if quantity <= 0:
            errors.append("params.quantity must be > 0")
    except Exception:
        errors.append("params.quantity must be numeric")

    try:
        cooldown = int(strategy_params.get("cooldown_bars", 0) or 0)
        if cooldown < 0:
            errors.append("params.cooldown_bars must be >= 0")
    except Exception:
        errors.append("params.cooldown_bars must be an integer")

    return errors
