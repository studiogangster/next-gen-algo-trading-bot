import hashlib
import json
import os
import time
import traceback
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Type

from brokers.kite_trade import ZerodhaBroker
from brokers.utils import login
from storage.redis_client import get_redis_client

SERVICE_NS = "service:auto_execution"

PENDING_ORDER_STATUSES = {
    "OPEN",
    "OPEN PENDING",
    "MODIFY PENDING",
    "TRIGGER PENDING",
    "AMO REQ RECEIVED",
    "VALIDATION PENDING",
    "PUT ORDER REQ RECEIVED",
}

SUCCESS_ORDER_STATUSES = {"COMPLETE"}
FAILED_ORDER_STATUSES = {"REJECTED", "CANCELLED"}
TERMINAL_ORDER_STATUSES = SUCCESS_ORDER_STATUSES | FAILED_ORDER_STATUSES


@dataclass
class TransactionOutcome:
    state: str
    reason: str
    release_lock: bool


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_json_loads(raw: str, default: Any) -> Any:
    try:
        return json.loads(raw)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def _derive_idempotency_key(payload: Dict[str, Any], user_id: str) -> str:
    material = {
        "user_id": user_id,
        "approval_id": payload.get("approval_id"),
        "strategy_id": payload.get("strategy_id"),
        "signal_id": payload.get("signal_id"),
        "signal_ts": payload.get("signal_ts") or payload.get("timestamp"),
        "orders": payload.get("orders"),
        "side": payload.get("side") or payload.get("action"),
        "symbol": payload.get("tradingsymbol") or payload.get("symbol"),
        "exchange": payload.get("exchange"),
        "quantity": payload.get("quantity"),
    }
    blob = json.dumps(material, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _normalize_orders(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    orders = payload.get("orders")
    if isinstance(orders, list) and orders:
        normalized: List[Dict[str, Any]] = []
        for item in orders:
            if not isinstance(item, dict):
                continue
            side = str(item.get("side") or item.get("action") or item.get("transaction_type") or "").strip().upper()
            tx_type = side if side in {"BUY", "SELL"} else str(item.get("transaction_type") or "").strip().upper()
            if tx_type not in {"BUY", "SELL"}:
                continue
            normalized.append(
                {
                    "variety": str(item.get("variety") or ZerodhaBroker.VARIETY_REGULAR),
                    "exchange": str(item.get("exchange") or ZerodhaBroker.EXCHANGE_NSE),
                    "tradingsymbol": str(item.get("tradingsymbol") or item.get("symbol") or "").strip(),
                    "transaction_type": tx_type,
                    "quantity": _safe_int(item.get("quantity"), 0),
                    "product": str(item.get("product") or ZerodhaBroker.PRODUCT_MIS),
                    "order_type": str(item.get("order_type") or ZerodhaBroker.ORDER_TYPE_MARKET),
                    "price": item.get("price"),
                    "validity": item.get("validity"),
                    "trigger_price": item.get("trigger_price"),
                    "disclosed_quantity": item.get("disclosed_quantity"),
                    "parent_order_id": item.get("parent_order_id"),
                }
            )
        return [o for o in normalized if o["tradingsymbol"] and o["quantity"] > 0]

    side = str(payload.get("side") or payload.get("action") or payload.get("transaction_type") or "").strip().upper()
    tx_type = side if side in {"BUY", "SELL"} else ""
    fallback = {
        "variety": str(payload.get("variety") or ZerodhaBroker.VARIETY_REGULAR),
        "exchange": str(payload.get("exchange") or ZerodhaBroker.EXCHANGE_NSE),
        "tradingsymbol": str(payload.get("tradingsymbol") or payload.get("symbol") or "").strip(),
        "transaction_type": tx_type,
        "quantity": _safe_int(payload.get("quantity"), 0),
        "product": str(payload.get("product") or ZerodhaBroker.PRODUCT_MIS),
        "order_type": str(payload.get("order_type") or ZerodhaBroker.ORDER_TYPE_MARKET),
        "price": payload.get("price"),
        "validity": payload.get("validity"),
        "trigger_price": payload.get("trigger_price"),
        "disclosed_quantity": payload.get("disclosed_quantity"),
        "parent_order_id": payload.get("parent_order_id"),
    }
    if fallback["tradingsymbol"] and fallback["quantity"] > 0 and fallback["transaction_type"] in {"BUY", "SELL"}:
        return [fallback]
    return []


def _fetch_cached_orders(redis_client: Any, user_id: str) -> List[Dict[str, Any]]:
    raw = redis_client.get(f"user:{user_id}:orders")
    if not raw:
        return []
    parsed = _safe_json_loads(raw, [])
    return parsed if isinstance(parsed, list) else []


def _fetch_cached_net_positions(redis_client: Any, user_id: str) -> List[Dict[str, Any]]:
    raw = redis_client.get(f"user:{user_id}:position:net")
    if not raw:
        return []
    parsed = _safe_json_loads(raw, [])
    return parsed if isinstance(parsed, list) else []


def _positions_map(positions: List[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for row in positions:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("tradingsymbol") or "").strip()
        if not symbol:
            continue
        out[symbol] = _safe_int(row.get("quantity"), 0)
    return out


def _expected_position_delta(orders: List[Dict[str, Any]]) -> Dict[str, int]:
    delta: Dict[str, int] = {}
    for order in orders:
        symbol = order.get("tradingsymbol")
        qty = _safe_int(order.get("quantity"), 0)
        side = str(order.get("transaction_type") or "").strip().upper()
        if not symbol or qty <= 0 or side not in {"BUY", "SELL"}:
            continue
        sign = 1 if side == "BUY" else -1
        delta[symbol] = delta.get(symbol, 0) + (sign * qty)
    return delta


def _build_latest_order_map(orders: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    # Cache payload usually includes latest snapshot per order_id.
    # If duplicates exist, last one in list wins.
    latest: Dict[str, Dict[str, Any]] = {}
    for row in orders:
        if not isinstance(row, dict):
            continue
        order_id = str(row.get("order_id") or "").strip()
        if not order_id:
            continue
        latest[order_id] = row
    return latest


def _order_status(row: Optional[Dict[str, Any]]) -> str:
    if not isinstance(row, dict):
        return "MISSING"
    return str(row.get("status") or "").strip().upper() or "UNKNOWN"


def _acquire_user_lock(redis_client: Any, user_id: str, token: str, ttl_sec: int) -> bool:
    key = f"exec:txn_lock:{user_id}"
    result = redis_client.set(key, token, nx=True, ex=ttl_sec)
    return bool(result)


def _renew_user_lock(redis_client: Any, user_id: str, token: str, ttl_sec: int) -> bool:
    script = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('EXPIRE', KEYS[1], ARGV[2])
else
  return 0
end
"""
    key = f"exec:txn_lock:{user_id}"
    return bool(redis_client.eval(script, 1, key, token, ttl_sec))


def _release_user_lock(redis_client: Any, user_id: str, token: str) -> bool:
    script = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end
"""
    key = f"exec:txn_lock:{user_id}"
    return bool(redis_client.eval(script, 1, key, token))


def _reserve_idempotency(redis_client: Any, idempotency_key: str, txn_id: str, ttl_sec: int) -> bool:
    key = f"exec:idemp:{idempotency_key}"
    return bool(redis_client.set(key, txn_id, nx=True, ex=ttl_sec))


def _set_txn_state(redis_client: Any, txn_id: str, payload: Dict[str, Any]) -> None:
    redis_client.set(f"exec:txn:{txn_id}", json.dumps(payload, sort_keys=True))


def _parse_signal_message(raw: str) -> Optional[Dict[str, Any]]:
    msg = _safe_json_loads(raw, None)
    return msg if isinstance(msg, dict) else None


def _set_service_key(redis_client: Any, key: str, value: str) -> None:
    redis_client.execute_command("SET", key, value)


def _wait_for_cache_terminal(
    redis_client: Any,
    user_id: str,
    tracked_order_ids: List[str],
    timeout_sec: float,
    lock_token: str,
    lock_ttl_sec: int,
) -> Tuple[bool, str, Dict[str, Dict[str, Any]]]:
    start = time.time()
    while (time.time() - start) <= timeout_sec:
        if not _renew_user_lock(redis_client, user_id, lock_token, lock_ttl_sec):
            return False, "lock_lost", {}

        cached = _fetch_cached_orders(redis_client, user_id)
        latest = _build_latest_order_map(cached)

        statuses = [_order_status(latest.get(oid)) for oid in tracked_order_ids]
        if statuses and all(s in SUCCESS_ORDER_STATUSES for s in statuses):
            return True, "all_complete", latest

        if any(s == "REJECTED" for s in statuses):
            return False, "rejected", latest

        time.sleep(0.25)

    cached = _fetch_cached_orders(redis_client, user_id)
    latest = _build_latest_order_map(cached)
    statuses = [_order_status(latest.get(oid)) for oid in tracked_order_ids]

    if statuses and all(s in SUCCESS_ORDER_STATUSES for s in statuses):
        return True, "all_complete", latest
    return False, "timeout_or_partial", latest


def _cancel_pending_for_transaction(
    broker: ZerodhaBroker,
    latest_order_map: Dict[str, Dict[str, Any]],
    tracked_order_ids: List[str],
) -> Tuple[int, int]:
    cancelled = 0
    failed = 0
    for oid in tracked_order_ids:
        row = latest_order_map.get(oid) or {}
        status = _order_status(row)
        if status in TERMINAL_ORDER_STATUSES:
            continue

        variety = str(row.get("variety") or ZerodhaBroker.VARIETY_REGULAR)
        parent_order_id = row.get("parent_order_id")
        try:
            broker.cancel_order(variety=variety, order_id=oid, parent_order_id=parent_order_id)
            cancelled += 1
        except Exception:
            failed += 1
    return cancelled, failed


def _wait_for_cancel_confirmation(
    redis_client: Any,
    user_id: str,
    tracked_order_ids: List[str],
    timeout_sec: float,
    lock_token: str,
    lock_ttl_sec: int,
) -> Tuple[bool, Dict[str, Dict[str, Any]]]:
    start = time.time()
    while (time.time() - start) <= timeout_sec:
        if not _renew_user_lock(redis_client, user_id, lock_token, lock_ttl_sec):
            return False, {}

        cached = _fetch_cached_orders(redis_client, user_id)
        latest = _build_latest_order_map(cached)
        statuses = [_order_status(latest.get(oid)) for oid in tracked_order_ids]

        if statuses and all(s in TERMINAL_ORDER_STATUSES for s in statuses):
            return True, latest

        time.sleep(0.25)

    cached = _fetch_cached_orders(redis_client, user_id)
    latest = _build_latest_order_map(cached)
    statuses = [_order_status(latest.get(oid)) for oid in tracked_order_ids]
    return bool(statuses and all(s in TERMINAL_ORDER_STATUSES for s in statuses)), latest


def _wait_for_expected_position(
    redis_client: Any,
    user_id: str,
    baseline: Dict[str, int],
    delta: Dict[str, int],
    timeout_sec: float,
    lock_token: str,
    lock_ttl_sec: int,
) -> bool:
    expected = {symbol: baseline.get(symbol, 0) + qty for symbol, qty in delta.items()}
    start = time.time()
    while (time.time() - start) <= timeout_sec:
        if not _renew_user_lock(redis_client, user_id, lock_token, lock_ttl_sec):
            return False

        current = _positions_map(_fetch_cached_net_positions(redis_client, user_id))
        ok = True
        for symbol, target_qty in expected.items():
            if current.get(symbol, 0) != target_qty:
                ok = False
                break
        if ok:
            return True
        time.sleep(0.25)
    return False


def _process_transaction(
    redis_client: Any,
    broker: ZerodhaBroker,
    user_id: str,
    signal_payload: Dict[str, Any],
    txn_id: str,
    lock_token: str,
    lock_ttl_sec: int,
    transaction_timeout_sec: float,
    cancel_timeout_sec: float,
) -> TransactionOutcome:
    orders = _normalize_orders(signal_payload)
    if not orders:
        return TransactionOutcome(state="FAILED_TERMINAL", reason="invalid_orders", release_lock=True)

    baseline_positions = _positions_map(_fetch_cached_net_positions(redis_client, user_id))
    expected_delta = _expected_position_delta(orders)

    state = {
        "txn_id": txn_id,
        "user_id": user_id,
        "approval_id": signal_payload.get("approval_id"),
        "strategy_id": signal_payload.get("strategy_id"),
        "idempotency_key": signal_payload.get("idempotency_key"),
        "state": "SUBMITTING",
        "orders": orders,
        "broker_order_ids": [],
        "created_at_ms": _now_ms(),
        "updated_at_ms": _now_ms(),
    }
    _set_txn_state(redis_client, txn_id, state)

    tracked_order_ids: List[str] = []
    for idx, order in enumerate(orders):
        if not _renew_user_lock(redis_client, user_id, lock_token, lock_ttl_sec):
            state["state"] = "MANUAL_REVIEW"
            state["reason"] = "lock_lost"
            state["updated_at_ms"] = _now_ms()
            _set_txn_state(redis_client, txn_id, state)
            return TransactionOutcome(state="MANUAL_REVIEW", reason="lock_lost", release_lock=False)

        order_request = {k: v for k, v in order.items() if v is not None}
        order_request["tag"] = f"txn:{txn_id}:{idx}"
        try:
            order_id = broker.place_order(**order_request)
            tracked_order_ids.append(str(order_id))
        except Exception as exc:
            state["state"] = "FAILED_RETRYABLE"
            state["reason"] = f"submit_error:{exc}"
            state["broker_order_ids"] = tracked_order_ids
            state["updated_at_ms"] = _now_ms()
            _set_txn_state(redis_client, txn_id, state)
            break

    if not tracked_order_ids:
        return TransactionOutcome(state="FAILED_TERMINAL", reason="no_orders_submitted", release_lock=True)

    state["state"] = "RECONCILING"
    state["broker_order_ids"] = tracked_order_ids
    state["updated_at_ms"] = _now_ms()
    _set_txn_state(redis_client, txn_id, state)

    all_complete, reason, latest = _wait_for_cache_terminal(
        redis_client=redis_client,
        user_id=user_id,
        tracked_order_ids=tracked_order_ids,
        timeout_sec=transaction_timeout_sec,
        lock_token=lock_token,
        lock_ttl_sec=lock_ttl_sec,
    )

    if all_complete:
        pos_ok = _wait_for_expected_position(
            redis_client=redis_client,
            user_id=user_id,
            baseline=baseline_positions,
            delta=expected_delta,
            timeout_sec=cancel_timeout_sec,
            lock_token=lock_token,
            lock_ttl_sec=lock_ttl_sec,
        )
        if pos_ok:
            state["state"] = "POSITION_VERIFIED"
            state["reason"] = "all_orders_complete_and_position_matched"
            state["updated_at_ms"] = _now_ms()
            _set_txn_state(redis_client, txn_id, state)
            return TransactionOutcome(state="POSITION_VERIFIED", reason="commit", release_lock=True)

        state["state"] = "MANUAL_REVIEW"
        state["reason"] = "orders_complete_but_position_unverified"
        state["updated_at_ms"] = _now_ms()
        _set_txn_state(redis_client, txn_id, state)
        return TransactionOutcome(state="MANUAL_REVIEW", reason="position_unverified", release_lock=False)

    cancelled, cancel_failed = _cancel_pending_for_transaction(
        broker=broker,
        latest_order_map=latest,
        tracked_order_ids=tracked_order_ids,
    )

    cancel_ok, latest_after_cancel = _wait_for_cancel_confirmation(
        redis_client=redis_client,
        user_id=user_id,
        tracked_order_ids=tracked_order_ids,
        timeout_sec=cancel_timeout_sec,
        lock_token=lock_token,
        lock_ttl_sec=lock_ttl_sec,
    )

    if cancel_ok:
        state["state"] = "ROLLED_BACK_CONFIRMED"
        state["reason"] = reason
        state["cancelled_count"] = cancelled
        state["cancel_failed_count"] = cancel_failed
        state["terminal_statuses"] = {
            oid: _order_status(latest_after_cancel.get(oid)) for oid in tracked_order_ids
        }
        state["updated_at_ms"] = _now_ms()
        _set_txn_state(redis_client, txn_id, state)
        return TransactionOutcome(state="ROLLED_BACK_CONFIRMED", reason=reason, release_lock=True)

    state["state"] = "MANUAL_REVIEW"
    state["reason"] = f"cancel_not_confirmed:{reason}"
    state["cancelled_count"] = cancelled
    state["cancel_failed_count"] = cancel_failed
    state["updated_at_ms"] = _now_ms()
    _set_txn_state(redis_client, txn_id, state)
    return TransactionOutcome(state="MANUAL_REVIEW", reason="cancel_unconfirmed", release_lock=False)


def run_auto_execution_loop(
    broker_cls: Type[ZerodhaBroker] = ZerodhaBroker,
    signal_queue_key: str = "signals:approved:queue",
    poll_timeout_sec: int = 1,
    lock_ttl_sec: int = 600,
    idempotency_ttl_sec: int = 86400,
    transaction_timeout_sec: float = 8.0,
    cancel_timeout_sec: float = 8.0,
    retry_enqueue_delay_sec: float = 0.25,
    lock_hold_ttl_on_manual_review_sec: int = 86400,
    deadletter_key: str = "signals:approved:deadletter",
) -> None:
    """
    Separate auto-execution service:
    - Reads approved signal payloads from Redis queue only.
    - Writes place/cancel requests to broker only.
    - Reads execution truth only from Redis order/position cache (fed by order-sync worker).
    - Enforces one active transaction lock per user account.
    """
    redis_client = get_redis_client()
    login_response = login()
    default_user_id = str(login_response.get("user_id") or os.getenv("USERID") or "").strip()
    if not default_user_id:
        raise RuntimeError("Could not resolve broker user_id from login response or USERID env.")

    broker = broker_cls()

    _set_service_key(redis_client, f"{SERVICE_NS}:last_error", "")
    print(
        "[auto_execution] started",
        {
            "queue": signal_queue_key,
            "default_user_id": default_user_id,
            "transaction_timeout_sec": transaction_timeout_sec,
            "cancel_timeout_sec": cancel_timeout_sec,
            "lock_ttl_sec": lock_ttl_sec,
        },
    )

    while True:
        try:
            _set_service_key(redis_client, f"{SERVICE_NS}:last_heartbeat_epoch_ms", str(_now_ms()))

            item = redis_client.brpop(signal_queue_key, timeout=max(1, int(poll_timeout_sec)))
            if not item:
                continue

            _, raw_payload = item
            signal_payload = _parse_signal_message(raw_payload)
            if not signal_payload:
                redis_client.lpush(deadletter_key, raw_payload)
                _set_service_key(redis_client, f"{SERVICE_NS}:last_error", "invalid_signal_payload")
                continue

            user_id = str(signal_payload.get("user_id") or default_user_id).strip()
            if not user_id:
                redis_client.lpush(deadletter_key, raw_payload)
                _set_service_key(redis_client, f"{SERVICE_NS}:last_error", "missing_user_id")
                continue

            signal_payload["idempotency_key"] = str(
                signal_payload.get("idempotency_key") or _derive_idempotency_key(signal_payload, user_id)
            )
            idempotency_key = signal_payload["idempotency_key"]
            txn_id = str(signal_payload.get("transaction_id") or f"txn-{_now_ms()}-{uuid.uuid4().hex[:8]}")
            lock_token = str(uuid.uuid4())

            if not _acquire_user_lock(redis_client, user_id, lock_token, lock_ttl_sec):
                # Account is busy with unresolved transaction.
                redis_client.lpush(signal_queue_key, raw_payload)
                time.sleep(retry_enqueue_delay_sec)
                continue

            if not _reserve_idempotency(redis_client, idempotency_key, txn_id, idempotency_ttl_sec):
                _release_user_lock(redis_client, user_id, lock_token)
                continue

            signal_payload["transaction_id"] = txn_id

            outcome = _process_transaction(
                redis_client=redis_client,
                broker=broker,
                user_id=user_id,
                signal_payload=signal_payload,
                txn_id=txn_id,
                lock_token=lock_token,
                lock_ttl_sec=lock_ttl_sec,
                transaction_timeout_sec=transaction_timeout_sec,
                cancel_timeout_sec=cancel_timeout_sec,
            )

            if outcome.release_lock:
                _release_user_lock(redis_client, user_id, lock_token)
            else:
                redis_client.expire(f"exec:txn_lock:{user_id}", lock_hold_ttl_on_manual_review_sec)

            _set_service_key(redis_client, f"{SERVICE_NS}:last_outcome", json.dumps(
                {
                    "txn_id": txn_id,
                    "user_id": user_id,
                    "state": outcome.state,
                    "reason": outcome.reason,
                    "ts_ms": _now_ms(),
                },
                sort_keys=True,
            ))
            _set_service_key(redis_client, f"{SERVICE_NS}:last_error", "")

            print(
                "[auto_execution] txn_terminal",
                {
                    "txn_id": txn_id,
                    "user_id": user_id,
                    "state": outcome.state,
                    "reason": outcome.reason,
                },
            )

        except Exception as exc:
            traceback.print_exc()
            try:
                _set_service_key(redis_client, f"{SERVICE_NS}:last_error", str(exc))
            except Exception:
                pass
            time.sleep(0.25)
