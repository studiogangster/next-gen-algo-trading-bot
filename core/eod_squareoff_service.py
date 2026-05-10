import time
import traceback
from datetime import datetime
from typing import Any, Dict, List, Tuple, Type
from zoneinfo import ZoneInfo

from brokers.kite_trade import ZerodhaBroker
from brokers.utils import login
from storage.redis_client import get_redis_client

IST = ZoneInfo("Asia/Kolkata")

PENDING_ORDER_STATUSES = {
    "OPEN",
    "OPEN PENDING",
    "MODIFY PENDING",
    "TRIGGER PENDING",
    "AMO REQ RECEIVED",
    "VALIDATION PENDING",
    "PUT ORDER REQ RECEIVED",
}

SERVICE_NS = "service:eod_squareoff"


def _parse_trigger_minutes(trigger_time_ist: str) -> int:
    try:
        hh_str, mm_str = trigger_time_ist.split(":")
        hh = int(hh_str)
        mm = int(mm_str)
    except Exception as exc:
        raise ValueError(f"Invalid time '{trigger_time_ist}'. Expected HH:MM in IST.") from exc
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        raise ValueError(f"Invalid time '{trigger_time_ist}'. Hour must be 00-23 and minute 00-59.")
    return hh * 60 + mm


def _safe_int(value: Any) -> int:
    try:
        return int(float(value))
    except Exception:
        return 0


def _cancel_pending_orders(
    broker: ZerodhaBroker,
    dry_run: bool,
) -> Tuple[int, int]:
    cancelled = 0
    skipped = 0

    orders = broker.orders()
    if not isinstance(orders, list):
        return cancelled, skipped

    for order in orders:
        if not isinstance(order, dict):
            skipped += 1
            continue
        status = str(order.get("status", "")).strip().upper()
        if status not in PENDING_ORDER_STATUSES:
            continue

        order_id = str(order.get("order_id", "")).strip()
        variety = str(order.get("variety", ZerodhaBroker.VARIETY_REGULAR)).strip() or ZerodhaBroker.VARIETY_REGULAR
        parent_order_id = order.get("parent_order_id")
        if not order_id:
            skipped += 1
            continue

        if dry_run:
            cancelled += 1
            continue

        try:
            broker.cancel_order(variety=variety, order_id=order_id, parent_order_id=parent_order_id)
            cancelled += 1
        except Exception:
            skipped += 1

    return cancelled, skipped


def _build_squareoff_orders(positions: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not isinstance(positions, dict):
        return out
    net_positions = positions.get("net", [])
    if not isinstance(net_positions, list):
        return out

    for pos in net_positions:
        if not isinstance(pos, dict):
            continue
        qty = _safe_int(pos.get("quantity", 0))
        if qty == 0:
            continue

        tradingsymbol = str(pos.get("tradingsymbol", "")).strip()
        exchange = str(pos.get("exchange", "")).strip()
        product = str(pos.get("product", ZerodhaBroker.PRODUCT_MIS)).strip() or ZerodhaBroker.PRODUCT_MIS
        if not tradingsymbol or not exchange:
            continue

        transaction_type = (
            ZerodhaBroker.TRANSACTION_TYPE_SELL
            if qty > 0
            else ZerodhaBroker.TRANSACTION_TYPE_BUY
        )
        out.append(
            {
                "variety": ZerodhaBroker.VARIETY_REGULAR,
                "exchange": exchange,
                "tradingsymbol": tradingsymbol,
                "transaction_type": transaction_type,
                "quantity": abs(qty),
                "product": product,
                "order_type": ZerodhaBroker.ORDER_TYPE_MARKET,
            }
        )
    return out


def _execute_squareoff(
    broker: ZerodhaBroker,
    dry_run: bool,
) -> Dict[str, int]:
    placed = 0
    failed = 0

    positions = broker.positions()
    plans = _build_squareoff_orders(positions)

    for plan in plans:
        if dry_run:
            placed += 1
            continue
        try:
            broker.place_order(**plan)
            placed += 1
        except Exception:
            failed += 1
    return {
        "positions_to_exit": len(plans),
        "orders_placed": placed,
        "orders_failed": failed,
    }


def run_eod_squareoff_loop(
    broker_cls: Type[ZerodhaBroker] = ZerodhaBroker,
    trigger_time_ist: str = "15:20",
    poll_interval: float = 15.0,
    dry_run: bool = False,
    cancel_pending_orders: bool = True,
    weekdays_only: bool = True,
) -> None:
    """
    Runs forever and triggers once per IST day at/after `trigger_time_ist`.
    Action:
    - optionally cancel pending broker orders
    - square off all non-zero net positions using market orders
    """
    trigger_minutes = _parse_trigger_minutes(trigger_time_ist)
    redis_client = get_redis_client()
    login_response = login()
    user_id = str(login_response.get("user_id", "")).strip() or "unknown"
    broker = broker_cls()

    last_run_key = f"{SERVICE_NS}:user:{user_id}:last_run_ist_date"
    last_error_key = f"{SERVICE_NS}:user:{user_id}:last_error"
    heartbeat_key = f"{SERVICE_NS}:user:{user_id}:last_heartbeat_epoch_ms"
    last_summary_key = f"{SERVICE_NS}:user:{user_id}:last_summary"

    print(
        "[eod_squareoff] started",
        {
            "user_id": user_id,
            "trigger_time_ist": trigger_time_ist,
            "poll_interval": poll_interval,
            "dry_run": dry_run,
            "cancel_pending_orders": cancel_pending_orders,
            "weekdays_only": weekdays_only,
        },
    )

    while True:
        now_ist = datetime.now(IST)
        today = now_ist.strftime("%Y-%m-%d")
        now_minutes = now_ist.hour * 60 + now_ist.minute
        weekday = now_ist.weekday()  # Mon=0 ... Sun=6
        should_run_today = now_minutes >= trigger_minutes
        allowed_day = (weekday < 5) if weekdays_only else True

        try:
            redis_client.execute_command("SET", heartbeat_key, str(int(now_ist.timestamp() * 1000)))
            last_run_date = redis_client.get(last_run_key)

            if allowed_day and should_run_today and last_run_date != today:
                cancelled = 0
                cancel_failed = 0
                if cancel_pending_orders:
                    cancelled, cancel_failed = _cancel_pending_orders(broker=broker, dry_run=dry_run)

                squareoff_summary = _execute_squareoff(broker=broker, dry_run=dry_run)
                summary = {
                    "user_id": user_id,
                    "date_ist": today,
                    "trigger_time_ist": trigger_time_ist,
                    "executed_at_ist": now_ist.isoformat(),
                    "dry_run": dry_run,
                    "cancelled_pending_orders": cancelled,
                    "cancel_failed_or_skipped": cancel_failed,
                    **squareoff_summary,
                }
                redis_client.execute_command("SET", last_run_key, today)
                redis_client.execute_command("SET", last_summary_key, str(summary))
                redis_client.execute_command("SET", last_error_key, "")
                print("[eod_squareoff] executed", summary)
        except Exception as exc:
            traceback.print_exc()
            redis_client.execute_command("SET", last_error_key, str(exc))
        finally:
            time.sleep(poll_interval)
