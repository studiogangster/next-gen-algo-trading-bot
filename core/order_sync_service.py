import json
import time
import traceback
from typing import Type

from brokers.kite_trade import ZerodhaBroker
from brokers.utils import login
from storage.redis_client import get_redis_client


def run_order_sync_loop(
    broker_cls: Type[ZerodhaBroker] = ZerodhaBroker,
    poll_interval: float = 0.5,
) -> None:
    """
    Continuously sync broker orders/positions into Redis.
    Writes heartbeat keys for observability.
    """
    redis_client = get_redis_client()
    response = login()
    user_id = response["user_id"]
    broker = broker_cls()

    while True:
        try:
            positions = broker.positions()
            orders = broker.orders()
            orders_count = len(orders) if isinstance(orders, list) else 0

            orders_key = f"user:{user_id}:orders"
            redis_client.execute_command("SET", orders_key, json.dumps(orders))

            base_position_key = f"user:{user_id}:position"
            net_count = 0
            day_count = 0
            if isinstance(positions, dict) and "net" in positions:
                net_count = len(positions["net"]) if isinstance(positions["net"], list) else 0
                redis_client.execute_command(
                    "SET", f"{base_position_key}:net", json.dumps(positions["net"])
                )
            if isinstance(positions, dict) and "day" in positions:
                day_count = len(positions["day"]) if isinstance(positions["day"], list) else 0
                redis_client.execute_command(
                    "SET", f"{base_position_key}:day", json.dumps(positions["day"])
                )

            now_ms = int(time.time() * 1000)
            redis_client.execute_command(
                "SET", "service:order_sync:last_success_epoch_ms", str(now_ms)
            )
            redis_client.execute_command(
                "SET", f"service:order_sync:user:{user_id}:last_success_epoch_ms", str(now_ms)
            )
            redis_client.execute_command(
                "SET", f"service:order_sync:user:{user_id}:last_orders_count", str(orders_count)
            )
            redis_client.execute_command(
                "SET", f"service:order_sync:user:{user_id}:last_positions_net_count", str(net_count)
            )
            redis_client.execute_command(
                "SET", f"service:order_sync:user:{user_id}:last_positions_day_count", str(day_count)
            )
            redis_client.execute_command(
                "SET", "service:order_sync:last_error", ""
            )
            print(
                "[order_sync]",
                {
                    "user_id": user_id,
                    "orders": orders_count,
                    "positions_net": net_count,
                    "positions_day": day_count,
                    "ts_ms": now_ms,
                },
            )
        except Exception as exc:
            traceback.print_exc()
            redis_client.execute_command("SET", "service:order_sync:last_error", str(exc))
        finally:
            time.sleep(poll_interval)
