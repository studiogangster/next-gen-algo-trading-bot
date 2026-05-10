# Auto Execution Service Contract

## Queue

- Redis list key: `signals:approved:queue` (configurable)
- Consumer command:
  - `python -m cli.main start-auto-execution --config-path config/config.yaml`

Service behavior:
- Reads approved signal payloads from Redis queue.
- Writes only `place_order` / `cancel_order` to broker.
- Uses only Redis cache for execution truth:
  - `user:{user_id}:orders`
  - `user:{user_id}:position:net`
- Enforces one active transaction lock per user account:
  - `exec:txn_lock:{user_id}`

## Payload format

The queue item must be a JSON object.

### Multi-order transaction (recommended)

```json
{
  "user_id": "AB1234",
  "approval_id": "approval-2026-05-10-001",
  "strategy_id": "strat-rsi-breakout",
  "signal_id": "sig-1f94d",
  "idempotency_key": "optional-explicit-key",
  "orders": [
    {
      "exchange": "NSE",
      "tradingsymbol": "RELIANCE",
      "side": "BUY",
      "quantity": 10,
      "product": "MIS",
      "order_type": "MARKET",
      "variety": "regular"
    },
    {
      "exchange": "NSE",
      "tradingsymbol": "TCS",
      "side": "SELL",
      "quantity": 5,
      "product": "MIS",
      "order_type": "MARKET",
      "variety": "regular"
    }
  ]
}
```

### Single-order transaction

```json
{
  "user_id": "AB1234",
  "approval_id": "approval-2026-05-10-002",
  "strategy_id": "strat-supertrend",
  "signal_id": "sig-2a911",
  "exchange": "NSE",
  "tradingsymbol": "INFY",
  "side": "BUY",
  "quantity": 20,
  "product": "MIS",
  "order_type": "MARKET",
  "variety": "regular"
}
```

## Failure semantics

- If all tracked orders complete and net position matches expected delta from cache:
  - transaction state -> `POSITION_VERIFIED`
  - user lock released
- If partial/timeout/reject:
  - pending orders are cancelled
  - service waits for terminal status confirmation via cache
  - transaction state -> `ROLLED_BACK_CONFIRMED` on success
  - user lock released only after confirmation
- If cancellation/position confirmation is ambiguous:
  - transaction state -> `MANUAL_REVIEW`
  - account lock is retained with long TTL

## Redis state keys

- `exec:txn:{txn_id}`: full transaction state JSON
- `exec:idemp:{idempotency_key}`: idempotency map -> `txn_id`
- `exec:txn_lock:{user_id}`: account lock token
- `service:auto_execution:last_heartbeat_epoch_ms`
- `service:auto_execution:last_outcome`
- `service:auto_execution:last_error`
- Deadletter queue: `signals:approved:deadletter`
