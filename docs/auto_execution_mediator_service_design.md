# Auto Execution Mediator Service Design (Signal -> Broker -> Verified Position)

## 1) Objective

Build a dedicated execution service that:
- Consumes generated + approved signals.
- Places/cancels orders at broker as a mediator (write path only).
- Confirms the outcome strictly from internal Redis order/position cache (fed by existing sync worker).
- Prevents duplicate or contradictory actions despite retries, timeouts, restarts, or partial failures.

This service must be reliable for HFT-like conditions where latency, concurrency, and broker inconsistencies are common.

---

## 2) Core Principles

1. Single source of intent:
- Every action starts as an immutable `ExecutionIntent` with a unique `intent_id`.
- No direct signal -> place_order bypass.

2. Idempotency first:
- Same approved signal (or same decision window) must map to same `idempotency_key`.
- Retries never create a second economic action.

3. State machine, not booleans:
- Order lifecycle and position lifecycle are explicit states with monotonic transitions.

4. Reconciliation over assumptions:
- Broker API ACK alone is not enough.
- Final truth for this service comes from reconcile loop on Redis cache (`orders`, `position:net`, `position:day`) maintained by existing sync worker.

5. Safety over speed under uncertainty:
- If uncertainty exceeds threshold (stale cache, broker down, unknown order state), fail-safe by pausing new risk-increasing actions.

---

## 3) Service Boundaries

### In scope
- Consume approved signals.
- Decide enter/exit action.
- Submit order request.
- Track pending/open/partial/complete/cancel/reject states.
- Verify resulting position delta.
- Handle retries/timeouts/cancels/recoveries.

### Out of scope
- Signal generation logic.
- Indicator math.
- Portfolio optimization across strategies (can be added later).

---

## 4) High-Level Architecture

Components:
- Signal Approval Feed (input): emits approved actionable signals.
- Execution Mediator Service (new): deterministic decision + state machine.
- Intent Store (Redis + durable journal): idempotency, locks, lifecycle state.
- Broker Adapter: place/cancel only (no read methods used by this service).
- Reconcile Worker: continuously validates intent state against Redis cache snapshots.
- Existing Order Sync Service: keeps `user:{id}:orders` and `user:{id}:position:{net/day}` fresh.

Data flow:
1. Approved signal arrives.
2. Normalize + derive `idempotency_key`.
3. Create or load existing `ExecutionIntent`.
4. Acquire user-account transaction lock.
5. Run pre-trade risk + current-position checks.
6. Send order to broker with deterministic tag/client ref (when supported).
7. Move intent to `BROKER_ACK_PENDING` / `ORDER_OPEN` / `RECONCILING`.
8. Reconcile loop confirms terminal outcome from Redis order/position cache + expected position delta.
9. Emit execution event and release lock.

---

## 5) Canonical Data Model

### 5.1 ExecutionIntent

Fields:
- `intent_id` (UUIDv7)
- `user_id`
- `strategy_id`
- `symbol`, `exchange`, `product`
- `side` (`BUY`/`SELL`)
- `target_qty`
- `signal_ts`, `signal_version`
- `approval_id`
- `idempotency_key` (deterministic hash)
- `desired_effect` (`OPEN_LONG`, `CLOSE_LONG`, `OPEN_SHORT`, `CLOSE_SHORT`, `FLIP`, `REDUCE`)
- `time_in_force_policy`
- `max_slippage_bps`
- `state`
- `broker_order_refs[]` (order ids seen)
- `filled_qty`, `avg_price`
- `position_before`, `position_after_expected`, `position_after_observed`
- `created_at`, `updated_at`, `expires_at`
- `last_error`, `retry_count`

### 5.2 Idempotency key formula (example)

`sha256(user_id|strategy_id|symbol|side|target_qty|signal_ts_rounded|approval_id|intent_type)`

Notes:
- Include `approval_id` so only explicitly approved signals can trade.
- `signal_ts_rounded` (for very high tick rates) avoids accidental drift due to ms jitter.

### 5.3 Redis keys (suggested)

- `exec:intent:{intent_id}` -> full JSON
- `exec:idemp:{idempotency_key}` -> `intent_id`
- `exec:txn_lock:{user_id}` -> account transaction lease lock
- `exec:active:{user_id}` -> sorted set of non-terminal transactions/intents
- `exec:outbox` -> stream of lifecycle events
- `exec:deadletter` -> failed intents requiring manual review

For stronger durability, append every state change to a persistent log (Postgres/Kafka/append-only Redis stream snapshot).

---

## 6) State Machine

States:
- `NEW`
- `VALIDATED`
- `SUBMITTING`
- `BROKER_ACKED` (request accepted with order id)
- `ORDER_OPEN`
- `ORDER_PARTIAL`
- `ORDER_FILLED`
- `CANCEL_REQUESTED`
- `ORDER_CANCELLED`
- `ORDER_REJECTED`
- `RECONCILING`
- `POSITION_VERIFIED`
- `FAILED_RETRYABLE`
- `FAILED_TERMINAL`
- `MANUAL_REVIEW`

Rules:
- State transitions must be compare-and-set (CAS).
- Terminal states: `POSITION_VERIFIED`, `FAILED_TERMINAL`, `MANUAL_REVIEW`.
- Never move backward (except explicit compensating intent, not state rewind).

---

## 7) Atomicity and Idempotency Strategy

### 7.1 Atomic create-or-load intent

Use atomic operation (Redis Lua or DB transaction):
- If `exec:idemp:{key}` exists -> return existing `intent_id`.
- Else create new intent + idemp mapping together.

This prevents duplicate intents during concurrent deliveries.

### 7.2 User-account transaction lease lock

Before submit:
- Acquire `exec:txn_lock:{user_id}` with TTL.
- Renew periodically while processing.
- On lock loss, worker must stop acting and let reconciler take over.

Prevents any new order for that user account while one transaction is unresolved.

### 7.3 Submit with broker correlation (write-only broker contract)

When placing order:
- Attach deterministic `tag`/`client_ref` = shortened `intent_id` or hash.
- On timeout after submit call, do not blindly resubmit.
- Do not query broker read APIs from this service.
- Wait for cache confirmation from `user:{id}:orders` for same correlation tag/order_id.
- If cache does not confirm within timeout, move to `MANUAL_REVIEW` (or guarded retry policy if explicitly enabled).

### 7.4 Reconcile before next action

New action for user account allowed only if:
- No unresolved prior transaction for that user.
- Cache freshness is within threshold.

### 7.5 Multi-order transaction semantics (hard requirement)

- A transaction may include one or more broker orders.
- Success criterion: all required orders for the transaction must reach confirmed terminal success via cache.
- If timeout occurs and none/partial orders are executed:
  - cancel all still-pending orders of that transaction,
  - wait for cancel/fill terminal confirmation via cache,
  - only then release `exec:txn_lock:{user_id}`.
- Until transaction reaches terminal state, no new orders are accepted for that user account.

---

## 8) Position Truth Model

Use cache-first truth model (hard requirement):
1. Fresh order-sync cache (`user:{id}:orders`, `user:{id}:position:net/day`) is the only read source for this service.
2. Locally derived expected position is advisory only; never terminal truth.

Freshness guardrails:
- If `service:order_sync:last_success_epoch_ms` older than threshold (e.g., >2s for HFT profile), mark cache stale.
- Under stale cache -> freeze all new transactions for that user account until freshness recovers.

---

## 9) Timeout, Cancel, and Replace Policy

Per intent timers:
- `submit_ack_timeout_ms` (no order id yet)
- `open_order_timeout_ms` (still pending/open)
- `reconcile_timeout_ms` (filled/cancelled but position not yet confirmed)

Behavior:
1. Submit timeout:
- Do not query broker reads.
- Wait for cache visibility by correlation tag/order_id.
- If not visible within timeout window, mark transaction `MANUAL_REVIEW` (or explicit guarded retry if policy allows).

2. Open/pending timeout:
- Request cancel.
- Move `CANCEL_REQUESTED`.
- Continue polling cache because fill may race with cancel.

3. Cancel timeout:
- If still ambiguous, set `MANUAL_REVIEW` and keep account lock active.

4. Replace logic (optional):
- For limit orders, cancel+replace only with strict retry caps.

---

## 10) Critical Edge Cases and Handling

1. Duplicate approved signals delivered:
- Collapses to same idempotency key -> same intent reused.

2. Service crash after broker accepted order, before local state save:
- Recovery waits for cache sync and reconstructs intent state from cached orders by correlation tag/order_id.

3. Network timeout on place_order, unknown broker result:
- Enter `RECONCILING_UNKNOWN_SUBMIT`; wait for cache confirmation before any retry/rollback decision.

4. Broker down / 5xx burst:
- Circuit breaker opens.
- Queue intents in `FAILED_RETRYABLE` with backoff.
- Allow emergency exits if alternate channel exists.

5. Redis temporary unavailability:
- Stop trading writes immediately (fail closed).
- Do not place order if intent cannot be durably recorded first.

6. Order sync service down (stale caches):
- Detect heartbeat stale.
- Do not degrade to broker reads from this service.
- Freeze account transactions until cache freshness is restored.

7. Partial fills then timeout:
- Track cumulative filled qty.
- Recompute residual qty before any re-order.
- Never resend full original qty.

8. Cancel request succeeded but delayed orderbook update:
- Keep in `RECONCILING` until observed terminal status or timeout.

9. Cancel/Fill race:
- If fill observed after cancel request, treat as filled and verify position.

10. Broker rejects due to RMS/margin:
- `FAILED_TERMINAL`; emit risk event; no automatic aggressive retries.

11. Manual broker-side order modification/cancel:
- Reconciler detects mismatch between intent and broker state.
- Mark `MANUAL_REVIEW` and require operator decision.

12. Strategy sends opposite signal while prior intent unresolved:
- Gate by user-account transaction lock and conflict policy:
  - Conservative: queue until resolved.
  - Aggressive: issue compensating intent only after known fill quantity.

13. Clock skew between systems:
- Use monotonic local timers for timeouts.
- Use broker timestamps only for audit ordering, not timeout math.

14. Out-of-order event delivery from message bus:
- Sequence by `intent_version` and CAS updates.

15. Broker returns duplicate/changed status records:
- State reducer must be idempotent and monotonic.

16. Exchange halt / auction / circuit limit:
- Orders may remain pending/rejected unexpectedly.
- Use instrument trading-state checks before submit.

17. Service restart with in-flight intents:
- On boot, load all non-terminal intents and resume reconcile loops first, then consume new signals.

18. Net position cache mismatch vs expected transaction outcome:
- Treat cache as operational truth for this service; mark `MANUAL_REVIEW` and wait for sync worker to converge.

19. Place succeeded, position unchanged (possible intraday square-off elsewhere or wrong product):
- Move `MANUAL_REVIEW`; prevent next dependent action.

20. Ghost orders (order appears in cache but not in our intents):
- Create synthetic `EXTERNAL_ORDER` record when they appear in cache but not in our intents; quarantine user account for human confirmation.

---

## 11) Security and Safety Controls

- Only approved signals can generate intent (`approval_id` required).
- HMAC-sign internal signal payloads to prevent tampering between services.
- Per-user and per-strategy risk caps:
  - max open orders
  - max gross exposure
  - max order value
  - kill switch on error-rate spikes
- Principle of least privilege for broker credentials.
- Audit log immutable append-only for every transition and broker payload hash.

---

## 12) Observability and SLOs

Metrics:
- `intent_created_total`
- `intent_terminal_total{state}`
- `intent_stuck_seconds{state}`
- `broker_submit_latency_ms`
- `reconcile_latency_ms`
- `idempotency_hit_total`
- `duplicate_submit_prevented_total`
- `cache_staleness_ms`
- `manual_review_total`

Alerts:
- No order-sync heartbeat for > N seconds.
- Spike in `FAILED_TERMINAL`.
- Any intent stuck in non-terminal > threshold.
- Reconciliation mismatch rate > threshold.

SLO examples:
- 99.9% intents reach terminal state within 5s in normal broker conditions.
- 0 duplicate economic orders per idempotency key.

---

## 13) Suggested Rollout Plan

Phase 0: Shadow mode
- Consume approved signals, create intents, simulate broker actions, no real orders.

Phase 1: Paper/live-dry mode
- Send to broker in small allowlist (few symbols, low qty).
- Strict manual review on any mismatch.

Phase 2: Controlled production
- Enable auto enter + auto exit with conservative timeouts.
- Keep kill-switch and per-symbol throttles.

Phase 3: Full production
- Expand symbol universe and strategy set.
- Add advanced policies (replace logic, smart routing, multi-broker failover).

---

## 14) Immediate Implementation Tasks for This Repo

1. New module: `core/auto_execution_service.py`
- Intent ingest, state machine reducer, broker submit/cancel orchestration.

2. New module: `core/execution_reconciler.py`
- Reconcile active intents against Redis order/position cache only.

3. Shared models: `core/execution_models.py`
- Intent schema, enums, state transition guards.

4. Redis key helpers:
- `storage/execution_store.py` with atomic idempotency + lock helpers (Lua/CAS).

5. Wire into engine config:
- Feature flag `enable_auto_execution_service`.

6. Add tests:
- Duplicate signals, timeout unknown submit, cancel-fill race, stale cache freeze, restart recovery, multi-order transaction rollback/cancel, partial fill residual qty logic.

---

## 15) Non-Negotiable Invariants

- No order submit without durable intent record.
- No second submit for same idempotency key unless cache absence is proven after timeout window.
- No broker read API usage from this service; execution status is inferred only from Redis cache.
- No new order for a user account while an existing transaction for that account is unresolved.
- If transaction is partial/ambiguous after timeout, cancel all pending orders in that transaction and wait for cache-confirmed terminal states before unlocking.
- Position-dependent follow-up actions only after position verification or explicit operator override.
