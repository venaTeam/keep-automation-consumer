# keep-automation-consumer

Consumer service for the Keep **Automations** feature. It reads the
**matched-alerts topic**, runs the Redis gates (idempotency + cooldown), and calls
the automation API's idempotent submit endpoint — committing the matched-topic
offset only after an API-confirmed submit.

> **Status: C8 skeleton + C9 idempotency gate + C10 cooldown gate + C11 submit
> pipeline.** Every message runs gates → `POST /internal/submit` → gate
> lifecycle, and its offset commits only once it settles.
> ⚠️ **Do not point a deployed consumer at the API before D17 lands:** today's
> stub answers `{"status":"ok"}`, which is not a contract acknowledgement, so
> every message retries forever (loud, nothing lost, nothing runs).

## Submit pipeline (C11)

Per message (spec §5.2, `src/bl/pipeline.py`):

| Result | Idem key | `run_id`-owned cooldown claim | Offset |
|---|---|---|---|
| idem `duplicate` | — | not claimed | **commit** — audit `duplicate` |
| cooldown `suppressed` | — | the winner's, untouched | **commit** — audit `cooldown` |
| 200 `accepted`, or `already_submitted` carrying this message's own `run_id` (its earlier accept's response was lost) | → `done` | compare-and-extend (armed) | **commit** |
| any other 200 `already_submitted` / `suppressed` | → `done` | compare-and-delete | **commit** |
| 429 `capacity` | `pending` | compare-and-delete | retry + `submit_capacity_backoffs_total` |
| 5xx / 408 / 401 / 403 / transport / timeout / 200 without a known status | `pending` | compare-and-delete | retry |
| 400 / 404 / 409 / 422 **with the API's refusal body** `{"status":"rejected"}` | `pending` | compare-and-delete | **commit (skip)** + `submit_rejected_total` + ERROR log |
| the same statuses without it (a misrouted URL's default 404, generic validation `{errors}` from schema drift), 413, any other 4xx | `pending` | compare-and-delete | retry — fixing the routing/limit recovers; a skip never would |
| request body unencodable (`NaN`/`Infinity` in `alert`) — nothing sent | `pending` | compare-and-delete | **commit (skip)** + `submit_rejected_total{status="unsendable"}` + ERROR log |
| unexpected exception inside an attempt | `pending` | compare-and-delete | retry + `pipeline_errors_total` — never committed past |
| Redis unreachable | gate skipped | gate skipped, **no Redis call** | per response; `gate_flags` say `skipped` |

- **`run_id`** is minted per delivered message (`uuid4`) and reused for every
  retry of it: it is the cooldown ownership token and the submit's trace id.
  Because it is reused, a retry that finds the cooldown key holding its own
  `run_id` (an earlier attempt's release, or its claim reply, was lost to a
  Redis blip) **re-enters** that claim instead of suppressing itself.
- **Retries happen in the worker**, re-running the whole sequence (exactly what
  a Kafka redelivery would do), with capped exponential backoff + jitter, until
  the message settles, the consumer stops, or its partition is revoked. The poll
  thread never waits on Redis or HTTP (ADR-007). While the API is down every
  worker ends up retrying, the pool saturates, all partitions pause and polling
  continues — Kafka is the queue.
- **`gate_flags`** ride the submit request (contracts §Submit) so the API's
  audit row records degraded runs: `idempotency` + `reason`, `cooldown` +
  `cooldown_reason` + `cooldown_missing_fields`.
- **Offsets** commit per partition at the contiguous settled position
  (`src/core/offset_tracker.py`): a message still retrying holds back its own
  partition only. Only the poll thread touches the Kafka client — async commits
  every `KAFKA_COMMIT_INTERVAL_SECONDS`, synchronous on revoke and at shutdown
  (after workers drain). A partition whose uncommitted window reaches
  `MAX_UNCOMMITTED_OFFSETS_PER_PARTITION` pauses alone. A polled message that
  cannot be dispatched yet is rewound (seek) and stays outstanding, so no commit
  passes it. Rebalance epochs make a stale worker's late settle a no-op.
- **Poison** is skipped and committed past, counted in
  `deserialize_errors_total`: an unparseable message, or one no submit could
  carry — `tenant_id`/`automation_id` not a non-empty string, `alert` not an
  object, or no `alert.history_id` (contracts §Matched message). An
  unsettled poison message would block its partition forever. Exceptions
  *inside* a submit attempt are not poison: they release the claim and retry.
- **Redis recovery** follows the C9 breaker: after an outage the gates resume
  within one `REDIS_BREAKER_OPEN_SECONDS` window (a burst arriving during the
  half-open probe still fails open around it).

| Env var | Default | |
|---|---|---|
| `AUTOMATION_API_URL` | *(required)* | http(s) base URL; startup fails without it, before joining the group |
| `INTERNAL_SERVICE_TOKEN` | *(empty)* | Sent as `Authorization: Bearer`; empty sets `submit_config_missing{setting="service_token"}` |
| `AUTOMATION_API_TIMEOUT_SECONDS` / `AUTOMATION_API_CONNECT_TIMEOUT_SECONDS` | `10` / `2` | Clamped to ≤12 / ≤5 so connect + write + read (httpx times each phase) stays under the 30s provisional cooldown TTL |
| `SUBMIT_RETRY_BACKOFF_INITIAL_SECONDS` / `SUBMIT_RETRY_BACKOFF_MAX_SECONDS` | `1` / `30` | Retry backoff; initial floored at 0.1s |
| `KAFKA_COMMIT_INTERVAL_SECONDS` | `1.0` | Async commit cadence |
| `MAX_UNCOMMITTED_OFFSETS_PER_PARTITION` | `1000` | Per-partition pause threshold |

**Revoke + shutdown drain.** A revoke wakes its partitions' workers (backoff
sleeps end), waits for their in-flight attempt, then commits what settled —
before the partitions can be assigned elsewhere, so one message is never
processed by two deliveries at once (a redelivery racing its own still-running
sibling would lose the cooldown claim to it and be suppressed). The wait is
bounded by `IN_FLIGHT_DRAIN_TIMEOUT_SECONDS` = connect + 2 × read timeout + 5s
(27s at the defaults, 34s at the caps; `revoke_drain_timeouts_total` counts give-ups). Shutdown
drains the same work, so set the pod's **`terminationGracePeriodSeconds` ≥ 35**
(the Kubernetes default of 30 is too short at the defaults).

**Crash redelivery depends on `session.timeout.ms` > 30s.** A hard crash (OOM,
SIGKILL) skips the drain and can leave a 30s provisional cooldown claim owned by
a dead `run_id`. Its partitions only move after the dead member's session
expires (librdkafka default 45s; measured ~60s to re-assignment), so the claim
is gone before the event is redelivered. Lowering `session.timeout.ms` under
~30s, or enabling static membership (`group.instance.id`), would let the
redelivered event lose the claim to its own dead sibling and be suppressed —
keep both as they are (spec gap recorded in VAULT, C11 follow-ups).

| Metric | |
|---|---|
| `..._submits_total{outcome}` | every attempt by classified outcome |
| `..._submit_capacity_backoffs_total` | 429s — sustained rate means the API's in-flight ceiling is saturated (→ us, spec §9.3) |
| `..._submit_rejected_total{status}` | **any** increase is an automation that will not run (→ us); `status` is the HTTP code or `unsendable` |
| `..._pipeline_errors_total` | submit attempts that raised unexpectedly (retried) — a bug or library surprise |
| `..._fail_open_submits_total{reason}` | submits sent without the idempotency gate — the loud Redis-down signal (→ us, spec §9.3) |
| `..._submit_duration_seconds` | acknowledgement latency |
| `..._messages_abandoned_total{reason}` | unsettled messages handed back on shutdown/revoke (redelivered, not lost) |
| `..._offset_commits_total` / `..._offset_commit_errors_total` | commits issued / failed (failed ones are re-offered) |
| `..._partitions_paused_uncommitted_window` | partitions paused behind a stuck message |
| `..._messages_rewound_total` | polled messages handed back to the fetcher |
| `..._revoke_drain_timeouts_total` | revokes that stopped waiting for in-flight submits |
| `..._submit_config_missing{setting}` | startup config gaps |

Suggested alert expressions for I34 (routing is I34's):
`increase(..._submit_rejected_total[5m]) > 0`;
`rate(..._fail_open_submits_total{reason="redis_unavailable"}[5m]) > 0`;
`rate(..._submit_capacity_backoffs_total[10m]) > 0` for 10m;
`..._partitions_paused_uncommitted_window > 0` for 10m.

## Idempotency gate (C9)

`SET idem:{history_id}:{automation_id} "pending" NX EX 24h` per message
(`automation-contracts.md` §Redis keys, spec §5.2 step 1):

| Redis says | Outcome | Action |
|---|---|---|
| NX succeeded | `claimed` | submit |
| NX failed, value `done` | `duplicate` | audit `suppressed`(duplicate), commit offset, **no** submit |
| NX failed, value `pending` or key gone | `ambiguous` | **submit** |
| unreachable / `REDIS_URL` unset | `fail_open` | **submit**, `gate_flags={idempotency: skipped, reason: redis_unavailable}` |

The gate is an optimisation, never the authority: the API's unique
`(history_id, automation_id)` constraint is what actually dedups, and it
short-circuits to `200 already submitted` without calling `/run`. So every
ambiguous case submits — dropping a message would trade a cheap duplicate for a
lost automation. `done` is written only on an **API-confirmed** submit
(`mark_done`, called by the C11 pipeline); there is no `release()` on failure, because
`pending` already routes a redelivery to a submit.

Redis being sick must cost ~0, not a timeout per message: socket timeouts bound
one message's latency, and a **circuit breaker** bounds the loop's throughput —
after `REDIS_BREAKER_FAILURE_THRESHOLD` consecutive failures the gate
short-circuits to fail-open without touching the socket, then lets one message
probe when the window expires. Redis-error tracebacks are budgeted (the metric
counts every failure; the log doesn't).

## Cooldown gate (C10)

The optional per-entity gate implements the contract-owned state machine:

```text
SET cooldown:{automation_id}:1:{entity_hash} {run_id} NX EX 30
```

The entity hash is deterministic: sort the declared fields, encode compact
Unicode JSON `[field,value]` pairs, then SHA-256. Missing declared fields run
without a key and carry a warning; an empty field list creates a
whole-automation key. Losing NX suppresses without refreshing the winner's TTL —
unless the key holds this message's own `run_id`: a Lua reclaim re-enters it and
refreshes the 30s TTL (a vanished key is simply claimed). A failed reclaim fails
open, since the claim might be our own.

The claim value is an ownership token. `activate()` uses Lua to compare the
stored value with `run_id` and, on match, atomically rotate it to `done` while
extending to full `cooldown_seconds` — arming retires the token, so an armed
claim can no longer be extended or deleted by anyone (a misplaced `release()`
cannot drop a live cooldown). `release()` compares before deleting a still-
provisional claim. A stale delivery therefore cannot mutate
a replacement claim. The exact 30-second provisional TTL covers the C11
submit/accept round trip (the submit timeout is clamped under it); there is no
watchdog. The pipeline calls `activate` only for `200 accepted` and `release`
for failures, 429s, definitive refusals and other 200 statuses. Its flags are
namespaced (`cooldown`, `cooldown_reason`, `cooldown_missing_fields`) so they
merge with the idempotency flags into the submit's `gate_flags`.

| Env var | Default | |
|---|---|---|
| `REDIS_URL` | *(empty)* | Empty = gates disabled, everything fails open |
| `IDEMPOTENCY_TTL_SECONDS` | `86400` | The contract value; floored at 1 (`EX 0` is a Redis error) |
| `REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS` / `REDIS_SOCKET_TIMEOUT_SECONDS` | `0.25` | Bounds worker occupancy during Redis failure |
| `REDIS_BREAKER_FAILURE_THRESHOLD` / `REDIS_BREAKER_OPEN_SECONDS` | `5` / `10.0` | Circuit breaker |
| `REDIS_ERROR_LOG_INTERVAL_SECONDS` | `30.0` | Traceback budget |
| `KAFKA_BACKPRESSURE_POLL_TIMEOUT_SECONDS` | `0.1` | Poll cadence while worker saturation pauses partitions |

Metrics — label children are pre-initialised, so an absent series only ever
means "not scraped":

| Metric | |
|---|---|
| `..._idempotency_decisions_total{outcome}` | `claimed` / `duplicate` / `ambiguous` / `fail_open`. **Ungated volume is this metric's `fail_open`, not `redis_errors`** — once the breaker is open no Redis call is attempted, so the error counter stops scaling with traffic |
| `..._cooldown_decisions_total{outcome}` | `disabled` / `claimed` / `suppressed` / `missing_field` / `fail_open` |
| `..._redis_errors_total{operation}` | the Redis-down signal (`claim` / `get` / `mark_done` plus cooldown claim/TTL/activate/release) |
| `..._gates_config_missing{setting}` | set at startup. `redis_url` = no URL configured; `redis_client` = URL set but the client could not be built (for example, a typo'd scheme) |
| `..._handle_errors_total` | messages whose processing raised and were skipped by the poll-loop guard |
| `..._worker_pool_saturated` / `..._worker_pool_saturation_events_total` | partitions paused because all workers are occupied; Kafka polling continues |
| `..._worker_pool_tasks_in_flight` | current occupied worker slots |

Suppression audit rows go through `src/bl/suppression_audit.py` — a Protocol
with a logging stub until D17/D19 expose the endpoint.

## Layout (mirrors keep-event-handler)

| Path | Purpose |
|---|---|
| `src/consumer_main.py` | Standalone entrypoint: metrics server + health server + blocking consume loop. |
| `src/main.py` | FastAPI health/metrics app (K8s probes / scrape). |
| `src/core/kafka_consumer.py` | Consumer (auto-commit off) → poll → bounded worker dispatch → contiguous per-partition commits; rebalance + shutdown handling. |
| `src/core/offset_tracker.py` | Per-partition commit positions (lowest outstanding offset) with assignment epochs. |
| `src/core/worker_pool.py` | Nonblocking bounded worker pool; saturation pauses partitions while Kafka polling continues. |
| `src/config/` | Env config + constants (topic, group, ports, worker-pool size, Redis, automation API, retry/commit tuning). |
| `src/core/redis_client.py` | Process-lifetime Redis client (build once — a per-call pool leaks fds). |
| `src/models/matched_message.py` | Matched-message shape (contracts §"Matched message"). |
| `src/bl/gates/idempotency.py` | Idempotency gate (C9). |
| `src/bl/pipeline.py` | C11 orchestration: gates → submit → gate lifecycle, retry loop. |
| `src/bl/submit_client.py` | `POST /internal/submit` client + response classification. |
| `src/models/submit.py` | Submit request/response mirror (contracts §Submit). |
| `src/bl/gates/cooldown.py` | Owned cooldown gate (C10). |
| `src/bl/gates/cooldown_redis.py` | Atomic Redis activation/release mechanics for owned cooldown claims. |
| `src/bl/gates/cooldown_key.py` | Cooldown key + entity-hash construction, mirroring `automation-contracts.md` §Redis keys (the doc is authoritative; golden byte/hash fixtures in tests pin the mirror). |
| `src/bl/suppression_audit.py` | Suppression audit Protocol + logging stub (D17 swaps it). |

Ports: health **8092**, metrics **8094**.

## Run locally

```bash
docker compose -f docker-compose.infra.yml up -d   # kafka + zookeeper
export REDIS_URL=redis://localhost:6379            # gates; unset = fail open
export AUTOMATION_API_URL=http://localhost:8083    # required (keep-automation-api)
export INTERNAL_SERVICE_TOKEN=...                  # same value as the API's
poetry install
poetry run python -m src.consumer_main             # start the consume loop
# in another shell, produce a test message to the matched-alerts topic
```

## Test

```bash
poetry run pytest
```
