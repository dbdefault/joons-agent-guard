# Recipes

Practical patterns for `joons_agent_guard`. Every snippet below is a complete
script that was run against this exact source (via
`PYTHONPATH=src python recipe.py`, or after `pip install .`); database files
are created in a temporary directory. Shared ground rules: nothing expires by
age, every recovery is an explicit decision, all state lives in one
SQLite file per coordination domain, and an `apply_pending` effect never
calls write methods of its own guard instance (`GuardError`) — see
`docs/guarantees.md` for the exact promise list.

---

## 1. A scheduled job that never overlaps itself

**Problem.** A cron-like loop starts the same job every minute; a run must
never overlap the previous one, not even across process restarts. When a
previous run crashed mid-way, the job must not wedge silently: someone
verifies what happened, then the stale lock is broken.

```python
"""Recipe 1 - a scheduled job that never overlaps itself, surviving crashes."""
import tempfile
from pathlib import Path

from joons_agent_guard import ResourceLock

db_path = Path(tempfile.mkdtemp()) / "jobs.db"
RESOURCE, OWNER = "nightly-report", "scheduler"
lock = ResourceLock(db_path)


def run_once(job_id):
    """One scheduled run: take the lock, do the work, release it."""
    if not lock.acquire(RESOURCE, OWNER, job_id):
        holder = lock.status(RESOURCE)
        print(f"skip {job_id}: lock held by {holder.owner}/{holder.job_id}")
        return False
    try:
        print(f"{job_id}: doing the work")
        return True
    finally:
        lock.release(RESOURCE, OWNER, job_id)  # also runs when the work raises


run_once("run-1")
run_once("run-2")   # sequential runs are fine: each releases before exiting

# --- a previous run crashed mid-way and never released ----------------------
lock.acquire(RESOURCE, OWNER, "run-3")     # imagine: the process dies right here
run_once("run-4")                          # refused - the stale lock is still held

# --- recovery: verify what happened, then break the stale lock --------------
holder = lock.status(RESOURCE)
print("stuck holder:", holder.owner, "/", holder.job_id)
# Outside this library: check whether run-3's work actually happened.
if holder is not None and holder.job_id == "run-3":
    print("break stale lock:", lock.break_lock(RESOURCE, holder.job_id))
run_once("run-5")                          # succeeds again
```

**Pitfalls**

- Use a fresh `job_id` per run (timestamp or uuid). `break_lock`'s safety
  comes from job-id matching: a reused id would let a stale break hit a live
  run.
- The lock is not a lease — nothing detects a dead holder. Automate the
  detection (check `status()` at startup), but keep `break_lock` an explicit,
  logged decision made after verifying whether the crashed run's work
  happened.
- There is no reentrancy: even the same process gets `False` from a second
  `acquire` while holding the lock.
- In the skip branch, `status()` can theoretically return `None` in a race;
  here `acquire()` returned `False`, which implies a holder existed at that
  moment. Defensive code should still handle `None`.

---

## 2. A fleet of agent workers, one circuit slot each

**Problem.** Many AI agent workers run jobs against flaky upstream services.
A worker runs one job at a time; after a failure it runs at most **one
recovery attempt** at a time, and nothing else starts, until one attempt
succeeds.

```python
"""Recipe 2 - workers that run one recovery attempt at a time after a failure."""
import tempfile
from pathlib import Path

from joons_agent_guard import CircuitBreaker

breaker = CircuitBreaker(Path(tempfile.mkdtemp()) / "fleet.db")
JOBS = {"job-a": "ok", "job-b": "fail", "job-c": "fail", "job-d": "ok"}


def do_job(job_id):
    if JOBS[job_id] == "fail":
        raise RuntimeError("upstream API returned 500")
    print(f"  ran {job_id}")


def dispatch(actor, job_id):
    if not breaker.begin(actor, job_id):
        print(f"  {actor} busy, pending={breaker.snapshot(actor).pending_job}")
        return False
    try:
        do_job(job_id)
    except Exception as exc:
        breaker.finish(actor, job_id, success=False, reason=str(exc))
        print(f"  {job_id} failed on {actor}")
        return False
    breaker.finish(actor, job_id, success=True)
    return True


ACTOR = "worker-1"
print("state:", breaker.snapshot(ACTOR).state)      # closed
dispatch(ACTOR, "job-a")                             # healthy run
dispatch(ACTOR, "job-b")                             # fails -> open
s = breaker.snapshot(ACTOR)
print("after failure:", s.state, s.consecutive_failures, s.last_failure_reason)

# While open, every start is the single allowed recovery attempt:
assert breaker.begin(ACTOR, "job-c")
print("during recovery:", breaker.snapshot(ACTOR).state)       # half_open
print("second start refused:", breaker.begin(ACTOR, "job-x"))  # False
dispatch(ACTOR, "job-c")                                       # refused while half_open
try:
    do_job("job-c")
except Exception as exc:
    breaker.finish(ACTOR, "job-c", success=False, reason=str(exc))
print("after failed recovery:", breaker.snapshot(ACTOR).state)  # open, failures 2

# The next recovery attempt succeeds and closes the circuit:
assert breaker.begin(ACTOR, "job-d")
do_job("job-d")
breaker.finish(ACTOR, "job-d", success=True)
print("after successful recovery:", breaker.snapshot(ACTOR).state)  # closed
```

**Pitfalls**

- `begin`/`finish` are a protocol: a job that never calls `finish` wedges the
  actor forever (there is no timeout). Make sure `finish` runs even when the
  job raises.
- A crashed worker keeps its slot. After an operator check, call
  `finish(actor, job_id, success=...)` if the outcome is known, otherwise
  `reset(actor, job_id)` (or `reset(actor)`).
- `finish` only succeeds for the currently pending `job_id`, so a stale
  worker cannot finalize a slot that was reset and re-begun.
- `open` never hard-stops traffic: every `begin` on an `open` actor *is* the
  recovery attempt. For a true stop until intervention, add a policy layer on
  top (e.g. refuse dispatch while
  `snapshot.consecutive_failures >= N`) and clear it with `reset`.
- `consecutive_failures` and `reason` are informational; there is no cooldown
  timer.

---

## 3. An idempotent webhook / payment endpoint

**Problem.** A payment endpoint receives retries with the same
`Idempotency-Key` header. The first request performs the charge; retries must
return the original answer without charging twice; a key reused with
different content must be rejected.

```python
"""Recipe 3 - an idempotent payment endpoint keyed by Idempotency-Key."""
import json
import tempfile
from pathlib import Path

from joons_agent_guard import ReplayGuard, request_digest
from joons_agent_guard.replay import COMPLETED, CONFLICT, NEW, PENDING

guard = ReplayGuard(Path(tempfile.mkdtemp()) / "api.db")
PAYMENTS = []


def payment_endpoint(principal, idempotency_key, method, path, body_bytes):
    """``principal`` comes from the authenticated transport (mTLS, signature),
    never from client-supplied headers."""
    digest = request_digest(method, path, body_bytes)

    verdict = guard.claim(principal, idempotency_key, digest)
    if verdict == NEW:
        payment = {"id": "pay-1", "charged": json.loads(body_bytes)["amount"]}
        PAYMENTS.append(payment)                      # the real-world effect
        guard.complete(principal, idempotency_key, digest, 200, payment)
        return 200, payment
    if verdict == COMPLETED:
        code, body = guard.response(principal, idempotency_key, digest)
        print("  replayed stored answer, no second charge")
        return code, body
    if verdict == PENDING:
        return 409, {"error": "request in progress or interrupted; retry later"}
    return 409, {"error": "Idempotency-Key reused with different content"}  # CONFLICT


body = json.dumps({"amount": 100}).encode()
print(payment_endpoint("customer-42", "key-1", "POST", "/payments", body))
print(payment_endpoint("customer-42", "key-1", "POST", "/payments", body))
print("charges performed:", len(PAYMENTS))

same_key_other_body = json.dumps({"amount": 999}).encode()
print(payment_endpoint("customer-42", "key-1", "POST", "/payments", same_key_other_body))
```

**Pitfalls**

- The principal must come from a trusted transport; never derive identity
  from the `Idempotency-Key` or any other client header.
- `pending` means "outcome unknown": answer 409 (retry later), never re-run
  the effect. A crash between the effect and `complete()` leaves the row
  pending — see recipe 6.
- `conflict` is terminal through the API: the same key with different content
  can never proceed. Make the digest cover everything semantically relevant —
  `request_digest(method, path, body, **fields)` accepts extra fields (query
  string, tenant id, ...).
- `complete()` returns `False` when the row is not pending with that digest;
  log it rather than ignoring it.
- The stored response must be JSON-serializable and is replayed verbatim —
  never put secrets in it (see `SECURITY.md`).

---

## 4. Client-side safe retries for outbound calls

**Problem.** Your service sends a request and the response never arrives:
the request may or may not have gone through. Retries must present exactly
one stable request identity to the receiver, no matter how many times or
from how many processes you retry.

```python
"""Recipe 4 - client-side retries that can never send two different requests."""
import hashlib
import tempfile
from pathlib import Path

from joons_agent_guard import IntentMismatch, ReplayGuard

db_path = Path(tempfile.mkdtemp()) / "outbound.db"
guard = ReplayGuard(db_path)
INTENT = "create-order-42"
BODY = b'{"amount": 100}'
attempts = {"n": 0}


def send():
    # Reserve BEFORE the first send; every retry gets the same context back.
    ctx = guard.reserve_outbound(INTENT, {
        "method": "POST",
        "path": "/orders",
        "body_sha256": hashlib.sha256(BODY).hexdigest(),
    })
    attempts["n"] += 1
    print(f"  attempt {attempts['n']}: POST /orders, Idempotency-Key={ctx['request_id']}")
    if attempts["n"] == 1:
        raise TimeoutError("no response arrived - outcome unknown")
    return 201, {"order_id": "ord-1"}


def place_order():
    try:
        return send()
    except (TimeoutError, ConnectionError):
        print("  unknown outcome -> retry with the SAME intent")
        return send()


print(place_order())
assert attempts["n"] == 2   # one retry, and both attempts carried one request_id

# Reusing the intent for different content is refused, original stands:
try:
    guard.reserve_outbound(INTENT, {"method": "POST", "path": "/orders", "body_sha256": "other"})
except IntentMismatch as exc:
    print("intent mismatch:", exc)
```

**Pitfalls**

- Reserve **before** the first send — the reservation is what makes every
  retry carry the same `request_id`.
- Retries must reuse the same `intent_id`; a new intent id means a new
  request identity.
- `fixed` values must be strings; store a body **hash**, never the body, and
  never secrets. `request_id` and `ts` are reserved keys (`ValueError`).
- `IntentMismatch` means the intent was already reserved with different
  content: stop and investigate; the original reservation stands.
- At-most-once holds end-to-end only if the receiver deduplicates on your
  `Idempotency-Key` (`ctx["request_id"]`).
- Reserved-but-never-sent intents accumulate (no delete API); plan retention
  out-of-band.

---

## 5. A database-only side effect made exactly-once

**Problem.** A money transfer must move rows and record its receipt atomically:
either both happen or neither does, even across crashes and retries — and a
retry must never debit twice.

```python
"""Recipe 5 - a money transfer whose effect and receipt commit together."""
import json
import sqlite3
import tempfile
from pathlib import Path

from joons_agent_guard import ReplayGuard, request_digest

db_path = Path(tempfile.mkdtemp()) / "ledger.db"
guard = ReplayGuard(db_path)

demo_db = sqlite3.connect(db_path)   # application tables live in the same DB file
demo_db.execute("CREATE TABLE accounts (id TEXT PRIMARY KEY, balance INTEGER NOT NULL)")
demo_db.execute("INSERT INTO accounts VALUES ('acct-1', 500), ('acct-2', 0)")
demo_db.commit()
demo_db.close()


def transfer(from_id, to_id, amount, request_id):
    body = json.dumps({"from": from_id, "to": to_id, "amount": amount}).encode()
    digest = request_digest("POST", "/transfer", body)

    def effect(conn):
        """SQLite-only: uses the passed connection, never commits itself."""
        conn.execute("UPDATE accounts SET balance = balance - ? WHERE id = ?", (amount, from_id))
        conn.execute("UPDATE accounts SET balance = balance + ? WHERE id = ?", (amount, to_id))
        left = conn.execute("SELECT balance FROM accounts WHERE id = ?", (from_id,)).fetchone()[0]
        return 200, {"ok": True, "balance_left": left}

    verdict = guard.claim("internal", request_id, digest)
    if verdict == "new":
        return guard.apply_pending("internal", request_id, digest, effect)
    if verdict == "pending":
        # A previous run crashed before commit: nothing was written, so
        # re-running the SQLite-only effect is safe.
        return guard.apply_pending("internal", request_id, digest, effect)
    return guard.response("internal", request_id, digest)   # completed


print(transfer("acct-1", "acct-2", 100, "xfer-1"))   # effect + receipt commit together
print(transfer("acct-1", "acct-2", 100, "xfer-1"))   # retry replays, no double debit
check = sqlite3.connect(db_path)
print(check.execute("SELECT id, balance FROM accounts ORDER BY id").fetchall())
check.close()
```

**Pitfalls**

- `effect` must be SQLite-only: use the passed connection, never commit or
  roll back yourself, never call write methods of the same guard instance
  (nested writes raise `GuardError`), and do no file/network/subprocess work
  inside it. Only then is re-running on a `pending` verdict safe.
- An effect exception rolls back everything — including the effect's own
  writes — and leaves the row pending. Treat an exception as "nothing
  happened".
- Application tables must live in the **same** database file as the guard:
  the atomicity is the single transaction, which cannot span two files.
- Never use `apply_pending` for effects outside SQLite; that path has no
  atomicity. Use the pattern of recipe 3 (external effect + `complete`) and
  recipe 6 (reconciliation) instead.

---

## 6. Reconciling stuck pending requests after a crash

**Problem.** A process claimed a request, the external effect may have
happened, and it died before `complete()`. The row is `pending` — outcome
unknown, never expired. Recovery is: find the stuck rows read-only, decide
per row what really happened, then record reality with `complete()`.

```python
"""Recipe 6 - reconciling requests stuck in pending after a crash."""
import json
import sqlite3
import tempfile
from pathlib import Path

from joons_agent_guard import ReplayGuard, request_digest

db_path = Path(tempfile.mkdtemp()) / "api.db"
guard = ReplayGuard(db_path)

# --- the crash: claimed, effect ran, complete never happened ----------------
digest = request_digest("POST", "/payments", json.dumps({"amount": 100}).encode())
guard.claim("customer-42", "key-9", digest)      # imagine the process dies here

# --- step 1: read-only inspection (startup sweep or operator script) --------
ro = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
stuck = ro.execute(
    "SELECT principal, request_id, digest, state FROM requests WHERE state='pending'"
).fetchall()
ro.close()
for principal, request_id, _stored_digest, _state in stuck:
    print("stuck pending:", principal, request_id)


def verified_downstream(principal, request_id):
    """Your out-of-band check: provider records, bank statement, logs.
    Returns True for this demo."""
    return True


# --- step 2: decide per row, then record reality with complete() ------------
for principal, request_id, stored_digest, _state in stuck:
    if verified_downstream(principal, request_id):
        guard.complete(principal, request_id, stored_digest, 200, {"status": "paid"})
        print("reconciled:", request_id)

# --- step 3: reconciled rows replay like any completed request --------------
print(guard.response("customer-42", "key-9", digest))
```

**Pitfalls**

- Inspect with a **read-only** connection, and never write to `requests` by
  hand: only `complete()`/`apply_pending()` may move a row forward, because
  they re-check the digest.
- `complete()` needs the stored digest — take it from the row. Recomputing it
  yourself is safe only if your digest inputs are deterministic and
  unchanged.
- Decide per row: the effect happened → `complete()` with the real outcome;
  it did not happen → run it, then `complete()`; you cannot tell and the
  effect touched the outside world → leave it pending and escalate. Rows
  whose effect was SQLite-only may resume via `apply_pending` (recipe 5).
- There is no API to delete rows: `pending`/`conflict` rows stay until
  reconciled, and completed rows accumulate — plan an archival procedure
  out-of-band.

## Showcase

`examples/agent_fleet_demo.py` is an end-to-end showcase that combines all
three components on one database file: an asyncio fleet of four workers,
each with its own `AsyncCircuitBreaker` run slot, processes twelve
file-edit jobs; every job first takes an `AsyncResourceLock` on its target
file (two jobs share one target, so their edits are serialised and the
waiting acquire attempts are counted as lock waits), every submission is
admitted through `AsyncReplayGuard` with `request_digest` (a deliberately
duplicated submission at the end is answered from the stored response
instead of running twice), and one worker fails its first two attempts
deterministically, so the trace shows its circuit go
`open -> half_open -> open -> half_open -> closed` while its failed jobs are
re-queued and still run exactly once elsewhere. The script asserts the
invariants (each job edited exactly once, no overlapping edits on one
target, duplicate replayed without a second run, final circuit states)
while the connections are open and exits 0; a full run takes well under a
second:

    PYTHONPATH=src python examples/agent_fleet_demo.py
