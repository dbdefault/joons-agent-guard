# Guarantees — joons_agent_guard 0.1.0

This document states, per component, what the library guarantees, what it
explicitly does **not** guarantee, and how to recover from each failure
scenario. Everything here is derived from the source in
`joons_agent_guard.lock`, `joons_agent_guard.circuit`,
`joons_agent_guard.replay`, `joons_agent_guard._db`, `joons_agent_guard.errors`
and verified with scripted probes against this exact code.

Scope: all three components coordinate processes that **share one SQLite
database file** (the "coordination domain"). `:memory:` is a private domain
per instance. Every guarantee below holds only inside one domain, on one
machine, on a local filesystem.

---

## 1. Ground rules common to all components

### Guaranteed

1. **Atomic writes.** Every multi-statement mutation runs inside
   `BEGIN IMMEDIATE ... COMMIT` via `_db.write_txn`. On any exception
   (including `BaseException`, so also `KeyboardInterrupt`) the transaction is
   rolled back and the exception re-raised. A failed call leaves the stored
   state exactly as it was before the call.
2. **Durable state.** All state lives in the SQLite file and survives process
   crashes and restarts. Reopening the same path sees the same state.
3. **Nothing expires by age.** No TTL, no background sweeper, no age check on
   any code path. Timestamps (`acquired`, `updated`, `ts`) are recorded for
   humans and never consulted to invalidate anything (see
   `docs/architecture.md`, "Why nothing expires by age").
4. **Identifier validation.** Every identifier argument (`resource`, `owner`,
   `job_id`, `actor`, `principal`, `request_id`, `digest`, `intent_id`,
   `expected_job_id`) must be a non-empty `str`, else
   `InvalidIdentifier` (subclass of both `GuardError` and `ValueError`) is
   raised *before* the database is touched — validation cannot leave partial
   writes.
5. **Writer serialization.** `BEGIN IMMEDIATE` takes the database write lock
   up front, so concurrent writers across processes are serialized; they wait
   up to the busy timeout instead of interleaving.
6. **Error discipline.** Misuse raises `GuardError` subclasses
   (`InvalidIdentifier`, `LockHeld`, `IntentMismatch`) — and plain
   `GuardError` for a nested write on an instance that already has a
   transaction open (item 7). SQLite-level failures surface as `sqlite3`
   exceptions (usually `OperationalError`) from the call. The library never
   swallows errors and never retries them internally.
7. **No nested writes.** While an instance has a transaction open — most
   notably inside an `apply_pending` effect — calling any of its write
   methods raises `GuardError` before the database is touched. The running
   transaction is unaffected by the rejected call; if the effect lets the
   error propagate, the outer transaction rolls back as usual.
8. **Thread-safe instances.** Every write transaction, every read, and
   `close()` hold the connection's re-entrant guard lock, so one instance can
   be shared between threads: statements of different threads never
   interleave inside a transaction (`tests/test_threads.py`). A shared
   instance serialises its calls.

### Not guaranteed

1. **Network filesystems.** Two machines sharing the DB file over a network
   drive voids every guarantee (see §5).
2. **Parallelism.** A shared instance processes one call at a time; thread
   safety comes from serialisation, not concurrency. For parallel reads, give
   each thread its own instance on the same file — one SQLite writer at a
   time still holds.
3. **Authentication.** `owner`, `actor`, and `principal` are plain strings
   recorded as given. Nothing verifies who is calling; `replay.py`'s own
   docstring states the principal must come from a trusted transport.
4. **Performance or availability.** Single file, single writer at a time.

### Infrastructure failures (apply to every component)

| Failure | Verified behavior | Recommended recovery |
|---|---|---|
| **DB busy timeout** — another writer holds the write lock longer than `timeout` (default 10 s for `ResourceLock`/`CircuitBreaker`, 15 s for `ReplayGuard`) | The call raises `sqlite3.OperationalError` ("database is locked"); state unchanged | Retry with backoff; construct with a larger `timeout`; reduce write contention. There is no internal retry |
| **Disk full** | The call raises `sqlite3.OperationalError`; the transaction is rolled back, so no partial rows (verified by forcing `SQLITE_FULL` via `PRAGMA max_page_count`). The original "database or disk is full" error is what propagates, and a failing `COMMIT` is rolled back too, so the connection stays usable (regression tests in `tests/test_txn.py`) | Free space and retry (the instance stays usable — verified). If a commit-time failure is suspected, close the instance, reopen, and run `PRAGMA integrity_check` |
| **Two machines, one network drive** | Outside the guarantees entirely: SQLite's cross-process locking requires reliable file locking, which network filesystems do not provide (broken on NFS per SQLite's own documentation, fragile on SMB). Worst case is lost updates or corruption | Do not do this. Keep the DB on a local disk per machine; for cross-machine coordination use a server-grade database, not this package. If it already happened, run `PRAGMA integrity_check` and reconcile by auditing the real-world effects — the package keeps no transactional log to replay |

---

## 2. ResourceLock

Exclusive locks on named resources, held by an `(owner, job_id)` pair.
State: one row per held lock in `resource_locks`; absent row = free.

### Guaranteed

1. **Mutual exclusion per resource.** `resource` is the primary key and
   `acquire` inserts with `INSERT OR IGNORE` inside `BEGIN IMMEDIATE`; it
   returns `True` if and only if it inserted the row. At most one holder
   exists at any time across the whole coordination domain.
2. **No reentrancy.** Acquiring while held — even by the very same
   `(owner, job_id)` — returns `False`.
3. **Release requires the exact triple.** `release` deletes only on an exact
   `(resource, owner, job_id)` match and returns `True` only if it deleted.
   A wrong owner, wrong `job_id`, or unknown resource returns `False` and
   leaves the lock untouched.
4. **Break is job-id-checked.** `break_lock` deletes only when
   `expected_job_id` matches the stored one; it cannot remove a lock that was
   re-acquired by a new job in the meantime. It deliberately ignores `owner`
   (it is operator recovery) — anyone who knows the resource name and current
   `job_id` can break it.
5. **`hold()` discipline.** Entering the context manager acquires or raises
   `LockHeld` carrying `.resource` and `.holder` (a `LockInfo` snapshot of the
   current holder); the body never runs when busy. Release happens in a
   `finally`, i.e. on normal exit *and* on exception.
6. **Visibility.** `status` returns `None` or a `LockInfo`
   (`resource`, `owner`, `job_id`, `acquired_at`); `list_locks` returns all
   locks ordered by `acquired` ascending. Both read committed state.
7. **Persistence, no expiry.** The lock survives close/reopen of the file and
   is never auto-expired; the only exits are `release` (triple match) and
   `break_lock` (job-id match).

### Not guaranteed

1. **Liveness.** A crashed holder wedges the resource *forever*. Nothing
   detects death; recovery is always an explicit `break_lock`.
2. **Fencing.** The lock is advisory: holding it does not stop anyone from
   touching the resource. Mutual exclusion holds only if every participant
   goes through this library before acting.
3. **Fairness or queueing.** `acquire` is try-or-fail; there is no wait queue,
   no ordering promise among contenders.
4. **Strict time ordering.** `list_locks` orders by wall-clock `acquired`;
   two acquires within one clock tick can share a timestamp and tie.

### Failure scenarios and recovery

| Scenario | Stored state after the crash | Recommended recovery |
|---|---|---|
| **Crash while holding the lock, before the effect** (lock taken, work not started) | Lock row intact; holder is gone | Operator confirms the work did not run → `break_lock(resource, job_id)` → re-acquire and run |
| **Crash after the effect, before release** (work done, lock not freed) | Lock row intact; effect state unknown to the library | Confirm the effect completed (outside the library) → `break_lock(resource, job_id)` or `release` with the known triple → do **not** re-run the work |
| Busy timeout / disk full / network drive | unchanged row / no partial row / guarantees void | See §1, Infrastructure failures |

---

## 3. CircuitBreaker

A **single run slot per actor** with last-outcome memory. Important: this is
not a classic threshold-plus-cooldown breaker — `open` never blocks starts by
itself; the next `begin` after `open` *is* the single recovery attempt
(`half_open`). `failures` is a counter, not a trip threshold.

States stored in `worker_circuits.state`: `closed`, `open`, `half_open`.
A pending job is tracked in `pending` (the `job_id`); `half_open` always has
a pending job set.

### Guaranteed

1. **One in-flight job per actor.** `begin` returns `False` if any job is
   already pending or the state is `half_open`; otherwise it claims the slot:
   a fresh or `closed` actor becomes `closed` with `pending = job_id`, an
   `open` actor becomes `half_open` (the recovery attempt).
2. **Only the pending job can finish.** `finish` updates the row only where
   `actor` *and* `pending = job_id` match; otherwise it returns `False` and
   changes nothing (unknown actor, wrong job, already finished).
3. **Outcome bookkeeping.** `finish(success=True)` → state `closed`,
   `failures = 0`, reason cleared. `finish(success=False)` → state `open`,
   `failures + 1`, `reason` stored. So `consecutive_failures` counts
   consecutive failed finishes and is zeroed only by a success or a `reset`.
4. **Operator reset is job-id-checked.** `reset(actor, expected_job_id)`
   applies only while that job is still the pending one — it cannot clobber a
   different in-flight job. `reset(actor)` clears unconditionally (both set
   `closed` / no pending / `failures = 0` / no reason). Both return `True`
   only if a row was updated — an actor that never began returns `False`.
5. **Visibility.** `snapshot` returns a `CircuitState`
   (`state`, `pending_job`, `consecutive_failures`,
   `last_failure_reason`); an actor with no row reports
   `closed / None / 0 / None`.
6. **Persistence, no expiry.** A job that started but never finished keeps
   its slot across restarts, forever, until `finish` or `reset`.

### Not guaranteed

1. **No timeout on the slot.** A crashed job (pending set) refuses every
   further `begin` until an explicit `finish` (outcome known) or `reset`
   (operator decision). The library will not self-heal, and there is no
   cooldown timer after `open`.
2. **Finish/reset ownership.** Any process that knows `actor` and the pending
   `job_id` can `finish` or `reset` it; the values are not authenticated.
3. **`failures` does not stop traffic.** Even after many failures, the next
   `begin` on an `open` actor is always admitted as the recovery attempt.

### Failure scenarios and recovery

| Scenario | Stored state after the crash | Recommended recovery |
|---|---|---|
| **Crash before the effect** (`begin` succeeded, work never started) | `pending = job_id`, state `closed` (or `half_open` for a recovery attempt); slot stuck | Verify nothing ran → `reset(actor, job_id)` → `begin` again |
| **Crash between the effect and `finish`** (effect possibly done, outcome unknown) | `pending = job_id`; slot stuck | Reconcile the real-world effect: if it verifiably completed → `finish(actor, job_id, success=True)`; if unknown or failed → `reset(actor)` (or `reset(actor, job_id)`) |
| **Recovery attempt (`half_open`) crashed** | `half_open` with `pending` set | Same two options as the row above |
| Busy timeout / disk full / network drive | unchanged / no partial row / guarantees void | See §1, Infrastructure failures |

---

## 4. ReplayGuard

Two independent sub-tools in one component:

* **Inbound** (`requests` table): run each `(principal, request_id)` at most
  once. Stored states are only `pending` and `completed`; `new` and
  `conflict` are verdicts returned by `claim`, never stored states.
* **Outbound** (`outbound_requests` table): pin a stable
  `request_id`/timestamp to an `intent_id` *before* sending, so retries
  present one request to the receiver.

### Guaranteed (inbound)

1. **`claim` returns `new` exactly once per `(principal, request_id)`.** The
   insert is primary-key protected (`ON CONFLICT DO NOTHING`) and the whole
   verdict is computed inside one `BEGIN IMMEDIATE` transaction — two
   processes racing get exactly one `new`.
2. **Later claims never re-run.** With the same `digest`, `claim` returns
   `pending` (work in flight or interrupted) or `completed` (replay the
   stored response); with a different `digest` it returns `conflict` without
   touching the row.
3. **Pending is terminal-by-neglect.** A `pending` row can only become
   `completed` through `complete`/`apply_pending` with the matching
   `digest`; it is never re-run and never unlocked by age. `complete` stores
   `(code, response)` only on a pending row with the matching digest and
   returns `True` only then (already completed, wrong digest, or unknown
   pair → `False`). `code` must be an `int` — a `bool` is rejected with
   `TypeError` before the transaction opens.
4. **`apply_pending` is exactly-once for SQLite-only effects.** The
   `effect(connection)` call and the `UPDATE ... state='completed'` run inside
   one transaction on one connection: they commit together or not at all
   (verified: an effect that raises leaves the row pending *and* rolls back
   the effect's own writes). If the request is not pending with this digest,
   it returns `None` and the effect never runs. The effect must not call
   write methods of the same instance: `write_txn` rejects a nested write
   with `GuardError`, and if the effect lets that error propagate the whole
   transaction rolls back.
5. **`response` replays only completed rows** with the matching digest, as
   `(code, response_deserialized_from_json)`. Responses are stored as JSON,
   so the replay is the JSON round-trip: tuples come back as lists and
   non-str dict keys as strings.

### Guaranteed (outbound)

1. **First reserve wins, retries are identical.** `reserve_outbound` inserts
   `fixed` + generated `request_id` (uuid4 hex) + `ts` under the
   `intent_id` primary key with `ON CONFLICT DO NOTHING`; every later call
   with the same `intent_id` — from any process — returns the same stored
   context regardless of what it passes.
2. **Intent reuse detection.** Reusing an `intent_id` with different `fixed`
   content raises `IntentMismatch` (the stored reservation survives). `fixed`
   must be `str → str` (`TypeError` otherwise) and must not contain
   `request_id` or `ts` (`ValueError`).
3. **No secrets or bodies stored.** Only the string fields you pass; put a
   body hash in `fixed`, never the body (the docstring's contract).

### Not guaranteed

1. **Exactly-once for effects outside SQLite.** `claim`/`complete` is an
   at-most-once *decision* plus a manual bookkeeping step. If you crash
   between a non-SQLite effect (HTTP call, file write, subprocess) and
   `complete`, the row stays `pending` and nothing re-runs or undoes it —
   your integration must reconcile.
2. **Pending has no owner, lease, or age.** The guard cannot distinguish
   "another process is working on it" from "the worker died".
3. **`conflict` is terminal through the API.** A row answers `conflict` to
   every differently-digested retry forever; there is no API to delete,
   overwrite, or reset it (no `DELETE` exists in the module). Rows are never
   removed by the library at all.
4. **Authentication.** The principal is taken as given; it must come from a
   trusted transport (mTLS identity, verified signature).
5. **Response serialization.** `complete` JSON-encodes the response *before*
   opening the transaction; a non-JSON-serializable response raises
   `TypeError` without touching the database — but it also means the request
   stays `pending` and the effect outcome must be reconciled by hand. The
   stored round-trip is not type-faithful: tuples come back as lists and
   non-str dict keys as strings. `code` must be an `int` (`bool` rejected) —
   checked before the transaction in `complete`, inside it in
   `apply_pending`, so a bad `code` there rolls the effect back.

### Failure scenarios and recovery

| Scenario | Stored state after the crash | Recommended recovery |
|---|---|---|
| **Crash after `claim` (`new`), before the effect** | Row `pending` | Manual reconciliation of pending requests: determine the effect did not run → run it → `complete(...)` with the real `code`/response |
| **Crash between the (external) effect and `complete`** | Row `pending`; effect state unknown | Reconcile out-of-band (check downstream side effects, use the pinned `request_id` as the receiver-side idempotency key) → then `complete(...)` with the actual outcome. **Never** silently re-run |
| **Duplicate arrives while pending** | Row `pending`; `claim` returns `pending` | Do not re-run; answer "retry later". Once completed, `response()` returns the stored answer |
| **Retry with different content** | Row unchanged; `claim` returns `conflict` | Reject the request; investigate why the digest changed. No API clears the conflict |
| **`reserve_outbound` crash before sending** | Intent reserved, `request_id` stable | Just retry the send with the same `intent_id` — same `request_id`, receiver-side dedup applies |
| **Crash after sending, before your own bookkeeping** | Reservation was written *before* the send, so a resend carries the same `request_id` | Resend safely; the receiver dedups on `request_id` |
| **SQLite-only effect + crash** | Either both effect and `completed` row committed, or neither (verified) | Nothing to reconcile — check `claim`/`response` |
| Busy timeout (default 15 s) / disk full / network drive | unchanged / no partial row / guarantees void | See §1, Infrastructure failures; a crash mid-effect may additionally require the reconciliation rows above |

---

## 5. Choosing the right tool

| You need | Use | Deliberately not provided |
|---|---|---|
| "Only one job may touch resource X at a time, across restarts" | `ResourceLock` | dead-holder detection, leases, fencing tokens |
| "At most one in-flight job per actor; one recovery attempt after a failure" | `CircuitBreaker` | trip thresholds, cooldown timers, slot timeouts |
| "Handle this inbound request at most once; replays get the stored answer" | `ReplayGuard.claim` / `complete` / `response` | re-running or expiring `pending` requests |
| "The effect and its recorded outcome must commit together" | `ReplayGuard.apply_pending` (SQLite-only effects) | atomicity for files, network, subprocesses |
| "Send this outbound request at most once per intent" | `ReplayGuard.reserve_outbound` | storing bodies or secrets |
| "Clear a dead holder / stuck job / pending request after a human check" | `break_lock` / `reset` / `complete` | automatic expiry of any of them |
| "Automatically expire stale state after N seconds" | — nothing here — | this is the library's core design decision |
