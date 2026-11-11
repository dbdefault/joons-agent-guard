# JOONS Agent Guard — FAQ

Answers for users of the `joons_agent_guard` package (durable SQLite-backed
locks, circuit breaker and replay guard). All examples below run as-is on
Python 3.9+ with no dependencies beyond the standard library; each one creates
or uses a file called `guard.sqlite` in the current directory.

---

### 1. Why do locks never expire, and how do I recover after a crash?

A lock whose holder died may be protecting work in an unknown state — half-finished writes, side effects that already happened — so silently taking the lock over could corrupt things. The library therefore never removes a lock by age: `acquire` keeps returning `False` until an operator has looked at the situation and explicitly broken the lock. Inspect the holder with `status()` or `list_locks()`, confirm the recorded `job_id` is really dead, then call `break_lock(resource, expected_job_id)`; it refuses to delete a lock that has since been re-acquired by a different job. The next `acquire` then succeeds. The command line offers the same recovery: `python -m joons_agent_guard --db guard.db break-lock RESOURCE --job-id ID --yes`.

```python
from joons_agent_guard import ResourceLock

lock = ResourceLock("guard.sqlite")
print('status:', lock.status("deploy-api"))                 # who holds it, and since when
print('break:', lock.break_lock("deploy-api", "job-42"))   # only after confirming the holder is dead
print('acquire:', lock.acquire("deploy-api", "worker-1", "job-43"))
```

### 2. What happens if my process dies while a replay request is pending?

The row stays in state `pending` forever: `claim` keeps returning `pending` for that key and never re-runs it by age, because `pending` means the outcome is unknown — it may have half-happened. That is the safety trade: no double execution, but the key is blocked until a human recovers it. For effects that are pure SQLite writes, use `apply_pending` as the reviewed recovery path: it runs your effect and stores the response in one transaction, so the effect happens at most once even if recovery itself is retried or raced. The effect must stay SQLite-only and must not call write methods of the same `ReplayGuard` instance — a nested write raises `GuardError` before anything is written. Stored responses are JSON, so `response()` returns the JSON round-trip (tuples become lists, non-str dict keys become strings); use JSON-stable types. For files, network calls or subprocesses the guard cannot roll anything back — find out what actually happened, then record the observed result with `complete()`.

```python
from joons_agent_guard import ReplayGuard, request_digest

guard = ReplayGuard("guard.sqlite")
digest = request_digest("POST", "/charge", b'{"order": 7}')

state = guard.claim("client-a", "req-001", digest)
if state == "new":
    # do the real work here, then record the outcome
    guard.complete("client-a", "req-001", digest, 200, {"charged": True})
elif state == "pending":
    # reviewed crash recovery for a SQLite-only effect: the effect and the
    # stored response commit together, at most once
    guard.apply_pending(
        "client-a", "req-001", digest,
        lambda conn: (200, "recovered"),  # run your SQL through conn here
    )
print(state, guard.response("client-a", "req-001", digest))
```

### 3. Can I share one DB file between the three components?

Yes. Each component creates only its own tables with `CREATE TABLE IF NOT EXISTS` — `resource_locks` for `ResourceLock`, `worker_circuits` for `CircuitBreaker`, and `requests` plus `outbound_requests` for `ReplayGuard` — so they coexist in one file without interference. The trade-off is contention: every write in any component takes the same SQLite write lock, so a slow `apply_pending` effect also delays lock and circuit operations. Share the file for convenience; give heavy, independent workloads separate files.

```python
from joons_agent_guard import CircuitBreaker, ReplayGuard, ResourceLock

path = "guard.sqlite"
with ResourceLock(path), CircuitBreaker(path), ReplayGuard(path):
    pass  # each component just creates its own tables in the same file
```

### 4. Can I share one instance between threads?

Yes. Every write transaction (`BEGIN IMMEDIATE` ... `COMMIT`) and every read holds the instance's internal re-entrant lock, so threads never interleave statements inside one transaction. A shared instance serialises its calls, so for more read parallelism you can also give each thread its own instance on the same path, as below; both patterns are covered by the test suite. Across processes, SQLite's file locking does the serialisation.

```python
import threading

from joons_agent_guard import ReplayGuard, request_digest

digest = request_digest("POST", "/job", b"{}")

def worker():
    guard = ReplayGuard("guard.sqlite")  # one instance per thread (sharing one also works)
    try:
        print(guard.claim("client-a", f"req-{threading.get_ident():x}", digest))
    finally:
        guard.close()

threads = [threading.Thread(target=worker) for _ in range(4)]
for t in threads:
    t.start()
for t in threads:
    t.join()
```

### 5. Does it work across machines?

No. SQLite is a local-disk database: every process that should cooperate must open the same file on the same machine, and the locks, circuit slots and replay decisions only exist there. Do not place the DB file on a network or cloud drive — file locking over network filesystems is unreliable and can silently break the guarantees. For several hosts, run the guard on the machine that owns the disk and expose its decisions to the others through your own API.

### 6. How big does the DB grow, and how do I prune old replay rows safely?

The DB grows with your request volume: one `requests` row per `(principal, request_id)` and one `outbound_requests` row per intent are kept forever, each storing the response as JSON text; lock and circuit tables stay tiny (a row per resource or actor). Prune manually with SQL — only rows with `state='completed'` that are older than any window in which a retry can still arrive; deleting a completed row makes a repeat of the same key claimable as `new` again. Never delete `pending` rows (their outcome is unknown; deleting one allows a double run). The `requests` table records only the claim time (`claimed`), so for a completed row's age you still apply your own convention — for example a date-structured `request_id`, or a completion time stored inside your response JSON. Deleted pages are reused by new rows; run `VACUUM` if you want the file to actually shrink.

```python
import sqlite3

from joons_agent_guard import ReplayGuard, request_digest

guard = ReplayGuard("guard.sqlite")   # ensures the tables exist
digest = request_digest("POST", "/backup", b"")
guard.claim("client-a", "2026-10-31-job", digest)      # a date prefix in request_id
guard.complete("client-a", "2026-10-31-job", digest, 200, {"ok": True})
guard.close()

con = sqlite3.connect("guard.sqlite")
# WARNING: completed rows only, and only older than your retry window.
# Never delete 'pending' rows. Completed rows carry no completion time, so the
# age filter comes from your own convention (here: a date prefix in request_id).
con.execute("DELETE FROM requests WHERE state='completed' AND request_id LIKE '2026-10-%'")
con.commit()
con.execute("VACUUM")  # return freed pages to the OS
con.close()
```

### 7. What timeout should I pick?

The constructor `timeout` (10 s for `ResourceLock` and `CircuitBreaker`, 15 s for `ReplayGuard` by default) is SQLite's busy timeout: how long a writer waits for another process's write transaction before raising `sqlite3.OperationalError: database is locked`. Normal operations are tiny single-row transactions that resolve in milliseconds, so the defaults are already generous; raise it (e.g. `ReplayGuard(path, timeout=30.0)`) when your `apply_pending` effects take seconds, because an effect runs inside the write transaction and blocks every other writer. The timeout is not an expiry for locks, pending requests or circuit slots — those never time out on their own.

### 8. How do I choose request_id, and what goes into the digest?

`request_id` must identify one logical operation and be identical across all of its retries — typically the idempotency key your caller supplies; the primary key is `(principal, request_id)`, so the same key under different principals never collides. The digest is what `request_digest` computes: a SHA-256 over canonical JSON containing the HTTP method, the path, the SHA-256 of the body, plus any extra string fields you pass as keywords — include everything that would change the operation's meaning. `body_sha256` is reserved (it is always computed from the body, so passing it as a keyword raises `ValueError`), and every field value must be a `str` (`TypeError` otherwise). If the same `(principal, request_id)` arrives with a different digest, `claim` returns `conflict` instead of executing or returning a response, which almost always means a client reused a key for different work. Comparing the plain strings `'new'` / `'pending'` / `'completed'` / `'conflict'` is equivalent to comparing the constants exported from `joons_agent_guard.replay`.

```python
from joons_agent_guard import ReplayGuard, request_digest

guard = ReplayGuard("guard.sqlite")
body = b'{"order": 7}'
idempotency_key = "k-9"
digest = request_digest("POST", "/charges", body, tenant="t1")
print(guard.claim("client-a", idempotency_key, digest))
```

### 9. Is it async-friendly?

Yes, via the bundled asyncio adapters `AsyncResourceLock`, `AsyncCircuitBreaker` and `AsyncReplayGuard` (Python 3.9+). Each adapter owns one synchronous instance and dispatches every call with `asyncio.to_thread`, so a contended SQLite write waits on a worker thread and the event loop stays responsive. The methods have the same names and semantics as coroutines, `hold` becomes an async context manager, and `async with` / `aclose()` close the instance; an `apply_pending` effect is a plain sync function running on the worker thread.

```python
import asyncio

from joons_agent_guard import AsyncReplayGuard, request_digest


async def handle(principal, request_id, digest):
    async with AsyncReplayGuard("guard.sqlite") as guard:
        state = await guard.claim(principal, request_id, digest)
        if state == "new":
            await guard.complete(principal, request_id, digest, 200, {"done": True})
        return state, await guard.response(principal, request_id, digest)


print(asyncio.run(handle("client-a", "req-async-1", request_digest("POST", "/async", b"{}"))))
```

### 10. Which Python versions and platforms are supported?

The package requires Python 3.9 or newer and depends on nothing outside the standard library, so it runs anywhere CPython ships its `sqlite3` module — Windows, macOS and Linux — with no compiler or native dependencies. There is no server to install and no daemon to run; the components are plain objects over one SQLite file. The DB file itself is portable across platforms, so you can move or copy it (just never let two machines write to it at once).

### 11. How is this different from a plain file lock?

An OS file lock is advisory, tied to a live process handle, and vanishes when that process exits — convenient, but it cannot tell you who held the resource or whether their work finished, and it stores nothing else. A lock here is a durable row recording owner, job id and acquisition time, and it deliberately survives crashes so the recovery decision stays with a human via `break_lock`. It also lives next to the replay and circuit tables, so one DB gives you exclusion, audit and at-most-once handling together.

### 12. How is this different from a retry library?

A retry helper re-executes your function after a failure — at-least-once execution, which is only safe for idempotent operations. `ReplayGuard` solves the other half of the problem: it does not schedule or pace retries at all, but guarantees that however many times the same request arrives, your handler runs at most once and later arrivals receive the stored response. The two compose cleanly: retry the delivery as often as you like and let the guard deduplicate the execution.

### 13. How is this different from an in-memory circuit breaker?

An in-memory breaker lives inside one process: its state is lost on restart and its open state is invisible to sibling processes, so a restarted worker can immediately repeat the failures another worker just recorded. This breaker keeps per-actor state — `closed`/`open`/`half_open`, consecutive failures, last reason — in SQLite, shared by every process using the same file. Its distinctive rule is a single run slot per actor: a job that started but never finished keeps the slot until `finish()` records the outcome or an operator calls `reset()`, and nothing is cleared by age.

### 14. What should my handler do when claim returns 'pending' or 'conflict'?

`pending` means another attempt may be running or may have died — the outcome is unknown, so the safe answer is a "try later" style response (for an HTTP service, 409 or 503), never a re-run of the work. `completed` means the work is done: return the stored response verbatim via `response()`. `conflict` means the same `(principal, request_id)` was presented with a different digest; treat it as a client error and do not execute anything.

### 15. When do I need reserve_outbound?

Use it for outbound idempotency: before sending a request whose receiver deduplicates by request id, persist a stable `request_id` and timestamp for the business intent. On any retry, call `reserve_outbound` again with the same `intent_id` and identical fixed content and you get the identical context back, so the receiver sees one request no matter how many times you send. Reusing an intent with different content raises `IntentMismatch` instead of silently sending something different; store hashes in `fixed`, never secrets or full bodies.

```python
from joons_agent_guard import ReplayGuard

guard = ReplayGuard("guard.sqlite")
context = guard.reserve_outbound(
    "email-batch-77",
    {"to": "ops@example.com", "body_sha256": "9f2c" * 16},
)
# use context["request_id"] in the outgoing request; on any retry, reserve the
# same intent_id again and you get the identical context back
print(context)
```

### 16. How do I inspect and recover state from the command line?

Use the bundled operator tool: `python -m joons_agent_guard --db guard.sqlite <command>`. It reads the same SQLite file the library uses, so run it on the machine that owns the disk. The read-only commands never change anything: `locks` lists held locks (resource, owner, job_id, age), `circuit ACTOR` shows one actor's snapshot, and `pending` lists replay requests in state `pending` with their age since claim; `pending --older-than SECONDS` keeps only rows claimed at least that long ago. Age is a hint for the operator, never a reason to re-run anything automatically. The two recovery commands mirror the audited library calls and never act without confirmation: `break-lock RESOURCE --job-id ID` and `reset-circuit ACTOR [--job-id ID]` print what they would do and exit 2 until you re-run them with `--yes`. Add `--json` for machine-readable output. Exit codes: 0 ok, 1 not found / nothing changed, 2 confirmation required or bad usage.

```console
python -m joons_agent_guard --db guard.sqlite locks
python -m joons_agent_guard --db guard.sqlite circuit worker-1
python -m joons_agent_guard --db guard.sqlite pending --older-than 3600
python -m joons_agent_guard --db guard.sqlite break-lock deploy-api --job-id job-42         # plan only, exit code 2
python -m joons_agent_guard --db guard.sqlite break-lock deploy-api --job-id job-42 --yes    # really break it
python -m joons_agent_guard --db guard.sqlite reset-circuit worker-1 --yes                   # operator reset
```

### 17. What happens if I cancel an async call?

Cancellation is prompt, but it is not an abort: `asyncio` cannot stop a call that is already running on a worker thread, so the task ends with `CancelledError` while the SQLite operation finishes on that thread. The commit lands asynchronously — state checked immediately after the cancellation can still show the pre-call value, so query again once the call has settled. A cancelled `claim` has committed the request as `pending`: claim the same `(principal, request_id, digest)` again (you get `pending` — never mint a fresh request_id), do the work, then `complete` with the same digest. A cancelled `apply_pending` either committed (the effect ran exactly once and `response()` serves the stored result) or rolled back completely (the row stays `pending`; re-apply re-runs the effect). An error the worker thread raises after the cancellation is discarded — query the state to learn the outcome. A cancelled `hold` entry waits for its `acquire` to finish and releases the lock if it was acquired (the body never runs), and a cancelled `hold` exit always releases. `aclose()` waits for calls that were already in flight before closing; calls started after `aclose()` began raise `sqlite3.ProgrammingError`.
