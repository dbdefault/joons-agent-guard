# JOONS Agent Guard

Durable safety primitives for AI agents and automation.

> Part of the [JOONS ecosystem](https://github.com/dbdefault/joons-ecosystem): MCP-Based AI Infrastructure Ecosystem.

Korean: [README.ko.md](README.ko.md)

[![CI](https://github.com/dbdefault/joons-agent-guard/actions/workflows/ci.yml/badge.svg)](https://github.com/dbdefault/joons-agent-guard/actions/workflows/ci.yml)

## At a glance

- **What**: durable locks, a per-worker circuit breaker and at-most-once
  request handling, all backed by one SQLite file. Stdlib only, Python 3.9+.
- **For**: agents and scripts that retry, crash and run in parallel on one
  machine, over one shared database file.
- **Not for**: cross-machine coordination, high-throughput pipelines (single
  writer; on the order of a hundred lock pairs per second on a developer
  disk, see `bench/README.md`), or anything that needs state to expire by age.
- **How**: install from source, point every process at the same database
  file, copy a recipe from `docs/recipes.md`, and inspect stuck state with
  `python -m joons_agent_guard --db guard.db locks`.
- **Trust**: locks are advisory, callers are not authenticated, and nothing
  expires. Read `docs/guarantees.md` before relying on it.

## Why

Agents and scripts retry, crash, and run in parallel. Without durable,
shared state, they run into three classic failures:

- **Duplicate actions** — a retried request runs twice: two orders, two emails,
  two deploys.
- **Two workers editing one file** — no single process owns the resource, so
  they overwrite each other's output.
- **Hammering a failing service** — a crashed worker is restarted and
  immediately starts again, before anyone has looked at the failure.

JOONS Agent Guard is a small, stdlib-only Python library (SQLite, no
dependencies) with three primitives that fix this:

- **ResourceLock** — exclusive locks on named resources, shared by every
  process using the same database file.
- **CircuitBreaker** — a single run slot per actor with a
  closed / open / half-open failure-recovery state machine.
- **ReplayGuard** — at-most-once handling of retried requests, plus stable
  request ids and timestamps for outbound calls.

## Install

Python 3.9 or later on Windows, macOS or Linux, with zero dependencies beyond
the standard library. The package is not on PyPI yet.

```bash
pip install git+https://github.com/dbdefault/joons-agent-guard.git
```

or from a source checkout:

```bash
git clone https://github.com/dbdefault/joons-agent-guard.git
cd joons-agent-guard
pip install .
```

## 30-second examples

All three components can share one SQLite database file; they use separate
tables. These examples create `guard.db` in your current directory and leave
it there: that file is your entire state, so delete it to start fresh (running
an example twice shows the idempotent path).

A resource is just a name that your processes agree on. The library never
opens or locks the file itself; do your own file work inside the block.

### Lock a resource

```python
from joons_agent_guard import LockHeld, ResourceLock

lock = ResourceLock("guard.db")

# Exclusive access to a named resource, shared by every process
# that opens the same database file.
try:
    with lock.hold("reports/daily.csv", owner="worker-1", job_id="job-42"):
        print("locked: no other worker can hold 'reports/daily.csv' now")
except LockHeld as exc:
    print(f"someone else holds the lock: {exc}")

# After the with-block the lock is released again.
print("status after release:", lock.status("reports/daily.csv"))

# Locks never expire: if a holder crashed, an operator checks the
# situation and then removes the lock explicitly.
# lock.break_lock("reports/daily.csv", expected_job_id="job-42")

lock.close()
```

### One run slot per actor

```python
from joons_agent_guard import CircuitBreaker

breaker = CircuitBreaker("guard.db")
actor = "deploy-worker"

if breaker.begin(actor, job_id="job-42"):
    try:
        print("running job-42: the only job this actor may run right now")
    except Exception as exc:
        breaker.finish(actor, "job-42", success=False, reason=str(exc))
    else:
        breaker.finish(actor, "job-42", success=True)
else:
    state = breaker.snapshot(actor)
    print(f"{state.pending_job} is still in flight; state={state.state}")

print("after the job:", breaker.snapshot(actor))
breaker.close()
```

### At-most-once requests

```python
from joons_agent_guard import ReplayGuard, request_digest

guard = ReplayGuard("guard.db")


def do_work(method, path, body):
    return {"echo": body.decode()}  # your real side effect goes here


def handle_request(principal, request_id, method, path, body):
    """Idempotent request handling: a retry gets the stored response."""
    digest = request_digest(method, path, body)
    outcome = guard.claim(principal, request_id, digest)

    if outcome == "completed":                     # a retry: replay the stored response
        return guard.response(principal, request_id, digest)
    if outcome == "pending":                       # a previous attempt is still in flight
        return (503, {"error": "previous attempt still in flight"})
    if outcome == "conflict":                      # same request_id, different payload
        return (409, {"error": "request_id reused with a different payload"})

    result = do_work(method, path, body)           # outcome == "new": run it once
    guard.complete(principal, request_id, digest, 200, result)
    return (200, result)


print(handle_request("client-a", "req-1", "POST", "/greet", b"hello"))
print(handle_request("client-a", "req-1", "POST", "/greet", b"hello"))  # replayed
guard.close()
```

## Guarantees and limits

This library is deliberately conservative. What it guarantees, and what it
deliberately does not:

- **Locks never expire.** A lock survives process restarts and crashes,
  because the holder's work may be in an unknown state. `release()` only works
  for the exact `(owner, job_id)` holder; `break_lock(resource,
  expected_job_id)` is the explicit recovery path for an operator who has
  checked that the old holder is dead.
- **Pending replay requests are never re-run automatically** and never unlock
  by age. `pending` means *unknown*: the previous caller may have died
  mid-request. Deciding to re-run (or not) is your integration's job.
- **Only `apply_pending` is exactly-once.** It runs a SQLite-only effect and
  stores the response in one transaction, so the effect and the recorded
  response commit together. If your effect touches files, the network, or
  subprocesses, `complete()` cannot be atomic with it: a crash between the
  effect and `complete()` leaves the request pending, and you must reconcile.
- **`reserve_outbound` must be called before sending.** It persists a stable
  `request_id` and timestamp per intent so retries reach the receiver as one
  request; reusing an intent with different content raises `IntentMismatch`.
  `fixed` is stored verbatim, so never put secrets or bodies in it — put a
  body hash there instead.
- **The library does not authenticate callers.** `principal`, `owner`, and
  `actor` values must come from a trusted transport (mTLS identity, a verified
  signature, ...). Different principals just get independent namespaces; that
  is isolation, not security.
- **Durability is one SQLite file.** Everything is only as durable as that
  file — back it up like any other database.

## API overview

| Component | What it gives you | Key calls |
| --- | --- | --- |
| `ResourceLock(path)` | Exclusive locks on named resources, shared across processes | `hold()`, `acquire()`, `release()`, `status()`, `list_locks()`, `break_lock()` |
| `CircuitBreaker(path)` | One run slot per actor; closed / open / half-open recovery | `begin()`, `finish()`, `snapshot()`, `reset()` |
| `ReplayGuard(path)` | At-most-once request handling; outbound idempotency keys | `claim()`, `complete()`, `response()`, `apply_pending()`, `reserve_outbound()` |
| `request_digest(...)` | Canonical SHA-256 digest of a request (method, path, body, extra fields) | used as the `digest` argument of `ReplayGuard` |
| Exceptions | All derive from `GuardError` | `LockHeld`, `InvalidIdentifier` (also a `ValueError`), `IntentMismatch` (also a `ValueError`) |

Every constructor takes an optional `timeout` (seconds) for how long SQLite
operations wait on a busy database.

## Platform notes

- All state lives in SQLite database files. The three components can share one
  file (separate tables) or use separate files.
- Put the database on a **local disk**. SQLite file locking is not reliable on
  network shares (NFS, SMB), and these primitives depend on that locking. Do
  not place the database on a network share.
- Concurrency model: each component owns one connection in autocommit mode and
  wraps every write in an explicit `BEGIN IMMEDIATE` transaction, so multiple
  processes on the same machine can use the same database file safely.
- Instances are thread-safe: every transaction, read, and `close()` holds an
  internal re-entrant lock, so threads sharing one instance never interleave
  inside a transaction (calls are serialized — use one instance per thread
  when you need read parallelism).

## Command line

Inspect and recover a guard database without writing code:

```bash
python -m joons_agent_guard --db guard.db locks
python -m joons_agent_guard --db guard.db pending --older-than 3600
python -m joons_agent_guard --db guard.db break-lock deploy-api --job-id job-42          # plan only, exit code 2
python -m joons_agent_guard --db guard.db break-lock deploy-api --job-id job-42 --yes     # really break it
```

`--json` switches to machine-readable output. The recovery commands
(`break-lock`, `reset-circuit`) never change anything unless `--yes` is
given. Exit codes: 0 ok, 1 not found / nothing changed, 2 confirmation
required or bad usage. See docs/api.md for the full reference.

## Documentation

- [docs/guarantees.md](docs/guarantees.md) — exactly what is and is not guaranteed, failure scenarios and recovery
- [docs/recipes.md](docs/recipes.md) — scheduled jobs, agent fleets, idempotent webhooks, safe retries
- [docs/faq.md](docs/faq.md) — common questions
- [docs/api.md](docs/api.md) — full API and CLI reference
- [docs/architecture.md](docs/architecture.md) — schema, transaction pattern, state diagrams
- [docs/mutation-testing.md](docs/mutation-testing.md) — how the test suite was checked against deliberate bugs

## Development

```bash
git clone https://github.com/dbdefault/joons-agent-guard.git
cd joons-agent-guard
pip install -e .[test]
python -m pytest
```

## License

Apache-2.0

---

Part of the JOONS open-source family.
