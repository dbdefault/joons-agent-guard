"""Thread-safety investigation: one shared instance vs one instance per thread.

The components open their sqlite3 connection with ``check_same_thread=False``,
so sharing ONE instance (and therefore ONE connection) across threads is
possible.  Each race runs 8 threads for 2 seconds and checks:

* the mutual-exclusion invariant actually held (no two threads hold the
  resource/slot at once, a begun job always finishes, a claimed request
  always completes);
* no unexpected exception was raised (``LockHeld``-style contention results
  such as ``begin() -> False`` or ``acquire() -> False`` are normal and not
  counted).

Part (a): one shared ResourceLock (acquire/release) on one DB file.
Part (b): one shared CircuitBreaker (begin/finish) and one shared ReplayGuard
          (claim/complete), same race shape.
Part (c): the same races with one instance PER thread on the same DB file.

Every write goes through ``_db.write_txn`` and every read through
``_db.query_one/query_all``; both hold the connection's re-entrant guard lock,
so a shared instance is safe across threads. Before that lock existed, the
shared-instance tests failed 5/5 runs with ``cannot start a transaction within
a transaction``. The per-thread tests rely on SQLite's file locking.
"""
import threading
import time

import pytest

from joons_agent_guard import CircuitBreaker, ReplayGuard, ResourceLock
from joons_agent_guard.replay import NEW, request_digest

THREADS = 8
RACE_SECONDS = 2.0



class _Monitor:
    """Collects invariant violations and unexpected exceptions from workers."""

    def __init__(self):
        self._lock = threading.Lock()
        self.violations = []
        self.errors = []

    def violation(self, message):
        with self._lock:
            self.violations.append(message)

    def error(self, worker_id, exc):
        with self._lock:
            self.errors.append("worker %d: %r" % (worker_id, exc))


def _report(monitor):
    lines = ["violations=%d errors=%d" % (len(monitor.violations), len(monitor.errors))]
    lines.extend("  " + m for m in monitor.violations[:3] + monitor.errors[:3])
    return "\n".join(lines)


def _assert_clean(monitor):
    problems = monitor.violations + monitor.errors
    assert not problems, "%d problem(s):\n%s" % (len(problems), _report(monitor))


def start_workers(worker_fn, count=THREADS):
    monitor = _Monitor()
    gate = threading.Barrier(count)
    deadline = time.monotonic() + RACE_SECONDS
    threads = [
        threading.Thread(target=worker_fn, args=(i, gate, deadline, monitor), daemon=True)
        for i in range(count)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return monitor


def _race_worker(worker_id, start_gate, deadline, monitor, claim, use):
    """Loop until the deadline: claim the slot, verify exclusivity, use, release.

    ``claim()`` -> truthy means this worker now exclusively holds the shared
    thing; ``use()`` must then succeed while holding it.  A falsy claim is a
    normal contention outcome (like LockHeld / begin()->False).
    """
    owner = "worker-%d" % worker_id
    start_gate.wait()
    holds = 0
    while time.monotonic() < deadline:
        try:
            if not claim(owner):
                continue  # someone else holds it: normal contention outcome
        except Exception as exc:  # noqa: BLE001 - any exception is a finding
            monitor.error(worker_id, exc)
            return
        holds += 1
        try:
            if not use(owner):
                monitor.violation("%s: use() failed while holding the slot (hold #%d)" % (owner, holds))
                return
        except Exception as exc:  # noqa: BLE001
            monitor.error(worker_id, exc)
            return


# ---------------------------------------------------------------------------
# (a) one shared ResourceLock instance
# ---------------------------------------------------------------------------

def test_shared_resource_lock_single_instance(tmp_path):
    lock = ResourceLock(tmp_path / "shared.db")
    held = {"lock": threading.Lock(), "count": 0}

    def claim(owner):
        return lock.acquire("the-resource", owner, owner + "-job")

    def use(owner):
        with held["lock"]:
            held["count"] += 1
            if held["count"] > 1:
                held["count"] -= 1
                return "TWO_HOLDERS %s" % owner
            released = lock.release("the-resource", owner, owner + "-job")
            held["count"] -= 1
        if not released:
            return "release returned False while holding"
        return True

    try:
        monitor = start_workers(lambda i, gate, deadline, mon: _race_worker(i, gate, deadline, mon, claim, use))
        _assert_clean(monitor)
    finally:
        lock.close()


# ---------------------------------------------------------------------------
# (b) one shared CircuitBreaker / ReplayGuard instance
# ---------------------------------------------------------------------------

def test_shared_circuit_breaker_single_instance(tmp_path):
    cb = CircuitBreaker(tmp_path / "shared.db")
    actor = "shared-actor"
    held = {"lock": threading.Lock(), "count": 0}

    def claim(owner):
        return cb.begin(actor, owner + "-job")

    def use(owner):
        with held["lock"]:
            held["count"] += 1
            if held["count"] > 1:
                held["count"] -= 1
                return "TWO_HOLDERS %s" % owner
            finished = cb.finish(actor, owner + "-job", success=True)
            held["count"] -= 1
        if not finished:
            return "finish returned False for the job this worker just began"
        return True

    try:
        monitor = start_workers(lambda i, gate, deadline, mon: _race_worker(i, gate, deadline, mon, claim, use))
        _assert_clean(monitor)
    finally:
        cb.close()


def test_shared_replay_guard_single_instance(tmp_path):
    guard = ReplayGuard(tmp_path / "shared.db")
    principal = "svc"
    digest = request_digest("POST", "/jobs", b"")
    counters = {"lock": threading.Lock(), "next": 0}

    def claim(owner):
        with counters["lock"]:
            counters["next"] += 1
            request_id = "req-%d" % counters["next"]
        decision = guard.claim(principal, request_id, digest)
        if decision != NEW:
            return "claim(%s) returned %r, expected 'new'" % (request_id, decision)
        return request_id

    def use(request_id):
        if not guard.complete(principal, request_id, digest, 200, {"done": True}):
            return "complete(%s) returned False after a 'new' claim" % request_id
        return True

    try:
        monitor = start_workers(lambda i, gate, deadline, mon: _race_worker(i, gate, deadline, mon, claim, use))
        _assert_clean(monitor)
    finally:
        guard.close()


# ---------------------------------------------------------------------------
# (c) one instance PER thread, same DB file
# ---------------------------------------------------------------------------

def test_per_thread_resource_lock_same_file(tmp_path):
    path = tmp_path / "perthread.db"
    held = {"lock": threading.Lock(), "count": 0}

    def worker(worker_id, start_gate, deadline, monitor):
        lock = ResourceLock(path)  # one instance per thread
        try:
            def claim(owner):
                return lock.acquire("the-resource", owner, owner + "-job")

            def use(owner):
                with held["lock"]:
                    held["count"] += 1
                    if held["count"] > 1:
                        held["count"] -= 1
                        return "TWO_HOLDERS %s" % owner
                    released = lock.release("the-resource", owner, owner + "-job")
                    held["count"] -= 1
                if not released:
                    return "release returned False while holding"
                return True

            _race_worker(worker_id, start_gate, deadline, monitor, claim, use)
        finally:
            lock.close()

    _assert_clean(start_workers(worker))


def test_per_thread_circuit_breaker_same_file(tmp_path):
    path = tmp_path / "perthread.db"
    actor = "shared-actor"
    held = {"lock": threading.Lock(), "count": 0}

    def worker(worker_id, start_gate, deadline, monitor):
        cb = CircuitBreaker(path)  # one instance per thread
        try:
            def claim(owner):
                return cb.begin(actor, owner + "-job")

            def use(owner):
                with held["lock"]:
                    held["count"] += 1
                    if held["count"] > 1:
                        held["count"] -= 1
                        return "TWO_HOLDERS %s" % owner
                    finished = cb.finish(actor, owner + "-job", success=True)
                    held["count"] -= 1
                if not finished:
                    return "finish returned False for the job this worker just began"
                return True

            _race_worker(worker_id, start_gate, deadline, monitor, claim, use)
        finally:
            cb.close()

    _assert_clean(start_workers(worker))


def test_per_thread_replay_guard_same_file(tmp_path):
    path = tmp_path / "perthread.db"
    principal = "svc"
    digest = request_digest("POST", "/jobs", b"")
    counters = {"lock": threading.Lock(), "next": 0}

    def worker(worker_id, start_gate, deadline, monitor):
        guard = ReplayGuard(path)  # one instance per thread
        try:
            def claim(owner):
                with counters["lock"]:
                    counters["next"] += 1
                    request_id = "req-%d" % counters["next"]
                decision = guard.claim(principal, request_id, digest)
                if decision != NEW:
                    return "claim(%s) returned %r, expected 'new'" % (request_id, decision)
                return request_id

            def use(request_id):
                if not guard.complete(principal, request_id, digest, 200, {"done": True}):
                    return "complete(%s) returned False after a 'new' claim" % request_id
                return True

            _race_worker(worker_id, start_gate, deadline, monitor, claim, use)
        finally:
            guard.close()

    _assert_clean(start_workers(worker))

    # at-most-once for ONE request id under racing: exactly one 'new'
    gate = threading.Barrier(THREADS)
    decisions = {"lock": threading.Lock(), "list": []}

    def one_shot(worker_id, start_gate, deadline, monitor):
        guard = ReplayGuard(path)
        try:
            start_gate.wait()
            decision = guard.claim(principal, "one-shot-request", digest)
            with decisions["lock"]:
                decisions["list"].append(decision)
        finally:
            guard.close()

    _assert_clean(start_workers(one_shot))
    new_count = decisions["list"].count(NEW)
    assert new_count == 1, "claim returned 'new' %d times for one request id: %r" % (
        new_count, decisions["list"])
