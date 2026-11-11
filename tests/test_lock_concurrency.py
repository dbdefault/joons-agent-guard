"""Multi-process and multi-thread concurrency tests for ResourceLock.

Every test shares one SQLite database file inside pytest's ``tmp_path`` so
all workers (processes and threads) contend on the exact same lock table.

Multiprocessing uses the ``spawn`` start method with module-level worker
functions: spawn is the only start method on Windows and it can only pickle
targets that are importable by qualified name, so no closures or lambdas may
be used as process targets. All joins use bounded deadlines so a hung worker
turns into a test failure instead of blocking the suite forever.
"""
import multiprocessing
import os
import queue as queue_mod
import sys
import threading
import time

# Make the in-tree package importable here and in every spawned child, even
# when the suite is run without PYTHONPATH set (children re-import this
# module, so the bootstrap runs on their side too).
_SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if os.path.isdir(_SRC) and _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pytest

from joons_agent_guard import ResourceLock

MP = multiprocessing.get_context("spawn")

# Wall-clock budget per test. Generous enough for slow CI machines (each test
# boots up to 8 fresh 'spawn' interpreters, which is slow on 2-core runners),
# but a hang or crash must surface as a failure, not an infinite wait.
TEST_DEADLINE_SECONDS = 40.0
BARRIER_TIMEOUT = 30.0
# Tolerance for the no-overlap check: a genuine lock bug makes two holders
# overlap by at least HOLD_SECONDS, far above this epsilon, which only
# absorbs small wall-clock backward jitter between processes.
OVERLAP_EPSILON = 0.001
HOLD_SECONDS = 0.01


# ---------------------------------------------------------------------------
# worker functions (module level: spawn pickles them by qualified name)
# ---------------------------------------------------------------------------

def _race_one_worker(db_path, resource, barrier, results):
    """All workers attempt the same resource at the same instant.

    The single winner keeps holding for a moment so every peer's one-shot
    attempt lands while the lock is held, then releases and reports the
    outcome of its own attempt.
    """
    try:
        owner = "proc-%d" % os.getpid()
        job_id = owner + "-job"
        with ResourceLock(db_path) as lock:
            barrier.wait(timeout=BARRIER_TIMEOUT)
            won = lock.acquire(resource, owner, job_id)
            if won:
                time.sleep(0.5)
                lock.release(resource, owner, job_id)
        results.put(("ok", owner, won))
    except BaseException as exc:  # report worker crashes instead of losing them
        results.put(("error", "%s: %s" % (type(exc).__name__, exc)))


def _race_distinct_worker(db_path, resource, results):
    """Each worker targets its own resource, so every worker must win."""
    try:
        owner = "proc-%d" % os.getpid()
        job_id = owner + "-job"
        with ResourceLock(db_path) as lock:
            won = lock.acquire(resource, owner, job_id)
            if won:
                lock.release(resource, owner, job_id)
        results.put(("ok", resource, won))
    except BaseException as exc:
        results.put(("error", "%s: %s" % (type(exc).__name__, exc)))


def _abandon_worker(db_path, resource):
    """Acquire a lock and exit without releasing it."""
    with ResourceLock(db_path) as lock:
        if not lock.acquire(resource, "abandoned-owner", "abandoned-job"):
            raise AssertionError("worker could not acquire the lock it is supposed to abandon")
    # Returning lets the process die while the lock row survives on disk.


def _thread_race_worker(db_path, resource, barrier, results, index):
    """Each thread uses its own ResourceLock instance on the shared file."""
    try:
        owner = "thread-%d" % index
        job_id = "job-%d" % index
        with ResourceLock(db_path) as lock:
            barrier.wait(timeout=BARRIER_TIMEOUT)
            won = lock.acquire(resource, owner, job_id)
            if won:
                time.sleep(0.2)
                lock.release(resource, owner, job_id)
        results.put(("ok", owner, won))
    except BaseException as exc:
        results.put(("error", "%s: %s" % (type(exc).__name__, exc)))


def _cycle_worker(db_path, resource, cycles, results):
    """Run acquire/hold/release cycles and report one interval per hold.

    Each interval spans from just after a successful acquire to just before
    the matching release. If two workers ever held the resource at the same
    time, their recorded intervals overlap.
    """
    try:
        owner = "proc-%d" % os.getpid()
        intervals = []
        with ResourceLock(db_path) as lock:
            for i in range(cycles):
                job_id = "%s-cycle-%d" % (owner, i)
                deadline = time.time() + TEST_DEADLINE_SECONDS
                while not lock.acquire(resource, owner, job_id):
                    if time.time() > deadline:
                        raise RuntimeError("%s: could not acquire %r for cycle %d" % (owner, resource, i))
                    time.sleep(0.002)
                start = time.time()
                time.sleep(HOLD_SECONDS)  # a broken lock would produce visible overlap
                end = time.time()
                if not lock.release(resource, owner, job_id):
                    raise RuntimeError("%s: release failed for cycle %d" % (owner, i))
                intervals.append({"owner": owner, "job_id": job_id, "start": start, "end": end})
        results.put(("ok", owner, intervals))
    except BaseException as exc:
        results.put(("error", "%s: %s" % (type(exc).__name__, exc)))


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _join_processes(procs, deadline):
    """Join every process before ``deadline``; fail (after terminating) on hangs or crashes."""
    for proc in procs:
        proc.join(max(1.0, deadline - time.time()))
    hung = [proc for proc in procs if proc.is_alive()]
    for proc in hung:
        proc.terminate()
        proc.join(5.0)
    if hung:
        pytest.fail("worker(s) hung and were terminated: %s" % ", ".join(p.name for p in hung))
    crashed = [(p.name, p.exitcode) for p in procs if p.exitcode not in (0, None)]
    if crashed:
        pytest.fail("worker(s) crashed: %s" % crashed)


def _collect(results, count):
    """Drain ``count`` records; treat missing records and worker errors as failures."""
    records = []
    for _ in range(count):
        try:
            records.append(results.get(timeout=20.0))
        except queue_mod.Empty:
            pytest.fail("only %d of %d workers reported a result" % (len(records), count))
    errors = [r for r in records if r[0] == "error"]
    if errors:
        pytest.fail("worker error(s): %s" % errors)
    return records


def _spawn(target, args, name):
    proc = MP.Process(target=target, args=args, name=name)
    proc.start()
    return proc


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_eight_processes_race_for_one_resource_exactly_one_wins(tmp_path):
    db_path = str(tmp_path / "race-one.sqlite")
    resource = "shared-resource"
    results = MP.Queue()
    barrier = MP.Barrier(8)
    procs = [
        _spawn(_race_one_worker, (db_path, resource, barrier, results), "race-one-%d" % i)
        for i in range(8)
    ]
    _join_processes(procs, time.time() + TEST_DEADLINE_SECONDS)

    records = _collect(results, 8)
    winners = [owner for kind, owner, won in records if won]
    losers = [owner for kind, owner, won in records if not won]
    assert len(winners) == 1, "expected exactly one winner, got %r" % winners
    assert len(losers) == 7, "expected seven losers, got %r" % losers

    # After the winner released, the resource must be free and usable again.
    with ResourceLock(db_path) as checker:
        assert checker.status(resource) is None
        assert checker.acquire(resource, "checker", "after-race")
        assert checker.release(resource, "checker", "after-race")


def test_eight_processes_acquire_eight_distinct_resources_all_win(tmp_path):
    db_path = str(tmp_path / "race-distinct.sqlite")
    results = MP.Queue()
    procs = [
        _spawn(_race_distinct_worker, (db_path, "resource-%d" % i, results), "race-distinct-%d" % i)
        for i in range(8)
    ]
    _join_processes(procs, time.time() + TEST_DEADLINE_SECONDS)

    records = _collect(results, 8)
    won = [res for kind, res, succeeded in records if succeeded]
    assert sorted(won) == ["resource-%d" % i for i in range(8)], "every distinct resource must be won, got %r" % won
    with ResourceLock(db_path) as checker:
        assert checker.list_locks() == []


def test_lock_survives_holder_process_exit_without_release(tmp_path):
    db_path = str(tmp_path / "abandon.sqlite")
    resource = "orphaned-resource"
    proc = _spawn(_abandon_worker, (db_path, resource), "abandon-0")
    _join_processes([proc], time.time() + TEST_DEADLINE_SECONDS)

    # The holder is gone, but the lock must still be held: no silent expiry.
    time.sleep(1.0)
    with ResourceLock(db_path) as checker:
        info = checker.status(resource)
        assert info is not None, "lock vanished after holder process exited"
        assert info.owner == "abandoned-owner"
        assert info.job_id == "abandoned-job"
        assert [lock.resource for lock in checker.list_locks()] == [resource]
        assert not checker.acquire(resource, "checker", "must-not-win")

        # Explicit audited recovery works, and only then is the resource free.
        assert checker.break_lock(resource, "abandoned-job")
        assert checker.status(resource) is None
        assert checker.acquire(resource, "checker", "after-recovery")
        assert checker.release(resource, "checker", "after-recovery")


def test_threads_with_own_instances_race_for_one_resource_exactly_one_wins(tmp_path):
    db_path = str(tmp_path / "threads.sqlite")
    resource = "thread-resource"
    results = queue_mod.Queue()
    barrier = threading.Barrier(8)
    threads = [
        threading.Thread(
            target=_thread_race_worker,
            args=(db_path, resource, barrier, results, i),
            name="thread-worker-%d" % i,
        )
        for i in range(8)
    ]
    deadline = time.time() + TEST_DEADLINE_SECONDS
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(max(0.1, deadline - time.time()))
    hung = [t.name for t in threads if t.is_alive()]
    if hung:
        pytest.fail("thread worker(s) hung: %s" % ", ".join(hung))

    records = _collect(results, 8)
    winners = [owner for kind, owner, won in records if won]
    assert len(winners) == 1, "expected exactly one winner, got %r" % winners
    with ResourceLock(db_path) as checker:
        assert checker.status(resource) is None


def test_repeated_acquire_release_cycles_across_processes_never_overlap(tmp_path):
    db_path = str(tmp_path / "cycles.sqlite")
    resource = "cycle-resource"
    processes, cycles = 4, 10
    results = MP.Queue()
    procs = [
        _spawn(_cycle_worker, (db_path, resource, cycles, results), "cycle-%d" % i)
        for i in range(processes)
    ]
    _join_processes(procs, time.time() + TEST_DEADLINE_SECONDS)

    records = _collect(results, processes)
    intervals = []
    for kind, owner, worker_intervals in records:
        assert len(worker_intervals) == cycles, "%s completed %d cycles, expected %d" % (
            owner, len(worker_intervals), cycles)
        intervals.extend(worker_intervals)
    assert len(intervals) == processes * cycles

    # Every cycle held the lock exactly once: job ids must be unique.
    job_ids = [iv["job_id"] for iv in intervals]
    assert len(set(job_ids)) == len(job_ids), "a job id acquired the lock more than once"

    # No two holders may ever overlap on the shared resource.
    intervals.sort(key=lambda iv: (iv["start"], iv["end"]))
    previous = None
    for interval in intervals:
        assert interval["end"] >= interval["start"], "degenerate interval: %r" % interval
        if previous is not None:
            assert interval["start"] >= previous["end"] - OVERLAP_EPSILON, (
                "holders overlapped on %r: %s held [%s, %s] while %s held [%s, %s]" % (
                    resource,
                    previous["owner"], previous["start"], previous["end"],
                    interval["owner"], interval["start"], interval["end"],
                ))
        previous = interval

    # After all cycles the resource is free.
    with ResourceLock(db_path) as checker:
        assert checker.status(resource) is None
