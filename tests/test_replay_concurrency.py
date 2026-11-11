"""T08: multi-process concurrency tests for joons_agent_guard.

Every test races 8 worker processes (multiprocessing ``spawn`` context) on one
SQLite file in ``tmp_path``. All worker functions are top-level so the suite
runs on Windows, where spawn re-imports them by name in a fresh interpreter.
"""
import multiprocessing as mp
import queue
import sqlite3
import time

import pytest

WORKERS = 8
# Generous budgets for slow CI runners: every wave boots 8 fresh 'spawn'
# interpreters, each re-importing this module (and pytest) in a new process.
COLLECT_TIMEOUT = 90.0  # seconds for all workers of one wave to report
JOIN_TIMEOUT = 30.0     # seconds for one process to exit


# ---------------------------------------------------------------------------
# Worker functions: top-level, spawn-picklable, exactly one queue message each.
# ---------------------------------------------------------------------------

def _claim_worker(db_path, principal, request_id, digest, q):
    try:
        from joons_agent_guard.replay import ReplayGuard
        with ReplayGuard(db_path) as guard:
            state = guard.claim(principal, request_id, digest)
        q.put(("ok", state))
    except BaseException as exc:  # surfaced in the parent, never swallowed
        q.put(("error", repr(exc)))


def _claim_then_response_worker(db_path, principal, request_id, digest, q):
    try:
        from joons_agent_guard.replay import ReplayGuard
        with ReplayGuard(db_path) as guard:
            state = guard.claim(principal, request_id, digest)
            got = guard.response(principal, request_id, digest)
        q.put(("ok", (state, got)))
    except BaseException as exc:
        q.put(("error", repr(exc)))


def _reserve_worker(db_path, intent_id, fixed, q):
    try:
        from joons_agent_guard.replay import ReplayGuard
        with ReplayGuard(db_path) as guard:
            context = guard.reserve_outbound(intent_id, fixed)
        q.put(("ok", context))
    except BaseException as exc:
        q.put(("error", repr(exc)))


def _apply_pending_worker(db_path, principal, request_id, digest, q):
    try:
        from joons_agent_guard.replay import ReplayGuard

        def bump(conn):
            conn.execute("UPDATE counter SET n = n + 1")
            return 200, "applied-once"

        with ReplayGuard(db_path) as guard:
            outcome = guard.apply_pending(principal, request_id, digest, bump)
        q.put(("ok", outcome))  # (200, "applied-once") or None
    except BaseException as exc:
        q.put(("error", repr(exc)))


def _circuit_begin_worker(db_path, actor, job_id, q):
    try:
        from joons_agent_guard.circuit import CircuitBreaker
        with CircuitBreaker(db_path) as breaker:
            won = breaker.begin(actor, job_id)
        q.put(("ok", (won, job_id)))
    except BaseException as exc:
        q.put(("error", repr(exc)))


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------

def _spawn_wave(target, arg_lists):
    """Start one spawn-context process per arg list; return their payloads.

    Appends ``q`` to every arg list; each worker must put one
    ``("ok", payload)`` or ``("error", repr(exc))`` message.
    """
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    procs = [ctx.Process(target=target, args=(*args, q)) for args in arg_lists]
    for p in procs:
        p.start()
    collected = []
    problems = []
    deadline = time.monotonic() + COLLECT_TIMEOUT
    for _ in procs:
        remaining = max(0.0, deadline - time.monotonic())
        try:
            collected.append(q.get(timeout=remaining))
        except queue.Empty:
            problems.append("timed out collecting worker results")
            break
    for p in procs:
        p.join(JOIN_TIMEOUT)
        if p.is_alive():
            p.terminate()
            p.join(5.0)
            problems.append(f"process pid={p.pid} did not exit in time; terminated")
        elif p.exitcode != 0:
            problems.append(f"process pid={p.pid} exited with code {p.exitcode}")
    errors = [msg for kind, msg in collected if kind == "error"]
    if problems or errors or len(collected) != len(procs):
        pytest.fail(
            f"worker problems={problems!r} errors={errors!r} "
            f"collected={len(collected)}/{len(procs)} payloads={collected!r}"
        )
    return [payload for _, payload in collected]


def _counter_setup(db_path):
    con = sqlite3.connect(db_path, timeout=15.0)
    try:
        con.execute("CREATE TABLE IF NOT EXISTS counter (n INTEGER NOT NULL)")
        con.execute("DELETE FROM counter")
        con.execute("INSERT INTO counter (n) VALUES (0)")
        con.commit()
    finally:
        con.close()


def _counter_value(db_path):
    con = sqlite3.connect(db_path, timeout=15.0)
    try:
        return con.execute("SELECT n FROM counter").fetchone()[0]
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_claim_race_yields_one_new_then_all_completed(tmp_path):
    """8 processes claim the same key: exactly one 'new', the rest 'pending';
    after completion, 8 concurrent claims all see 'completed' with an
    identical response()."""
    from joons_agent_guard.replay import ReplayGuard, request_digest

    db_path = str(tmp_path / "guard.sqlite")
    principal, request_id = "svc-alpha", "req-claim-1"
    digest = request_digest("POST", "/payments", b"{}", tenant="t1")

    states = _spawn_wave(
        _claim_worker, [(db_path, principal, request_id, digest)] * WORKERS
    )
    assert sorted(states) == ["new"] + ["pending"] * (WORKERS - 1)

    with ReplayGuard(db_path) as guard:
        assert guard.complete(principal, request_id, digest, 200, {"ok": True, "n": 7})

    outcomes = _spawn_wave(
        _claim_then_response_worker,
        [(db_path, principal, request_id, digest)] * WORKERS,
    )
    assert [state for state, _ in outcomes] == ["completed"] * WORKERS
    assert all(got == (200, {"ok": True, "n": 7}) for _, got in outcomes)


def test_reserve_outbound_race_returns_identical_context(tmp_path):
    """8 processes reserve the same intent_id with the same fixed content and
    all receive the identical persisted context."""
    from joons_agent_guard.replay import ReplayGuard

    db_path = str(tmp_path / "outbound.sqlite")
    fixed = {
        "endpoint": "payments.example.internal",
        "method": "POST",
        "body_sha256": "ab" * 32,
    }
    contexts = _spawn_wave(
        _reserve_worker, [(db_path, "intent-send-1", fixed)] * WORKERS
    )
    first = contexts[0]
    assert all(context == first for context in contexts)
    assert set(first) == set(fixed) | {"request_id", "ts"}
    assert len(first["request_id"]) == 32
    for key, value in fixed.items():
        assert first[key] == value
    with ReplayGuard(db_path) as guard:
        assert guard.reserve_outbound("intent-send-1", fixed) == first


def test_apply_pending_effect_runs_exactly_once(tmp_path):
    """8 processes race apply_pending on one pending request: the counter
    effect runs exactly once and the losers get None."""
    from joons_agent_guard.replay import ReplayGuard, request_digest

    db_path = str(tmp_path / "apply.sqlite")
    _counter_setup(db_path)
    principal, request_id = "svc-beta", "req-apply-1"
    digest = request_digest("POST", "/increment", b"{}")

    with ReplayGuard(db_path) as guard:
        assert guard.claim(principal, request_id, digest) == "new"

    outcomes = _spawn_wave(
        _apply_pending_worker, [(db_path, principal, request_id, digest)] * WORKERS
    )
    applied = [o for o in outcomes if o is not None]
    skipped = [o for o in outcomes if o is None]
    assert len(applied) == 1 and applied[0] == (200, "applied-once")
    assert len(skipped) == WORKERS - 1
    assert _counter_value(db_path) == 1
    with ReplayGuard(db_path) as guard:
        assert guard.response(principal, request_id, digest) == (200, "applied-once")


def test_circuit_begin_race_admits_one(tmp_path):
    """8 processes race CircuitBreaker.begin for the same actor: exactly one
    wins the run slot, and finishing it frees the slot again."""
    from joons_agent_guard.circuit import CircuitBreaker

    db_path = str(tmp_path / "circuit.sqlite")
    actor = "worker-gamma"
    results = _spawn_wave(
        _circuit_begin_worker,
        [(db_path, actor, f"job-{i}") for i in range(WORKERS)],
    )
    winners = [job for won, job in results if won]
    losers = [job for won, job in results if not won]
    assert len(winners) == 1 and len(losers) == WORKERS - 1

    with CircuitBreaker(db_path) as breaker:
        snap = breaker.snapshot(actor)
        assert snap.pending_job == winners[0]
        assert snap.state in ("closed", "half_open")
        assert breaker.finish(actor, winners[0], success=True)
        snap = breaker.snapshot(actor)
        assert snap.state == "closed" and snap.pending_job is None
        assert breaker.begin(actor, "job-next")
