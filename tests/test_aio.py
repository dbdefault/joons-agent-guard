"""Asyncio adapter tests (AsyncResourceLock / AsyncCircuitBreaker / AsyncReplayGuard).

Plain pytest functions drive the adapters with ``asyncio.run`` (no
pytest-asyncio plugin). Covered:

* 20 tasks race ``acquire`` on ONE shared adapter — exactly one winner;
* ``hold`` (async context manager) releases even when the body raises, and
  raises LockHeld when the resource is busy;
* ``AsyncReplayGuard`` claim/complete round trip and ``apply_pending`` effect;
* the event loop stays responsive while a blocking write waits on a busy
  database held by a raw ``sqlite3`` connection (busy timeout 0.3 s): a ticker
  coroutine keeps running until the raw connection commits and the write lands.
"""
import asyncio
import contextlib
import os
import sqlite3
import sys

import pytest

# Make the in-tree package importable here even when the suite is run without
# PYTHONPATH set.
_SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if os.path.isdir(_SRC) and _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from joons_agent_guard import (
    AsyncCircuitBreaker,
    AsyncReplayGuard,
    AsyncResourceLock,
    LockHeld,
    request_digest,
)


# ---------------------------------------------------------------------------
# ResourceLock adapter
# ---------------------------------------------------------------------------

def test_twenty_tasks_race_acquire_exactly_one_winner(tmp_path):
    async def main():
        async with AsyncResourceLock(str(tmp_path / "guard.db")) as lock:
            results = await asyncio.gather(*[
                lock.acquire("the-resource", "owner-%d" % i, "job-%d" % i)
                for i in range(20)
            ])
            return results

    results = asyncio.run(main())
    assert sum(1 for won in results if won) == 1
    assert all(isinstance(won, bool) for won in results)


def test_hold_releases_on_exception(tmp_path):
    async def main():
        async with AsyncResourceLock(str(tmp_path / "guard.db")) as lock:
            raised = False
            try:
                async with lock.hold("res", "owner-1", "job-1"):
                    raise RuntimeError("boom")
            except RuntimeError:
                raised = True
            assert raised
            # the async context manager must have released on the way out
            assert await lock.acquire("res", "owner-2", "job-2") is True
            assert await lock.release("res", "owner-2", "job-2") is True

    asyncio.run(main())


def test_hold_raises_lock_held_when_busy(tmp_path):
    async def main():
        async with AsyncResourceLock(str(tmp_path / "guard.db")) as lock:
            assert await lock.acquire("res", "owner-1", "job-1") is True
            with pytest.raises(LockHeld) as excinfo:
                async with lock.hold("res", "owner-2", "job-2"):
                    pass  # must never run
            assert excinfo.value.holder.job_id == "job-1"

    asyncio.run(main())


def test_status_list_locks_break_lock(tmp_path):
    async def main():
        async with AsyncResourceLock(str(tmp_path / "guard.db")) as lock:
            assert await lock.acquire("res-a", "owner-1", "job-1") is True
            info = await lock.status("res-a")
            assert (info.owner, info.job_id) == ("owner-1", "job-1")
            assert [entry.resource for entry in await lock.list_locks()] == ["res-a"]
            assert await lock.break_lock("res-a", expected_job_id="job-1") is True
            assert await lock.status("res-a") is None

    asyncio.run(main())


# ---------------------------------------------------------------------------
# CircuitBreaker adapter
# ---------------------------------------------------------------------------

def test_circuit_breaker_begin_finish_snapshot_reset(tmp_path):
    async def main():
        async with AsyncCircuitBreaker(str(tmp_path / "guard.db")) as breaker:
            assert await breaker.begin("worker", "job-1") is True
            assert await breaker.begin("worker", "job-2") is False  # slot busy
            snap = await breaker.snapshot("worker")
            assert snap.pending_job == "job-1"
            assert await breaker.finish("worker", "job-1", success=False, reason="boom") is True
            snap = await breaker.snapshot("worker")
            assert snap.state == "open"
            assert snap.consecutive_failures == 1
            assert snap.last_failure_reason == "boom"
            # the next begin is the recovery attempt and lands in half_open
            assert await breaker.begin("worker", "job-3") is True
            assert (await breaker.snapshot("worker")).state == "half_open"
            assert await breaker.reset("worker", expected_job_id="job-3") is True
            assert (await breaker.snapshot("worker")).state == "closed"

    asyncio.run(main())


# ---------------------------------------------------------------------------
# ReplayGuard adapter
# ---------------------------------------------------------------------------

def test_replay_claim_complete_round_trip(tmp_path):
    async def main():
        async with AsyncReplayGuard(str(tmp_path / "guard.db")) as guard:
            digest = request_digest("POST", "/work", b"1")
            assert await guard.claim("client-a", "req-1", digest) == "new"
            assert await guard.claim("client-a", "req-1", digest) == "pending"
            assert await guard.complete("client-a", "req-1", digest, 200, {"ok": True}) is True
            assert await guard.claim("client-a", "req-1", digest) == "completed"
            assert await guard.response("client-a", "req-1", digest) == (200, {"ok": True})

    asyncio.run(main())


def test_apply_pending_runs_effect_exactly_once(tmp_path):
    async def main():
        async with AsyncReplayGuard(str(tmp_path / "guard.db")) as guard:
            digest = request_digest("POST", "/work", b"1")
            assert await guard.claim("client-a", "req-1", digest) == "new"

            def effect(conn):
                conn.execute("CREATE TABLE IF NOT EXISTS effect_log (note TEXT NOT NULL)")
                conn.execute("INSERT INTO effect_log(note) VALUES ('done')")
                return 201, {"applied": True}

            assert await guard.apply_pending(
                "client-a", "req-1", digest, effect) == (201, {"applied": True})
            # second call sees the completed state and never re-runs the effect
            assert await guard.apply_pending("client-a", "req-1", digest, effect) is None
            assert await guard.response("client-a", "req-1", digest) == (201, {"applied": True})

    asyncio.run(main())


def test_reserve_outbound_returns_identical_context(tmp_path):
    async def main():
        async with AsyncReplayGuard(str(tmp_path / "guard.db")) as guard:
            first = await guard.reserve_outbound("intent-1", {"a": "1"})
            again = await guard.reserve_outbound("intent-1", {"a": "1"})
            assert again == first
            assert first["request_id"] and first["ts"]

    asyncio.run(main())


# ---------------------------------------------------------------------------
# the event loop is not blocked by a waiting write
# ---------------------------------------------------------------------------

def test_event_loop_keeps_running_while_write_waits_on_busy_db(tmp_path):
    path = str(tmp_path / "guard.db")
    lock = AsyncResourceLock(path, timeout=0.3)  # busy timeout 300 ms
    blocker = sqlite3.connect(path, isolation_level=None, check_same_thread=False)

    async def main():
        # a raw connection holds the SQLite write lock across the whole race
        blocker.execute("BEGIN IMMEDIATE")
        ticks = []

        async def ticker():
            while True:
                ticks.append("tick")
                await asyncio.sleep(0.02)

        try:
            ticker_task = asyncio.create_task(ticker())
            try:
                await asyncio.sleep(0.05)        # the ticker is running
                write_task = asyncio.create_task(lock.acquire("res", "owner-1", "job-1"))
                await asyncio.sleep(0.1)         # the write now waits on the busy DB
                # commit the raw connection from a worker thread; the waiting
                # write lands once SQLite hands the write lock over
                await asyncio.to_thread(blocker.commit)
                won = await asyncio.wait_for(write_task, timeout=5.0)
            finally:
                ticker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await ticker_task
            return won, len(ticks)
        finally:
            await lock.aclose()

    try:
        won, ticks = asyncio.run(main())
    finally:
        blocker.close()

    assert won is True, "the write must succeed once the busy DB is committed"
    assert ticks >= 3, (
        "the event loop stopped running while a write waited on the busy DB "
        "(%d ticks during the wait)" % ticks
    )
