"""Regression tests for the aio cancellation fixes (G1 and G6 of
aio_review/AIO_CANCEL_REVIEW.md), built from the scripts under
aio_review/repro_aio/. Plain pytest functions drive the adapters with
``asyncio.run`` (no pytest-asyncio).

* G1 — a cancelled ``hold()`` ENTRY used to leave the lock held (the body
  never ran while the worker-thread acquire committed). Fixed: the entry
  shields its own acquire, waits for it to land on cancellation, releases
  the lock when it was acquired, and re-raises CancelledError.
* G6 — ``aclose()`` used to close while a call was still parked before its
  guarded transaction, breaking that call with a raw
  ``sqlite3.ProgrammingError``. Fixed: the adapter tracks in-flight calls
  and ``aclose()`` waits for them to finish before closing.
"""
import asyncio
import contextlib
import os
import sqlite3
import sys
import threading

_SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if os.path.isdir(_SRC) and _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from joons_agent_guard import AsyncReplayGuard, AsyncResourceLock, request_digest


# ---------------------------------------------------------------------------
# G1: cancelled hold() entry
# ---------------------------------------------------------------------------

def test_g1_cancelled_hold_entry_releases_the_acquired_lock(tmp_path, monkeypatch):
    async def main():
        lock = AsyncResourceLock(str(tmp_path / "g.db"))
        loop = asyncio.get_running_loop()
        orig = lock._sync.acquire
        acquire_started = asyncio.Event()
        proceed = threading.Event()

        def slow_acquire(resource, owner, job_id):
            # park the worker thread mid-acquire so the cancel lands there
            loop.call_soon_threadsafe(acquire_started.set)
            proceed.wait(5)
            return orig(resource, owner, job_id)

        monkeypatch.setattr(lock._sync, "acquire", slow_acquire)
        entered = {"body": False}

        async def body():
            async with lock.hold("res", "owner-1", "job-1"):
                entered["body"] = True

        task = asyncio.create_task(body())
        await asyncio.wait_for(acquire_started.wait(), timeout=5)
        task.cancel()                    # cancel while hold() is acquiring
        proceed.set()                    # let the worker-thread acquire land
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=5)
        info = await lock.status("res")  # may need the landed commit: query once more
        if info is not None:
            await asyncio.sleep(0.05)    # the detached commit lands; poll once
            info = await lock.status("res")
        await lock.aclose()
        return entered["body"], info

    entered, info = asyncio.run(main())
    assert entered is False, "the body must never run when the entry is cancelled"
    assert info is None, "a cancelled hold entry must not leave the lock held"

    async def again():
        async with AsyncResourceLock(str(tmp_path / "g.db")) as lock:
            return await lock.acquire("res", "owner-2", "job-2")

    assert asyncio.run(again()) is True    # the resource is free for the next holder


# ---------------------------------------------------------------------------
# G6: aclose() waits for in-flight calls (even ones parked before their
# guarded transaction) instead of breaking them with a closed database
# ---------------------------------------------------------------------------

def test_g6_aclose_waits_for_a_parked_inflight_call(tmp_path, monkeypatch):
    async def main():
        guard = AsyncReplayGuard(str(tmp_path / "g.db"))
        loop = asyncio.get_running_loop()
        orig = guard._sync.claim
        claim_started = asyncio.Event()
        proceed = threading.Event()

        def parked_claim(principal, request_id, digest):
            # park the worker thread before the guarded transaction
            loop.call_soon_threadsafe(claim_started.set)
            proceed.wait(5)
            return orig(principal, request_id, digest)

        monkeypatch.setattr(guard._sync, "claim", parked_claim)
        digest = request_digest("POST", "/x", b"1")
        task = asyncio.create_task(guard.claim("p", "req-1", digest))
        await asyncio.wait_for(claim_started.wait(), timeout=5)
        close_task = asyncio.create_task(guard.aclose())  # must wait, not break
        proceed.set()
        state = await asyncio.wait_for(task, timeout=5)
        await asyncio.wait_for(close_task, timeout=5)
        # calls started after aclose() began raise the documented error
        post = None
        try:
            await guard.claim("p", "req-2", request_digest("POST", "/x", b"2"))
        except sqlite3.ProgrammingError as exc:
            post = exc
        return state, post

    state, post = asyncio.run(main())
    assert state == "new", "the parked in-flight call must finish instead of breaking"
    assert isinstance(post, sqlite3.ProgrammingError), (
        "calls started after aclose() began must raise the documented error")
