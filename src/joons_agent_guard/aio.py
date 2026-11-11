"""Asyncio adapters: coroutine wrappers over the synchronous components.

Each adapter owns one synchronous instance (``ResourceLock``, ``CircuitBreaker``
or ``ReplayGuard``) and dispatches every blocking call with
:func:`asyncio.to_thread` (Python >= 3.9), so a contended SQLite write waits on
a worker thread instead of stalling the event loop. Method names and semantics
are unchanged — they become coroutines; ``hold`` becomes an async context
manager. Concurrent coroutines sharing one adapter are safe: the underlying
instance serialises its own statements with its re-entrant guard lock, exactly
as with threads. An ``apply_pending`` effect is a plain sync function and runs
on the worker thread inside the write transaction.

Cancellation: asyncio cannot abort a call that is already running on a worker
thread. Cancelling an in-flight adapter call therefore ends the task promptly
with ``CancelledError`` while the SQLite operation continues on the thread and
still commits or fails on its own. Know these consequences:

* The commit lands asynchronously: state you check immediately after the
  cancellation can still show the pre-call value. Query again once the worker
  thread has finished (or poll).
* A cancelled ``claim`` has committed the request as ``pending``. Claim the
  same ``(principal, request_id, digest)`` again — you get ``pending``; never
  mint a fresh request_id — do the work, then ``complete`` with the same
  digest.
* A cancelled ``apply_pending`` is recoverable in both branches: either it
  committed (the effect ran exactly once, ``response()`` serves the stored
  result, a re-apply returns ``None``) or it rolled back completely (the row
  stays ``pending`` and a retry re-runs the effect).
* An error the worker thread raises after the cancellation is discarded:
  query the state to learn the outcome instead of relying on the exception.
* A cancelled ``hold`` entry waits for its ``acquire`` to finish and releases
  the lock when it was acquired (the body never runs), so a cancelled entry
  never leaves the lock held; a cancelled ``hold`` exit always releases.

Lifecycle: ``await adapter.aclose()`` closes the underlying instance, and
``async with`` does the same on exit (``__aenter__`` returns the adapter).
``aclose()`` tracks in-flight calls and waits for them to finish before
closing; calls started after ``aclose()`` began raise
``sqlite3.ProgrammingError`` — the same error a closed connection raises.
Constructors are synchronous and create the schema on the calling thread;
construct adapters before entering the event loop when that matters.
"""
import asyncio
import contextlib
import functools
import sqlite3
import threading
from typing import Any, Callable, List, Mapping, Optional, Tuple

from .circuit import CircuitBreaker, CircuitState
from .errors import LockHeld
from .lock import LockInfo, ResourceLock
from .replay import ReplayGuard

__all__ = ["AsyncCircuitBreaker", "AsyncReplayGuard", "AsyncResourceLock"]


class _AsyncWrapper:
    """Shared lifecycle for the adapters: track in-flight calls, close safely."""

    _sync: Any  # the owned synchronous component instance

    def _init_tracking(self) -> None:
        self._cond = threading.Condition()
        self._inflight = 0
        self._closed = False

    def _enter_call(self) -> None:
        """Count one dispatched call; refuse calls that arrive after closing."""
        with self._cond:
            if self._closed:
                raise sqlite3.ProgrammingError(
                    "this adapter is closed; aclose() was already called")
            self._inflight += 1

    def _exit_call(self) -> None:
        with self._cond:
            self._inflight -= 1
            self._cond.notify_all()

    async def _dispatch(self, func, *args, **kwargs):
        """Run one blocking call on a worker thread, tracked for ``aclose``.

        The thread work is shielded: cancelling the caller never cancels a call
        that has not started yet, so every counted call always runs and counts
        itself down when the thread is really done, and ``aclose`` never closes
        under a running call.
        """
        @functools.wraps(func)
        def tracked(*a, **k):
            try:
                return func(*a, **k)
            finally:
                self._exit_call()

        self._enter_call()
        task = asyncio.ensure_future(asyncio.to_thread(tracked, *args, **kwargs))
        return await asyncio.shield(task)

    async def aclose(self) -> None:
        """Close the underlying synchronous instance.

        Waits for calls that were already in flight to finish first (G6): a
        call parked before its transaction is never broken by the close.
        Safe to call more than once.
        """
        with self._cond:
            already_closed = self._closed
            self._closed = True
        if already_closed:
            return
        while True:                          # drain in-flight calls (tracked)
            with self._cond:
                if self._inflight == 0:
                    break
            await asyncio.sleep(0.01)
        await asyncio.to_thread(self._sync.close)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc) -> None:
        await self.aclose()


class AsyncResourceLock(_AsyncWrapper):
    """Coroutine wrapper around :class:`~joons_agent_guard.ResourceLock`."""

    def __init__(self, path, *, timeout: float = 10.0):
        self._sync = ResourceLock(path, timeout=timeout)
        self._init_tracking()

    async def acquire(self, resource: str, owner: str, job_id: str) -> bool:
        """Take the lock; ``False`` if anyone already holds it."""
        return await self._dispatch(self._sync.acquire, resource, owner, job_id)

    async def release(self, resource: str, owner: str, job_id: str) -> bool:
        """Release only if both owner and job_id match the current holder."""
        return await self._dispatch(self._sync.release, resource, owner, job_id)

    async def status(self, resource: str) -> Optional[LockInfo]:
        """Return the current holder of ``resource``, or ``None``."""
        return await self._dispatch(self._sync.status, resource)

    async def list_locks(self) -> List[LockInfo]:
        """Return all held locks, oldest acquisition first."""
        return await self._dispatch(self._sync.list_locks)

    async def break_lock(self, resource: str, expected_job_id: str) -> bool:
        """Operator recovery: remove the lock while ``expected_job_id`` holds it."""
        return await self._dispatch(self._sync.break_lock, resource, expected_job_id)

    def hold(self, resource: str, owner: str, job_id: str):
        """Async context manager: acquires or raises LockHeld; releases on exit.

        Cancelling while the entry is acquiring is safe: the entry waits for
        its own ``acquire`` to finish and releases the lock when it was
        acquired, so a cancelled entry never leaves the lock held.
        """
        return self._hold(resource, owner, job_id)

    @contextlib.asynccontextmanager
    async def _hold(self, resource: str, owner: str, job_id: str):
        acquire_task = asyncio.ensure_future(self.acquire(resource, owner, job_id))
        try:
            acquired = await asyncio.shield(acquire_task)
        except asyncio.CancelledError:
            # G1: the entry was cancelled, but the worker-thread acquire may
            # still commit. Wait for it to finish, undo it when it took the
            # lock, then re-raise — a cancelled entry never leaves the lock
            # held. (A second cancel or an acquire failure lands here too;
            # the state stays queryable via status().)
            acquired = False
            try:
                acquired = await acquire_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            if acquired:
                await self.release(resource, owner, job_id)
            raise
        if not acquired:
            raise LockHeld(resource, await self.status(resource))
        try:
            yield
        finally:
            await self.release(resource, owner, job_id)


class AsyncCircuitBreaker(_AsyncWrapper):
    """Coroutine wrapper around :class:`~joons_agent_guard.CircuitBreaker`."""

    def __init__(self, path, *, timeout: float = 10.0):
        self._sync = CircuitBreaker(path, timeout=timeout)
        self._init_tracking()

    async def begin(self, actor: str, job_id: str) -> bool:
        """Claim the actor's run slot; ``False`` if a job is already pending."""
        return await self._dispatch(self._sync.begin, actor, job_id)

    async def finish(self, actor: str, job_id: str, *, success: bool,
                     reason: Optional[str] = None) -> bool:
        """Record the outcome of the pending job; ``False`` if it is not pending."""
        return await self._dispatch(self._sync.finish, actor, job_id,
                                    success=success, reason=reason)

    async def snapshot(self, actor: str) -> CircuitState:
        """Return the actor's state, pending job, failures and reason."""
        return await self._dispatch(self._sync.snapshot, actor)

    async def reset(self, actor: str, expected_job_id: Optional[str] = None) -> bool:
        """Operator recovery: clear the actor back to closed."""
        return await self._dispatch(self._sync.reset, actor,
                                    expected_job_id=expected_job_id)


class AsyncReplayGuard(_AsyncWrapper):
    """Coroutine wrapper around :class:`~joons_agent_guard.ReplayGuard`."""

    def __init__(self, path, *, timeout: float = 15.0):
        self._sync = ReplayGuard(path, timeout=timeout)
        self._init_tracking()

    async def claim(self, principal: str, request_id: str, digest: str) -> str:
        """Return ``new``, ``pending``, ``completed`` or ``conflict``."""
        return await self._dispatch(self._sync.claim, principal, request_id, digest)

    async def complete(self, principal: str, request_id: str, digest: str,
                       code: int, response: Any) -> bool:
        """Store the response of a pending request; ``False`` if not pending."""
        return await self._dispatch(self._sync.complete, principal, request_id,
                                    digest, code, response)

    async def response(self, principal: str, request_id: str,
                       digest: str) -> Optional[Tuple[int, Any]]:
        """Return the stored ``(code, response)`` of a completed request, or ``None``."""
        return await self._dispatch(self._sync.response, principal, request_id, digest)

    async def apply_pending(self, principal: str, request_id: str, digest: str,
                            effect: Callable[[Any], Tuple[int, Any]]) -> Optional[Tuple[int, Any]]:
        """Run a SQLite-only ``effect(connection)`` and store its response atomically.

        ``effect`` is a plain sync function; it runs on the worker thread
        inside the write transaction and must not use asyncio.
        """
        return await self._dispatch(self._sync.apply_pending, principal,
                                    request_id, digest, effect)

    async def reserve_outbound(self, intent_id: str, fixed: Mapping[str, str]) -> dict:
        """Persist a stable request_id/timestamp for an intent before sending it."""
        return await self._dispatch(self._sync.reserve_outbound, intent_id, fixed)
