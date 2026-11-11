"""Single-process pytest suite for joons_agent_guard.lock.ResourceLock.

Covers acquire/release semantics, lock independence, LockInfo reporting,
break_lock safety, the hold() context manager, persistence across reopen
with no silent expiry, identifier validation, and the context-manager /
:memory: lifecycle.
"""
import sqlite3
import time

import pytest

from joons_agent_guard import GuardError, InvalidIdentifier, LockHeld, LockInfo, ResourceLock

# Values that must be rejected for every identifier argument.
BAD_IDS = ["", None, 123, b"res-id"]

# Slightly longer than the coarsest wall-clock tick so that "acquired" order
# is deterministic even on platforms with low-resolution time.time().
TICK = 0.05


@pytest.fixture()
def lock_path(tmp_path):
    return tmp_path / "guard.db"


@pytest.fixture()
def lock(lock_path):
    rl = ResourceLock(lock_path)
    yield rl
    rl.close()


# -- acquire ---------------------------------------------------------------

def test_acquire_succeeds_exactly_once(lock):
    assert lock.acquire("res", "owner-a", "job-1") is True
    # Same holder, same owner with a new job, same job with a new owner, and
    # a fully different holder must all be rejected.
    assert lock.acquire("res", "owner-a", "job-1") is False
    assert lock.acquire("res", "owner-a", "job-2") is False
    assert lock.acquire("res", "owner-b", "job-1") is False
    assert lock.acquire("res", "owner-b", "job-2") is False
    # The original holder is untouched by all failed attempts.
    info = lock.status("res")
    assert (info.owner, info.job_id) == ("owner-a", "job-1")
    assert len(lock.list_locks()) == 1


# -- release ---------------------------------------------------------------

def test_release_requires_matching_owner_and_job_id(lock):
    assert lock.acquire("res", "owner-a", "job-1") is True

    # Wrong owner with the right job: refused, lock kept.
    assert lock.release("res", "owner-b", "job-1") is False
    assert lock.status("res").owner == "owner-a"

    # Right owner, wrong job: refused, lock kept.
    assert lock.release("res", "owner-a", "job-2") is False
    assert lock.status("res").job_id == "job-1"

    # Unknown resource: refused.
    assert lock.release("no-such-resource", "owner-a", "job-1") is False

    # Exactly matching pair: released, and a second release reports False.
    assert lock.release("res", "owner-a", "job-1") is True
    assert lock.status("res") is None
    assert lock.release("res", "owner-a", "job-1") is False

    # The freed resource is acquirable by a different holder.
    assert lock.acquire("res", "owner-b", "job-2") is True


# -- independence of resources --------------------------------------------

def test_different_resources_are_independent(lock):
    assert lock.acquire("res-1", "owner-a", "job-1") is True
    assert lock.acquire("res-2", "owner-b", "job-2") is True

    # Both are busy, independently.
    assert lock.acquire("res-1", "owner-c", "job-3") is False
    assert lock.acquire("res-2", "owner-a", "job-1") is False

    # Releasing one leaves the other held.
    assert lock.release("res-1", "owner-a", "job-1") is True
    assert lock.status("res-1") is None
    assert lock.status("res-2").owner == "owner-b"

    # The freed resource can be re-acquired while res-2 is still held.
    assert lock.acquire("res-1", "owner-d", "job-4") is True
    assert [entry.resource for entry in lock.list_locks()] == ["res-2", "res-1"]


# -- status / list_locks ---------------------------------------------------

def test_status_returns_lock_info(lock):
    assert lock.status("res") is None  # before acquisition
    before = time.time()
    assert lock.acquire("res", "owner-a", "job-1") is True
    after = time.time()

    info = lock.status("res")
    assert isinstance(info, LockInfo)
    assert info.resource == "res"
    assert info.owner == "owner-a"
    assert info.job_id == "job-1"
    assert isinstance(info.acquired_at, float)
    assert before - 1.0 <= info.acquired_at <= after + 1.0


def test_list_locks_orders_by_acquired_time(lock):
    assert lock.list_locks() == []  # empty database

    # Acquire in an order that is NOT alphabetical, so an accidental
    # ORDER BY resource cannot fake the expected sequence.
    plan = [
        ("delta", "owner-a", "job-1"),
        ("charlie", "owner-b", "job-2"),
        ("alpha", "owner-c", "job-3"),
    ]
    marks = []
    for resource, owner, job_id in plan:
        assert lock.acquire(resource, owner, job_id) is True
        marks.append(time.time())
        time.sleep(TICK)

    locks = lock.list_locks()
    assert [entry.resource for entry in locks] == ["delta", "charlie", "alpha"]
    assert [entry.owner for entry in locks] == ["owner-a", "owner-b", "owner-c"]
    assert [entry.job_id for entry in locks] == ["job-1", "job-2", "job-3"]

    acquired = [entry.acquired_at for entry in locks]
    assert acquired == sorted(acquired)  # ordered by acquisition time
    # Every entry falls inside the span of the whole acquisition phase.
    assert marks[0] - 1.0 <= acquired[0]
    assert acquired[-1] <= marks[-1] + TICK + 1.0
    assert all(isinstance(entry, LockInfo) for entry in locks)


# -- break_lock ------------------------------------------------------------

def test_break_lock_only_removes_matching_job(lock):
    # Unknown resource: nothing to break.
    assert lock.break_lock("res", "job-1") is False

    assert lock.acquire("res", "owner-a", "job-1") is True

    # Wrong job id: refused, lock kept.
    assert lock.break_lock("res", "job-2") is False
    assert lock.status("res").job_id == "job-1"

    # Matching job id: removed regardless of who broke it.
    assert lock.break_lock("res", "job-1") is True
    assert lock.status("res") is None

    # A lock re-acquired by a new job must not be removable with the stale
    # job id, and still breaks with the current one.
    assert lock.acquire("res", "owner-b", "job-2") is True
    assert lock.break_lock("res", "job-1") is False
    assert lock.status("res").job_id == "job-2"
    assert lock.break_lock("res", "job-2") is True
    assert lock.status("res") is None


# -- hold() context manager ------------------------------------------------

def test_hold_acquires_and_releases_on_normal_exit(lock):
    with lock.hold("res", "owner-a", "job-1"):
        info = lock.status("res")
        assert (info.owner, info.job_id) == ("owner-a", "job-1")
        # The resource is busy even for the holder while inside the block.
        assert lock.acquire("res", "owner-a", "job-1") is False
    assert lock.status("res") is None
    assert lock.acquire("res", "owner-b", "job-2") is True


def test_hold_releases_on_exception(lock):
    with pytest.raises(RuntimeError, match="boom"):
        with lock.hold("res", "owner-a", "job-1"):
            assert lock.status("res").owner == "owner-a"
            raise RuntimeError("boom")
    assert lock.status("res") is None
    assert lock.acquire("res", "owner-b", "job-2") is True


def test_hold_raises_lock_held_carrying_holder(lock):
    assert lock.acquire("res", "owner-a", "job-1") is True

    ran = False
    with pytest.raises(LockHeld) as excinfo:
        with lock.hold("res", "owner-b", "job-2"):
            ran = True  # the body must never run for a busy resource
    assert ran is False
    assert excinfo.value.resource == "res"
    holder = excinfo.value.holder
    assert isinstance(holder, LockInfo)
    assert (holder.resource, holder.owner, holder.job_id) == ("res", "owner-a", "job-1")

    # Re-entering with the very same holder is also refused.
    with pytest.raises(LockHeld):
        with lock.hold("res", "owner-a", "job-1"):
            ran = True
    assert ran is False

    # The original lock survived both attempts.
    assert lock.status("res").owner == "owner-a"


# -- persistence / no expiry ----------------------------------------------

def test_locks_persist_across_reopen_and_never_expire(tmp_path):
    path = tmp_path / "persist.db"
    first = ResourceLock(path)
    try:
        assert first.acquire("res", "owner-a", "job-1") is True
        info_before = first.status("res")
    finally:
        first.close()

    time.sleep(0.2)  # if any expiry existed, this window would trigger it

    second = ResourceLock(path)
    try:
        info_after = second.status("res")
        assert info_after == info_before  # identical fields, same acquired_at
        assert second.acquire("res", "owner-b", "job-2") is False
        assert second.release("res", "owner-b", "job-2") is False
        assert second.break_lock("res", "job-2") is False
        assert [entry.resource for entry in second.list_locks()] == ["res"]

        # A fresh instance on the same file can still use the normal API.
        assert second.release("res", "owner-a", "job-1") is True
        assert second.status("res") is None
    finally:
        second.close()


# -- identifier validation -------------------------------------------------

IDENT_CASES = [
    ("acquire", "resource"),
    ("acquire", "owner"),
    ("acquire", "job_id"),
    ("release", "resource"),
    ("release", "owner"),
    ("release", "job_id"),
    ("status", "resource"),
    ("break_lock", "resource"),
    ("break_lock", "expected_job_id"),
    ("hold", "resource"),
    ("hold", "owner"),
    ("hold", "job_id"),
]


ARG_NAMES = {
    "acquire": ("resource", "owner", "job_id"),
    "release": ("resource", "owner", "job_id"),
    "status": ("resource",),
    "break_lock": ("resource", "expected_job_id"),
    "hold": ("resource", "owner", "job_id"),
}

GOOD_ARGS = {
    "resource": "res",
    "owner": "owner-a",
    "job_id": "job-1",
    "expected_job_id": "job-1",
}


@pytest.mark.parametrize("method,field", IDENT_CASES)
@pytest.mark.parametrize("bad", BAD_IDS)
def test_invalid_identifier_rejected(lock, method, field, bad):
    kwargs = {name: GOOD_ARGS[name] for name in ARG_NAMES[method]}
    kwargs[field] = bad
    if method == "hold":
        # hold() is lazy: the check fires when the context manager is entered.
        with pytest.raises(InvalidIdentifier):
            with lock.hold(**kwargs):
                pass
    else:
        with pytest.raises(InvalidIdentifier):
            getattr(lock, method)(**kwargs)
    # list_locks() takes no identifier arguments, so it has nothing to reject.


def test_invalid_acquire_writes_nothing(lock):
    with pytest.raises(InvalidIdentifier):
        lock.acquire("res", None, "job-1")
    assert lock.status("res") is None
    assert lock.list_locks() == []


def test_invalid_release_keeps_lock(lock):
    assert lock.acquire("res", "owner-a", "job-1") is True
    with pytest.raises(InvalidIdentifier):
        lock.release("res", "", "job-1")
    assert lock.status("res").owner == "owner-a"


def test_invalid_identifier_is_value_error_and_guard_error():
    assert issubclass(InvalidIdentifier, ValueError)
    assert issubclass(InvalidIdentifier, GuardError)


# -- lifecycle: context manager and :memory: -------------------------------

def test_resource_lock_works_as_context_manager(lock_path):
    with ResourceLock(lock_path) as rl:
        assert isinstance(rl, ResourceLock)
        assert rl.acquire("res", "owner-a", "job-1") is True
        assert rl.acquire("res", "owner-b", "job-2") is False
        assert rl.release("res", "owner-a", "job-1") is True

    # Exiting the with block closed the underlying connection.
    with pytest.raises(sqlite3.ProgrammingError):
        rl.status("res")


def test_resource_lock_supports_memory_database():
    with ResourceLock(":memory:") as rl:
        assert rl.acquire("res", "owner-a", "job-1") is True
        assert rl.acquire("res", "owner-b", "job-2") is False
        info = rl.status("res")
        assert (info.owner, info.job_id) == ("owner-a", "job-1")
        assert [entry.resource for entry in rl.list_locks()] == ["res"]
        assert rl.release("res", "owner-a", "job-1") is True
        assert rl.status("res") is None
