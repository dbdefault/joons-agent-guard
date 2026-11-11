"""Storage-failure behaviour tests for the three joons_agent_guard components.

ResourceLock, CircuitBreaker and ReplayGuard all sit on one SQLite file, so
this suite exercises what happens when that file misbehaves:

* the path is a directory, so construction must fail with a clear sqlite3 error;
* the file is made read-only with ``os.chmod`` (permissions restored in a
  ``finally`` block so tmp_path cleanup still works on Windows), and writes
  must raise ``sqlite3.OperationalError`` while leaving no partial state
  (skipped for root on POSIX, which bypasses permission bits);
* another connection holds ``BEGIN EXCLUSIVE``: a component built with
  ``timeout=0.2`` must fail fast (bounded at 5 s to absorb slow CI runners),
  then succeed once the blocker commits;
* a component used after ``close()`` must raise ``sqlite3.ProgrammingError``;
* ``ReplayGuard.apply_pending`` must keep the request pending and roll the
  effect's own writes back when the effect raises or returns a response that
  is not JSON-serialisable.

Everything is asserted through the public API only.
"""
import os
import sqlite3
import time

import pytest

from joons_agent_guard import CircuitBreaker, ReplayGuard, ResourceLock, request_digest
from joons_agent_guard.replay import COMPLETED, NEW, PENDING

# (component class, write method, method args, expected result once the
# storage is writable again)
WRITE_CASES = [
    pytest.param(ResourceLock, "acquire", ("res-1", "owner-1", "job-1"), True,
                 id="ResourceLock.acquire"),
    pytest.param(CircuitBreaker, "begin", ("actor-1", "job-1"), True,
                 id="CircuitBreaker.begin"),
    pytest.param(ReplayGuard, "claim", ("principal-1", "request-1", "digest-1"), NEW,
                 id="ReplayGuard.claim"),
]

# Read-only probes that must keep working when the file cannot be written.
READ_PROBES = {
    ResourceLock: lambda c: c.status("res-1"),
    CircuitBreaker: lambda c: c.snapshot("actor-1"),
    ReplayGuard: lambda c: c.response("principal-1", "request-1", "digest-1"),
}


# On POSIX, root bypasses file permission bits, so a chmod(0o444) file stays
# writable and the read-only scenario cannot be simulated at all.
IS_ROOT = hasattr(os, "geteuid") and os.geteuid() == 0


def _make_guard_with_side_table(path):
    """ReplayGuard plus a plain side table an effect can write into."""
    guard = ReplayGuard(path)
    raw = sqlite3.connect(path)
    try:
        raw.execute("CREATE TABLE effect_side (marker TEXT NOT NULL)")
        raw.commit()
    finally:
        raw.close()
    return guard


def _count_side_rows(path):
    raw = sqlite3.connect(path)
    try:
        return raw.execute("SELECT COUNT(*) FROM effect_side").fetchone()[0]
    finally:
        raw.close()


# ---------------------------------------------------------------------------
# construction and access failures
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("cls", [ResourceLock, CircuitBreaker, ReplayGuard], ids=lambda c: c.__name__)
def test_directory_path_raises_clear_sqlite_error_at_construction(cls, tmp_path):
    """A path that already exists as a directory cannot be opened as a database."""
    not_a_db = tmp_path / "actually-a-directory"
    not_a_db.mkdir()
    with pytest.raises(sqlite3.OperationalError):
        cls(str(not_a_db))


@pytest.mark.skipif(IS_ROOT, reason="root ignores file permission bits: a chmod-based read-only file is still writable")
@pytest.mark.parametrize("cls, op, args, expected", WRITE_CASES)
def test_readonly_file_writes_raise_and_leave_no_partial_state(cls, op, args, expected, tmp_path):
    """Writes to a read-only database file raise OperationalError and change nothing."""
    path = str(tmp_path / "readonly.sqlite")
    setup = cls(path)  # create the schema while the file is still writable
    setup.close()

    os.chmod(path, 0o444)  # read-only (on Windows this sets the READONLY attribute)
    try:
        with cls(path) as readonly:
            with pytest.raises(sqlite3.OperationalError):
                getattr(readonly, op)(*args)
            READ_PROBES[cls](readonly)  # reads must still work on the read-only file
    finally:
        os.chmod(path, 0o644)  # restore so tmp_path cleanup can remove the file

    # No partial state: the very same write succeeds as if nothing happened.
    with cls(path) as restored:
        assert getattr(restored, op)(*args) == expected


@pytest.mark.parametrize("cls, op, args, expected", WRITE_CASES)
def test_exclusive_write_lock_blocks_then_allows(cls, op, args, expected, tmp_path):
    """A foreign BEGIN EXCLUSIVE makes the component fail fast, then recover after commit."""
    path = str(tmp_path / "exclusive.sqlite")
    with cls(path, timeout=0.2) as component:
        blocker = sqlite3.connect(path, isolation_level=None)
        try:
            blocker.execute("BEGIN EXCLUSIVE")
            start = time.monotonic()
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                getattr(component, op)(*args)
            elapsed = time.monotonic() - start
            # Lower bound: the 0.2s busy timeout must actually be honoured.
            # Upper bound: generous margin for slow CI runners; a regression
            # that ignores the timeout would block for the 10-15 s default
            # (or forever) and still fail here.
            assert 0.05 <= elapsed < 5.0, (
                "should fail fast after the 0.2s busy timeout, took %.3fs" % elapsed)
            blocker.execute("COMMIT")
            assert getattr(component, op)(*args) == expected
        finally:
            blocker.close()


@pytest.mark.parametrize("cls, op, args, expected", WRITE_CASES)
def test_using_component_after_close_raises_programming_error(cls, op, args, expected, tmp_path):
    """Any operation after close() must raise sqlite3.ProgrammingError."""
    component = cls(str(tmp_path / "closed.sqlite"))
    component.close()
    with pytest.raises(sqlite3.ProgrammingError):
        getattr(component, op)(*args)


# ---------------------------------------------------------------------------
# ReplayGuard.apply_pending failure modes
# ---------------------------------------------------------------------------

def test_apply_pending_effect_crash_leaves_request_pending_and_rolls_back(tmp_path):
    """An effect that raises keeps the request pending and undoes its own writes."""
    path = str(tmp_path / "replay-crash.sqlite")
    principal, request_id = "principal-1", "request-1"
    digest = request_digest("POST", "/things", b"body-1")
    with _make_guard_with_side_table(path) as guard:
        assert guard.claim(principal, request_id, digest) == NEW

        def failing_effect(conn):
            conn.execute("INSERT INTO effect_side (marker) VALUES ('written')")
            raise RuntimeError("effect failed")

        with pytest.raises(RuntimeError, match="effect failed"):
            guard.apply_pending(principal, request_id, digest, failing_effect)

        # The request is still pending (state unknown), never completed.
        assert guard.claim(principal, request_id, digest) == PENDING
        assert guard.response(principal, request_id, digest) is None
        # The effect's writes were rolled back together with the transaction.
        assert _count_side_rows(path) == 0

        # Recovery: a retry with a working effect runs once and completes.
        def working_effect(conn):
            conn.execute("INSERT INTO effect_side (marker) VALUES ('written')")
            return 201, {"done": True}

        assert guard.apply_pending(principal, request_id, digest, working_effect) == (201, {"done": True})
        assert guard.claim(principal, request_id, digest) == COMPLETED
        assert guard.response(principal, request_id, digest) == (201, {"done": True})
        assert _count_side_rows(path) == 1


def test_apply_pending_unserialisable_response_leaves_request_pending(tmp_path):
    """A non-JSON-serialisable response raises TypeError and keeps the request pending."""
    path = str(tmp_path / "replay-unserialisable.sqlite")
    principal, request_id = "principal-2", "request-2"
    digest = request_digest("POST", "/things", b"body-2")
    with _make_guard_with_side_table(path) as guard:
        assert guard.claim(principal, request_id, digest) == NEW

        def bad_effect(conn):
            conn.execute("INSERT INTO effect_side (marker) VALUES ('written')")
            return 200, {"payload": object()}  # json.dumps raises TypeError

        with pytest.raises(TypeError):
            guard.apply_pending(principal, request_id, digest, bad_effect)

        # Still pending, nothing stored, effect writes rolled back.
        assert guard.claim(principal, request_id, digest) == PENDING
        assert guard.response(principal, request_id, digest) is None
        assert _count_side_rows(path) == 0

        def working_effect(conn):
            conn.execute("INSERT INTO effect_side (marker) VALUES ('written')")
            return 200, {"payload": "ok"}

        assert guard.apply_pending(principal, request_id, digest, working_effect) == (200, {"payload": "ok"})
        assert guard.claim(principal, request_id, digest) == COMPLETED
        assert _count_side_rows(path) == 1
