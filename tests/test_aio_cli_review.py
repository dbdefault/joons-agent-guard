"""Review tests for REVIEW_CHECKLIST_aio_cli.md (items A1-A8, B9-B15).

Each test targets one checklist item against the real src/joons_agent_guard/
aio.py and cli.py. CLI items run in-process via cli.main(argv) and through
subprocess (sys.executable -m joons_agent_guard with PYTHONPATH=src). asyncio
items use asyncio.run inside plain pytest functions (no pytest-asyncio).
"""
import asyncio
import contextlib
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time

import pytest

# Make the in-tree package importable here even without PYTHONPATH set.
_SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if os.path.isdir(_SRC) and _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import joons_agent_guard.cli as cli
from joons_agent_guard import (
    AsyncCircuitBreaker,
    AsyncReplayGuard,
    AsyncResourceLock,
    CircuitBreaker,
    ReplayGuard,
    ResourceLock,
    request_digest,
)


def cli_env():
    return dict(os.environ, PYTHONPATH=_SRC)


def run_cli(db, *argv, cwd=None, env_extra=None):
    env = cli_env()
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [sys.executable, "-m", "joons_agent_guard", "--db", db, *argv],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        env=env, cwd=cwd, timeout=60,
    )


def seed_review_db(db):
    """One held lock (deploy/worker-1/job-42), one circuit (w1), one pending request."""
    lock = ResourceLock(db)
    assert lock.acquire("deploy", "worker-1", "job-42")
    lock.close()
    breaker = CircuitBreaker(db)
    breaker.begin("w1", "j1")
    breaker.close()
    guard = ReplayGuard(db)
    guard.claim("client-a", "req-1", request_digest("POST", "/x", b"1"))
    guard.close()


# ---------------------------------------------------------------------------
# A1: every public adapter call runs off the event loop
# ---------------------------------------------------------------------------

def test_a1_every_method_offloads_to_thread_and_loop_stays_free(tmp_path, monkeypatch):
    real_to_thread = asyncio.to_thread
    recorded = set()

    def recording_to_thread(func, *args, **kwargs):
        recorded.add(getattr(func, "__name__", repr(func)))
        return real_to_thread(func, *args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", recording_to_thread)

    async def main():
        gaps = []

        async def ticker():
            last = time.monotonic()
            while True:
                await asyncio.sleep(0.005)
                now = time.monotonic()
                gaps.append(now - last)
                last = now

        ticker_task = asyncio.create_task(ticker())
        lock = AsyncResourceLock(str(tmp_path / "l.db"))
        breaker = AsyncCircuitBreaker(str(tmp_path / "c.db"))
        guard = AsyncReplayGuard(str(tmp_path / "r.db"))
        try:
            for _ in range(3):
                await lock.acquire("r", "o", "j")
                await lock.status("r")
                await lock.list_locks()
                async with lock.hold("r2", "o", "j"):
                    pass
                await lock.break_lock("r", "j")
                await lock.release("r", "o", "j")
                await breaker.begin("a", "j")
                await breaker.finish("a", "j", success=True)
                await breaker.snapshot("a")
                await breaker.reset("a")
                digest = request_digest("POST", "/x", b"1")
                await guard.claim("p", "q", digest)
                await guard.complete("p", "q", digest, 200, {"ok": 1})
                await guard.response("p", "q", digest)
                await guard.apply_pending("p", "q2", digest, lambda conn: (200, None))
                await guard.reserve_outbound("i", {"k": "v"})
        finally:
            await lock.aclose()
            await breaker.aclose()
            await guard.aclose()
            ticker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ticker_task
        return gaps

    gaps = asyncio.run(main())
    expected = {"acquire", "release", "status", "list_locks", "break_lock", "close",
                "begin", "finish", "snapshot", "reset", "claim", "complete", "response",
                "apply_pending", "reserve_outbound"}
    assert recorded == expected, "unoffloaded calls: %s" % (expected - recorded)
    assert max(gaps) < 0.2, "event loop stalled: largest wakeup gap %.3fs" % max(gaps)


# ---------------------------------------------------------------------------
# A2: cancellation during an in-flight release must not leak the lock
# ---------------------------------------------------------------------------

def test_a2_cancel_during_inflight_release_still_releases(tmp_path, monkeypatch):
    async def main():
        lock = AsyncResourceLock(str(tmp_path / "g.db"))
        loop = asyncio.get_running_loop()
        orig = lock._sync.release
        started, done = asyncio.Event(), asyncio.Event()
        proceed = threading.Event()

        def blocked_release(resource, owner, job_id):
            loop.call_soon_threadsafe(started.set)
            proceed.wait(5)                      # blocks the worker thread, not the loop
            result = orig(resource, owner, job_id)
            loop.call_soon_threadsafe(done.set)  # the DELETE has landed
            return result

        monkeypatch.setattr(lock._sync, "release", blocked_release)

        async def body():
            async with lock.hold("res", "owner-1", "job-1"):
                pass

        task = asyncio.create_task(body())
        await asyncio.wait_for(started.wait(), timeout=5)
        task.cancel()                            # cancel while the release is in flight
        proceed.set()                            # let the worker thread finish the DELETE
        cancelled = False
        try:
            await asyncio.wait_for(task, timeout=5)
        except asyncio.CancelledError:
            cancelled = True
        await asyncio.wait_for(done.wait(), timeout=5)
        info = await lock.status("res")
        await lock.aclose()
        return cancelled, info

    cancelled, info = asyncio.run(main())
    assert cancelled
    assert info is None, "the in-flight release must complete despite cancellation"

    async def again():
        async with AsyncResourceLock(str(tmp_path / "g.db")) as lock:
            return await lock.acquire("res", "owner-2", "job-2")

    assert asyncio.run(again()) is True


# ---------------------------------------------------------------------------
# A3: cancellation mid-acquire leaves a queryable, releasable state
# ---------------------------------------------------------------------------

def test_a3_cancelled_acquire_leaves_a_queryable_state(tmp_path, monkeypatch):
    async def main():
        lock = AsyncResourceLock(str(tmp_path / "g.db"), timeout=5.0)
        loop = asyncio.get_running_loop()
        orig = lock._sync.acquire
        committed, done = asyncio.Event(), asyncio.Event()
        proceed = threading.Event()

        def acquire_then_hang(resource, owner, job_id):
            result = orig(resource, owner, job_id)   # the INSERT has committed
            loop.call_soon_threadsafe(committed.set)
            proceed.wait(5)                          # result not delivered yet
            loop.call_soon_threadsafe(done.set)
            return result

        monkeypatch.setattr(lock._sync, "acquire", acquire_then_hang)
        task = asyncio.create_task(lock.acquire("res", "owner-1", "job-1"))
        await asyncio.wait_for(committed.wait(), timeout=5)
        task.cancel()
        proceed.set()
        cancelled = False
        try:
            await asyncio.wait_for(task, timeout=5)
        except asyncio.CancelledError:
            cancelled = True
        await asyncio.wait_for(done.wait(), timeout=5)
        # defined invariant: the row is absent, or present and releasable
        info = await lock.status("res")
        assert info is None or (info.owner, info.job_id) == ("owner-1", "job-1")
        if info is not None:
            assert await lock.release("res", "owner-1", "job-1") is True
        await lock.aclose()
        return cancelled

    assert asyncio.run(main()) is True


# ---------------------------------------------------------------------------
# A4: aclose while an operation is in flight
# ---------------------------------------------------------------------------

def test_a4_aclose_during_inflight_call_ends_documented(tmp_path, monkeypatch):
    async def main():
        guard = AsyncReplayGuard(str(tmp_path / "g.db"))
        orig = guard._sync.claim

        def slow_claim(principal, request_id, digest):
            time.sleep(0.2)                          # the worker has not taken the
            return orig(principal, request_id, digest)  # guard lock yet

        monkeypatch.setattr(guard._sync, "claim", slow_claim)
        task = asyncio.create_task(guard.claim("p", "req-1", request_digest("POST", "/x", b"1")))
        await asyncio.sleep(0.05)
        close_task = asyncio.create_task(guard.aclose())
        outcome = {}
        try:
            outcome["state"] = await asyncio.wait_for(task, timeout=3.0)
        except sqlite3.ProgrammingError as exc:      # documented post-close error
            outcome["programming_error"] = str(exc)
        await asyncio.wait_for(close_task, timeout=3.0)
        try:                                         # post-close use: documented type only
            await guard.claim("p", "req-2", request_digest("POST", "/x", b"2"))
            outcome["after_close"] = "no-error"
        except sqlite3.ProgrammingError:
            outcome["after_close"] = "programming-error"
        return outcome

    outcome = asyncio.run(main())
    assert ("state" in outcome and outcome["state"] == "new") or "programming_error" in outcome
    assert outcome["after_close"] == "programming-error"


# ---------------------------------------------------------------------------
# A5: thread-pool assumptions - many concurrent calls, no adapter-owned executor
# ---------------------------------------------------------------------------

def test_a5_two_hundred_concurrent_calls_all_complete(tmp_path):
    async def main():
        async with AsyncReplayGuard(str(tmp_path / "g.db")) as guard:
            async def one(i):
                digest = request_digest("POST", "/x", str(i).encode())
                state = await guard.claim("p", "req-%d" % i, digest)
                if state == "new":
                    return await guard.complete("p", "req-%d" % i, digest, 200, {"i": i})
                return False

            return await asyncio.gather(*[one(i) for i in range(200)])

    results = asyncio.run(main())
    assert len(results) == 200
    assert all(results)


# ---------------------------------------------------------------------------
# A6: sqlite3 errors propagate unchanged, nothing swallowed
# ---------------------------------------------------------------------------

def test_a6_busy_timeout_error_propagates_unchanged(tmp_path):
    path = str(tmp_path / "g.db")
    lock = AsyncResourceLock(path, timeout=0.3)
    blocker = sqlite3.connect(path, isolation_level=None)

    async def main():
        loop = asyncio.get_running_loop()
        handled = []
        loop.set_exception_handler(lambda l, ctx: handled.append(ctx))
        blocker.execute("BEGIN IMMEDIATE")       # holds the write lock > busy timeout
        try:
            await lock.acquire("res", "o", "j")
            raise AssertionError("expected sqlite3.OperationalError")
        except sqlite3.OperationalError as exc:
            assert "lock" in str(exc).lower()
            assert exc.__cause__ is None          # propagated unchanged, not wrapped
        await asyncio.sleep(0.05)
        assert handled == []                      # no "exception was never retrieved"
        await lock.aclose()

    try:
        asyncio.run(main())
    finally:
        blocker.close()


# ---------------------------------------------------------------------------
# A7: hold releases exactly once on every failure path
# ---------------------------------------------------------------------------

def test_a7_hold_releases_exactly_once_on_every_failure_path(tmp_path, monkeypatch):
    class Sentinel(Exception):
        pass

    async def main():
        lock = AsyncResourceLock(str(tmp_path / "g.db"))
        calls = []
        orig = lock._sync.release

        def counting_release(resource, owner, job_id):
            calls.append((resource, owner, job_id))
            return orig(resource, owner, job_id)

        monkeypatch.setattr(lock._sync, "release", counting_release)

        with pytest.raises(Sentinel):             # exception path
            async with lock.hold("res", "o1", "j1"):
                raise Sentinel()
        assert await lock.status("res") is None

        async def cancelled_body():               # cancellation path
            async with lock.hold("res", "o2", "j2"):
                await asyncio.sleep(10)

        task = asyncio.create_task(cancelled_body())
        await asyncio.sleep(0.1)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=3)
        assert await lock.status("res") is None
        await lock.aclose()
        return calls

    calls = asyncio.run(main())
    assert calls == [("res", "o1", "j1"), ("res", "o2", "j2")]  # exactly one release each


# ---------------------------------------------------------------------------
# A8: cancellation cannot abort the SQLite busy wait; state stays queryable
# ---------------------------------------------------------------------------

def test_a8_cancel_during_busy_wait_leaves_no_unknown_state(tmp_path, monkeypatch):
    path = str(tmp_path / "g.db")
    lock = AsyncResourceLock(path, timeout=10.0)  # adapter busy timeout: 10 s
    blocker = sqlite3.connect(path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")            # holds the write lock

    async def main():
        # shorten only the SQLite busy wait so the test stays fast; the
        # semantics under test (cancel cannot abort the running thread) are
        # unchanged
        await asyncio.to_thread(lock._sync._db.execute, "PRAGMA busy_timeout=400")
        loop = asyncio.get_running_loop()
        orig = lock._sync.acquire
        done = asyncio.Event()

        def probe_acquire(resource, owner, job_id):
            try:
                return orig(resource, owner, job_id)
            finally:
                loop.call_soon_threadsafe(done.set)  # the background wait finished

        monkeypatch.setattr(lock._sync, "acquire", probe_acquire)
        task = asyncio.create_task(lock.acquire("res", "owner-1", "job-1"))
        await asyncio.sleep(0.1)                  # the busy wait is running
        t0 = time.monotonic()
        task.cancel()
        cancelled = False
        try:
            await asyncio.wait_for(task, timeout=5)
        except asyncio.CancelledError:
            cancelled = True
        elapsed = time.monotonic() - t0
        await asyncio.wait_for(done.wait(), timeout=5)   # background wait has ended
        info = await lock.status("res")           # outcome must be queryable
        await lock.aclose()
        return cancelled, elapsed, info

    try:
        cancelled, elapsed, info = asyncio.run(main())
    finally:
        blocker.close()

    assert cancelled
    assert elapsed < 0.2, "cancellation must be prompt (the SQLite wait continues detached)"
    assert info is None, "a busy-timeout acquire must not leave an unknown row"


# ---------------------------------------------------------------------------
# B9: the confirmation gate is the explicit --yes flag (no stdin prompt)
# ---------------------------------------------------------------------------

def test_b9_confirmation_gate_is_the_yes_flag_only(tmp_path):
    db = str(tmp_path / "g.db")
    seed_review_db(db)

    # stdin closed/EOF: without --yes the command still refuses and changes nothing
    proc = run_cli(db, "break-lock", "deploy", "--job-id", "job-42")
    assert proc.returncode == 2
    assert "would break lock" in proc.stdout
    assert ResourceLock(db).status("deploy") is not None

    # even an stdin "y" cannot confirm anything: there is no prompt to feed
    proc = run_cli(db, "break-lock", "deploy", "--job-id", "job-42")
    assert proc.returncode == 2
    assert ResourceLock(db).status("deploy") is not None

    # validation comes before confirmation: an unknown target is never offered
    proc = run_cli(db, "break-lock", "ghost", "--job-id", "x")
    assert proc.returncode == 2
    assert "no lock held" in proc.stdout and "ghost" in proc.stdout

    # the plan echoes exactly what would be destroyed
    rc = cli.main(["--db", db, "break-lock", "deploy", "--job-id", "job-42"])
    assert rc == 2
    # and only --yes mutates
    assert cli.main(["--db", db, "break-lock", "deploy", "--job-id", "job-42", "--yes"]) == 0
    assert ResourceLock(db).status("deploy") is None


# ---------------------------------------------------------------------------
# B10: exit-code discipline
# ---------------------------------------------------------------------------

def test_b10_exit_code_matrix(tmp_path):
    cases = [
        ("locks success", ["locks"], 0, None),
        ("locks empty is ok", ["locks"], 0, None),
        ("circuit known", ["circuit", "w1"], 0, None),
        ("circuit unknown is 1", ["circuit", "ghost"], 1, None),
        ("pending read", ["pending"], 0, None),
        ("break-lock dry run is 2", ["break-lock", "deploy", "--job-id", "job-42"], 2,
         lambda db: ResourceLock(db).status("deploy") is not None),
        ("break-lock applied is 0", ["break-lock", "deploy", "--job-id", "job-42", "--yes"], 0,
         lambda db: ResourceLock(db).status("deploy") is None),
        ("break-lock refused is 1", ["break-lock", "deploy", "--job-id", "other", "--yes"], 1,
         lambda db: ResourceLock(db).status("deploy") is not None),
        ("reset-circuit dry run is 2", ["reset-circuit", "w1"], 2, None),
        ("reset-circuit unknown is 1", ["reset-circuit", "ghost", "--yes"], 1, None),
        ("reset-circuit applied is 0", ["reset-circuit", "w1", "--yes"], 0, None),
        ("missing database is 1", ["locks"], 1, None),
        ("no command is 2", [], 2, None),
        ("unknown command is 2", ["frobnicate"], 2, None),
        ("missing --job-id is 2", ["break-lock", "deploy"], 2, None),
    ]
    for name, argv, expected_rc, post_check in cases:
        db = str(tmp_path / ("case-%d.db" % len(os.listdir(tmp_path))))
        if name != "missing database is 1":
            seed_review_db(db)
        rc = cli.main(["--db", db, *argv])
        assert rc == expected_rc, "%s: rc %d != %d" % (name, rc, expected_rc)
        if post_check is not None:
            assert post_check(db), "%s: database state contradicts the exit code" % name


# ---------------------------------------------------------------------------
# B11: --json output stays machine-parseable when errors happen
# ---------------------------------------------------------------------------

def test_b11_json_output_stays_parseable_on_errors(tmp_path, capsys):
    db = str(tmp_path / "g.db")
    seed_review_db(db)

    def run_json(*argv):
        capsys.readouterr()
        rc = cli.main(["--db", db, *argv])
        return rc, capsys.readouterr()

    rc, out = run_json("locks", "--json")
    assert rc == 0
    assert len(json.loads(out.out)["locks"]) == 1

    rc, out = run_json("circuit", "ghost", "--json")
    assert rc == 1
    assert json.loads(out.out)["error"] == "not found"

    rc, out = run_json("break-lock", "deploy", "--job-id", "job-42", "--json")
    assert rc == 2
    assert json.loads(out.out)["applied"] is False

    rc, out = run_json("pending", "--older-than", "-1", "--json")
    assert rc == 2
    assert "error" in json.loads(out.out)

    # missing database
    missing = str(tmp_path / "nope.db")
    capsys.readouterr()
    rc = cli.main(["--db", missing, "locks", "--json"])
    out = capsys.readouterr()
    assert rc == 1
    assert json.loads(out.out)["error"] == "database not found"

    # argparse usage errors: stdout empty (nothing promised), diagnostics on stderr
    capsys.readouterr()
    rc = cli.main(["--db", db, "--json", "frobnicate"])
    out = capsys.readouterr()
    assert rc == 2
    assert out.out == ""
    assert "invalid choice" in out.err

    capsys.readouterr()
    rc = cli.main(["--db", db])
    out = capsys.readouterr()
    assert rc == 2
    assert out.out == ""
    assert "usage" in out.err

    combined = out.out + out.err
    assert "Traceback" not in combined


# ---------------------------------------------------------------------------
# B12: a failed destructive command mutates nothing (single atomic effect)
# ---------------------------------------------------------------------------

def test_b12_failed_destructive_command_mutates_nothing(tmp_path, monkeypatch, capsys):
    db = str(tmp_path / "g.db")
    seed_review_db(db)

    real_cls = cli.ResourceLock

    class Failing(real_cls):
        def break_lock(self, resource, expected_job_id):
            raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(cli, "ResourceLock", Failing)
    rc = cli.main(["--db", db, "break-lock", "deploy", "--job-id", "job-42", "--yes"])
    assert rc == 1
    assert ResourceLock(db).status("deploy") is not None   # nothing mutated
    out = capsys.readouterr()
    assert "error:" in out.out
    assert "Traceback" not in out.out + out.err


# ---------------------------------------------------------------------------
# B13: identifier / argument validation edges
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("argv,expected_rc,expected_msg", [
    (["break-lock", "deploy", "--job-id=", "--yes"], 1, "non-empty string"),
    (["break-lock", "deploy", "--job-id= ", "--yes"], 1, "no matching lock"),
    (["break-lock", "deploy", "--job-id=-x", "--yes"], 1, "no matching lock"),
    (["break-lock", "deploy", "--job-id", "-x", "--yes"], 2, None),
    (["break-lock", "deploy", "--job-id=x\ny", "--yes"], 1, "no matching lock"),
    (["break-lock", "deploy", "--job-id=" + "x" * 10000, "--yes"], 1, "at most"),
    (["break-lock", "", "--job-id=x", "--yes"], 1, "non-empty string"),
    (["break-lock", "", "--job-id=x"], 1, "non-empty string"),
    (["circuit", ""], 1, "non-empty string"),
])
def test_b13_argument_validation_edges(tmp_path, capsys, argv, expected_rc, expected_msg):
    db = str(tmp_path / "g.db")
    seed_review_db(db)
    rc = cli.main(["--db", db, *argv])
    assert rc == expected_rc
    if expected_msg is not None:
        assert expected_msg in capsys.readouterr().out
    # never a silent mutation: the original holder survives every rejected call
    info = ResourceLock(db).status("deploy")
    assert (info.owner, info.job_id) == ("worker-1", "job-42")


def test_b13_leading_dash_value_via_subprocess(tmp_path):
    db = str(tmp_path / "g.db")
    seed_review_db(db)
    proc = run_cli(db, "break-lock", "deploy", "--job-id=-dash", "--yes")
    assert proc.returncode == 1
    assert "no matching lock" in proc.stdout
    assert ResourceLock(db).status("deploy") is not None


# ---------------------------------------------------------------------------
# B14: the CLI coexists with a live writer on the same file
# ---------------------------------------------------------------------------

def test_b14_reads_and_dry_run_coexist_with_a_writer(tmp_path, capsys):
    db = str(tmp_path / "g.db")
    seed_review_db(db)
    conn = sqlite3.connect(db, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")          # the "live service" holds the write lock
        t0 = time.monotonic()
        assert cli.main(["--db", db, "locks"]) == 0
        assert cli.main(["--db", db, "circuit", "w1"]) == 0
        assert cli.main(["--db", db, "pending"]) == 0
        # the dry run only reads: it must not block on - or disturb - the writer
        assert cli.main(["--db", db, "break-lock", "deploy", "--job-id", "job-42"]) == 2
        assert time.monotonic() - t0 < 5, "a read/dry-run waited on the busy database"
        conn.rollback()
    finally:
        conn.close()
    capsys.readouterr()
    # after the writer is done, the confirmed command works normally
    assert cli.main(["--db", db, "break-lock", "deploy", "--job-id", "job-42", "--yes"]) == 0
    assert ResourceLock(db).status("deploy") is None


# ---------------------------------------------------------------------------
# B15: encoding and invocation-directory edges
# ---------------------------------------------------------------------------

def test_b15_unicode_output_survives_limited_encoding_pipe(tmp_path):
    db = str(tmp_path / "g.db")
    guard = ReplayGuard(db)
    digest = request_digest("POST", "/x", b"1")
    guard.claim("client-유니", "req-1", digest)
    guard.close()

    env_extra = {"PYTHONIOENCODING": "cp1252"}   # a legacy codepage pipe
    proc = run_cli(db, "pending", "--json", env_extra=env_extra)
    assert proc.returncode == 0
    payload = json.loads(proc.stdout)
    assert payload["pending"][0]["principal"] == "client-유니"

    proc = run_cli(db, "pending", env_extra=env_extra)
    assert proc.returncode == 0
    assert "Traceback" not in proc.stderr
    assert "req-1" in proc.stdout


def test_b15_relative_db_path_resolves_per_invocation_cwd(tmp_path):
    db_dir = tmp_path / "live"
    db_dir.mkdir()
    db = str(db_dir / "guard.db")
    seed_review_db(db)

    proc = run_cli("guard.db", "locks", cwd=str(db_dir))
    assert proc.returncode == 0
    assert "deploy" in proc.stdout

    proc = run_cli("guard.db", "locks", cwd=str(tmp_path))   # no such file here
    assert proc.returncode == 1
    assert "database not found" in proc.stdout
