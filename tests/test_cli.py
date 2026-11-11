"""Operator CLI tests (``python -m joons_agent_guard --db PATH <command>``).

Every command and every exit code is covered twice:

* in-process, by calling ``cli.main(argv)`` and asserting the returned exit
  code plus the captured stdout;
* in a subprocess, via ``sys.executable -m joons_agent_guard`` with
  ``PYTHONPATH=src`` in the environment, asserting the process exit code and
  its stdout (exactly how an operator invokes the tool).

The recovery dry-runs (no ``--yes``) are additionally checked to leave the
database untouched.
"""
import json
import os
import sqlite3
import subprocess
import sys
import time

import pytest

# Make the in-tree package importable here and in the subprocess, even when
# the suite is run without PYTHONPATH set.
_SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if os.path.isdir(_SRC) and _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from joons_agent_guard import CircuitBreaker, ReplayGuard, ResourceLock, request_digest
from joons_agent_guard import cli


# ---------------------------------------------------------------------------
# fixtures and helpers
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "guard.sqlite")


@pytest.fixture()
def populated_db(db_path):
    """One held lock, two circuits (open / pending slot), one pending + one completed request."""
    lock = ResourceLock(db_path)
    assert lock.acquire("deploy", "worker-1", "job-43") is True
    lock.close()

    breaker = CircuitBreaker(db_path)
    assert breaker.begin("w1", "job-a") is True
    assert breaker.finish("w1", "job-a", success=False, reason="upstream 500") is True  # open
    assert breaker.begin("w2", "job-p") is True  # closed with a pending slot
    breaker.close()

    guard = ReplayGuard(db_path)
    pending_digest = request_digest("POST", "/work", b"1")
    assert guard.claim("client-a", "req-1", pending_digest) == "new"  # stays pending
    done_digest = request_digest("POST", "/work", b"2")
    assert guard.claim("client-b", "req-2", done_digest) == "new"
    assert guard.complete("client-b", "req-2", done_digest, 200, {"ok": True}) is True
    guard.close()
    return db_path


def inproc(db, *argv):
    """Run the CLI in-process; return (exit code, captured stdout)."""
    return cli.main(["--db", db, *argv])


def inproc_out(capsys):
    return capsys.readouterr().out


def subproc(db, *argv):
    """Run the CLI exactly like an operator: python -m joons_agent_guard."""
    env = dict(os.environ)
    env["PYTHONPATH"] = _SRC
    return subprocess.run(
        [sys.executable, "-m", "joons_agent_guard", "--db", db, *argv],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        env=env, timeout=60,
    )


def json_out(capsys):
    return json.loads(capsys.readouterr().out)


# ---------------------------------------------------------------------------
# locks
# ---------------------------------------------------------------------------

class TestLocks:
    def test_lists_held_locks_in_process(self, populated_db, capsys):
        assert inproc(populated_db, "locks") == 0
        out = inproc_out(capsys)
        assert "RESOURCE" in out and "OWNER" in out and "JOB_ID" in out and "AGE" in out
        assert "deploy" in out and "worker-1" in out and "job-43" in out

    def test_lists_held_locks_as_json(self, populated_db, capsys):
        assert inproc(populated_db, "locks", "--json") == 0
        payload = json_out(capsys)
        assert len(payload["locks"]) == 1
        entry = payload["locks"][0]
        assert entry["resource"] == "deploy"
        assert entry["owner"] == "worker-1"
        assert entry["job_id"] == "job-43"
        assert 0.0 <= entry["age_seconds"] < 60.0

    def test_empty_locks_is_ok(self, db_path, capsys):
        ResourceLock(db_path).close()  # existing file, no locks held
        assert inproc(db_path, "locks") == 0
        assert "no held locks" in inproc_out(capsys)

    def test_missing_database_is_not_found(self, tmp_path, capsys):
        assert inproc(str(tmp_path / "nope.sqlite"), "locks") == 1
        assert "database not found" in inproc_out(capsys)

    def test_missing_database_is_not_found_via_subprocess(self, tmp_path):
        proc = subproc(str(tmp_path / "nope.sqlite"), "locks")
        assert proc.returncode == 1
        assert "database not found" in proc.stdout

    def test_locks_via_subprocess_json(self, populated_db):
        proc = subproc(populated_db, "locks", "--json")
        assert proc.returncode == 0
        entry = json.loads(proc.stdout)["locks"][0]
        assert (entry["resource"], entry["owner"], entry["job_id"]) == ("deploy", "worker-1", "job-43")


# ---------------------------------------------------------------------------
# circuit
# ---------------------------------------------------------------------------

class TestCircuit:
    def test_snapshot_of_open_actor(self, populated_db, capsys):
        assert inproc(populated_db, "circuit", "w1") == 0
        out = inproc_out(capsys)
        assert "actor: w1" in out
        assert "state: open" in out
        assert "pending_job: (none)" in out
        assert "consecutive_failures: 1" in out
        assert "last_failure_reason: upstream 500" in out

    def test_snapshot_with_pending_slot(self, populated_db, capsys):
        assert inproc(populated_db, "circuit", "w2") == 0
        out = inproc_out(capsys)
        assert "state: closed" in out
        assert "pending_job: job-p" in out

    def test_snapshot_as_json(self, populated_db, capsys):
        assert inproc(populated_db, "circuit", "w1", "--json") == 0
        payload = json_out(capsys)
        assert payload == {"actor": "w1", "state": "open", "pending_job": None,
                           "consecutive_failures": 1, "last_failure_reason": "upstream 500"}

    def test_unknown_actor_is_not_found(self, populated_db, capsys):
        assert inproc(populated_db, "circuit", "ghost") == 1
        assert "no circuit recorded" in inproc_out(capsys)

    def test_circuit_via_subprocess(self, populated_db):
        assert subproc(populated_db, "circuit", "w1").returncode == 0
        assert subproc(populated_db, "circuit", "ghost").returncode == 1


# ---------------------------------------------------------------------------
# pending
# ---------------------------------------------------------------------------

class TestPending:
    def test_lists_only_pending_rows(self, populated_db, capsys):
        digest = request_digest("POST", "/work", b"1")
        assert inproc(populated_db, "pending") == 0
        out = inproc_out(capsys)
        assert "PRINCIPAL" in out and "REQUEST_ID" in out and "DIGEST" in out
        assert "client-a" in out and "req-1" in out
        assert digest[:12] in out          # only the prefix is shown
        assert digest not in out           # never the full digest
        assert "client-b" not in out       # completed rows are not listed
        assert "req-2" not in out

    def test_pending_as_json(self, populated_db, capsys):
        digest = request_digest("POST", "/work", b"1")
        assert inproc(populated_db, "pending", "--json") == 0
        rows = json_out(capsys)["pending"]
        assert len(rows) == 1
        age = rows[0].pop("age_seconds")
        assert rows == [{"principal": "client-a", "request_id": "req-1", "digest_prefix": digest[:12]}]
        assert 0.0 <= age < 3600.0

    def test_empty_pending_is_ok(self, db_path, capsys):
        ReplayGuard(db_path).close()  # requests table exists, nothing pending
        assert inproc(db_path, "pending") == 0
        assert "no pending requests" in inproc_out(capsys)

    def test_pending_without_requests_table_is_ok(self, db_path, capsys):
        ResourceLock(db_path).close()  # no requests table at all
        assert inproc(db_path, "pending") == 0
        assert "no pending requests" in inproc_out(capsys)

    def test_older_than_zero_lists_rows(self, populated_db, capsys):
        assert inproc(populated_db, "pending", "--older-than", "0") == 0
        out = inproc_out(capsys)
        assert "client-a" in out

    def test_older_than_excludes_recent_claims(self, populated_db, capsys):
        # the requests were claimed moments ago
        assert inproc(populated_db, "pending", "--older-than", "3600") == 0
        out = inproc_out(capsys)
        assert "client-a" not in out
        assert "no pending requests older than" in out

    def test_older_than_lists_old_claims(self, populated_db, capsys):
        conn = sqlite3.connect(str(populated_db))
        conn.execute("UPDATE requests SET claimed = ?", (time.time() - 7200,))
        conn.commit()
        conn.close()
        assert inproc(populated_db, "pending", "--older-than", "3600") == 0
        assert "client-a" in inproc_out(capsys)

    def test_older_than_as_json(self, populated_db, capsys):
        assert inproc(populated_db, "pending", "--older-than", "3600", "--json") == 0
        payload = json_out(capsys)
        assert payload["pending"] == []
        assert payload["older_than"] == 3600.0

    def test_pending_json_reports_age(self, populated_db, capsys):
        assert inproc(populated_db, "pending", "--json") == 0
        rows = json_out(capsys)["pending"]
        assert rows and all(0.0 <= r["age_seconds"] < 3600.0 for r in rows)

    def test_negative_older_than_is_bad_usage(self, populated_db, capsys):
        assert inproc(populated_db, "pending", "--older-than", "-1") == 2

    def test_pending_via_subprocess(self, populated_db):
        proc = subproc(populated_db, "pending")
        assert proc.returncode == 0
        assert "client-a" in proc.stdout and "req-1" in proc.stdout
        proc = subproc(populated_db, "pending", "--older-than", "3600")
        assert proc.returncode == 0
        assert "client-a" not in proc.stdout


# ---------------------------------------------------------------------------
# break-lock
# ---------------------------------------------------------------------------

class TestBreakLock:
    def test_without_yes_prints_plan_and_changes_nothing(self, populated_db, capsys):
        assert inproc(populated_db, "break-lock", "deploy", "--job-id", "job-43") == 2
        out = inproc_out(capsys)
        assert "would break lock" in out and "deploy" in out and "worker-1" in out
        assert ResourceLock(populated_db).status("deploy") is not None  # still held

    def test_dry_run_json(self, populated_db, capsys):
        assert inproc(populated_db, "break-lock", "deploy", "--job-id", "job-43", "--json") == 2
        payload = json_out(capsys)
        assert payload["applied"] is False
        assert payload["would_break"] is True
        assert payload["current_holder"] == {"owner": "worker-1", "job_id": "job-43"}

    def test_yes_breaks_the_matching_lock(self, populated_db, capsys):
        assert inproc(populated_db, "break-lock", "deploy", "--job-id", "job-43", "--yes") == 0
        assert "broke lock" in inproc_out(capsys)
        assert ResourceLock(populated_db).status("deploy") is None  # really gone

    def test_yes_json(self, populated_db, capsys):
        assert inproc(populated_db, "break-lock", "deploy", "--job-id", "job-43", "--yes", "--json") == 0
        assert json_out(capsys) == {"resource": "deploy", "job_id": "job-43", "broken": True}

    def test_yes_with_stale_job_id_changes_nothing(self, populated_db, capsys):
        assert inproc(populated_db, "break-lock", "deploy", "--job-id", "old-job", "--yes") == 1
        assert "no matching lock" in inproc_out(capsys)
        assert ResourceLock(populated_db).status("deploy") is not None  # untouched

    def test_yes_on_unlocked_resource_changes_nothing(self, populated_db, capsys):
        assert inproc(populated_db, "break-lock", "no-such-resource", "--job-id", "x", "--yes") == 1
        assert "no matching lock" in inproc_out(capsys)

    def test_missing_job_id_is_bad_usage(self, populated_db):
        assert inproc(populated_db, "break-lock", "deploy") == 2

    def test_via_subprocess_dry_run_then_apply(self, populated_db):
        dry = subproc(populated_db, "break-lock", "deploy", "--job-id", "job-43")
        assert dry.returncode == 2
        assert "would break lock" in dry.stdout
        assert ResourceLock(populated_db).status("deploy") is not None  # unchanged

        applied = subproc(populated_db, "break-lock", "deploy", "--job-id", "job-43", "--yes")
        assert applied.returncode == 0
        assert "broke lock" in applied.stdout
        assert ResourceLock(populated_db).status("deploy") is None


# ---------------------------------------------------------------------------
# reset-circuit
# ---------------------------------------------------------------------------

class TestResetCircuit:
    def test_without_yes_prints_plan_and_changes_nothing(self, populated_db, capsys):
        assert inproc(populated_db, "reset-circuit", "w1") == 2
        out = inproc_out(capsys)
        assert "would reset circuit" in out and "w1" in out
        snap = CircuitBreaker(populated_db).snapshot("w1")
        assert snap.state == "open" and snap.consecutive_failures == 1  # untouched

    def test_dry_run_json(self, populated_db, capsys):
        assert inproc(populated_db, "reset-circuit", "w1", "--json") == 2
        payload = json_out(capsys)
        assert payload["applied"] is False
        assert payload["would_reset"] is True
        assert payload["current"]["state"] == "open"

    def test_yes_resets_to_closed(self, populated_db, capsys):
        assert inproc(populated_db, "reset-circuit", "w1", "--yes") == 0
        assert "reset circuit" in inproc_out(capsys)
        snap = CircuitBreaker(populated_db).snapshot("w1")
        assert (snap.state, snap.pending_job, snap.consecutive_failures) == ("closed", None, 0)

    def test_yes_with_job_id_only_resets_while_pending(self, populated_db, capsys):
        assert inproc(populated_db, "reset-circuit", "w2", "--job-id", "wrong", "--yes") == 1
        assert CircuitBreaker(populated_db).snapshot("w2").pending_job == "job-p"  # untouched
        assert inproc(populated_db, "reset-circuit", "w2", "--job-id", "job-p", "--yes") == 0
        assert CircuitBreaker(populated_db).snapshot("w2").pending_job is None

    def test_yes_unknown_actor_changes_nothing(self, populated_db, capsys):
        assert inproc(populated_db, "reset-circuit", "ghost", "--yes") == 1
        assert "no matching circuit" in inproc_out(capsys)

    def test_via_subprocess_dry_run_then_apply(self, populated_db):
        dry = subproc(populated_db, "reset-circuit", "w1")
        assert dry.returncode == 2
        assert "would reset circuit" in dry.stdout

        applied = subproc(populated_db, "reset-circuit", "w1", "--yes")
        assert applied.returncode == 0
        assert "reset circuit" in applied.stdout
        snap = CircuitBreaker(populated_db).snapshot("w1")
        assert snap.state == "closed" and snap.consecutive_failures == 0


# ---------------------------------------------------------------------------
# bad usage and help (exit code 2)
# ---------------------------------------------------------------------------

class TestUsage:
    @pytest.mark.parametrize("argv", [
        ("locks",),                      # missing --db
        ("no-such-command",),            # unknown command
        ("break-lock", "deploy"),        # missing required --job-id
        ("circuit",),                    # missing actor
        ("--bogus-flag",),               # unknown global flag
    ])
    def test_bad_usage_in_process_returns_2(self, populated_db, argv):
        assert cli.main(list(argv)) == 2

    def test_no_command_returns_2(self, populated_db):
        assert cli.main([]) == 2

    def test_help_returns_0(self, capsys):
        assert cli.main(["--help"]) == 0
        out = inproc_out(capsys)
        for name in ("locks", "circuit", "pending", "break-lock", "reset-circuit"):
            assert name in out

    @pytest.mark.parametrize("argv", [
        ["--db", "x.db"],                # no command
        ["no-such-command"],
        ["break-lock", "deploy"],        # missing --job-id
    ])
    def test_bad_usage_via_subprocess_returns_2(self, populated_db, argv):
        assert subproc(populated_db, *argv).returncode == 2

    def test_help_via_subprocess(self):
        proc = subproc("unused.db", "--help")
        assert proc.returncode == 0
        assert "locks" in proc.stdout and "reset-circuit" in proc.stdout


# ---------------------------------------------------------------------------
# every command and exit code through the real entry point, end to end
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("argv,expected_rc,expect_in_out", [
    (["locks"], 0, "deploy"),
    (["circuit", "w1"], 0, "state: open"),
    (["circuit", "ghost"], 1, "no circuit recorded"),
    (["pending"], 0, "client-a"),
    (["break-lock", "deploy", "--job-id", "job-43"], 2, "would break lock"),
    (["break-lock", "deploy", "--job-id", "wrong", "--yes"], 1, "no matching lock"),
    (["break-lock", "deploy", "--job-id", "job-43", "--yes"], 0, "broke lock"),
    (["reset-circuit", "w1"], 2, "would reset circuit"),
    (["reset-circuit", "ghost", "--yes"], 1, "no matching circuit"),
    (["reset-circuit", "w1", "--yes"], 0, "reset circuit"),
])
def test_end_to_end_matrix_via_subprocess(populated_db, argv, expected_rc, expect_in_out):
    proc = subproc(populated_db, *argv)
    assert proc.returncode == expected_rc
    assert expect_in_out in proc.stdout
