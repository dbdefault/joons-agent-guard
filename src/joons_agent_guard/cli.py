"""Operator command line for joons_agent_guard.

``python -m joons_agent_guard --db PATH <command>`` inspects the state a guard
database holds and runs the two audited recovery operations:

* ``locks``                    - list held locks (resource, owner, job_id, age)
* ``circuit ACTOR``            - show one actor's circuit snapshot
* ``pending [--older-than S]`` - list replay requests stuck in state ``pending``
* ``break-lock RESOURCE --job-id ID [--yes]``
* ``reset-circuit ACTOR [--job-id ID] [--yes]``

The read commands use plain ``SELECT`` statements only: they never create
tables, claim requests or change state in any way. ``pending`` therefore
tolerates a database that never used ``ReplayGuard`` (no ``requests`` table).
The two recovery commands call the library's own audited API
(``ResourceLock.break_lock`` / ``CircuitBreaker.reset``) and only act when
``--yes`` is given; without it they print what would happen and exit 2.

``--older-than`` lists only pending rows whose recorded claim time is at
least ``SECONDS`` ago (rows without a claim time are never listed by it).
Age is information for the operator, never a reason to re-run anything
automatically: pending still means "unknown".

Output is plain text by default and JSON with ``--json``.
Exit codes: 0 ok, 1 not found / nothing changed, 2 confirmation required or
bad usage.
"""
import argparse
import json
import math
import os
import sqlite3
import time
from pathlib import Path
from typing import List, Optional, Tuple

from . import CircuitBreaker, GuardError, ResourceLock
from .errors import require_ids

DIGEST_PREFIX_CHARS = 12
_READ_TIMEOUT = 10.0

# how long a guard connection waits for another process's write transaction
_BUSY_TIMEOUT_MS = 10000


# ---------------------------------------------------------------------------
# small output / formatting helpers
# ---------------------------------------------------------------------------

def _echo(line: str) -> None:
    """print() that survives consoles or pipes that cannot encode the content."""
    try:
        print(line)
    except UnicodeEncodeError:
        print(line.encode("ascii", "backslashreplace").decode("ascii"))


def _print(opts, lines, payload) -> None:
    """Emit the human-readable lines, or the JSON payload, on stdout."""
    if opts.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        for line in lines:
            _echo(line)


def _table(headers: List[str], rows: List[List[str]]) -> List[str]:
    """Two-space-aligned text columns; the last column is not padded."""
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    lines = ["  ".join(h.ljust(w) for h, w in zip(headers, widths)).rstrip()]
    for row in rows:
        lines.append("  ".join(c.ljust(w) for c, w in zip(row, widths)).rstrip())
    return lines


def _human_age(seconds: float) -> str:
    total = max(0, int(seconds))
    if total < 60:
        return "%ds" % total
    if total < 3600:
        return "%dm%02ds" % (total // 60, total % 60)
    if total < 86400:
        return "%dh%02dm" % (total // 3600, (total % 3600) // 60)
    return "%dd%02dh" % (total // 86400, (total % 86400) // 3600)


def _require_db(opts) -> bool:
    """Refuse to run against a path that does not exist (exit 1, report it)."""
    if os.path.isfile(opts.db):
        return True
    _print(opts, ["database not found: %s" % opts.db],
           {"error": "database not found", "db": str(opts.db)})
    return False


# ---------------------------------------------------------------------------
# read-only database access (plain SELECTs; a missing table simply means empty)
# ---------------------------------------------------------------------------

def _connect_read(path):
    """Open the database strictly read-only.

    A crashed writer can leave a hot journal; a writable connection would
    silently roll it back and rewrite the file. Read commands must not change
    evidence, so they open with ``mode=ro`` and report the error instead.
    """
    uri = Path(path).resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=_READ_TIMEOUT, isolation_level=None)
    conn.execute("PRAGMA busy_timeout=%d" % _BUSY_TIMEOUT_MS)
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA trusted_schema=OFF")
    return conn


def _select(conn, sql: str, params: tuple = ()) -> List[Tuple]:
    try:
        return conn.execute(sql, params).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            return []  # the component was never used in this database
        raise


def _select_one(conn, sql: str, params: tuple = ()) -> Optional[Tuple]:
    rows = _select(conn, sql, params)
    return rows[0] if rows else None


# ---------------------------------------------------------------------------
# commands (each returns the process exit code)
# ---------------------------------------------------------------------------

def _cmd_locks(opts) -> int:
    if not _require_db(opts):
        return 1
    conn = _connect_read(opts.db)
    try:
        rows = _select(conn, "SELECT resource, owner, job_id, acquired FROM resource_locks ORDER BY acquired")
    finally:
        conn.close()
    now = time.time()
    entries = [{"resource": r[0], "owner": r[1], "job_id": r[2],
                "age_seconds": round(max(0.0, now - r[3]), 3)} for r in rows]
    if opts.json:
        _print(opts, [], {"locks": entries})
        return 0
    if not rows:
        _print(opts, ["no held locks"], {})
        return 0
    lines = _table(["RESOURCE", "OWNER", "JOB_ID", "AGE"],
                   [[r[0], r[1], r[2], _human_age(now - r[3])] for r in rows])
    _print(opts, lines, {})
    return 0


def _cmd_circuit(opts) -> int:
    if not _require_db(opts):
        return 1
    require_ids(actor=opts.actor)
    conn = _connect_read(opts.db)
    try:
        row = _select_one(conn, "SELECT state, pending, failures, reason FROM worker_circuits WHERE actor=?",
                          (opts.actor,))
    finally:
        conn.close()
    if row is None:
        _print(opts, ["no circuit recorded for actor '%s'" % opts.actor],
               {"error": "not found", "actor": opts.actor})
        return 1
    payload = {"actor": opts.actor, "state": row[0], "pending_job": row[1],
               "consecutive_failures": row[2], "last_failure_reason": row[3]}
    lines = [
        "actor: %s" % opts.actor,
        "state: %s" % row[0],
        "pending_job: %s" % (row[1] if row[1] is not None else "(none)"),
        "consecutive_failures: %s" % row[2],
        "last_failure_reason: %s" % (row[3] if row[3] is not None else "(none)"),
    ]
    _print(opts, lines, payload)
    return 0


def _cmd_pending(opts) -> int:
    if not _require_db(opts):
        return 1
    conn = _connect_read(opts.db)
    try:
        rows = _select(conn,
                       "SELECT principal, request_id, digest, claimed FROM requests WHERE state='pending' "
                       "ORDER BY principal, request_id")
    finally:
        conn.close()
    now = time.time()
    if opts.older_than is not None:
        rows = [r for r in rows if r[3] is not None and now - r[3] >= opts.older_than]
    entries = [{"principal": r[0], "request_id": r[1], "digest_prefix": r[2][:DIGEST_PREFIX_CHARS],
                "age_seconds": None if r[3] is None else round(max(0.0, now - r[3]), 3)}
               for r in rows]
    payload = {"pending": entries}
    if opts.older_than is not None:
        payload["older_than"] = opts.older_than
    lines_headers = ["PRINCIPAL", "REQUEST_ID", "DIGEST", "AGE"]
    if opts.json:
        _print(opts, [], payload)
        return 0
    if not entries:
        if opts.older_than is not None:
            _print(opts, ["no pending requests older than %ss" % _human_age(opts.older_than)], {})
        else:
            _print(opts, ["no pending requests"], {})
        return 0
    lines = _table(lines_headers, [[e["principal"], e["request_id"], e["digest_prefix"],
                                    "?" if e["age_seconds"] is None else _human_age(e["age_seconds"])]
                                   for e in entries])
    _print(opts, lines, {})
    return 0


def _cmd_break_lock(opts) -> int:
    if not _require_db(opts):
        return 1
    require_ids(resource=opts.resource, job_id=opts.job_id)  # validate before the dry run
    if not opts.yes:
        conn = _connect_read(opts.db)
        try:
            row = _select_one(conn, "SELECT owner, job_id FROM resource_locks WHERE resource=?",
                              (opts.resource,))
        finally:
            conn.close()
        holder = {"owner": row[0], "job_id": row[1]} if row else None
        if holder:
            line = ("would break lock on resource '%s' for job_id '%s' (currently held by owner '%s'); "
                    "nothing changed; re-run with --yes to apply"
                    % (opts.resource, opts.job_id, holder["owner"]))
        else:
            line = ("no lock held on resource '%s'; there is nothing to break; "
                    "nothing changed; re-run with --yes to apply" % opts.resource)
        _print(opts, [line],
               {"resource": opts.resource, "job_id": opts.job_id, "applied": False,
                "would_break": holder is not None, "current_holder": holder})
        return 2
    lock = ResourceLock(opts.db)
    try:
        broken = lock.break_lock(opts.resource, opts.job_id)
    finally:
        lock.close()
    if broken:
        _print(opts, ["broke lock on resource '%s' for job_id '%s'" % (opts.resource, opts.job_id)],
               {"resource": opts.resource, "job_id": opts.job_id, "broken": True})
        return 0
    _print(opts, ["no matching lock on resource '%s' for job_id '%s'; nothing changed"
                  % (opts.resource, opts.job_id)],
           {"resource": opts.resource, "job_id": opts.job_id, "broken": False})
    return 1


def _cmd_reset_circuit(opts) -> int:
    if not _require_db(opts):
        return 1
    require_ids(actor=opts.actor)                # validate before the dry run
    if opts.job_id is not None:
        require_ids(expected_job_id=opts.job_id)
    if not opts.yes:
        conn = _connect_read(opts.db)
        try:
            row = _select_one(conn, "SELECT state, pending, failures, reason FROM worker_circuits WHERE actor=?",
                              (opts.actor,))
        finally:
            conn.close()
        current = None if row is None else {
            "state": row[0], "pending_job": row[1], "consecutive_failures": row[2],
            "last_failure_reason": row[3]}
        if current is None:
            line = ("no circuit recorded for actor '%s'; resetting would change nothing; "
                    "nothing changed; re-run with --yes to apply" % opts.actor)
        else:
            scope = ("only while job_id '%s' is still pending" % opts.job_id) if opts.job_id else "unconditionally"
            line = ("would reset circuit for actor '%s' to closed %s (state '%s', pending_job %s, "
                    "consecutive_failures %s); nothing changed; re-run with --yes to apply"
                    % (opts.actor, scope, current["state"],
                       current["pending_job"] if current["pending_job"] is not None else "(none)",
                       current["consecutive_failures"]))
        _print(opts, [line],
               {"actor": opts.actor, "job_id": opts.job_id, "applied": False,
                "would_reset": current is not None, "current": current})
        return 2
    breaker = CircuitBreaker(opts.db)
    try:
        reset = breaker.reset(opts.actor, expected_job_id=opts.job_id)
    finally:
        breaker.close()
    if reset:
        _print(opts, ["reset circuit for actor '%s' to closed" % opts.actor],
               {"actor": opts.actor, "job_id": opts.job_id, "reset": True})
        return 0
    _print(opts, ["no matching circuit for actor '%s'%s; nothing changed"
                  % (opts.actor, " (job_id '%s')" % opts.job_id if opts.job_id else "")],
           {"actor": opts.actor, "job_id": opts.job_id, "reset": False})
    return 1


# ---------------------------------------------------------------------------
# argument parsing
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="joons_agent_guard",
        usage="%(prog)s --db PATH [--json] COMMAND ...",
        description="Operator tool for joons_agent_guard: inspect held locks, circuit snapshots and "
                    "pending replay requests, and run the audited recovery operations.",
        epilog="exit codes: 0 ok, 1 not found / nothing changed, 2 confirmation required or bad usage",
    )
    parser.add_argument("--db", default=None, metavar="PATH",
                        help="path to the guard's SQLite database file (required)")
    parser.add_argument("--json", action="store_true", default=False,
                        help="emit machine-readable JSON instead of plain text")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND", required=True)

    def _common(p):
        # accept the global flags after the command as well; hidden from the
        # per-command help because the top-level -h documents them
        p.add_argument("--db", default=argparse.SUPPRESS, metavar="PATH", help=argparse.SUPPRESS)
        p.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help=argparse.SUPPRESS)

    p = sub.add_parser("locks", help="list held locks (resource, owner, job_id, age)")
    _common(p)
    p.set_defaults(func=_cmd_locks)

    p = sub.add_parser("circuit", help="show one actor's circuit snapshot")
    p.add_argument("actor", help="actor name to inspect")
    _common(p)
    p.set_defaults(func=_cmd_circuit)

    p = sub.add_parser("pending", help="list replay requests in state 'pending' (read-only)")
    p.add_argument("--older-than", type=float, default=None, metavar="SECONDS",
                   help="only list requests claimed at least SECONDS ago")
    _common(p)
    p.set_defaults(func=_cmd_pending)

    p = sub.add_parser("break-lock", help="break a lock whose holder is confirmed dead")
    p.add_argument("resource", help="locked resource to break")
    p.add_argument("--job-id", required=True, metavar="ID",
                   help="job_id that must currently hold the lock (refuses stale ids)")
    p.add_argument("--yes", action="store_true",
                   help="actually break the lock; without it only print what would happen")
    _common(p)
    p.set_defaults(func=_cmd_break_lock)

    p = sub.add_parser("reset-circuit", help="reset one actor's circuit back to closed")
    p.add_argument("actor", help="actor whose circuit should be reset")
    p.add_argument("--job-id", default=None, metavar="ID",
                   help="only reset while this job is still the actor's pending one")
    p.add_argument("--yes", action="store_true",
                   help="actually reset the circuit; without it only print what would happen")
    _common(p)
    p.set_defaults(func=_cmd_reset_circuit)
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """Run one command; return the process exit code (0/1/2)."""
    parser = _build_parser()
    try:
        opts = parser.parse_args(argv)
        if not getattr(opts, "db", None):
            parser.error("the following arguments are required: --db")
    except SystemExit as exc:  # usage error or --help
        return exc.code if isinstance(exc.code, int) else 0
    older_than = getattr(opts, "older_than", None)
    if older_than is not None and (not math.isfinite(older_than) or older_than < 0):
        _print(opts, ["error: --older-than must be a finite number >= 0"],
               {"error": "--older-than must be a finite number >= 0"})
        return 2
    try:
        return int(opts.func(opts))
    except (sqlite3.Error, GuardError) as exc:
        _print(opts, ["error: %s" % exc], {"error": str(exc)})
        return 1
