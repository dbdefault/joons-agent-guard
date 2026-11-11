"""Durable per-actor circuit breaker with a single run slot.

States:
  closed     - healthy; the next job may start.
  open       - the last finished job failed; the next job that starts is the
               recovery attempt (state becomes half_open).
  half_open  - a recovery attempt is running; nothing else may start.

A job that started but never finished (crash, timeout, unknown result) keeps
its slot. It is never cleared by age: finish it explicitly, or call
``reset`` after an operator has checked what happened.
"""
import time
from dataclasses import dataclass
from typing import Optional

from . import _db
from .errors import require_ids

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS worker_circuits ("
    "actor TEXT PRIMARY KEY, state TEXT NOT NULL, pending TEXT, failures INTEGER NOT NULL, "
    "reason TEXT, updated REAL)"
)


#: Failure reasons are truncated to this many characters before they are stored.
MAX_REASON_CHARS = 4096


@dataclass(frozen=True)
class CircuitState:
    actor: str
    state: str  # closed | open | half_open
    pending_job: Optional[str]
    consecutive_failures: int
    last_failure_reason: Optional[str]


class CircuitBreaker:
    def __init__(self, path, *, timeout: float = 10.0):
        self._db = _db.connect(path, timeout)
        self._db.execute(_SCHEMA)

    def begin(self, actor: str, job_id: str) -> bool:
        """Claim the actor's run slot for ``job_id``. False if a job is already pending."""
        require_ids(actor=actor, job_id=job_id)
        with _db.write_txn(self._db):
            row = self._db.execute(
                "SELECT state, pending FROM worker_circuits WHERE actor=?", (actor,)
            ).fetchone()
            if row and (row[1] is not None or row[0] == "half_open"):
                return False
            state = "half_open" if row and row[0] == "open" else "closed"
            self._db.execute(
                "INSERT INTO worker_circuits VALUES (?,?,?,0,NULL,?) ON CONFLICT(actor) DO UPDATE SET "
                "state=excluded.state, pending=excluded.pending, updated=excluded.updated",
                (actor, state, job_id, time.time()),
            )
        return True

    def finish(self, actor: str, job_id: str, *, success: bool, reason: Optional[str] = None) -> bool:
        """Record the outcome of the pending job. False if ``job_id`` is not the pending job.

        A failure ``reason`` longer than MAX_REASON_CHARS is truncated before it is stored.
        """
        require_ids(actor=actor, job_id=job_id)
        if reason is not None:
            reason = str(reason)[:MAX_REASON_CHARS]
        with _db.write_txn(self._db):
            cur = self._db.execute(
                "UPDATE worker_circuits SET state=?, pending=NULL, "
                "failures=CASE WHEN ? THEN 0 ELSE failures+1 END, reason=?, updated=? "
                "WHERE actor=? AND pending=?",
                ("closed" if success else "open", bool(success), None if success else reason,
                 time.time(), actor, job_id),
            )
        return cur.rowcount == 1

    def snapshot(self, actor: str) -> CircuitState:
        require_ids(actor=actor)
        row = _db.query_one(
            self._db, "SELECT state, pending, failures, reason FROM worker_circuits WHERE actor=?", (actor,)
        )
        if not row:
            return CircuitState(actor, "closed", None, 0, None)
        return CircuitState(actor, row[0], row[1], row[2], row[3])

    def reset(self, actor: str, expected_job_id: Optional[str] = None) -> bool:
        """Operator recovery: clear the actor back to closed.

        If ``expected_job_id`` is given, only reset while that job is still the pending one.
        """
        require_ids(actor=actor)
        if expected_job_id is not None:
            require_ids(expected_job_id=expected_job_id)
        with _db.write_txn(self._db):
            if expected_job_id is None:
                cur = self._db.execute(
                    "UPDATE worker_circuits SET state='closed', pending=NULL, failures=0, reason=NULL, updated=? "
                    "WHERE actor=?", (time.time(), actor))
            else:
                cur = self._db.execute(
                    "UPDATE worker_circuits SET state='closed', pending=NULL, failures=0, reason=NULL, updated=? "
                    "WHERE actor=? AND pending=?", (time.time(), actor, expected_job_id))
        return cur.rowcount == 1

    def close(self) -> None:
        _db.close(self._db)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
