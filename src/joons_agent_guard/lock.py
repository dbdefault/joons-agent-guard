"""Durable, SQLite-backed resource locks.

A lock is held by an (owner, job_id) pair and survives process restarts.
Locks never expire on their own: if a holder crashed, its work may be in an
unknown state, so only an explicit, audited ``break_lock`` removes it.
"""
import contextlib
import time
from dataclasses import dataclass
from typing import List, Optional

from . import _db
from .errors import LockHeld, require_ids

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS resource_locks ("
    "resource TEXT PRIMARY KEY, owner TEXT NOT NULL, job_id TEXT NOT NULL, acquired REAL NOT NULL)"
)


@dataclass(frozen=True)
class LockInfo:
    resource: str
    owner: str
    job_id: str
    acquired_at: float


class ResourceLock:
    """Exclusive locks on named resources, shared by every process using the same DB file."""

    def __init__(self, path, *, timeout: float = 10.0):
        self._db = _db.connect(path, timeout)
        self._db.execute(_SCHEMA)

    # -- core API ---------------------------------------------------------
    def acquire(self, resource: str, owner: str, job_id: str) -> bool:
        """Take the lock. Returns False if anyone (including this holder) already holds it."""
        require_ids(resource=resource, owner=owner, job_id=job_id)
        with _db.write_txn(self._db):
            cur = self._db.execute(
                "INSERT OR IGNORE INTO resource_locks VALUES (?,?,?,?)",
                (resource, owner, job_id, time.time()),
            )
        return cur.rowcount == 1

    def release(self, resource: str, owner: str, job_id: str) -> bool:
        """Release only if both owner and job_id match the current holder."""
        require_ids(resource=resource, owner=owner, job_id=job_id)
        with _db.write_txn(self._db):
            cur = self._db.execute(
                "DELETE FROM resource_locks WHERE resource=? AND owner=? AND job_id=?",
                (resource, owner, job_id),
            )
        return cur.rowcount == 1

    def status(self, resource: str) -> Optional[LockInfo]:
        require_ids(resource=resource)
        row = _db.query_one(
            self._db, "SELECT resource, owner, job_id, acquired FROM resource_locks WHERE resource=?", (resource,)
        )
        return LockInfo(*row) if row else None

    def list_locks(self) -> List[LockInfo]:
        rows = _db.query_all(
            self._db, "SELECT resource, owner, job_id, acquired FROM resource_locks ORDER BY acquired"
        )
        return [LockInfo(*r) for r in rows]

    def break_lock(self, resource: str, expected_job_id: str) -> bool:
        """Operator recovery: remove a lock whose holder is known to be dead.

        ``expected_job_id`` must match, so a lock re-acquired by a new job in
        the meantime is never removed by mistake.
        """
        require_ids(resource=resource, expected_job_id=expected_job_id)
        with _db.write_txn(self._db):
            cur = self._db.execute(
                "DELETE FROM resource_locks WHERE resource=? AND job_id=?", (resource, expected_job_id)
            )
        return cur.rowcount == 1

    @contextlib.contextmanager
    def hold(self, resource: str, owner: str, job_id: str):
        """``with lock.hold(...):`` acquires or raises LockHeld; releases on exit."""
        if not self.acquire(resource, owner, job_id):
            raise LockHeld(resource, self.status(resource))
        try:
            yield
        finally:
            self.release(resource, owner, job_id)

    # -- lifecycle --------------------------------------------------------
    def close(self) -> None:
        _db.close(self._db)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
