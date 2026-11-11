"""Small SQLite helpers. Every component owns one connection in autocommit mode
and opens explicit ``BEGIN IMMEDIATE`` transactions for writes.

A component instance may be shared between threads: every write transaction
and every read goes through the connection's re-entrant ``guard_lock``, so
statements of different threads never interleave inside one transaction.
Across processes, SQLite's own file locking serialises writers.
"""
import contextlib
import sqlite3
import threading

from .errors import GuardError


class _Connection(sqlite3.Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.guard_lock = threading.RLock()


def connect(path, timeout):
    """Open ``path`` (str/PathLike or ':memory:') in autocommit mode."""
    db = sqlite3.connect(path, timeout=timeout, isolation_level=None,
                         check_same_thread=False, factory=_Connection)
    db.execute("PRAGMA busy_timeout=%d" % int(timeout * 1000))
    return db


@contextlib.contextmanager
def write_txn(db):
    """BEGIN IMMEDIATE ... COMMIT, rolling back on any exception.

    The original exception is always the one that propagates: if SQLite has
    already rolled the transaction back by itself (e.g. SQLITE_FULL), the
    explicit ROLLBACK is skipped. A failing COMMIT is rolled back too, so the
    connection is never left inside a stale transaction.
    """
    with db.guard_lock:
        if db.in_transaction:
            raise GuardError(
                "nested write on the same instance: an apply_pending effect must not call "
                "write methods of the instance that is running it")
        db.execute("BEGIN IMMEDIATE")
        try:
            yield db
            db.execute("COMMIT")
        except BaseException:
            if db.in_transaction:
                try:
                    db.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
            raise


def close(db):
    """Close the connection after any in-flight transaction of another thread finishes."""
    with db.guard_lock:
        db.close()


def query_one(db, sql, params=()):
    """Run a read-only statement and return one row (or None) under the guard lock."""
    with db.guard_lock:
        return db.execute(sql, params).fetchone()


def query_all(db, sql, params=()):
    """Run a read-only statement and return all rows under the guard lock."""
    with db.guard_lock:
        return db.execute(sql, params).fetchall()
