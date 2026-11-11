"""Regression tests for _db.write_txn error handling."""
import sqlite3

import pytest

from joons_agent_guard import ResourceLock
from joons_agent_guard import _db


def _full_db(path):
    lock = ResourceLock(path)
    lock.acquire("seed", "o", "j")
    n = lock._db.execute("PRAGMA page_count").fetchone()[0]
    lock._db.execute("PRAGMA max_page_count=%d" % n)
    return lock


def test_disk_full_error_is_not_masked(tmp_path):
    lock = _full_db(tmp_path / "g.db")
    with pytest.raises(sqlite3.OperationalError) as err:
        for i in range(10000):  # fill the last free page(s) until SQLite reports a full DB
            lock.acquire("big-%05d-" % i + "x" * 1000, "o", "y" * 1000)
    assert "full" in str(err.value).lower()
    assert not lock._db.in_transaction
    assert lock.release("seed", "o", "j")  # connection still usable
    lock.close()


def test_exception_inside_block_rolls_back(tmp_path):
    db = _db.connect(str(tmp_path / "t.db"), 1.0)
    db.execute("CREATE TABLE t (x INTEGER)")
    with pytest.raises(RuntimeError):
        with _db.write_txn(db):
            db.execute("INSERT INTO t VALUES (1)")
            raise RuntimeError("boom")
    assert db.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 0
    assert not db.in_transaction
    db.close()
