"""Regression tests for the pre-release security review."""
import sqlite3

import pytest

from joons_agent_guard import CircuitBreaker, CorruptStorage, InvalidIdentifier, ReplayGuard, ResourceLock, cli
from joons_agent_guard.circuit import MAX_REASON_CHARS
from joons_agent_guard.errors import MAX_IDENTIFIER_CHARS


def test_identifier_length_is_capped(tmp_path):
    lock = ResourceLock(tmp_path / "g.db")
    assert lock.acquire("r" * MAX_IDENTIFIER_CHARS, "o", "j")
    with pytest.raises(InvalidIdentifier):
        lock.acquire("r" * (MAX_IDENTIFIER_CHARS + 1), "o", "j")
    lock.close()


def test_failure_reason_is_truncated(tmp_path):
    cb = CircuitBreaker(tmp_path / "c.db")
    cb.begin("w", "j")
    cb.finish("w", "j", success=False, reason="x" * (MAX_REASON_CHARS * 3))
    assert len(cb.snapshot("w").last_failure_reason) == MAX_REASON_CHARS
    cb.close()


def test_response_size_is_capped_and_configurable(tmp_path):
    guard = ReplayGuard(tmp_path / "g.db", max_response_bytes=100)
    guard.claim("p", "r", "d")
    with pytest.raises(ValueError):
        guard.complete("p", "r", "d", 200, {"blob": "x" * 200})
    assert guard.claim("p", "r", "d") == "pending"
    assert guard.complete("p", "r", "d", 200, {"ok": 1})
    guard.close()
    unlimited = ReplayGuard(tmp_path / "u.db", max_response_bytes=None)
    unlimited.claim("p", "r", "d")
    assert unlimited.complete("p", "r", "d", 200, {"blob": "x" * 2_000_000})
    unlimited.close()


def test_nan_response_is_rejected(tmp_path):
    guard = ReplayGuard(tmp_path / "g.db")
    guard.claim("p", "r", "d")
    with pytest.raises(ValueError):
        guard.complete("p", "r", "d", 200, {"x": float("nan")})
    guard.close()


def test_corrupt_stored_response_raises_corrupt_storage(tmp_path):
    path = tmp_path / "g.db"
    guard = ReplayGuard(path)
    guard.claim("p", "r", "d")
    guard.complete("p", "r", "d", 200, {"secret": "do-not-leak"})
    raw = sqlite3.connect(str(path))
    raw.execute("UPDATE requests SET response='{not json'")
    raw.commit()
    raw.close()
    with pytest.raises(CorruptStorage) as err:
        guard.response("p", "r", "d")
    assert "do-not-leak" not in str(err.value) and "{not json" not in str(err.value)
    guard.close()


def test_cli_read_commands_do_not_modify_the_file(tmp_path):
    path = tmp_path / "g.db"
    lock = ResourceLock(path)
    lock.acquire("res", "o", "j")
    lock.close()
    before = path.read_bytes()
    assert cli.main(["--db", str(path), "locks"]) == 0
    assert cli.main(["--db", str(path), "pending"]) == 0
    assert path.read_bytes() == before
