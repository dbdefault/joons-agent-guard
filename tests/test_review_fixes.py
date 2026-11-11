"""Regression tests for the issues found in the independent code review."""
import pytest

from joons_agent_guard import CircuitBreaker, GuardError, InvalidIdentifier, ReplayGuard, request_digest


def test_nested_write_inside_effect_raises_guard_error(tmp_path):
    guard = ReplayGuard(tmp_path / "g.db")
    d1, d2 = request_digest("POST", "/a", b"1"), request_digest("POST", "/b", b"2")
    assert guard.claim("p", "outer", d1) == "new"
    assert guard.claim("p", "inner", d2) == "new"

    def effect(db):
        guard.complete("p", "inner", d2, 200, {})  # same instance: forbidden
        return 200, {}

    with pytest.raises(GuardError):
        guard.apply_pending("p", "outer", d1, effect)
    assert guard.claim("p", "outer", d1) == "pending"
    assert guard.claim("p", "inner", d2) == "pending"
    guard.close()


def test_reset_validates_expected_job_id(tmp_path):
    cb = CircuitBreaker(tmp_path / "c.db")
    assert cb.begin("w", "job-1")
    for bad in ("", 123):
        with pytest.raises(InvalidIdentifier):
            cb.reset("w", expected_job_id=bad)
    assert cb.snapshot("w").pending_job == "job-1"
    cb.close()


@pytest.mark.parametrize("bad_code", ["200", True, 2.5, None])
def test_complete_rejects_non_int_code(tmp_path, bad_code):
    guard = ReplayGuard(tmp_path / "g.db")
    d = request_digest("POST", "/a", b"1")
    guard.claim("p", "r", d)
    with pytest.raises(TypeError):
        guard.complete("p", "r", d, bad_code, {})
    assert guard.claim("p", "r", d) == "pending"
    guard.close()


def test_apply_pending_rejects_non_int_code(tmp_path):
    guard = ReplayGuard(tmp_path / "g.db")
    d = request_digest("POST", "/a", b"1")
    guard.claim("p", "r", d)
    with pytest.raises(TypeError):
        guard.apply_pending("p", "r", d, lambda db: ("ok", {}))
    assert guard.claim("p", "r", d) == "pending"
    guard.close()
