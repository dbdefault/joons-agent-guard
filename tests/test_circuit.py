"""Single-process pytest suite for the durable per-actor CircuitBreaker.

Covers the full closed -> open -> half_open lifecycle, the single run slot,
finish/reset guards, per-actor independence, durability across reopen,
identifier validation, the context manager and ':memory:'.
"""
import sqlite3

import pytest

from joons_agent_guard import CircuitBreaker, CircuitState, InvalidIdentifier


@pytest.fixture()
def cb():
    breaker = CircuitBreaker(":memory:")
    yield breaker
    breaker.close()


def fail_once(breaker, actor, job_id, reason="boom"):
    """Claim the slot and finish it with a failure; returns the snapshot after."""
    assert breaker.begin(actor, job_id) is True
    assert breaker.finish(actor, job_id, success=False, reason=reason) is True
    return breaker.snapshot(actor)


# ---------------------------------------------------------------------------
# Fresh actor / snapshot
# ---------------------------------------------------------------------------

def test_fresh_actor_snapshot_is_closed(cb):
    snap = cb.snapshot("never-seen")
    assert isinstance(snap, CircuitState)
    assert snap.actor == "never-seen"
    assert snap.state == "closed"
    assert snap.pending_job is None
    assert snap.consecutive_failures == 0
    assert snap.last_failure_reason is None


# ---------------------------------------------------------------------------
# begin: run slot claim
# ---------------------------------------------------------------------------

def test_begin_claims_slot_and_refuses_second_begin(cb):
    assert cb.begin("a", "job-1") is True
    snap = cb.snapshot("a")
    assert snap.pending_job == "job-1"
    assert snap.state == "closed"
    # any second begin while pending is refused, same or different job id
    assert cb.begin("a", "job-2") is False
    assert cb.begin("a", "job-1") is False
    snap = cb.snapshot("a")
    assert snap.pending_job == "job-1"
    assert snap.state == "closed"


# ---------------------------------------------------------------------------
# finish: success and failure outcomes
# ---------------------------------------------------------------------------

def test_finish_failure_opens_and_stores_reason(cb):
    snap = fail_once(cb, "a", "job-1", reason="upstream 500")
    assert snap.state == "open"
    assert snap.pending_job is None
    assert snap.consecutive_failures == 1
    assert snap.last_failure_reason == "upstream 500"


def test_finish_success_resets_failures_and_clears_reason(cb):
    assert fail_once(cb, "a", "job-1", reason="boom").consecutive_failures == 1
    # recovery attempt after the failure
    assert cb.begin("a", "job-2") is True
    assert cb.snapshot("a").state == "half_open"
    assert cb.finish("a", "job-2", success=True) is True
    snap = cb.snapshot("a")
    assert snap.state == "closed"
    assert snap.pending_job is None
    assert snap.consecutive_failures == 0
    assert snap.last_failure_reason is None


# ---------------------------------------------------------------------------
# open -> half_open recovery flow
# ---------------------------------------------------------------------------

def test_open_begin_moves_to_half_open_and_blocks_until_finish(cb):
    fail_once(cb, "a", "job-1", reason="boom")
    assert cb.snapshot("a").state == "open"
    assert cb.begin("a", "rec") is True
    snap = cb.snapshot("a")
    assert snap.state == "half_open"
    assert snap.pending_job == "rec"
    # failures and reason survive the transition into half_open
    assert snap.consecutive_failures == 1
    assert snap.last_failure_reason == "boom"
    # nothing else may begin until the recovery attempt finishes
    assert cb.begin("a", "other") is False
    assert cb.begin("a", "rec") is False
    assert cb.finish("a", "rec", success=True) is True
    snap = cb.snapshot("a")
    assert snap.state == "closed"
    assert snap.consecutive_failures == 0
    assert snap.last_failure_reason is None
    # healthy again: a new job may start
    assert cb.begin("a", "next") is True


def test_half_open_success_returns_to_closed(cb):
    fail_once(cb, "a", "job-1")
    assert cb.begin("a", "rec") is True
    assert cb.snapshot("a").state == "half_open"
    assert cb.finish("a", "rec", success=True) is True
    assert cb.snapshot("a").state == "closed"


def test_consecutive_failures_count_up(cb):
    for attempt in range(1, 4):
        job = "job-%d" % attempt
        assert cb.begin("a", job) is True
        expected_state = "closed" if attempt == 1 else "half_open"
        assert cb.snapshot("a").state == expected_state
        assert cb.finish("a", job, success=False, reason="boom-%d" % attempt) is True
        snap = cb.snapshot("a")
        assert snap.state == "open"
        assert snap.pending_job is None
        assert snap.consecutive_failures == attempt
        assert snap.last_failure_reason == "boom-%d" % attempt


# ---------------------------------------------------------------------------
# finish guards: wrong job id / unknown actor
# ---------------------------------------------------------------------------

def test_finish_with_wrong_job_id_while_pending_changes_nothing(cb):
    assert cb.begin("a", "job-1") is True
    assert cb.finish("a", "job-2", success=True) is False
    assert cb.finish("a", "job-2", success=False, reason="boom") is False
    snap = cb.snapshot("a")
    assert snap.state == "closed"
    assert snap.pending_job == "job-1"
    # the slot is still held by job-1
    assert cb.begin("a", "job-3") is False
    # and job-1 can still finish properly afterwards
    assert cb.finish("a", "job-1", success=False, reason="boom") is True
    assert cb.snapshot("a").state == "open"


def test_finish_after_completion_is_rejected(cb):
    cb.begin("a", "job-1")
    cb.finish("a", "job-1", success=True)
    before = cb.snapshot("a")
    assert cb.finish("a", "job-1", success=True) is False
    assert cb.finish("a", "job-1", success=False, reason="late") is False
    assert cb.snapshot("a") == before


def test_finish_unknown_actor_returns_false(cb):
    assert cb.finish("ghost", "job-1", success=True) is False
    assert cb.finish("ghost", "job-1", success=False, reason="boom") is False
    snap = cb.snapshot("ghost")
    assert snap.state == "closed"
    assert snap.pending_job is None
    assert snap.consecutive_failures == 0
    assert snap.last_failure_reason is None


# ---------------------------------------------------------------------------
# reset
# ---------------------------------------------------------------------------

def test_reset_without_expected_job_id_clears_state(cb):
    fail_once(cb, "a", "job-1", reason="boom")
    fail_once(cb, "a", "job-2", reason="boom-2")
    snap = cb.snapshot("a")
    assert snap.state == "open"
    assert snap.consecutive_failures == 2
    assert cb.reset("a") is True
    snap = cb.snapshot("a")
    assert snap.state == "closed"
    assert snap.pending_job is None
    assert snap.consecutive_failures == 0
    assert snap.last_failure_reason is None


def test_reset_with_expected_job_id(cb):
    cb.begin("a", "job-1")
    # mismatch: nothing changes, slot still held
    assert cb.reset("a", expected_job_id="other-job") is False
    snap = cb.snapshot("a")
    assert snap.state == "closed"
    assert snap.pending_job == "job-1"
    # match: cleared back to closed
    assert cb.reset("a", expected_job_id="job-1") is True
    snap = cb.snapshot("a")
    assert snap.state == "closed"
    assert snap.pending_job is None
    assert snap.consecutive_failures == 0


def test_reset_mismatch_on_open_actor_leaves_state_unchanged(cb):
    fail_once(cb, "a", "job-1", reason="boom")
    before = cb.snapshot("a")
    assert before.state == "open" and before.pending_job is None
    # job-1 is no longer pending, so the guarded reset must not fire
    assert cb.reset("a", expected_job_id="job-1") is False
    assert cb.snapshot("a") == before


def test_reset_mismatch_in_half_open_leaves_state_unchanged(cb):
    fail_once(cb, "a", "job-1", reason="boom")
    assert cb.begin("a", "rec") is True
    before = cb.snapshot("a")
    assert before.state == "half_open" and before.pending_job == "rec"
    assert cb.reset("a", expected_job_id="wrong") is False
    assert cb.snapshot("a") == before


def test_reset_unknown_actor_returns_false(cb):
    assert cb.reset("ghost") is False
    assert cb.reset("ghost", expected_job_id="job-1") is False


# ---------------------------------------------------------------------------
# per-actor independence
# ---------------------------------------------------------------------------

def test_actors_are_independent(cb):
    assert cb.begin("a", "shared-job") is True
    assert cb.begin("b", "shared-job") is True  # same job id, different actor
    assert cb.begin("a", "other") is False      # a is busy, b is unaffected
    assert cb.finish("a", "shared-job", success=False, reason="boom") is True
    a, b = cb.snapshot("a"), cb.snapshot("b")
    assert a.state == "open"
    assert a.consecutive_failures == 1
    assert b.state == "closed"
    assert b.pending_job == "shared-job"
    assert b.consecutive_failures == 0
    assert cb.begin("b", "next") is False  # b still holds its slot
    assert cb.begin("a", "rec") is True    # a goes through half_open
    assert cb.snapshot("a").state == "half_open"
    assert cb.snapshot("b").state == "closed"


# ---------------------------------------------------------------------------
# durability across reopen
# ---------------------------------------------------------------------------

def test_state_persists_across_reopen(tmp_path):
    path = tmp_path / "circuits.db"
    first = CircuitBreaker(path)
    assert first.begin("worker", "job-1") is True
    assert first.begin("other", "job-2") is True
    assert first.finish("other", "job-2", success=False, reason="crash") is True
    first.close()

    second = CircuitBreaker(path)
    # the pending job survives and is never auto-cleared
    snap = second.snapshot("worker")
    assert snap.state == "closed"
    assert snap.pending_job == "job-1"
    assert second.begin("worker", "job-3") is False
    # the original pending job can still be finished after reopen
    assert second.finish("worker", "job-1", success=True) is True
    assert second.snapshot("worker").pending_job is None

    other = second.snapshot("other")
    assert other.state == "open"
    assert other.consecutive_failures == 1
    assert other.last_failure_reason == "crash"
    # the recovery flow continues normally on the reopened db
    assert second.begin("other", "job-4") is True
    assert second.snapshot("other").state == "half_open"
    second.close()


# ---------------------------------------------------------------------------
# identifier validation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad", ["", None, 42, b"actor"])
def test_begin_rejects_invalid_actor(cb, bad):
    with pytest.raises(InvalidIdentifier):
        cb.begin(bad, "job-1")


@pytest.mark.parametrize("bad", ["", None, 42, b"job"])
def test_begin_rejects_invalid_job_id(cb, bad):
    with pytest.raises(InvalidIdentifier):
        cb.begin("a", bad)


@pytest.mark.parametrize("bad_job", ["", None, 42])
def test_finish_rejects_invalid_ids(cb, bad_job):
    with pytest.raises(InvalidIdentifier):
        cb.finish("a", bad_job, success=True)
    with pytest.raises(InvalidIdentifier):
        cb.finish(None, "job-1", success=True)


@pytest.mark.parametrize("bad", ["", None, 42])
def test_snapshot_and_reset_reject_invalid_actor(cb, bad):
    with pytest.raises(InvalidIdentifier):
        cb.snapshot(bad)
    with pytest.raises(InvalidIdentifier):
        cb.reset(bad)
    with pytest.raises(InvalidIdentifier):
        cb.reset(bad, expected_job_id="job-1")


# ---------------------------------------------------------------------------
# context manager and ':memory:'
# ---------------------------------------------------------------------------

def test_context_manager_closes_memory_db():
    with CircuitBreaker(":memory:") as cb:
        snap = cb.snapshot("a")
        assert snap.state == "closed"
        assert cb.begin("a", "job-1") is True
        assert cb.finish("a", "job-1", success=True) is True
        assert cb.snapshot("a").state == "closed"
    # the connection is really closed after the with block
    with pytest.raises(sqlite3.ProgrammingError):
        cb.snapshot("a")
