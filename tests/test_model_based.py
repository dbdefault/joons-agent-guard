"""Model-based randomized tests (standard library only).

Each test drives one component with OPS random operations (default 500; set
GUARD_MODEL_OPS=2000 or more for a long soak run) drawn from a
``random.Random`` with a fixed seed, and compares every result with a
pure-Python model written from the component's docstring:

* ``ResourceLock``  - dict model ``resource -> (owner, job_id)``,
  including a final ``list_locks()`` comparison.
* ``CircuitBreaker`` - pure-Python state machine (closed/open/half_open,
  single pending slot, failures/reason bookkeeping).
* ``ReplayGuard``   - dict model ``(principal, request_id) -> [digest, state]``.

The DB file is closed and reopened every 250 operations so persistence is
exercised inside the random stream.  On any mismatch the assertion message
names the seed, the operation index and the operation.
"""
import os
import random

import pytest

from joons_agent_guard import CircuitBreaker, ReplayGuard, ResourceLock
from joons_agent_guard.replay import COMPLETED, CONFLICT, NEW, PENDING, request_digest

SEEDS = [11, 22, 33, 44, 55]
OPS = int(os.environ.get("GUARD_MODEL_OPS", "500"))
REOPEN_EVERY = 125


def _check(seed, index, op, expected, actual):
    assert actual == expected, "seed=%d op#%d %s: expected %r, got %r" % (
        seed, index, op, expected, actual)


def _reopen_every(index, component, factory):
    """Close and reopen the DB file every REOPEN_EVERY operations."""
    if (index + 1) % REOPEN_EVERY == 0:
        component.close()
        return factory()
    return component


# ---------------------------------------------------------------------------
# ResourceLock vs dict model
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("seed", SEEDS, ids=["seed%d" % s for s in SEEDS])
def test_resource_lock_model(tmp_path, seed):
    path = tmp_path / ("locks-%d.db" % seed)
    rng = random.Random(seed)
    resources = ["r%d" % i for i in range(4)]
    owners = ["o%d" % i for i in range(3)]
    jobs = ["j%d" % i for i in range(5)]
    holder = {}  # resource -> (owner, job_id); absent means free

    def open_lock():
        return ResourceLock(path)

    lock = open_lock()
    try:
        for index in range(OPS):
            op = rng.choices(["acquire", "release", "break_lock", "status"],
                             weights=[45, 25, 15, 15])[0]
            res = rng.choice(resources)
            if op == "acquire":
                owner, job = rng.choice(owners), rng.choice(jobs)
                expected = res not in holder
                if expected:
                    holder[res] = (owner, job)
                actual = lock.acquire(res, owner, job)
                _check(seed, index, "acquire(%s, %s, %s)" % (res, owner, job), expected, actual)
            elif op == "release":
                if holder and rng.random() < 0.4:
                    res = rng.choice(sorted(holder))
                    owner, job = holder[res]
                else:
                    owner, job = rng.choice(owners), rng.choice(jobs)
                expected = holder.get(res) == (owner, job)
                if expected:
                    del holder[res]
                actual = lock.release(res, owner, job)
                _check(seed, index, "release(%s, %s, %s)" % (res, owner, job), expected, actual)
            elif op == "break_lock":
                if holder and rng.random() < 0.4:
                    res = rng.choice(sorted(holder))
                    job = holder[res][1]
                else:
                    job = rng.choice(jobs)
                expected = res in holder and holder[res][1] == job
                if expected:
                    del holder[res]
                actual = lock.break_lock(res, job)
                _check(seed, index, "break_lock(%s, %s)" % (res, job), expected, actual)
            else:  # status
                info = lock.status(res)
                if res in holder:
                    _check(seed, index, "status(%s)" % res,
                           (res,) + holder[res], (info.resource, info.owner, info.job_id))
                    assert isinstance(info.acquired_at, float) and info.acquired_at > 0, \
                        "seed=%d op#%d status(%s): bad acquired_at %r" % (seed, index, res, info.acquired_at)
                else:
                    _check(seed, index, "status(%s)" % res, None, info)
            lock = _reopen_every(index, lock, open_lock)

        expected_list = sorted((res,) + held for res, held in holder.items())
        infos = lock.list_locks()
        actual_list = sorted((i.resource, i.owner, i.job_id) for i in infos)
        _check(seed, OPS, "list_locks()", expected_list, actual_list)
        for info in infos:
            assert isinstance(info.acquired_at, float) and info.acquired_at > 0, \
                "seed=%d list_locks(): bad acquired_at %r" % (seed, info.acquired_at)
    finally:
        lock.close()


# ---------------------------------------------------------------------------
# CircuitBreaker vs docstring state machine
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("seed", SEEDS, ids=["seed%d" % s for s in SEEDS])
def test_circuit_breaker_model(tmp_path, seed):
    path = tmp_path / ("circuits-%d.db" % seed)
    rng = random.Random(seed)
    actors = ["a%d" % i for i in range(3)]
    jobs = ["j%d" % i for i in range(6)]
    # actor -> {"state", "pending", "failures", "reason"}; absent == no row
    model = {}

    def open_cb():
        return CircuitBreaker(path)

    cb = open_cb()
    try:
        for index in range(OPS):
            op = rng.choices(["begin", "finish", "reset", "snapshot"],
                             weights=[40, 35, 15, 10])[0]
            actor = rng.choice(actors)
            m = model.get(actor)
            if op == "begin":
                job = rng.choice(jobs)
                expected = not (m and (m["pending"] is not None or m["state"] == "half_open"))
                if expected:
                    model[actor] = {
                        "state": "half_open" if m and m["state"] == "open" else "closed",
                        "pending": job,
                        "failures": m["failures"] if m else 0,
                        "reason": m["reason"] if m else None,
                    }
                actual = cb.begin(actor, job)
                _check(seed, index, "begin(%s, %s)" % (actor, job), expected, actual)
            elif op == "finish":
                if m and m["pending"] is not None and rng.random() < 0.7:
                    job = m["pending"]
                else:
                    job = rng.choice(jobs)
                success = rng.random() < 0.5
                reason = "err-%d" % index
                expected = m is not None and m["pending"] == job
                if expected:
                    m["pending"] = None
                    if success:
                        m.update(state="closed", failures=0, reason=None)
                    else:
                        m.update(state="open", failures=m["failures"] + 1, reason=reason)
                actual = cb.finish(actor, job, success=success, reason=reason)
                _check(seed, index, "finish(%s, %s, success=%s, reason=%r)" % (actor, job, success, reason),
                       expected, actual)
            elif op == "reset":
                if rng.random() < 0.5:
                    expected = m is not None
                    if expected:
                        model[actor] = {"state": "closed", "pending": None, "failures": 0, "reason": None}
                    actual = cb.reset(actor)
                    desc = "reset(%s)" % actor
                else:
                    if m and m["pending"] is not None and rng.random() < 0.7:
                        job = m["pending"]
                    else:
                        job = rng.choice(jobs)
                    expected = m is not None and m["pending"] == job
                    if expected:
                        model[actor] = {"state": "closed", "pending": None, "failures": 0, "reason": None}
                    actual = cb.reset(actor, expected_job_id=job)
                    desc = "reset(%s, expected_job_id=%r)" % (actor, job)
                _check(seed, index, desc, expected, actual)
            else:  # snapshot
                if m is None:
                    expected = (actor, "closed", None, 0, None)
                else:
                    expected = (actor, m["state"], m["pending"], m["failures"], m["reason"])
                snap = cb.snapshot(actor)
                actual = (snap.actor, snap.state, snap.pending_job,
                          snap.consecutive_failures, snap.last_failure_reason)
                _check(seed, index, "snapshot(%s)" % actor, expected, actual)
            cb = _reopen_every(index, cb, open_cb)
    finally:
        cb.close()


# ---------------------------------------------------------------------------
# ReplayGuard vs dict model
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("seed", SEEDS, ids=["seed%d" % s for s in SEEDS])
def test_replay_guard_model(tmp_path, seed):
    path = tmp_path / ("replay-%d.db" % seed)
    rng = random.Random(seed)
    principals = ["svc-a", "svc-b"]
    request_ids = ["req-%d" % i for i in range(6)]
    digests = {}  # request_id -> [(tag, digest), (tag, digest)]
    for rid in request_ids:
        digests[rid] = [
            ("d0", request_digest("POST", "/items/" + rid, b"body-0")),
            ("d1", request_digest("POST", "/items/" + rid, b"body-1")),
        ]
    # (principal, request_id) -> [digest, state, code, response]; absent == unseen
    model = {}

    def open_guard():
        return ReplayGuard(path)

    guard = open_guard()
    try:
        for index in range(OPS):
            op = rng.choices(["claim", "complete", "response"], weights=[50, 30, 20])[0]
            principal = rng.choice(principals)
            rid = rng.choice(request_ids)
            tag, digest = rng.choice(digests[rid])
            key = (principal, rid)
            m = model.get(key)
            if op == "claim":
                if m is None:
                    model[key] = [digest, PENDING, None, None]
                    expected = NEW
                elif m[0] == digest:
                    expected = m[1]
                else:
                    expected = CONFLICT
                actual = guard.claim(principal, rid, digest)
                _check(seed, index, "claim(%s, %s, %s)" % (principal, rid, tag), expected, actual)
            elif op == "complete":
                code = rng.choice([200, 201])
                payload = {"seq": index}
                expected = m is not None and m[0] == digest and m[1] == PENDING
                if expected:
                    m[1] = COMPLETED
                    m[2], m[3] = code, payload
                actual = guard.complete(principal, rid, digest, code, payload)
                _check(seed, index, "complete(%s, %s, %s, %d)" % (principal, rid, tag, code),
                       expected, actual)
            else:  # response
                expected = None
                if m is not None and m[0] == digest and m[1] == COMPLETED:
                    expected = (m[2], m[3])
                actual = guard.response(principal, rid, digest)
                _check(seed, index, "response(%s, %s, %s)" % (principal, rid, tag), expected, actual)
            guard = _reopen_every(index, guard, open_guard)
    finally:
        guard.close()
