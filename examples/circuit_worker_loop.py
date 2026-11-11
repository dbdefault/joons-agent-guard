"""A worker loop running jobs against a flaky service, guarded by CircuitBreaker.

What this shows
---------------
* begin()/finish() around each job and the breaker's single run slot
  (a second begin() while a job is pending returns False).
* A deterministic fake service that fails on jobs 2 and 3.
* State transitions visible via snapshot() after every step:
  closed -> (job 2 fails) open -> (job 3 begins) half_open -> (job 3 fails) open
  -> (job 4 begins) half_open -> (job 4 succeeds) closed.
  A failed job marks the breaker open; the next job that starts is the
  recovery attempt (half_open) until it finishes.

The DB lives in a tempfile.TemporaryDirectory; the script asserts the expected
transitions and exits 0.

Run:  PYTHONPATH=src python examples/circuit_worker_loop.py
"""
import os
import tempfile

from joons_agent_guard import CircuitBreaker

ACTOR = "worker-1"


class FakeService:
    """Deterministic fake remote service: always fails on jobs 2 and 3."""

    FAILING = {"job-2", "job-3"}

    def call(self, job_id):
        if job_id in self.FAILING:
            raise RuntimeError(f"{job_id} blew up at the service")
        return f"ok:{job_id}"


def show(breaker, label):
    s = breaker.snapshot(ACTOR)
    print(f"    snapshot({label}): state={s.state:<9} pending={s.pending_job!r} "
          f"failures={s.consecutive_failures} reason={s.last_failure_reason!r}")
    return s


def run_job(breaker, service, job_id):
    print(f"job {job_id}:")
    if not breaker.begin(ACTOR, job_id):
        print("  begin() -> False: another job is pending; skipping")
        return
    s = show(breaker, "after begin")
    if job_id == "job-2":  # the run slot is exclusive while a job is pending
        print(f"  concurrent begin('job-2b') -> {breaker.begin(ACTOR, 'job-2b')} (single run slot)")

    try:
        result = service.call(job_id)
    except RuntimeError as exc:
        assert breaker.finish(ACTOR, job_id, success=False, reason=str(exc))
        print(f"  finish(success=False, reason={str(exc)!r})")
        s = show(breaker, "after finish")
        if job_id == "job-2":
            assert s.state == "open", f"expected open after first failure, got {s.state}"
    else:
        assert breaker.finish(ACTOR, job_id, success=True)
        print(f"  finish(success=True) result={result!r}")
        s = show(breaker, "after finish")
        if job_id in ("job-3", "job-4"):
            assert s.state == "closed", f"expected closed after recovery success, got {s.state}"
        if job_id == "job-3":
            assert s.consecutive_failures == 0
    return s


def main():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "guard.sqlite3")
        breaker = CircuitBreaker(db_path)
        try:
            show(breaker, "fresh")
            after = []
            for i in range(1, 6):
                after.append(run_job(breaker, FakeService(), f"job-{i}"))
            final = breaker.snapshot(ACTOR)
            walk = [s.state for s in after if s is not None]
            print(f"state walk after each finish: {' -> '.join(walk)}")
            assert final.state == "closed" and final.consecutive_failures == 0
            assert walk == ["closed", "open", "open", "closed", "closed"]
            print("recovery attempts ran in half_open: after begin of job-3 and job-4")
        finally:
            breaker.close()
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
