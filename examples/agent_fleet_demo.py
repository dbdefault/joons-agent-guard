"""An asyncio agent fleet using all three components together on one DB file.

What this shows
---------------
* A fleet of 4 async workers, each with its own AsyncCircuitBreaker run slot,
  pulling 12 file-edit jobs from assigned queues plus a shared retry pool.
* Every job first takes an AsyncResourceLock on its target file before
  editing it; two jobs (worker-1's first and worker-2's first) share one
  target, so their edits serialise and the loser's acquire attempts are
  counted as lock waits.
* Every submission is admitted through AsyncReplayGuard with request_digest.
  After the fleet finishes, the same submission is replayed: claim returns
  ``completed`` and the stored response is served instead of running again.
* worker-4 fails its first two attempts deterministically (before touching
  any file), so its circuit goes open -> half_open -> open -> half_open ->
  closed while the failed jobs are re-queued and still run exactly once on
  another worker.

The three components share one database file (fleet.db) inside a
tempfile.TemporaryDirectory that is deleted on exit. The script asserts the
invariants (each job edited exactly once, no overlapping edits on one target,
duplicate replayed without a re-run, circuit transitions and final state)
while the connections are still open, and exits 0.

Run:  PYTHONPATH=src python examples/agent_fleet_demo.py
"""
import asyncio
import json
import tempfile
import time
from collections import deque
from pathlib import Path

from joons_agent_guard import (
    AsyncCircuitBreaker,
    AsyncReplayGuard,
    AsyncResourceLock,
    request_digest,
)

PRINCIPAL = "fleet"
EDIT_SECONDS = 0.06       # simulated edit work while holding the lock
POLL_SECONDS = 0.01       # retry interval while waiting for a held lock

# Shared target: worker-1's first job and worker-2's first job both edit it,
# so they start concurrently and the lock serialisation is always observable.
JOBS = [
    {"id": "job-01", "target": "notes-1.txt"},
    {"id": "job-02", "target": "notes-2.txt"},
    {"id": "job-03", "target": "notes-3.txt"},
    {"id": "job-04", "target": "notes-4.txt"},
    {"id": "job-05", "target": "notes-5.txt"},
    {"id": "job-06", "target": "notes-6.txt"},
    {"id": "job-07", "target": "notes-7.txt"},
    {"id": "job-08", "target": "notes-8.txt"},
    {"id": "job-09", "target": "shared-notes.txt"},
    {"id": "job-10", "target": "shared-notes.txt"},
    {"id": "job-11", "target": "notes-11.txt"},
    {"id": "job-12", "target": "notes-12.txt"},
]
ASSIGNMENT = {
    "worker-1": ["job-09", "job-01", "job-05"],
    "worker-2": ["job-10", "job-02", "job-06"],
    "worker-3": ["job-03", "job-07", "job-11"],
    "worker-4": ["job-04", "job-08", "job-12"],   # worker-4 is the flaky one
}
FLAKY = "worker-4"
FLAKY_FAILURES = 2                      # its first two attempts fail


class SimulatedWorkerCrash(Exception):
    """Deterministic pre-effect failure of the flaky worker."""


async def main() -> None:
    started = time.perf_counter()
    state = {
        "settled": 0,
        "runs": {job["id"]: 0 for job in JOBS},
        "results": {},
        "waits": [],            # (job_id, resource, attempt)
        "intervals": {},        # target -> [(start, end, job_id), ...]
        "transitions": [],      # (worker, before, after, failures)
        "prev_circuit": {name: "closed" for name in ASSIGNMENT},
        "attempts": {name: 0 for name in ASSIGNMENT},
    }
    retry_pool = deque()

    with tempfile.TemporaryDirectory(prefix="fleet-demo-") as tmp:
        work = Path(tmp) / "work"
        work.mkdir()
        db = Path(tmp) / "fleet.db"
        print("database: fleet.db (temporary directory, removed on exit)")

        async with (AsyncResourceLock(db) as alock,
                    AsyncCircuitBreaker(db) as acircuit,
                    AsyncReplayGuard(db) as aguard):
            digests = {
                job["id"]: request_digest(
                    "POST", "/edit",
                    json.dumps({"target": job["target"],
                                "line": f"edited-by-{job['id']}"},
                               sort_keys=True).encode())
                for job in JOBS
            }

            # -- admission: every submission goes through the replay guard --
            for job in JOBS:
                verdict = await aguard.claim(PRINCIPAL, job["id"], digests[job["id"]])
                assert verdict == "new", (job["id"], verdict)
                print(f"[admit] {job['id']}  target={job['target']}  verdict=new")

            def record_circuit(worker: str, before: str, after: str, failures: int) -> None:
                if before != after:
                    state["transitions"].append((worker, before, after, failures))
                    state["prev_circuit"][worker] = after
                    if worker == FLAKY:
                        print(f"[circuit] {worker}: {before} -> {after}  (failures={failures})")

            async def run_job(worker: str, job: dict) -> None:
                try:
                    ok = await acircuit.begin(worker, job["id"])
                    assert ok, f"{worker} could not claim its own slot"
                    snap = await acircuit.snapshot(worker)
                    record_circuit(worker, state["prev_circuit"][worker],
                                   snap.state, snap.consecutive_failures)

                    state["attempts"][worker] += 1
                    if worker == FLAKY and state["attempts"][worker] <= FLAKY_FAILURES:
                        raise SimulatedWorkerCrash(
                            f"attempt {state['attempts'][worker]}")

                    resource = f"target:{job['target']}"
                    attempt = 0
                    while True:
                        attempt += 1
                        if await alock.acquire(resource, worker, job["id"]):
                            break
                        state["waits"].append((job["id"], resource, attempt))
                        print(f"[lock  ] {job['id']}  waiting for {resource} "
                              f"(attempt {attempt})")
                        await asyncio.sleep(POLL_SECONDS)
                    try:
                        t0 = time.perf_counter()
                        await asyncio.sleep(EDIT_SECONDS)          # the "edit"
                        marker = f"edited-by-{job['id']}\n"
                        with (work / job["target"]).open("a", encoding="utf-8") as fh:
                            fh.write(marker)
                        t1 = time.perf_counter()
                        state["intervals"].setdefault(job["target"], []).append(
                            (t0, t1, job["id"]))
                        state["runs"][job["id"]] += 1
                        result = {"worker": worker, "target": job["target"],
                                  "bytes": len(marker)}
                    finally:
                        await alock.release(resource, worker, job["id"])

                    await acircuit.finish(worker, job["id"], success=True)
                    snap = await acircuit.snapshot(worker)
                    record_circuit(worker, state["prev_circuit"][worker],
                                   snap.state, snap.consecutive_failures)
                    await aguard.complete(PRINCIPAL, job["id"], digests[job["id"]],
                                          200, result)
                    state["results"][job["id"]] = result
                    state["settled"] += 1
                    print(f"[done  ] {job['id']}  {worker}  target={job['target']} "
                          f"(+{len(marker)} bytes)")
                except SimulatedWorkerCrash as exc:
                    await acircuit.finish(worker, job["id"], success=False,
                                          reason="simulated worker crash")
                    snap = await acircuit.snapshot(worker)
                    record_circuit(worker, state["prev_circuit"][worker],
                                   snap.state, snap.consecutive_failures)
                    print(f"[fail  ] {job['id']}  {worker}  {exc} "
                          f"(re-queued; circuit={snap.state})")
                    retry_pool.append(job)
                except Exception as exc:                       # defensive: never wedge
                    await acircuit.finish(worker, job["id"], success=False,
                                          reason=f"unexpected: {exc}")
                    retry_pool.append(job)

            async def worker_loop(name: str, own: deque) -> None:
                while state["settled"] < len(JOBS):
                    job = None
                    if own:
                        job = own.popleft()
                    elif retry_pool:
                        job = retry_pool.popleft()
                    else:
                        await asyncio.sleep(0.02)
                        continue
                    print(f"[run   ] {job['id']}  {name}  target={job['target']}")
                    await run_job(name, job)

            queues = {name: deque(JOBS_BY_ID[jid] for jid in ids)
                      for name, ids in ASSIGNMENT.items()}
            await asyncio.gather(*(worker_loop(name, queues[name])
                                   for name in ASSIGNMENT))

            # -- deliberately duplicated submission ----------------------------
            verdict = await aguard.claim(PRINCIPAL, "job-01", digests["job-01"])
            stored = await aguard.response(PRINCIPAL, "job-01", digests["job-01"])
            print(f"[replay] job-01  duplicate submission: verdict={verdict}, "
                  f"stored response replayed (not re-run)")

            elapsed = time.perf_counter() - started
            flaky_chain = " -> ".join(t[1] for t in state["transitions"]
                                      if t[0] == FLAKY) + " -> " + state["prev_circuit"][FLAKY]
            print("---- summary ----")
            print(f"elapsed          : {elapsed:.1f}s")
            print(f"submissions      : 12 admitted as new, 1 duplicate replayed "
                  f"from stored response (0 extra runs)")
            print(f"jobs executed    : {state['settled']} of {len(JOBS)}, each exactly "
                  f"once; failed attempts: {FLAKY_FAILURES} (re-queued)")
            print(f"lock waits       : {len(state['waits'])} on shared target "
                  f"(two jobs share shared-notes.txt; edits serialised, no overlap)")
            print(f"circuit {FLAKY}: {flaky_chain}  "
                  f"(deterministic failures: {FLAKY_FAILURES})")

            # -- invariants (connections still open) ---------------------------
            assert state["settled"] == len(JOBS), "not every job settled"
            assert all(runs == 1 for runs in state["runs"].values()), \
                f"each job must edit exactly once: {state['runs']}"
            assert verdict == "completed" and stored is not None, "duplicate not replayed"
            assert stored == (200, state["results"]["job-01"]), "replayed response mismatch"
            assert state["runs"]["job-01"] == 1, "duplicate caused a second run"
            for target, intervals in state["intervals"].items():
                ordered = sorted(intervals)
                for (_, end, _), (next_start, _, _) in zip(ordered, ordered[1:]):
                    assert next_start >= end, f"overlapping edits on {target}"
            shared = (work / "shared-notes.txt").read_text(encoding="utf-8").splitlines()
            assert sorted(shared) == ["edited-by-job-09", "edited-by-job-10"], shared
            assert state["waits"], "expected at least one observed lock wait"
            assert all(resource.endswith("shared-notes.txt")
                       for _, resource, _ in state["waits"]), state["waits"]
            assert [(t[1], t[2], t[3]) for t in state["transitions"] if t[0] == FLAKY] == [
                ("closed", "open", 1),
                ("open", "half_open", 1),
                ("half_open", "open", 2),
                ("open", "half_open", 2),
                ("half_open", "closed", 0),
            ], state["transitions"]
            assert all(state["prev_circuit"][name] == "closed"
                       for name in ASSIGNMENT if name != FLAKY), \
                "a healthy worker's circuit did not stay closed"
            assert await alock.list_locks() == [], "a lock was not released"
            for name in ASSIGNMENT:
                snap = await acircuit.snapshot(name)
                assert snap.state == "closed" and snap.pending_job is None, (name, snap)
            print("invariants       : all asserted OK")


JOBS_BY_ID = {job["id"]: job for job in JOBS}

if __name__ == "__main__":
    asyncio.run(main())
