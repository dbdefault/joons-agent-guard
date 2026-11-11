"""Small standard-library benchmark for the joons_agent_guard primitives.

Measures pair throughput (pairs per second) and per-pair latency (p50/p95)
for ResourceLock acquire+release, CircuitBreaker begin+finish and
ReplayGuard claim+complete on temporary SQLite database files that are
deleted afterwards. Numbers depend heavily on the local disk and OS; see
bench/README.md for how to read them.
"""
import argparse
import multiprocessing
import queue as queue_module
import shutil
import tempfile
import time
from pathlib import Path

from joons_agent_guard import CircuitBreaker, ReplayGuard, ResourceLock

WORKER_START_TIMEOUT = 120.0   # seconds for a spawned worker to become ready
RESULT_TIMEOUT = 300.0         # seconds to wait for one worker's samples


def _percentile(samples, pct):
    """Nearest-rank percentile of an iterable of latencies in seconds."""
    ordered = sorted(samples)
    if not ordered:
        return float("nan")
    rank = max(0, min(len(ordered) - 1, round(pct / 100.0 * (len(ordered) - 1))))
    return ordered[rank]


def _summarize(scenario, pairs, wall_seconds, samples):
    return {
        "scenario": scenario,
        "pairs": pairs,
        "pairs_per_sec": pairs / wall_seconds if wall_seconds > 0 else float("nan"),
        "p50_ms": _percentile(samples, 50) * 1000.0,
        "p95_ms": _percentile(samples, 95) * 1000.0,
    }


# -- single-process scenarios -------------------------------------------------

def _run_lock_single(path, ops):
    lock = ResourceLock(path)
    samples = []
    start = time.perf_counter()
    for i in range(ops):
        t0 = time.perf_counter()
        lock.acquire("bench-lock", "owner", f"job-{i}")
        lock.release("bench-lock", "owner", f"job-{i}")
        samples.append(time.perf_counter() - t0)
    wall = time.perf_counter() - start
    lock.close()
    return ops, wall, samples


def _run_circuit_single(path, ops):
    breaker = CircuitBreaker(path)
    samples = []
    start = time.perf_counter()
    for i in range(ops):
        t0 = time.perf_counter()
        breaker.begin("bench-actor", f"job-{i}")
        breaker.finish("bench-actor", f"job-{i}", success=True)
        samples.append(time.perf_counter() - t0)
    wall = time.perf_counter() - start
    breaker.close()
    return ops, wall, samples


def _run_replay_single(path, ops):
    guard = ReplayGuard(path)
    samples = []
    start = time.perf_counter()
    for i in range(ops):
        t0 = time.perf_counter()
        digest = f"digest-{i}"
        guard.claim("bench", f"req-{i}", digest)
        guard.complete("bench", f"req-{i}", digest, 200, {"ok": True})
        samples.append(time.perf_counter() - t0)
    wall = time.perf_counter() - start
    guard.close()
    return ops, wall, samples


# -- multi-process scenario (spawn; top-level function required) --------------

def _lock_worker(db_path, resource, owner, ops, queue, ready, go):
    lock = ResourceLock(db_path)
    ready.set()
    go.wait()
    samples = []
    for i in range(ops):
        t0 = time.perf_counter()
        lock.acquire(resource, owner, f"{owner}-job-{i}")
        lock.release(resource, owner, f"{owner}-job-{i}")
        samples.append(time.perf_counter() - t0)
    lock.close()
    queue.put(samples)


def _run_lock_multiprocess(path, procs, ops, *, contend):
    ctx = multiprocessing.get_context("spawn")
    ops_per_proc = max(1, ops // procs)
    queue = ctx.Queue()
    ready = [ctx.Event() for _ in range(procs)]
    go = ctx.Event()
    workers = []
    for k in range(procs):
        if contend:
            resource, owner = "bench-contended", f"owner-{k}"
        else:
            resource, owner = f"bench-distinct-{k}", f"owner-{k}"
        workers.append(ctx.Process(
            target=_lock_worker,
            args=(str(path), resource, owner, ops_per_proc, queue, ready[k], go)))
    for process in workers:
        process.start()
    for event in ready:
        if not event.wait(timeout=WORKER_START_TIMEOUT):
            raise RuntimeError("benchmark worker failed to start (import error?)")
    go.set()
    start = time.perf_counter()
    samples = []
    for _ in workers:
        try:
            samples.extend(queue.get(timeout=RESULT_TIMEOUT))
        except queue_module.Empty:
            raise RuntimeError("benchmark worker did not return results in time")
    for process in workers:
        process.join()
    wall = time.perf_counter() - start
    if any(process.exitcode != 0 for process in workers):
        raise RuntimeError("a benchmark worker exited with an error")
    return procs * ops_per_proc, wall, samples


# -- driver -------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Benchmark joons_agent_guard primitives on temporary SQLite files.")
    parser.add_argument("--ops", type=int, default=2000,
                        help="operation pairs per scenario (default: 2000)")
    parser.add_argument("--procs", type=int, default=4,
                        help="processes for the multi-process lock scenarios (default: 4)")
    args = parser.parse_args(argv)
    if args.ops < 1 or args.procs < 1:
        parser.error("--ops and --procs must be >= 1")

    tmp = Path(tempfile.mkdtemp(prefix="joons-agent-guard-bench-"))
    try:
        lock_db = tmp / "lock.db"
        circuit_db = tmp / "circuit.db"
        replay_db = tmp / "replay.db"

        rows = []
        pairs, wall, samples = _run_lock_single(lock_db, args.ops)
        rows.append(_summarize("ResourceLock acquire+release, 1 process", pairs, wall, samples))

        pairs, wall, samples = _run_lock_multiprocess(lock_db, args.procs, args.ops, contend=False)
        rows.append(_summarize(
            f"ResourceLock acquire+release, {args.procs} processes, distinct resources",
            pairs, wall, samples))

        pairs, wall, samples = _run_lock_multiprocess(lock_db, args.procs, args.ops, contend=True)
        rows.append(_summarize(
            f"ResourceLock acquire+release, {args.procs} processes, 1 contended resource",
            pairs, wall, samples))

        pairs, wall, samples = _run_circuit_single(circuit_db, args.ops)
        rows.append(_summarize("CircuitBreaker begin+finish, 1 process", pairs, wall, samples))

        pairs, wall, samples = _run_replay_single(replay_db, args.ops)
        rows.append(_summarize("ReplayGuard claim+complete, 1 process", pairs, wall, samples))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print("| scenario | pairs | pairs/s | p50 (ms) | p95 (ms) |")
    print("|---|---:|---:|---:|---:|")
    for row in rows:
        print(f"| {row['scenario']} | {row['pairs']} | "
              f"{row['pairs_per_sec']:.0f} | {row['p50_ms']:.2f} | {row['p95_ms']:.2f} |")


if __name__ == "__main__":
    main()
