"""Two worker processes append to one shared report file under ResourceLock.hold().

What this shows
---------------
* Two real OS processes (multiprocessing 'spawn') coordinate through the same
  SQLite-backed ResourceLock DB file.
* Each worker appends a multi-line block to the shared report file only while
  holding the lock, so blocks from different workers never interleave.
* The script prints who held the lock, both on successful acquisition and on
  LockHeld collisions (the exception carries the current holder).

The DB and report files live in a tempfile.TemporaryDirectory, and the script
self-checks the report before exiting 0.

Run:  PYTHONPATH=src python examples/lock_shared_file.py
"""
import multiprocessing
import os
import tempfile
import time

from joons_agent_guard import LockHeld, ResourceLock

RESOURCE = "shared-report"
TICKS_PER_WORKER = 3
LINES_PER_BLOCK = 3  # begin / work / end


def write_block(report_path, owner, tick):
    """Append one multi-line block; only safe while holding the lock."""
    with open(report_path, "a", encoding="utf-8") as f:
        f.write(f"{owner}|{tick}|begin\n")
        f.write(f"{owner}|{tick}|work\n")
        f.write(f"{owner}|{tick}|end\n")


def worker_main(db_path, report_path, owner, job_id):
    with ResourceLock(db_path) as lock:
        for tick in range(TICKS_PER_WORKER):
            while True:
                try:
                    with lock.hold(RESOURCE, owner, job_id):
                        print(f"[{owner}/{job_id}] holds lock on {RESOURCE!r} (tick {tick})", flush=True)
                        write_block(report_path, owner, tick)
                        time.sleep(0.05)  # hold it long enough that the other worker collides
                        break
                except LockHeld as exc:
                    holder = exc.holder.owner if exc.holder else "someone"
                    print(f"[{owner}/{job_id}] lock busy, held by {holder}; retrying", flush=True)
                    time.sleep(0.02)
    print(f"[{owner}/{job_id}] finished", flush=True)


def check_no_interleaving(report_path):
    """Verify every block of LINES_PER_BLOCK lines came from a single worker."""
    with open(report_path, encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    owners_in_order = []
    for i in range(0, len(lines), LINES_PER_BLOCK):
        chunk = lines[i:i + LINES_PER_BLOCK]
        assert len(chunk) == LINES_PER_BLOCK, f"truncated block at end: {chunk}"
        assert all(line.split("|")[2] == part for line, part in zip(chunk, ("begin", "work", "end"))), \
            f"corrupt block: {chunk}"
        owners = {line.split("|")[0] for line in chunk}
        assert len(owners) == 1, f"interleaved writes detected: {chunk}"
        owners_in_order.append(owners.pop())
    return owners_in_order


def main():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "guard.sqlite3")
        report_path = os.path.join(tmp, "report.txt")

        ctx = multiprocessing.get_context("spawn")  # fresh interpreter per child
        processes = [
            ctx.Process(target=worker_main, args=(db_path, report_path, name, name.lower().replace("worker-", "job-")))
            for name in ("worker-A", "worker-B")
        ]
        for p in processes:
            p.start()
        for p in processes:
            p.join(60.0)  # bounded so a hung child cannot block CI forever
            if p.is_alive():
                p.terminate()
                p.join(5.0)
            assert p.exitcode == 0, f"child exited with {p.exitcode}"

        with open(report_path, encoding="utf-8") as f:
            print("--- report file ---")
            print(f.read().rstrip())
        owners = check_no_interleaving(report_path)
        print(f"--- {len(owners)} blocks, all intact, no interleaving: {owners}")
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
