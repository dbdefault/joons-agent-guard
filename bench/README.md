# Benchmarks

`bench/bench.py` is a small standard-library benchmark for the three
primitives. One **pair** is one `acquire()`+`release()`, one `begin()`+`finish()`,
or one `claim()`+`complete()`. It reports **pairs per second** and the
**p50/p95 latency of one pair** for five scenarios, using temporary SQLite
database files that are deleted afterwards.

## How to run

```bash
# from a source checkout
PYTHONPATH=src python bench/bench.py --ops 2000 --procs 4

# or after installation
python bench/bench.py --ops 2000 --procs 4
```

- `--ops N` (default 2000) — number of pairs per scenario.
- `--procs P` (default 4) — process count for the two multi-process lock
  scenarios (workers are started with the `spawn` start method).

The two multi-process scenarios use P workers on the same database file:
one where each worker acquires its **own** resource, and one where all
workers **contend** for a single resource. Process startup is excluded from
the measurement (workers synchronize on an event before the clock starts).

## How to read the numbers

- **The absolute numbers depend heavily on the local disk and OS.** Every
  pair is two SQLite write transactions, and a commit is only as fast as the
  filesystem's `fsync`. Do not compare numbers across machines; compare
  scenarios within one run on one machine.
- **SQLite serialises writers.** Even the "distinct resources" scenario uses
  one database file, so its throughput stays close to the single-process
  number — that is the database doing its job, not a defect of the locks.
- **The contended-resource scenario measures queueing, not a bug.** All but
  one waiting `acquire()` must wait for the current holder to release; the
  latency of that row includes that wait (bounded by the constructor
  `timeout`), and its throughput is capped by the single-writer speed of the
  database file.
- The latency columns cover the whole pair, including both SQLite commits
  and any waiting; they are not the library's function-call overhead.
- p50/p95 are nearest-rank percentiles over all pairs of a scenario.

## Example run, not a guarantee

Example output of `PYTHONPATH=src python bench/bench.py --ops 300 --procs 2`;
treat every number as one measurement on one machine at one moment — never
as a guarantee:

| scenario | pairs | pairs/s | p50 (ms) | p95 (ms) |
|---|---:|---:|---:|---:|
| ResourceLock acquire+release, 1 process | 300 | 94 | 10.16 | 14.03 |
| ResourceLock acquire+release, 2 processes, distinct resources | 300 | 85 | 10.85 | 17.51 |
| ResourceLock acquire+release, 2 processes, 1 contended resource | 300 | 94 | 9.69 | 15.60 |
| CircuitBreaker begin+finish, 1 process | 300 | 98 | 9.54 | 14.85 |
| ReplayGuard claim+complete, 1 process | 300 | 98 | 9.57 | 14.48 |
