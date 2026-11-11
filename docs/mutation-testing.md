# MUTATION_REPORT.md — T17: mutation testing of the test suite

Package under review: `joons_agent_guard` 0.1.0 (stdlib-only, SQLite).
Environment: Python 3.14.7, pytest 9.1.1, 64-bit, no network, no package install.
Date: 2026-11-12.

## Method

For each mutant below:

1. Copy `src/` to `./_mutant/src/` (pristine copy; `src/` itself is never modified).
2. Apply ONE small change in `./_mutant/src/joons_agent_guard/` (exact diffs in the Appendix).
3. Run, from the repository root:

   ```
   PYTHONPATH=_mutant/src python -m pytest -q -p no:cacheprovider -x
   ```

4. Killed = the suite exits non-zero. With `-x`, the reported killing test is the
   first failing test in pytest's run order. "Seconds" is the wall-clock time of the
   pytest process (including interpreter/startup overhead), so it is not comparable
   to the in-CSI time printed inside the summary line.
5. Restore: `./_mutant/src` was re-created fresh from `src/` before every mutant.

The pristine `src/` was never edited: the control run passed before the matrix,
and a final full-suite run after all `_mutant` work and deletion also passed
(208 passed in 25.43s).

## Control run (no mutation)

`PYTHONPATH=src python -m pytest -q -p no:cacheprovider -x` → **208 passed in 25.42s**, exit code 0.

## Results

| # | Mutant (one change)                | File        | Killed | Killing test                                                                  | Seconds | pytest summary          |
|---|------------------------------------|-------------|--------|-------------------------------------------------------------------------------|---------|-------------------------|
| 1 | `acquire` always returns True      | `lock.py`   | yes    | `tests/test_lock.py::test_acquire_succeeds_exactly_once`                      | 2.9     | 1 failed, 46 passed     |
| 2 | `release` ignores `job_id`         | `lock.py`   | yes    | `tests/test_lock.py::test_release_requires_matching_owner_and_job_id`         | 3.0     | 1 failed, 47 passed     |
| 3 | `break_lock` ignores `expected_job_id` | `lock.py` | yes  | `tests/test_lock.py::test_break_lock_only_removes_matching_job`               | 3.3     | 1 failed, 51 passed     |
| 4 | `begin` allows a second job while one is pending | `circuit.py` | yes | `tests/test_circuit.py::test_begin_claims_slot_and_refuses_second_begin`    | 0.4     | 1 failed, 1 passed      |
| 5 | `open` → `closed` instead of `half_open` on begin | `circuit.py` | yes | `tests/test_circuit.py::test_finish_success_resets_failures_and_clears_reason` | 0.4   | 1 failed, 3 passed      |
| 6 | `finish` does not increment failures | `circuit.py` | yes  | `tests/test_circuit.py::test_finish_failure_opens_and_stores_reason`          | 0.5     | 1 failed, 2 passed      |
| 7 | `claim` returns `'new'` for a conflicting digest | `replay.py` | yes | `tests/test_replay.py::TestClaim::test_conflict_while_pending`               | 9.0     | 1 failed, 134 passed    |
| 8 | `complete()` allowed when state is not pending | `replay.py` | yes | `tests/test_replay.py::TestComplete::test_complete_works_exactly_once`       | 8.8     | 1 failed, 138 passed    |
| 9 | `apply_pending` runs the effect after COMMIT, stores the response in a second transaction | `replay.py` | yes | `tests/test_failure_modes.py::test_apply_pending_effect_crash_leaves_request_pending_and_rolls_back` | 2.8 | 1 failed, 44 passed |
| 10 | `reserve_outbound` generates a new `request_id` on every call | `replay.py` | yes | `tests/test_replay.py::TestReserveOutbound::test_retry_returns_identical_context` | 9.2 | 1 failed, 160 passed |
| 11 | `write_txn` uses plain `BEGIN` (deferred) instead of `BEGIN IMMEDIATE` | `_db.py` | yes | `tests/test_threads.py::test_per_thread_circuit_breaker_same_file`        | 21.9    | 1 failed, 204 passed    |
| 12 | `write_txn` without the `guard_lock` | `_db.py`   | yes    | `tests/test_threads.py::test_shared_resource_lock_single_instance`            | 11.4    | 1 failed, 200 passed    |

**Score: 12 / 12 killed.**

## Why each mutant dies (short notes)

- M1: a second `acquire` of a held resource must report `False`; the mutant reports `True`.
- M2: `release` with a wrong `job_id` must be refused and keep the lock; the mutant deletes by `resource` + `owner` only.
- M3: a stale `expected_job_id` must not remove a lock re-acquired by a new job; the mutant deletes by `resource` only.
- M4: the actor's run slot is single; the mutant lets `begin("a", "job-2")` succeed while `"job-1"` is pending.
- M5: the first `begin` after a failure is the recovery attempt and must flip the state to `half_open`; the mutant leaves it `closed`, so the recovery state is invisible.
- M6: a failed finish must raise `consecutive_failures` to 1; the mutant leaves the counter at its old value.
- M7: the same `(principal, request_id)` with a different digest must return `conflict`; the mutant returns `new`, i.e. it would re-run a replayed request.
- M8: `complete()` on an already-completed request must return `False` and keep the first response; the mutant overwrites it (second `complete` returns `True`).
- M9: with the effect moved after COMMIT, a failing effect leaves the request already marked `completed` with a NULL response and the effect's own partial writes committed; the suite asserts the request is still `pending` and the effect's writes were rolled back, so exactly-once is observably broken.
- M10: retrying the same `intent_id` must return the identical context; the mutant returns a fresh `request_id` on every call, so a retry no longer maps to one outbound request.
- M11: observed failure is `OperationalError('database is locked')` from 7 of 8 worker threads. With deferred `BEGIN`, `CircuitBreaker.begin` reads (SHARED lock) and then writes inside its transaction; under contention the SHARED→write upgrade fails immediately (busy handler not invoked for the upgrade path), so workers crash instead of serializing.
- M12: observed failures are `OperationalError('cannot start a transaction within a transaction')` and `OperationalError('not an error')` — without the re-entrant `guard_lock`, two threads sharing one connection interleave their `BEGIN`/`COMMIT` pairs and corrupt each other's transactions.

## Reproducibility of the two concurrency-dependent kills

Mutants 11 and 12 are killed by timed thread races, so both were run a second
time from a fresh `src` copy. Both were killed again by the same tests:

- M11: `test_per_thread_circuit_breaker_same_file` — killed in 22.3s.
- M12: `test_shared_resource_lock_single_instance` — killed in 12.0s.

## Surviving mutants and `tests/test_mutants_extra.py`

No mutant survived (12/12 killed), so no `tests/test_mutants_extra.py` was
required by the task and none was created. The suite already contains focused
regression coverage for every mutated behavior listed above.

## Reviewer observation on harness coverage (no effect on the score)

`tests/test_lock_concurrency.py` inserts the real `../src` at the front of
`sys.path` so that `spawn`-started worker processes can import the package.
Under mutation runs this means the multi-process tests in that file (and the
spawn workers of `test_replay_concurrency.py`) always exercise the pristine
package, not the mutant, and can never kill anything. They also cannot cause a
false kill (pristine code passes them). All 12 kills above come from the
single-process and in-process-thread tests, which do see the mutant.

## Cleanup

- `./_mutant` deleted at the end (verified: it is absent from the repository root).
- Helper work files used to drive the matrix were removed; the exact mutations
  are documented in the Appendix below and are trivially re-appliable.
- `src/` unmodified: pristine suite passes before and after (208 passed).

## Appendix — exact mutations applied (all inside `_mutant/src/joons_agent_guard/`)

1. `lock.py` (`ResourceLock.acquire`): `return cur.rowcount == 1` → `return True`
   (after the `INSERT OR IGNORE ... time.time()` statement).
2. `lock.py` (`ResourceLock.release`): SQL `"DELETE FROM resource_locks WHERE resource=? AND owner=? AND job_id=?"` with params `(resource, owner, job_id)` → SQL `"DELETE FROM resource_locks WHERE resource=? AND owner=?"` with params `(resource, owner)`.
3. `lock.py` (`ResourceLock.break_lock`): `"DELETE FROM resource_locks WHERE resource=? AND job_id=?", (resource, expected_job_id)` → `"DELETE FROM resource_locks WHERE resource=?", (resource,)`.
4. `circuit.py` (`CircuitBreaker.begin`): `if row and (row[1] is not None or row[0] == "half_open"):` → `if row and row[0] == "half_open":`.
5. `circuit.py` (`CircuitBreaker.begin`): `state = "half_open" if row and row[0] == "open" else "closed"` → `state = "closed"`.
6. `circuit.py` (`CircuitBreaker.finish`): `failures=CASE WHEN ? THEN 0 ELSE failures+1 END` → `failures=CASE WHEN ? THEN 0 ELSE failures END`.
7. `replay.py` (`ReplayGuard.claim`): `return stored[1] if stored[0] == digest else CONFLICT` → `return stored[1] if stored[0] == digest else NEW`.
8. `replay.py` (`ReplayGuard.complete`): `WHERE principal=? AND request_id=? AND digest=? AND state='pending'` → `WHERE principal=? AND request_id=? AND digest=?`.
9. `replay.py` (`ReplayGuard.apply_pending`): inside the transaction, the effect call and its response store were replaced by `UPDATE requests SET state='completed', code=NULL, response=NULL ...`; the effect then runs after the `with _db.write_txn(...)` block, and its `(code, response)` is stored in a second transaction.
10. `replay.py` (`ReplayGuard.reserve_outbound`): `return context` → `return candidate` (the freshly generated candidate, i.e. a new `request_id` on every call; the stored reservation is untouched).
11. `_db.py` (`write_txn`): `db.execute("BEGIN IMMEDIATE")` → `db.execute("BEGIN")`.
12. `_db.py` (`write_txn`): `with db.guard_lock:` → `if True:` (the transaction body no longer holds the connection's re-entrant guard lock).

RESULT: task=T17 status=done killed=12/12 files=MUTATION_REPORT.md
