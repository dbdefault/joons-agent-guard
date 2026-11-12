# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-11-12

### Added

- `ResourceLock` — durable exclusive locks on named resources, held by an
  `(owner, job_id)` pair, with explicit `break_lock` recovery; locks never
  expire on their own.
- `CircuitBreaker` — durable per-actor single run slot with
  `closed` / `open` / `half_open` states and an operator `reset`.
- `ReplayGuard` — at-most-once handling of retried inbound requests
  (`claim`, `complete`, `response`, `apply_pending`) and stable outbound
  intents (`reserve_outbound`).
- `request_digest` — SHA-256 helper for canonical request digests.
- Operator command line `python -m joons_agent_guard` — `locks`, `circuit`,
  `pending [--older-than]`, and confirmation-gated `break-lock` /
  `reset-circuit`.
- Asyncio adapters `AsyncResourceLock`, `AsyncCircuitBreaker` and
  `AsyncReplayGuard` — thin wrappers that own one synchronous instance and run
  every blocking call via `asyncio.to_thread` (Python >= 3.9), exposing the
  same method names as coroutines, `hold` as an async context manager, and
  `aclose()` / `async with` lifecycle.

### Notes

- Shared instances are thread-safe: every write transaction, every read, and
  `close()` hold the connection's re-entrant guard lock, so statements of
  different threads never interleave inside one transaction (a shared
  instance serialises its calls).
- Nested writes raise `GuardError`: an `apply_pending` effect must not call
  write methods of the same instance while its transaction is running.
- `complete` and `apply_pending` require `code` to be an `int` (a `bool` is
  rejected with `TypeError`).
- `request_digest` rejects `body_sha256` as an extra field (`ValueError`; it
  is always computed from `body`) and requires `str` field values
  (`TypeError` otherwise).
- Responses are stored as JSON: tuples come back as lists and non-str dict
  keys as strings, so use JSON-stable types. NaN/Infinity are rejected.
- Limits: identifiers at most 1024 characters, failure reasons truncated to
  4096 characters, replay responses at most 1 MiB of JSON by default
  (`max_response_bytes=None` disables the cap).
- `CorruptStorage` is raised when a stored response cannot be decoded.
- CLI read commands open the database read-only and never modify it.
