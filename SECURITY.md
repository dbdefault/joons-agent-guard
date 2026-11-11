# Security Policy

## Supported versions

| Version | Supported |
| --- | --- |
| 0.1.x | Yes |
| older | No |

## Reporting a vulnerability

Report privately via **GitHub Security Advisories**: on this repository's
**Security** tab, choose **Report a vulnerability**. Do not open a public
issue or pull request for a security problem.

Please include the affected version, a minimal reproduction, and your
assessment of the impact. We respond as soon as possible and credit
reporters on request.

## Security model

- **No caller authentication.** `owner`, `actor`, and `principal` are plain
  strings the library records as given and never verifies. Feed them from a
  trusted transport (mTLS identity, verified signature), never from
  client-supplied headers. Anyone who can reach the database can act as
  anyone.
- **Database file permissions are the caller's job.** All state — lock
  holders, circuit state, request digests, stored responses — lives in one
  SQLite file. Anyone who can read the file sees everything; anyone who can
  write it bypasses every guarantee. Restrict the file to the service
  account that needs it. The library never sets a file mode: the file is
  created with your process umask (often world-readable 0644 on POSIX), so
  create it in a private directory or `chmod 600` it before first use; on
  Windows the parent directory's ACL applies.
- **Size limits.** Identifiers are limited to 1024 characters
  (`InvalidIdentifier`), circuit failure reasons are truncated to 4096
  characters, and each stored replay response is limited to 1 MiB of JSON by
  default (`ReplayGuard(..., max_response_bytes=None)` removes the cap).
  NaN and Infinity are rejected because they are not valid JSON.
- **Corrupted or tampered data fails loudly.** A stored response that cannot
  be decoded raises `CorruptStorage`; its message names the request but
  never contains the stored bytes.
- **The CLI reads without writing.** Read commands open the database with
  SQLite's read-only mode, so inspecting a crashed service never rewrites the
  file (a hot journal is reported as an error instead of being rolled back).
- **Holder disclosure is intentional.** `LockHeld` messages and the CLI show
  the current holder's `owner`/`job_id` so operators can act on them.
- **Do not store secrets in responses.** `ReplayGuard` stores responses as
  JSON text and replays them later, and `reserve_outbound` stores the
  `fixed` fields verbatim. Store hashes or opaque tokens, never credentials
  or raw bodies.
- **Nothing expires by age.** Stuck state (held locks, pending requests,
  claimed circuit slots) persists until an explicit operator action; plan
  the procedures that clear it. See `docs/guarantees.md`.
- **Locks are advisory.** They coordinate only processes that use the same
  database file through this library, and only on a local filesystem — not
  network drives.
