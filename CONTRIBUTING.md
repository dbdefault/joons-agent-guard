# Contributing

Thanks for helping improve this safety library. Small, obvious, well-tested
changes beat clever ones here.

## Setup

Requires Python >= 3.9. The package uses only the standard library at
runtime.

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv/Scripts/activate
pip install -e ".[test]"
```

## Tests

```bash
python -m pytest -q
```

- Every behavior change comes with a test that fails without the change.
- Tests are single-process and use temporary database files. Do not add
  tests that depend on timing, network access, or shared state.
- The guarantees in `docs/guarantees.md` are the contract. If your change
  affects them, update the document in the same pull request.

## Ground rules

- **Standard library only at runtime.** No new runtime dependencies, ever.
  Development/test tools (pytest, CI) are fine.
- Stored schemas are created with `CREATE TABLE IF NOT EXISTS` and must keep
  existing database files working after an upgrade; never require a
  migration.
- Keep the failure philosophy intact: nothing expires by age, recovery is
  always an explicit, job-id-checked operator action. Proposals that change
  this should be discussed in an issue first.
- Never store secrets, credentials, or raw request bodies in the database.

## Pull requests

- Keep each pull request small and focused: one behavioral change, with its
  tests, documentation updates, and a `CHANGELOG.md` entry under the current
  unreleased section.
- CI runs the test suite on Linux, Windows, and macOS across Python
  3.9/3.11/3.13, plus every script under `examples/`. Keep both green.
- Describe what changed, why, and how you verified it.

## Reporting bugs

- Regular bugs: open a GitHub issue using the bug report template, with a
  minimal reproduction, the behavior the documentation promises, and the
  behavior you observed.
- Security issues: do not open a public issue or pull request. See
  `SECURITY.md` and report via a private GitHub Security Advisory.
