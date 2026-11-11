## What

One behavioral change per pull request; describe it in a sentence or two.

## Why

The problem this solves, with a link to the issue if one exists.

## Verification

- [ ] `python -m pytest -q` passes locally
- [ ] Tests added or updated for the change
- [ ] Standard library only; no new runtime dependencies
- [ ] Existing database files keep working (schema untouched or compatible)
- [ ] `docs/guarantees.md` still accurate (updated if affected)
- [ ] `CHANGELOG.md` entry added under the unreleased section

## Notes

Anything reviewers should look at closely.
