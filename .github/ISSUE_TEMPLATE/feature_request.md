---
name: Feature request
about: Suggest an improvement
title: 'feature: '
labels: enhancement
---

**Problem or use case**

What are you trying to do, and what is missing or painful today?

**Proposed change**

The API or behavior you would like, with a short usage sketch.

**Constraints to keep in mind**

- Standard library only at runtime; no new runtime dependencies.
- Nothing may expire by age; recovery stays explicit and job-id-checked.
- Changes must keep `docs/guarantees.md` accurate.
- Existing database files must keep working (no migration requirement).

**Alternatives considered**

Other approaches you evaluated and why they are worse.
