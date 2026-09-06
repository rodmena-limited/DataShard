# Issue #81 — Release 0.9.0: DuckDB layer + ingestion speed (docs, changelog, probes, audit pass, publish)

Part of the uplift plan (0.9.0). 

EARS SPEC:
- When #78-#80 are closed, the release shall bump the version to 0.9.0, add a CHANGELOG entry with measured numbers, add the duckdb optional extra and include duckdb in the dev extra, document the analytics layer and batch-per-commit guidance in README/docs.
- Before publishing, the release shall pass: pytest, mypy strict, ruff, audit/evaluations/run_all.sh (all probes), an adversarial pass over the new modules with at least one new probe (probe_v090_*), and the external OVH e2e probe.
- After publishing, the release shall be installed back from PyPI into a clean venv and smoke-tested (to_arrow, sql, append_arrow), then tagged v0.9.0 and pushed.
- The release ticket shall close with the exercised / not exercised / uncertain statement.

## Outcome (2026-09-06)

Released as 0.9.0 (a151a95, tag v0.9.0, PyPI). #78, #79, #80 closed; harness 33/33; the #80 latency target was not met (3.02 s vs 3.08 s) and is deferred to the 0.10 protocol change.
