# Issue #44 — Re-audit datashard 0.6.0 for production-worthiness certification

EARS SPEC:

- The auditor shall re-verify every fix ticket #13–#43 against current source.
- The auditor shall execute the test suite and static analysis (pytest, mypy, ruff) and report results.
- The auditor shall check git history for committed secrets and verify #13's status.
- The auditor shall hunt for new defects introduced or missed by the remediation, with adversarial verification of each finding.
- If a remaining data-loss, corruption, or lost-update path is found, then the report shall flag it as a certification blocker.
- When all bank-grade pillars pass and no blocker remains, the auditor shall certify as production-ready; else deny with a ranked remediation list.

## Outcome (2026-07-24)

**Verdict: NOT YET PRODUCTION-WORTHY — certification denied pending #45–#49 (+#13).**
Full report: `../AUDIT_REPORT_2.md`.

- All prior P0–P2 code fixes (#14–#43) verified genuinely present; 123 confirmations, 23 reviewer claims refuted by adversarial verification.
- Secrets picture corrected: no live AWS creds ever committed (only `AKIAIOSFODNN7EXAMPLE` doc key + MinIO placeholders); #13 downgraded to key-rotation hygiene, stays open.
- New findings, all ticketed: **#45 P0 (reproduced: symlinked-root GC deletes entire table)**, #46 P1 (not_in returns NULL rows, reproduced), #47 P1 (read path escapes table sandbox), #48 P1 (dangling snapshot-id fail-open commit), #49 P1 (append_files skips schema validation), #50–#51 P2 clusters (S3/lock robustness; Iceberg fidelity), #52 P3 hygiene cluster.
- Empirical: pytest 107 pass / 3 skip; mypy strict clean; ruff clean.
- Path to sign-off: fix #45–#49 (≈1 day), rotate keys (#13), re-run reproductions, then certify. #50–#51 required only for bank-grade tier.
