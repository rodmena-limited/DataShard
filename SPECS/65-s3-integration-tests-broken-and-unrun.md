# Issue #65 — [P1 test rigor] S3 integration tests fail (2 of 3) against any S3 server and never run in CI; test tables are never cleaned up

Found by adversarial audit #55 (2026-09-06). Priority: high. Probe convention: PASS = claim holds, FAIL = defect reproduced.

EARS SPEC:
- The S3 integration tests shall pass against a moto S3 server started by the test session (no external credentials) and shall run in the house CI (ci.rodmena.co.uk), never GitHub Actions.
- Tests shall assert table behaviour (row_count, scan, snapshots, current_snapshot) and shall not assert storage.exists() on directory-like paths.
- Integration tests shall delete every object they create; the production bucket currently holds 7 leftover test_table_* prefixes.
- Verification: `pytest tests/test_s3_integration.py` green against moto in CI.

EVIDENCE (CONFIRMED live): against moto, test_s3_storage_create_table and test_s3_storage_write_and_read FAIL on `assert table.storage.exists("metadata")` / `exists("data")` - the strict exists() introduced by #50 made these False, and CI skips the module (no credentials), so the S3 code path has had zero automated coverage since 0.7.0. Bucket listing on the configured endpoint shows test_table_158e2247/ ... test_table_e1782255/ leftovers.
