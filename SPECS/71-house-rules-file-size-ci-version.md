# Issue #71 — [P3 house rules] 6 source files + 1 test file exceed the 500-line cap (transaction.py 1329); GitHub Actions workflow present; __version__ stale in dev install

Found by adversarial audit #55 (2026-09-06). Priority: low. Probe convention: PASS = claim holds, FAIL = defect reproduced.

EARS SPEC:
- No file under src/datashard or tests shall exceed 500 lines: split transaction.py (1329: Table read path -> table.py/scan.py, Transaction, TransactionManager), storage_backend.py (920 -> local + s3 modules), data_operations.py (780), metadata_manager.py (633), file_manager.py (535), lock_provider.py (521), tests/test_scan_features.py (691).
- CI shall run on ci.rodmena.co.uk; .github/workflows/ci.yml is flagged for migration and left untouched until the operator asks.
- datashard.__version__ shall equal the pyproject version in editable installs (fall back to reading pyproject.toml when dist metadata is stale; currently reports 0.5.1 vs 0.7.2).
- Verification: audit/evaluations/probe_house_rules_and_claims.py file-size, workflow and version lines shall PASS.
