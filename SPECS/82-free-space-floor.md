# #82 — Free-space floor sized for datashard's own writes (0.9.1)

## Requirements (EARS)

- The disk-space guard **shall** refuse a write only when free bytes < max(4 × write size, `DATASHARD_MIN_FREE_BYTES`), with a default floor of **64 MiB** (was 1 GiB in 0.8.0–0.9.0).
- The percentage thresholds **shall** remain warnings only (unchanged from #70).
- **When** the floor refuses a write, the error **shall** name the free bytes, the required bytes, and the environment variable that adjusts the floor.

## Evidence

CONFIRMED live during the 0.9.0 release verification: the wheel installed back from PyPI into a clean venv raised
`OSError: Insufficient disk space: 0.88 GB free, need at least 1.00 GB` from `create_table` under /tmp
(a 4 GB tmpfs with 0.88 GB free) for a 2 KB metadata file. The #70 unit test stubbed `shutil.disk_usage`
and never met a real small disk - a self-confirming check.

## Verification

- `tests/test_audit_55_fixes.py::test_disk_space_guard_*`: 500 MB free → allowed for 2 KB; 10 MB free → refused;
  500 MB free → refused for a 200 MB write (4× rule); 400 GB free on a 96 %-full volume → allowed.
- 0.9.1 wheel installed back from PyPI, smoke test run with the table under /tmp (0.88 GB free) → must pass.

## Outcome
(filled at release)

**Outcome (2026-09-06):** shipped as 0.9.1 (https://pypi.org/project/datashard/0.9.1/). Exercised: 198 unit tests,
33/33 probes in `audit/evaluations/run_all.sh`, the wheel installed back from PyPI into a clean venv and the smoke
test (create_table, append_arrow, sql, to_arrow, parquet_paths) run with the table under /tmp holding 517 MB free -
the condition under which 0.9.0 failed. Not exercised: a genuinely full disk on a real filesystem (the refusal path
is covered by the stubbed unit test only).
