# Issue #70 — [P2] Disk-space guard refuses all writes at 95% used regardless of free bytes (400 GB free on 10 TB -> OSError)

Found by adversarial audit #55 (2026-09-06). Priority: medium. Probe convention: PASS = claim holds, FAIL = defect reproduced.

EARS SPEC:
- check_disk_space shall refuse a write only when free bytes < max(2 x required_bytes, DATASHARD_MIN_FREE_BYTES [default 1 GiB]); the percentage thresholds shall only WARN.
- Verification: audit/evaluations/probe_disk_threshold_blocks_writes_with_free_space.py shall PASS.

EVIDENCE (CONFIRMED, deterministic stub of shutil.disk_usage): 10 TB volume, 400 GB free -> append_records raised OSError 'Disk critically full: 96.0% used (threshold: 95.0%)'. Every write including the commit-point hint is refused, turning the lake read-only. disk_utils.py:54-96.
