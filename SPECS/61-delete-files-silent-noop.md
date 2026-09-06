# Issue #61 — [P1] delete_files() silently succeeds for unknown or slash-mismatched paths and commits an empty 'delete' snapshot

Found by adversarial audit #55 (2026-09-06). Priority: high. Probe convention: PASS = claim holds, FAIL = defect reproduced.

EARS SPEC:
- When delete_files() is committed, the transaction shall normalise requested paths and manifest entry paths identically (strip leading '/') before matching.
- If any requested path is not present in the base snapshot, then commit() shall raise FileNotFoundError naming the missing paths and shall not create a snapshot.
- A delete that removes zero files shall never create a snapshot.
- Verification: audit/evaluations/probe_delete_files_silent_noop.py shall PASS.

EVIDENCE (CONFIRMED live): delete_files(['/data/does_not_exist.parquet']) -> commit returned True, snapshots 1->2 (operation 'delete'), rows unchanged. A file appended as 'data/ext.parquet' and deleted as '/data/ext.parquet' survived (rows 2->2); the exact string form deletes it. Violates issuedb memory 'no_silent_noop_apis'.

Synthesis (localised fix): solution domain = path normalisation + fail-closed API contract; concept = validate-then-apply in Transaction._commit_file_ops (transaction.py:507-547).
