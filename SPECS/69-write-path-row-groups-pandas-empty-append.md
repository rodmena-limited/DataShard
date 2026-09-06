# Issue #69 — [P2 performance] Write path: 1000-row row groups, records converted to Arrow twice, append_pandas via to_dict (6-11x slower), empty append commits a snapshot

Found by adversarial audit #55 (2026-09-06). Priority: medium. Probe convention: PASS = claim holds, FAIL = defect reproduced.

EARS SPEC:
- write_data_file shall convert records to ONE Arrow table and write it with pq.write_table(row_group_size=...) so a 50k-row append yields 1 row group (currently 50 groups of 1000).
- append_pandas shall convert with pa.Table.from_pandas(df, schema) (no to_dict('records')) and shall be within 2x of a raw pyarrow write (currently 6-11x).
- If append_data() receives zero records, then it shall be a no-op (no data file, no snapshot) or raise ValueError; append_records([]) currently commits an empty file and a snapshot.
- Verification: probe_row_group_layout.py, probe_append_pandas_path.py, probe_house_rules_and_claims.py (empty-append line) shall PASS.

EVIDENCE (CONFIRMED live): 50000 rows -> 50 row groups, 1-column read 2.7-3.4x slower than a single-group file; append_pandas 300k rows 0.50-0.96 s vs 0.08-0.09 s native; append_records([]) -> True, snapshots=1, data files=1. Root cause: data_operations.py:560-566 batches of 1000 into ParquetWriter.write_batch (one row group per call); transaction.py:153-176 df.to_dict('records').
