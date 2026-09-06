# Issue #62 — [P1] Schema validation ignores field order: a reordered schema is accepted and every subsequent scan fails on concat

Found by adversarial audit #55 (2026-09-06). Priority: high. Probe convention: PASS = claim holds, FAIL = defect reproduced.

EARS SPEC:
- When append_data()/append_pandas() receives a schema whose fields equal the table's persisted fields as a set, the writer shall serialise the data file in the TABLE's persisted field order (the caller's schema may only be checked for compatibility, never dictate layout).
- The arrow schema cache in DataFileManager shall be keyed by a fingerprint of the schema's fields (ids, names, types, required, order), never by schema_id alone.
- When concatenating per-file tables, scan()/to_pandas() shall align columns to the table schema order before concat so files written by older versions with a different order remain readable.
- Verification: audit/evaluations/probe_schema_field_order_bricks_scan.py shall PASS.

EVIDENCE (CONFIRMED live): table schema [id, name]; a fresh process appended with [name, id] (same ids/types/required) - accepted; the next scan() raised pyarrow.lib.ArrowInvalid (schema mismatch on concat). Root cause: transaction.py:192-217 _schema_signature is an unordered set; data_operations.py:444-467 caches the arrow schema per schema_id. This is the same 'table bricked by one append' class #49 fixed for append_files.
