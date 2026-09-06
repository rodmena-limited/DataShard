# Issue #64 — [P1] create_table() silently ignores a conflicting schema on an existing table

Found by adversarial audit #55 (2026-09-06). Priority: high. Probe convention: PASS = claim holds, FAIL = defect reproduced.

EARS SPEC:
- If create_table(path, schema) finds an existing table whose persisted schema differs (field ids, names, types, required or order), then create_table shall raise SchemaMismatchError (a TableExistsError subclass) unless if_exists='ignore' is passed, in which case it shall log a WARNING naming both schemas.
- Verification: audit/evaluations/probe_create_table_conflicting_schema.py shall PASS.

EVIDENCE (CONFIRMED live): table created with [id:long]; create_table(path, [price:double]) returned the table with fields ['id'], no exception, no warning (iceberg.py:43-52 only warns when the existing schema is EMPTY).
