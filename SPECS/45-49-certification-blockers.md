# Issues #45–#49 — Audit #44 certification blockers

Remediation of the five findings that blocked production certification of datashard 0.6.0
(see `../AUDIT_REPORT_2.md`). Regression tests: `tests/test_audit_44_fixes.py`.

## #45 — P0 data loss: GC via a symlinked table root deletes the whole table

EARS SPEC:

- When the table root is opened through a symlink, `LocalStorageBackend.list_files` shall
  return paths relative to the CANONICAL (symlink-resolved) table root.
- While garbage collecting, if a storage listing returns a path outside the table root, the
  collector shall abort without deleting anything (reachability is undeterminable).
- When GC runs against a symlinked root over a healthy table, it shall delete nothing live.

Implementation:
- `storage_backend.py`: new `LocalStorageBackend._real_base_path()` — single canonical base
  used by both `_resolve_path` and `list_files`; `list_files` now computes `relpath` against
  it and raises if a produced path still escapes the root.
- `garbage_collector.py::_gc_prefix`: independent fail-closed guard — a listed path starting
  with `..` raises `GarbageCollectionAborted`.

## #46 — P1 correctness: `not_in` returned NULL rows

EARS SPEC:

- When a scan filter uses `in` or `not_in`, rows whose filtered column is NULL shall never
  match (the contract documented on `Table.scan`), including when the value set contains
  NULL or is empty.

Implementation: `filters.py::_build_condition` — `in`/`not_in` drop NULLs from the value set
and AND the result with `field.is_valid()`; `not_in ()` becomes `is_valid()` rather than
"every row".

## #47 — P1 security: read path bypassed the table-root sandbox

EARS SPEC:

- While reading or writing through PyArrow, the resolved path shall lie inside the table root.
- If a manifest entry resolves outside the table root, the operation shall raise instead of
  opening the file.

Implementation: `data_operations.py::_get_arrow_path` — Iceberg-style (`/data/…`, `/metadata/…`)
and relative paths route through `LocalStorageBackend._resolve_path`; a true absolute path is
honoured only when contained in the canonical table root, else `ValueError`. Dead
`Transaction._resolve_file_path` now delegates to the same resolver.

## #48 — P1 fail-open: dangling `current_snapshot_id` dropped all prior data

EARS SPEC:

- If `current_snapshot_id` is set but resolves to no snapshot, commit shall abort rather than
  build a snapshot from an empty base manifest set.
- If `current_snapshot_id` is set but resolves to no snapshot, reads shall raise rather than
  report the broken table as empty.

Implementation: `transaction.py::_commit_file_ops` raises `RuntimeError` on a dangling id;
`Table._get_all_data_files` raises instead of returning `[]`.

## #49 — P1 integrity: `append_files()` skipped schema validation

EARS SPEC:

- When appending pre-built parquet `DataFile`s to a table with a persisted schema, the file's
  stored schema shall be validated against the table schema (names, order, types, nullability
  — exactly what `pa.concat_tables` requires).
- If the file's schema cannot be read, the append shall be rejected.
- Tables without a persisted schema keep the previous behaviour (nothing to validate against),
  matching `append_data(records)`.

Implementation: `transaction.py::Transaction._validate_file_schema`, called from
`append_files`.

## Outcome (2026-07-25)

- All five fixed; `tests/test_audit_44_fixes.py` adds 15 regression tests (12 fail against the
  unfixed source, verified by stashing the fixes).
- Both original audit reproductions (symlink-GC data loss, `not_in` NULL rows) re-run clean.
- Suite: 122 passed / 3 skipped (S3, no creds). `mypy src/datashard` clean, `ruff check src` clean.
