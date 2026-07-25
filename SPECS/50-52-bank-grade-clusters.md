# Issues #50–#52 — Audit #44 P2/P3 clusters (bank-grade tier)

Remediation of the clusters that were not certification blockers but are required for
bank-grade certification (#50, #51) plus the hygiene cluster (#52).
Regression tests: `tests/test_audit_44_p2_fixes.py`.

## #51 — Iceberg fidelity & auditability

EARS SPEC:

- When a snapshot is committed, it shall be assigned a monotonic sequence number derived
  from the base metadata's `last_sequence_number`, and that number shall be stamped on the
  manifest entries the same commit writes.
- When a manifest is rewritten, carried-over files shall keep the sequence number they were
  added with (inheritance) — a rewrite shall never re-date existing data.
- When a snapshot is committed, it shall record the `schema_id` it was written against.
- When metadata is committed, the superseded metadata file shall be appended to
  `metadata_log`, trimmed to `write.metadata.previous-versions-max` (default 100).
- When snapshots are expired or deleted, no surviving snapshot shall retain a
  `parent_snapshot_id` that references a removed snapshot; it shall be repointed to its
  nearest surviving ancestor, or None.
- While a transaction is committing, its manifests and manifest lists shall be protected
  from garbage collection until the commit completes or the marker is abandoned.
- The legacy `FileManager.cleanup_orphaned_files` shall not delete files (no grace period,
  no in-flight protection, no fail-closed reachability); it shall raise and direct callers
  to `Table.garbage_collect`.
- When the same data file appears in more than one manifest under different path spellings,
  the scan shall read it once.

Implementation: `data_structures.py` (`Snapshot.sequence_number`, `DataFile.sequence_number`),
`snapshot_manager.py` (`repoint_parents_to_surviving_ancestors`, sequence/schema assignment),
`file_manager.py` (entry/manifest sequence numbers, `pre_write_hook`, disabled legacy cleanup),
`metadata_manager.py` (`_append_metadata_log`), `garbage_collector.py` (marker payload →
protected path, protection applied to the manifests prefix), `transaction.py`
(`_register_inflight`, sequence plumbing, normalized dedup).

Known and accepted: manifest entries' `added_snapshot_id` may reference an expired snapshot.
Manifests are immutable; rewriting history to erase the reference would falsify it. This
matches Apache Iceberg.

## #50 — S3 & locking robustness

EARS SPEC:

- `delete_file`, `get_size`, `get_modified_time` and `list_files` shall retry transient S3
  errors, as the read paths already do.
- If an S3 error is permanent (credentials, permissions, missing bucket), the retry helper
  shall fail immediately instead of retrying.
- `exists(path)` shall answer True only for an object at that exact key, unless the path is
  directory-like (trailing `/`).
- While holding a non-CAS polling lock, the provider shall refuse to renew a lease that has
  already lapsed, and `is_held()` shall report False past the lease deadline.
- When the polling (non-CAS) provider is selected, it shall warn that locking is best effort.
- At the commit fence, a single transient error on the ownership check shall be retried once
  before reporting "not held" (which fails closed).

Implementation: `s3_consistency.py` (`PERMANENT_S3_ERROR_CODES`, `is_permanent_s3_error`),
`storage_backend.py` (retries + strict `exists`), `lock_provider.py` (lease deadline, warning,
single fence retry).

## #52 — Hygiene

EARS SPEC:

- The documented version shall track `pyproject.toml`.
- The package shall install a `datashard` console script.
- The maturity classifier and `requires-python` floor shall reflect reality (Beta, ≥3.10 —
  3.9 reached EOL in October 2025).
- Dead `to_pyarrow_filter` (which silently dropped IS_NULL) shall be removed.
- `{"col": None}` shall raise, naming `is_null`, instead of silently matching zero rows.
- GC shall log per-file deletions at DEBUG and a count at INFO.
- Schema-compatibility checks shall catch only schema-mismatch errors, not every exception.
- `DataFileWriter.open` shall not leak its temp file when the writer cannot be constructed.

Implementation: `docs/conf.py`, `pyproject.toml`, `filters.py`, `garbage_collector.py`,
`data_operations.py`.

## Outcome (2026-07-25)

- All three clusters implemented; `tests/test_audit_44_p2_fixes.py` adds 19 tests
  (15 fail against the unfixed source, verified by stashing the fixes).
- Suite: 141 passed / 3 skipped (S3, no creds). `mypy src/datashard` clean, `ruff check .`
  clean (including the two pre-existing test lint errors, now fixed).
