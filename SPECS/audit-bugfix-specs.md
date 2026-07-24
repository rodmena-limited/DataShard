# datashard audit bugfix specs (issuedb #13–#43)

Derived from audit #10 (see `AUDIT_REPORT.md`). Each ticket's authoritative EARS
spec lives in issuedb (`issuedb-cli context <ID>`); this file is the in-repo
mirror required by the engineering workflow. Keep both in sync as specs evolve.

**Status (2026-07-24): #14–#43 IMPLEMENTED, VERIFIED, CLOSED** (107 tests pass,
mypy/ruff clean; regression tests in `tests/test_audit_fixes.py`). **#13 OPEN** —
code-side secrets removed; key rotation + git-history purge pending (maintainer).

## P0 — certification blockers
- **#13 (security):** The repository shall not contain live credentials in any tracked file or git history; discovered keys shall be rotated and purged before release; secrets present only in untracked files shall still be rotated.
- **#14 (data-loss):** While a transaction has durably committed its snapshot, if a later step raises, then it shall not delete any file referenced by the committed snapshot; the version-hint write is the commit point.
- **#15 (data-loss):** If reading the base snapshot's manifest list fails during commit, then the commit shall abort rather than proceed with an empty manifest set.
- **#16 (data-loss):** If the garbage collector cannot read a reachable manifest list or manifest, then it shall abort without deleting any file (fail closed).
- **#17 (data-loss):** While a transaction has written but not committed data files, the garbage collector shall not delete those files regardless of grace period.
- **#18 (data-loss):** When a table is created with a schema, the metadata shall persist it; a schema-less append shall use the persisted schema or raise, never write zero columns.
- **#19 (correctness):** If any data file/manifest/manifest-list read fails during a scan, then the scan shall raise, not return partial/empty results.
- **#20 (correctness):** When a scan filter uses is_null/is_not_null, scan() and to_pandas() shall return only rows satisfying that predicate.
- **#21 (correctness):** When a committed transaction contains overwrite_by_filter, the data shall reflect the overwrite; if unimplemented, the API shall raise NotImplementedError.
- **#22 (data-loss):** If the version-hint is missing/unreadable while v*.metadata.json files exist, then the system shall recover the latest version from metadata; initialize_table shall refuse an existing table and take the lock.

## P1 — high
- **#23:** expire_snapshots shall remove matching snapshots or raise (no silent no-op).
- **#24:** delete_snapshot shall repoint current to the newest remaining snapshot by snapshot_log/timestamp, not max(id).
- **#25:** Unrecognized filter operators shall raise ValueError.
- **#26:** Appends with fields absent from / incompatible with the schema shall raise; extra fields shall not be dropped; a divergent-schema append shall not break scan().
- **#27:** Manifest entry snapshot_id and manifest-list added_snapshot_id shall equal the committed Snapshot id.
- **#28:** Manifest filenames shall include a per-writer unique token (collision-free under concurrency).
- **#29:** A resolved storage path outside the table base (sibling-prefix or symlink) shall be rejected (realpath + boundary check).
- **#30:** S3-backed locking shall guarantee ≤1 holder and fence stale holders; commit shall re-validate lock ownership before the version-hint write.
- **#31:** FileLock blocking acquire shall raise TimeoutError on timeout; no-primitive fallback shall not silently lose exclusion.

## P2 — medium
- **#32:** GC shall scan the real manifest directory (metadata/manifests).
- **#33:** Local data files shall be fsync'd before the atomic rename.
- **#34:** Manifest min/max bounds shall be stored type-faithfully (no stringify/re-infer) so pruning cannot skip matching files.
- **#35:** Snapshot retention shall be opt-in and documented, not silent, and not keyed to write.metadata.previous-versions-max.
- **#36:** Provided column_sizes/value_counts/null_value_counts shall serialize without error, or be rejected explicitly.
- **#37:** Carried-over files in rewritten manifests shall be status EXISTING with their original added_snapshot_id.
- **#38:** Directory creation shall route through the storage backend; no local dirs for S3 tables.
- **#39:** S3 exists() shall retry transient errors via the consistency handler.
- **#40:** write_pandas_file shall size via the resolved path and populate checksum/bounds.
- **#41:** Duplicate schema field ids shall raise at Schema construction.
- **#42:** Reads shall verify stored checksums and raise on mismatch.

## P3 — low (cluster)
- **#43:** __version__ shall match pyproject; remove tracked .bak; remove dead/landmine code; fix README examples/claims; begin() shall reset _operations; empty commits shall not create snapshots; document null semantics; bound TransactionManager; de-correlate S3 lock jitter; stop per-write INFO logging by default.
