# Changelog

All notable changes to DataShard will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.7.2] - 2026-08-12

S3 reads now work on providers pyarrow cannot talk to (#54).

### Fixed

- **Parquet reads go through DataShard's own storage backend instead of
  pyarrow's `S3FileSystem`.** Against OVH Object Storage, pyarrow's bundled AWS
  SDK sends an `x-amz-checksum-mode` header on GetObject that OVH rejects:

      AWS Error [code 134] during GetObject operation:
      Value for x-amz-checksum-mode header is invalid.

  boto3 reads the identical object with the identical credentials without
  complaint, so the S3 backend now serves reads too. The failure mode was
  particularly unhelpful: writes go through boto3, so `create_table` succeeded
  and the metadata appeared in the bucket, then the **first append** died
  validating the file it had just written.

### Added

- `StorageBackend.open_seekable(path)` — a seekable binary file object.
  `S3RangeFile` implements it over ranged GETs, so pyarrow still reads only a
  parquet footer rather than the whole object. Measured on OVH: 65 KB fetched to
  validate a 361 KB file, 462 KB for a 2.9 MB file. A `BytesIO(read_file(path))`
  shortcut would have downloaded 100% of every file to read a schema.

  `open_seekable` is deliberately **not** abstract, so existing third-party
  `StorageBackend` subclasses keep working; they raise only if a parquet read is
  attempted.

### Changed

- `mypy --strict` is now clean across all 19 source modules (was 9 errors:
  unparameterised `set`/`dict`/`tuple` annotations and four `Any` returns).

### Notes

- The path-traversal guard (#47) still runs on every read. The first cut of this
  fix bypassed it — it still contained the path but reported `FileNotFoundError`
  rather than refusing, which the audit suite caught.
- `tests/test_s3_integration.py` has two failures unrelated to this change,
  present before and after it: they assert `exists("metadata")` on a prefix,
  which `exists()` deliberately does not answer True for. The test expectation is
  wrong; the restraint in `exists()` is what stops a missing data file passing
  validation.

## [0.7.1] - 2026-08-12

FreeBSD-compatibility fix (#53).

### Fixed

- **pyarrow is now pinned below 25.0.0** (`pyarrow>=10.0.0,<25.0.0`). FreeBSD's
  ports provide `py312-pyarrow-24.0.0` as the newest pyarrow; the previous
  unbounded `>=10.0.0` let pip resolve to a pyarrow with no FreeBSD wheel and
  fail at source-build time. The ceiling makes the packaged 24.0.0 satisfy the
  dependency (e.g. via `--system-site-packages`). Verified against pyarrow
  24.0.0: full test suite passes (141 passed).
- **mypy now treats pyarrow as opaque** (`follow_imports = "skip"`). pyarrow
  24's bundled partial stubs do not declare `pc.Expression`, `pc.is_in`,
  `pc.min`/`pc.max` or `pyarrow.fs.S3FileSystem` even though they exist at
  runtime, and `ignore_errors` only silences errors *inside* pyarrow — so the
  lint job failed under the newly pinned pyarrow 24. This keeps type-checking
  green under both pyarrow 24 and 25 with no runtime change.

## [0.7.0] - 2026-07-25

Re-audit (#44) remediation. See `AUDIT_REPORT_2.md`. Fixes tickets #45–#52.

> **Upgrade note — behavior changes.** Manifest paths are now strictly
> table-relative on read as well as write: a data file outside the table root is
> refused instead of opened. `append_files()` validates the parquet schema of the
> files it is given. `{"col": None}` filters raise instead of silently matching
> nothing. `FileManager.cleanup_orphaned_files()` raises — use
> `Table.garbage_collect()`. Minimum Python is now 3.10.

### Fixed — data loss / correctness ⚠️

- **Garbage collection through a symlinked table root no longer deletes the
  whole table** (#45). `list_files` computed paths against the raw base while
  walking the resolved tree, so every live file looked like an orphan. Paths are
  now computed against one canonical base, and GC aborts if a listing ever
  returns a path outside the table root.
- **`not_in` no longer returns NULL rows** (#46); `in`/`not_in` never match NULL,
  as documented, including when the value set contains NULL or is empty.
- **A dangling `current_snapshot_id` fails closed** (#48): commit aborts instead
  of building a snapshot from an empty base (which dropped all prior data), and
  reads raise instead of reporting a broken table as an empty one.
- **`append_files()` validates schemas** (#49): a parquet file whose schema
  diverges from the table's is rejected at append time instead of breaking every
  later scan.
- **Cross-manifest de-duplication normalizes paths** (#51), so a file listed as
  `/data/x.parquet` and `data/x.parquet` is no longer read twice.

### Fixed — security

- **The read path is sandboxed to the table root** (#47). `_get_arrow_path` no
  longer returns absolute paths as-is, so a tampered manifest entry cannot make
  the reader open arbitrary files; absolute paths inside the table still work.

### Fixed — Iceberg fidelity & auditability (#51)

- Snapshots now carry real **sequence numbers** (monotonic, inherited unchanged
  by carried-over files) and their **`schema_id`**; `last_sequence_number` is
  maintained.
- The **metadata log** records every superseded metadata file, trimmed to
  `write.metadata.previous-versions-max` (default 100).
- Expiring or deleting snapshots **repoints survivors to their nearest surviving
  ancestor** instead of leaving dangling `parent_snapshot_id` references.
- GC **in-flight protection now covers manifests and manifest lists**, not just
  data files, so a commit in progress cannot be swept by a concurrent GC.
- The unsafe legacy `FileManager.cleanup_orphaned_files` now raises.

### Fixed — S3 & locking robustness (#50)

- `delete_file`, `get_size`, `get_modified_time` and `list_files` retry transient
  S3 errors like the read paths do.
- Permanent S3 errors (AccessDenied, NoSuchBucket, bad credentials …) fail fast
  instead of being retried five times.
- `exists()` no longer answers True for an object path merely because objects
  exist *under* it; only directory-like paths use the prefix listing.
- The non-CAS polling lock refuses to renew a lease that has already lapsed (the
  interleaving that could resurrect a stolen lock), reports `is_held()` False
  past its lease, warns loudly at construction, and the commit fence retries a
  single transient error before failing closed.

### Changed — hygiene (#52)

- Docs version is derived from `pyproject.toml`; classifier moved to Beta; a
  `datashard` console script is installed; minimum Python raised to 3.10.
- Removed the dead `to_pyarrow_filter` (it silently dropped `IS_NULL`).
- GC logs per-file deletions at DEBUG with an INFO summary; schema-compatibility
  checks no longer swallow every exception; a failed parquet-writer construction
  no longer leaks its temp file.

## [0.6.0] - 2026-07-24

Full audit and bank-grade remediation. See `AUDIT_REPORT.md`. Fixes tickets #14–#43.

> **Upgrade note — breaking behavior changes.** Several APIs now fail loudly
> where they previously failed silently: `overwrite_by_filter` raises
> `NotImplementedError`; unknown filter operators raise `ValueError`; appends
> with fields outside the schema raise; `scan()`/`to_pandas()` verify data-file
> checksums by default (disable via `verify_checksums=False` or
> `DATASHARD_VERIFY_CHECKSUMS=false`) and raise on unreadable files instead of
> returning partial results.

### Fixed — data loss / correctness (was silent) ⚠️

- **Post-commit failures no longer delete committed data.** A failure after the
  durable commit point can never trigger a rollback that removes committed
  files; ambiguous commit-point writes keep their data (`AmbiguousCommitError`).
- **Commit and GC now fail closed.** An unreadable base manifest aborts the
  commit instead of silently dropping all prior data; the garbage collector
  aborts (`GarbageCollectionAborted`) if any reachable manifest can't be read
  instead of deleting live files. GC now scans the correct manifest directory.
- **`create_table(schema=...)` persists the schema.** Schema-less appends use it
  or raise, instead of silently writing zero-column files.
- **Scans propagate read errors** instead of returning partial/empty results,
  and verify data-file checksums by default (`DATASHARD_VERIFY_CHECKSUMS`).
- **`is_null`/`is_not_null` filters are applied** in every scan API; unknown
  filter operators now raise instead of silently becoming equality.
- **`overwrite_by_filter` raises `NotImplementedError`** and `expire_snapshots`
  actually expires (never the current snapshot) instead of being silent no-ops.
- Version-hint is now a recoverable hint (rebuilt from metadata files); a lost
  hint no longer causes destructive re-initialization; `initialize_table`
  refuses to overwrite an existing table.

### Fixed — concurrency / integrity

- Manifest and manifest-list filenames include a per-writer UUID (no collisions).
- One snapshot id is shared by manifests, manifest list, and the snapshot
  (lineage joins resolve). Delete-rewrites preserve `EXISTING` status.
- S3 locks use conditional writes (CAS) for create/takeover/renew with commit
  fencing; `FileLock` enforces its timeout; local data files are fsync'd.
- Schema field-id/name uniqueness validated; append schema validated; type-
  faithful column bounds; path resolution hardened (realpath + boundary check).
- Snapshot retention is opt-in (`datashard.snapshot.retention-count`), never
  silent.

### Security

- Removed hardcoded S3 credentials from test scripts (now read from env).
  **Action required:** rotate the previously-exposed keys and purge them from
  git history (ticket #13).

### Tests

- Added `tests/test_audit_fixes.py` (28 failure-path/concurrency tests). Suite:
  107 passed / 3 skipped; mypy --strict and ruff clean.

## [0.4.0] - 2025-11-30

### Breaking Changes ⚠️

- **Manifest Format Migration (JSON to Avro)**
  - Manifest and manifest list files are now written in Avro format using `fastavro` instead of JSON.
  - This aligns with the Apache Iceberg specification and improves I/O performance and storage efficiency.
  - **Backward Compatibility:** The reader includes a fallback mechanism to read legacy JSON manifests, so existing tables remain accessible. However, all new writes will generate Avro files.
- **New Dependency:** Added `fastavro>=1.4.0` to requirements.

### Added

- **S3-Native Distributed Locking** 🔒
  - Replaced the unsafe local `FileLock` for S3 tables with a robust S3-native locking mechanism using conditional writes (`If-None-Match`).
  - Ensures safe concurrent writes in distributed environments (AWS Lambda, Kubernetes, EC2) without external dependencies like DynamoDB.
  - Introduced `LockProvider` abstraction (`LocalLockProvider`, `S3LockProvider`) in `src/datashard/lock_provider.py`.

- **Metadata Compaction / Snapshot Pruning** 🧹
  - Implemented automatic pruning of old snapshots from `metadata.json` to prevent $O(N)$ file size growth.
  - New table property `write.metadata.previous-versions-max` controls retention (default: 100 snapshots).
  - Solves the scalability bottleneck where commit times increased linearly with table history.

- **Data Integrity Verification** 🛡️
  - Computed SHA-256 checksums for all data files during write.
  - Stored checksums in Avro manifests.
  - Added verification logic to detect data corruption during reads.
  - Added `checksum` field to `DataFile` structure.

### Fixed

- **Snapshot ID Overflow:** Fixed an issue where generated Snapshot IDs could exceed Avro's signed 64-bit integer limit.
- **S3 Concurrency Safety:** Fixed a critical race condition where distributed workers could overwrite each other's commits on S3 due to reliance on local filesystem locks.

## [0.3.3] - 2025-11-27

### Added

#### Query Optimization Features 🚀

- **Predicate Pushdown** (`filter` parameter)
  - Filter at parquet level using PyArrow's native filtering
  - Reduces I/O by 90%+ for selective queries
  - Supports: equality, comparison (`>`, `<`, `>=`, `<=`), `in`, `between`
  - Example: `table.scan(filter={"status": "failed"})`
  - Example: `table.scan(filter={"age": (">", 30)})`

- **Partition Pruning** (automatic with filters)
  - Skips files based on column min/max statistics
  - Column bounds computed during write and stored in manifest
  - Can skip 99% of files for time-range queries
  - Zero configuration required - works automatically

- **Parallel Reading** (`parallel` parameter)
  - Multi-threaded file reading using ThreadPoolExecutor
  - 2-4x speedup on multi-core systems
  - Example: `table.scan(parallel=True)` (all cores)
  - Example: `table.scan(parallel=4)` (4 threads)

- **Streaming API** (memory-efficient iteration)
  - `scan_batches(batch_size)` - yields record batches
  - `iter_records()` - yields individual records
  - `iter_pandas(chunksize)` - yields DataFrame chunks
  - Process 100GB tables with ~100MB memory footprint

#### New Methods on Table class

- `scan(columns, filter, parallel)` - Enhanced with new parameters
- `to_pandas(columns, filter, parallel)` - Enhanced with new parameters
- `scan_batches(batch_size, columns, filter)` - Streaming batch iteration
- `iter_records(columns, filter)` - Single record iteration
- `iter_pandas(chunksize, columns, filter)` - DataFrame chunk iteration

#### New Module

- `filters.py` - Filter expression parsing and conversion
  - `FilterOp` enum for filter operations
  - `FilterExpression` dataclass
  - `parse_filter_dict()` - Parse user-friendly filter syntax
  - `to_pyarrow_filter()` - Convert to PyArrow format
  - `prune_files_by_bounds()` - File pruning logic

### Changed

- **DataFileManager.write_data_file()** now computes column bounds (min/max)
- **Manifest files** now store `lower_bounds` and `upper_bounds` for each data file
- **pyproject.toml** updated with C901 complexity ignore for filter functions

### Technical Details

**Filter Syntax:**
```python
{"column": value}                    # column == value
{"column": ("==", value)}            # column == value
{"column": (">", value)}             # column > value
{"column": ("in", [v1, v2])}         # column in [v1, v2]
{"column": ("between", (lo, hi))}    # lo <= column <= hi
```

**Column Bounds Storage:**
- Computed using `pyarrow.compute.min()` and `pyarrow.compute.max()`
- Stored in manifest JSON as `lower_bounds` and `upper_bounds` dicts
- Keys are field IDs (integers), values are the min/max values

**Performance Impact:**
| Feature | Improvement |
|---------|-------------|
| Predicate Pushdown | 90%+ I/O reduction for selective queries |
| Partition Pruning | 99% file reduction for time-range queries |
| Parallel Reading | 2-4x speedup on multi-core systems |
| Streaming API | Constant memory for any table size |

## [0.2.4] - 2025-11-17

### Fixed

#### Critical Local Filesystem Data Write Bugs 🔧
- **Fixed missing directory creation in DataFileWriter** (`data_operations.py:196`)
  - Previously, temporary parquet file creation would fail with `FileNotFoundError` if the target directory didn't exist
  - Now ensures parent directory exists with `os.makedirs(temp_dir, exist_ok=True)` before creating temporary files
  - This fix prevents file write failures when writing data to newly created tables

- **Fixed relative path handling for local filesystem** (`data_operations.py:294-298`)
  - Previously, file paths were not converted to absolute paths, causing files to be written relative to current working directory
  - Now correctly converts relative paths to absolute paths by joining with `storage.base_path`
  - Also fixed file size check to use the absolute `arrow_path` instead of relative `file_path`
  - This ensures parquet files are written to the correct table directory

- **Impact**: Without these fixes, `table.append_records()` would fail for local filesystem tables
  - ❌ Before: `table.append_records(data, schema)` raised `FileNotFoundError`
  - ✅ After: Data is correctly written to `{table_path}/data/*.parquet`

### Changed

- **Path handling** in `data_operations.py`
  - `_get_arrow_path()` now handles relative paths for local filesystem by joining with base path
  - File size retrieval now uses correct absolute path for local files

## [0.2.3] - 2025-11-14

### Fixed

#### Critical S3 Table Path Bug 🔧
- **Fixed `create_storage_backend()` ignoring table_path for S3 storage**
  - Previously, all S3 tables were created at the bucket root regardless of the `table_path` parameter
  - Now correctly combines `DATASHARD_S3_PREFIX` environment variable with `table_path`
  - Example: `create_table("logs/workflows", schema)` now creates `s3://bucket/logs/workflows/` instead of `s3://bucket/`
  - This fix is critical for multi-table applications using S3 storage

- **Impact**: Without this fix, multiple tables would overwrite each other's data in S3
  - ❌ Before: `create_table("table1", schema)` and `create_table("table2", schema)` both wrote to `s3://bucket/`
  - ✅ After: Tables correctly write to `s3://bucket/table1/` and `s3://bucket/table2/`

### Changed

- **S3 prefix handling** in `storage_backend.py`
  - `create_storage_backend()` now constructs full S3 prefix from both environment variable and table path
  - Logic: `full_prefix = f"{env_prefix}/{table_path}"` (with proper path normalization)
  - Maintains backward compatibility: empty prefixes handled correctly

## [0.2.2] - 2025-01-14

### Added

#### S3-Compatible Storage Support 🎯
- **Complete S3 backend implementation** for distributed workflows
  - AWS S3, MinIO, DigitalOcean Spaces, Wasabi support
  - Environment variable configuration (`DATASHARD_STORAGE_TYPE=s3`)
  - Transparent API - same code works for local and S3 storage

- **Storage Backend Abstraction** (`storage_backend.py`)
  - `StorageBackend` abstract base class
  - `LocalStorageBackend` for filesystem operations
  - `S3StorageBackend` for S3-compatible storage
  - `create_storage_backend()` factory function

- **PyArrow S3FileSystem Integration**
  - Native S3 support for Parquet files
  - Efficient columnar data I/O
  - Compression support maintained

- **Dual-API Architecture**
  - boto3 S3 client for metadata (JSON files, manifests)
  - PyArrow S3FileSystem for data (Parquet files)
  - Coordinated through unified StorageBackend interface

- **Comprehensive Documentation**
  - New `docs/S3_STORAGE.md` - Complete S3 usage guide
  - Updated `README.md` with S3 examples
  - Configuration guide for AWS, MinIO, and other providers
  - Performance benchmarks and cost analysis
  - Security best practices

- **S3 Integration Tests**
  - Full test suite with MinIO
  - Table creation, read/write operations
  - Multiple concurrent transactions
  - Cross-region scenario testing

#### Dependencies
- Added `boto3>=1.26.0` as optional dependency
  - Install with `pip install datashard[s3]`
  - Included in dev dependencies

### Changed

- **All Core Components Updated for S3**
  - `file_manager.py` - Uses storage backend for all file operations
  - `data_operations.py` - S3-aware Parquet I/O with PyArrow
  - `metadata_manager.py` - Storage backend for JSON metadata
  - `iceberg.py` - Storage backend creation in table operations
  - `transaction.py` - Fixed path handling for S3 compatibility

- **Path Handling**
  - Normalized path operations to work with both local and S3
  - Fixed directory existence checks for S3 prefix-based approach
  - Updated file path construction for cross-platform compatibility

### Fixed

- S3 "directory" existence checks now properly query object prefixes
- Path separators normalized for S3 compatibility
- PyArrow S3 paths include bucket prefix for correct routing

### Technical Details

**Storage Backend Interface:**
- `read_file(path) -> bytes`
- `write_file(path, content)`
- `read_json(path) -> dict`
- `write_json(path, data)`
- `exists(path) -> bool`
- `list_files(prefix) -> List[str]`
- `delete_file(path)`
- `makedirs(path, exist_ok)`
- `get_size(path) -> int`

**Environment Variables:**
```bash
DATASHARD_STORAGE_TYPE=s3          # Enable S3 backend
DATASHARD_S3_ENDPOINT=<url>        # S3 endpoint URL
DATASHARD_S3_ACCESS_KEY=<key>      # Access key ID
DATASHARD_S3_SECRET_KEY=<secret>   # Secret access key
DATASHARD_S3_BUCKET=<bucket>       # Bucket name
DATASHARD_S3_REGION=<region>       # AWS region
DATASHARD_S3_PREFIX=<prefix>       # Optional prefix
```

## [0.2.1] - 2025-01-13

### Added
- Workflow execution logging integration
- Comprehensive documentation

### Changed
- Improved pandas integration
- Enhanced schema validation

## [0.2.0] - 2025-01-12

### Added
- Initial public release
- ACID transactions
- Time travel queries
- Safe concurrent writes
- pandas integration
- Optimistic Concurrency Control (OCC)

### Core Features
- Apache Iceberg-inspired architecture
- Pure Python implementation
- No Java dependencies
- Local filesystem storage

---

[0.3.3]: https://github.com/rodmena-limited/datashard/compare/v0.2.4...v0.3.3
[0.2.4]: https://github.com/rodmena-limited/datashard/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/rodmena-limited/datashard/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/rodmena-limited/datashard/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/rodmena-limited/datashard/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/rodmena-limited/datashard/releases/tag/v0.2.0
