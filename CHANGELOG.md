# Changelog

All notable changes to DataShard will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.10.1] - 2026-09-08

### Fixed
- **A table's directory is a location, never a partition scheme (#93).** Reads inferred
  Hive-style partitioning from the filesystem path, so the physical location of a table leaked
  into its data. A table stored under a path containing a `key=value` segment was affected in
  one of two ways:
  - **the key matches a column** - every read failed with
    `ArrowTypeError: Unable to merge: Field <col> has incompatible types: string vs
    dictionary<values=string, indices=int32, ordered=0>`. Had the merge succeeded, the directory
    name would have **overwritten** the value the rows actually carry, which for the reporting
    user is a different instrument (the directory holds the exchange pair, the column holds the
    venue's pair);
  - **the key matches no column** - a column parsed from the directory name was **silently
    added** to every row, so a scan returned a column that is not in the table's schema. This
    variant produced no error and had not been noticed.

  Every parquet read now goes through one helper that turns inference off, and a test fails the
  build if a bare `pq.read_table` call is reintroduced. Affected releases: **0.8.0 through
  0.10.0** (0.8.1, 0.9.1 and 0.10.0 verified directly against the released packages); 0.7.2 is
  unaffected. Local backend only - S3 reads pass a file object, which never triggers dataset
  discovery, now pinned by a test so the two backends cannot drift.

  Reported by crypto-trader on 2026-09-08 with a self-contained reproduction, against tables
  laid out as `<root>/symbol=<pair>/day=<date>/`. **No data was ever written incorrectly**: the
  defect was entirely in the read path, and an affected table reads correctly as soon as it is
  opened by 0.10.1. Note that `row_count()` reads metadata only, so it kept returning the right
  answer throughout - a health check built on it would have reported green.

## [0.10.0] - 2026-09-06

**datashard tables are now Apache Iceberg v2 tables on disk.** DuckDB's `iceberg` extension,
pyiceberg, Spark and Trino read them natively - the lock-in is gone. **Existing tables must be
migrated once with `datashard migrate <table>`, and there is no downgrade afterwards.**

### Migration (required, one command per table)

```
datashard migrate /path/to/table            # or: python -m datashard migrate ...
datashard migrate /path/to/table --dry-run  # report what would be rewritten, write nothing
```

Migration writes one new metadata version with Iceberg manifests for the existing snapshot
chain; **data files are not rewritten or moved**. It is idempotent, takes the table lock, and
finally renames the old root `metadata.version-hint.text` to `.migrated` so 0.9.x and earlier
fail closed on the table instead of committing a divergent lineage. Verified against tables
written by the released 0.7.2 and 0.9.1: every row and snapshot preserved, and the old client
provably cannot write to a migrated table
(`audit/evaluations/probe_v0100_migration_from_released_versions.py`).

Opening an un-migrated table now raises `LegacyLayoutError` naming the command. Nothing is
written when it does.

### Added
- **Iceberg v2 on-disk format (#84, #85, #87, #88).** Kebab-case `metadata/v{N}.metadata.json`
  with `refs`, `snapshot-log`, `metadata-log` and Iceberg summaries; Avro manifests and manifest
  lists carrying the spec's field-ids; column bounds in Iceberg single-value binary form; all
  paths absolute URIs (`file:///...`, `s3://bucket/prefix/...`); `PARQUET:field_id` on every
  column datashard writes, plus `schema.name-mapping.default` so files written without field ids
  (including migrated ones) still resolve for foreign readers.
- **`datashard migrate` and `datashard relocate` CLI (#89, #87).** `Table.relocate()` rewrites a
  moved table's metadata so foreign readers work without `allow_moved_paths`; `Table.location`
  exposes the URI they should be pointed at.
- **Foreign-reader acceptance probe (#90):** `audit/evaluations/probe_v0100_foreign_readers.py`
  compares full rows - not counts - between datashard, DuckDB `iceberg_scan` and pyiceberg after
  create, appends, `delete_files`, manifest compaction, `expire_snapshots` + `garbage_collect`,
  time travel, migration, a parquet file without field ids, and S3 through DuckDB's httpfs. It
  carries a negative control that falsifies a bound and requires the foreign readers to return
  the wrong rows, so a bounds-encoding regression cannot pass it silently.

### Changed
- **The commit point is now the exclusive creation of `metadata/v{N+1}.metadata.json`** (#86):
  `If-None-Match: *` on S3, temp file + fsync + `os.link` locally. The version number is the
  commit identity, so two writers can never produce the same version even with the metadata lock
  removed - proved by a probe that disables the lock and races six processes, with a control run
  showing the same race losing updates when the exclusive create is replaced by a plain write.
  `last_commit_id` is gone, and `AmbiguousMetadataError` can no longer arise.
- **`metadata/version-hint.text` is advisory** and holds a plain integer, as Iceberg's Hadoop
  tables expect. It is written after the commit point with only-if-greater semantics; a hint that
  lags a durable commit (a writer that died between the two writes) is healed by the next reader,
  and a writer heals it through the commit conflict. A hint write failure after the commit point
  no longer fails the commit - the rows are durable.
- Manifests and manifest lists moved from `metadata/manifests/` to `metadata/` with Iceberg
  naming (`snap-<id>-1-<uuid>.avro`, `<uuid>-m0.avro`); the old directory is still read and is
  reclaimed by `garbage_collect()`.
- Snapshot summaries use Iceberg's keys (`operation`, `added-records`, `total-records`, ...);
  datashard's manifest-list integrity moved under `datashard.*` keys, and their absence now means
  "unverified", never "corrupt", so a foreign writer's snapshot is readable.
- Free-form `partition_values` passed to `append_*` are recorded as a datashard-only manifest
  field. Real Iceberg partitioning (spec-driven, with pruning) ships in 0.11; a `partition_spec`
  with fields is refused rather than silently ignored.
- A table created without a schema now adopts the first append's schema into its metadata, so
  foreign readers see the columns instead of an empty struct. If another writer persists a
  different schema first, the commit fails before the commit point instead of committing a data
  file the table's schema cannot describe.
- **`uuid` and `fixed` columns are refused for NEW tables.** datashard writes a parquet string
  for `uuid`, which pyiceberg rejects ("Cannot promote string to uuid"), and a bare `fixed` is
  not a valid Iceberg type (it needs a length). Use `string` / `binary`: the parquet bytes are
  identical, so the change is a no-op on disk. Existing tables with such columns keep working in
  datashard, and `datashard migrate` reports them in `columns_foreign_readers_may_reject`.

### Performance
| Measurement | 0.9.1 | 0.10.0 |
|---|---|---|
| single-row append (S3 calls) | 16 | 16 |
| 5-file scan (S3 calls) | 14 | 14 |
| `current_snapshot()` (S3 calls) | 2 | 3 |
| commit latency on OVH, 4 writers x 5 appends (p50) | 3.02 s | 2.54 s |

The commit is one exclusive PUT instead of a metadata write followed by a read-modify-write of
the version hint, which is where the OVH latency went. The one extra READ call is a HEAD for
`v{N+1}`: it is what stops a lagging hint from hiding committed rows. The write path does not pay
it - a commit conflict reveals the same thing - so append cost is unchanged. Measured with
`audit/evaluations/probe_s3_request_count.py` and
`audit/evaluations/probe_external_ovh_cas_commit_e2e.py` against OVH Object Storage
(s3.uk.io.cloud.ovh.net), where each request costs roughly 190 ms.

### Fixed
- Garbage collection sweeps only datashard's own manifest objects (directly under `metadata/`,
  plus the pre-0.10 `metadata/manifests/`). An operator's files parked elsewhere under
  `metadata/` are left alone.
- Column bounds of wide decimals are no longer lost or rounded. The encoder used
  `Decimal.quantize()` / `scaleb()`, which honour the decimal context's 28 significant digits, so
  every bound of a `decimal(38, s)` column beyond that width silently disappeared. Bounds are now
  computed with exact integer arithmetic. Found in the pre-release adversarial pass.
- A bound is never coerced. A value whose Python type does not match the column (a `Decimal` for a
  `long`, a `str` for an `int` - reachable through the loose type inference of pre-0.8 legacy
  bounds during migration) previously became a truncated bound, e.g. 1.5 recorded as a minimum of
  1. Foreign readers prune on those bytes, so that would have made them skip matching rows; such
  values now yield no bound at all. Found in the pre-release adversarial pass.
- A pre-0.10 table carrying a decorative partition spec now migrates (the spec is dropped and
  reported, since pre-0.10 data files were never partitioned by it) instead of failing.
- The local commit point falls back to an exclusive open on filesystems without hard links,
  with a warning that the fallback is atomic but not crash-safe.

### Known limitations
- **datashard must be the only writer** of a table until the REST catalog client (1.0). pyiceberg
  and Spark commit with their own metadata naming and do not maintain `version-hint.text`, so
  their commits are invisible to datashard and to DuckDB-by-directory, and their files would be
  reclaimed by `garbage_collect()`. Reading from any engine is fully supported.
- A table written by a merge-on-read engine (positional or equality delete files) is **refused**
  rather than read with the deletes ignored. Applying them ships in 1.0.
- Foreign readers can lag one version for a few seconds after a writer crashes between the commit
  point and the hint write, until any datashard reader or writer heals the hint.
- Iceberg's complex types (struct, list, map) remain unsupported; datashard's schema validation
  already refused them.

## [0.9.1] - 2026-09-06

### Fixed
- **Free-space floor refused every write on small volumes (#82).** The absolute floor introduced in 0.8.0 (#70)
  defaulted to 1 GiB, so a table under /tmp, in a container or on any volume with less than a gigabyte free
  failed with `Insufficient disk space` for a 2 KB metadata write. The default floor is now 64 MiB and a write
  is refused when free bytes < max(4 x write size, floor). `DATASHARD_MIN_FREE_BYTES` still overrides it.
  Found by the 0.9.0 post-publish smoke test, which failed on this exact condition.

## [0.9.0] - 2026-09-06

First release of the uplift plan: DuckDB as the analytics layer and Arrow as the
ingestion path. No format change; 0.8.x tables read unchanged.

### Added

- `Table.to_arrow(columns, filter, parallel, verify_checksums, snapshot_id)` - the
  verified read path as a `pyarrow.Table` (schema-preserving when empty).
- `Table.to_duckdb(connection=None, view_name="t", **scan_kwargs)` and
  `Table.sql(query, alias="t", **scan_kwargs)` - run DuckDB SQL over the table, results as
  Arrow; `Table.parquet_paths(snapshot_id)` and `Table.duckdb_s3_secret_sql()` for DuckDB's
  native `read_parquet` fast path (documented as bypassing page-CRC verification). New extra
  `datashard[duckdb]`.
- `Transaction.append_arrow(table)` / `Table.append_arrow(table)` - Arrow tables written in
  one pass: unknown columns refused, absent optional columns null-filled, columns reordered
  and cast to the table's types (a DuckDB result appends as is); `append_pandas` now shares
  this writer.

### Changed

- A commit's manifest and manifest-list GC markers are written in one concurrent batch on
  S3 (`StorageBackend.write_files`). S3 calls per single-row append stay at 16; OVH
  single-writer p50 3.08 s -> 3.02 s. The other calls depend on each other, so latency
  moves substantially only with the 0.10 commit-protocol change.

## [0.8.1] - 2026-09-06

Re-audit of 0.8.0 (issuedb #75): the remediation itself was put under the same
falsification pressure as the original code. Nine new probes (`audit/evaluations/probe_v080_*`);
two defects found in the new garbage-collector code, both fixed here. Verified unchanged: 0.7.2
tables read, append and garbage-collect under 0.8.x and remain readable by 0.7.2; compaction
preserves current and historical file sets; 240 local and 60 S3 commits under multi-process
contention with compaction firing lost nothing.

### Fixed

- **GC on S3 judged object age against the client clock.** A GC host running ahead of the
  object store by more than the grace period deleted a commit that landed during the run
  (its marker already removed, unreachable in GC's view) and left the table unreadable. GC
  now takes its start instant from the storage's clock (S3: the HTTP `Date` header) and
  warns when host and server disagree by more than 60 s (#77).
- **GC could reclaim the metadata file the newest version's log references** when a
  commit landed during the run: the log was taken from the view read at GC start. The
  current version, its log and `write.metadata.previous-versions-max` are re-read at reclaim
  time, and every metadata file inside that version window is kept regardless (#76).

### Changed

- GC takes object modification times from the listing (`LastModified` on S3, one stat during
  the walk locally) instead of one HEAD/stat per candidate, so a sweep over many orphans costs
  O(listing pages) (#77). New `StorageBackend.list_files_with_mtime()` and `clock_ms()`.

## [0.8.0] - 2026-09-06

Remediation of adversarial audit #55 (`AUDIT_REPORT_3.md`): five reproduced data-loss /
lost-commit paths, five correctness defects, the performance profile, and the house-rule
debt. Every finding has a probe under `audit/evaluations/` (21 of 22 failed on 0.7.2, all
pass now) and a regression test in `tests/test_audit_55_fixes.py`.

### Fixed - data loss / lost commits

- **GC deleted every live file of a table whose path is a string prefix of `data` /
  `metadata`** (a table literally called `data`): `_normalize_path` stripped `table_path`
  as a plain prefix. Paths are normalised by one rule on both sides now (#56).
- **GC deleted files committed while it was running**: markers are loaded before the
  metadata is read, the age cutoff is measured from the GC start instant, caller files
  passed to `append_files()` get in-flight markers too, and a grace period under 5 minutes
  needs `allow_short_grace=True` (#57).
- **A manifest truncated on an Avro block boundary was read as a shorter list** - partial
  scans, wrong `row_count()`, and GC deleting the files that fell off. Manifests are now
  verified against the `manifest_length` and a new sha256 recorded in the manifest list;
  the manifest list against the length/sha256 recorded in the snapshot summary. Any
  mismatch raises `CorruptDataError` and aborts GC (#58).
- **The polling S3 lock could admit two holders, losing a commit that had returned
  `True`.** The S3 backend now probes the endpoint with conditional PUTs and uses the
  compare-and-swap lock and commit point when they are honoured (AWS S3, MinIO, OVH Object
  Storage - verified live against `s3.uk.io.cloud.ovh.net`); an endpoint that ignores
  preconditions, or `DATASHARD_S3_USE_CONDITIONAL_WRITES=false`, is **refused** unless
  `DATASHARD_S3_ALLOW_UNSAFE_LOCK=1` (#59).
- **Version-hint recovery could resurrect a never-committed metadata file** left by a
  crashed or out-raced writer. A clean commit failure now deletes its own metadata file;
  a recovery with several files at the top version raises `AmbiguousMetadataError`;
  `Table.repair_version_hint(name)` resolves it explicitly (#60).

### Fixed - correctness

- `delete_files()` raises `FileNotFoundError` for a path no snapshot references and matches
  paths with or without a leading `/`; it never commits an empty `delete` snapshot (#61).
- A schema with the table's fields in a different order was accepted and bricked every
  scan (`ArrowInvalid` on concat). Files are written in the table's persisted field order,
  the arrow-schema cache is keyed by field fingerprint, and scans align column order (#62).
- S3 `list_files('data')` also listed `data_export/...`, so GC deleted sibling-prefix
  objects; listings use a `/` delimiter (#63).
- `create_table()` on an existing table with a **different** schema raises
  `SchemaMismatchError` (`if_exists="ignore"` keeps the old behaviour with a warning) (#64).
- The S3 integration tests asserted `exists("metadata")`, failed against every S3 server
  and never ran in CI. They now run against a per-session moto server (or the configured
  `DATASHARD_S3_*` endpoint), assert behaviour, and delete their prefixes (#65).
- A percentage disk guard refused all writes at 95 % used with 400 GB free; the guard is
  now an absolute floor: `max(2 x write, DATASHARD_MIN_FREE_BYTES [1 GiB])` (#70).
- **Interpreter abort at exit** (`terminate called without an active exception`) after any
  process that wrote and read parquet: pyarrow 22's threaded readers over a Python file
  object on CPython 3.13. Local reads now hand pyarrow the path; S3 reads go through the
  range reader single-threaded per file (parallelism comes from `scan(parallel=...)`).

### Changed - performance

- **Integrity mode**: `verify_checksums` is `"page"` by default - parquet page CRCs on the
  bytes a read touches - instead of a whole-file sha256 that downloaded every byte of
  every file for any projection. `"full"` keeps the old behaviour, `"off"` disables it (#66).
- **S3 round trips**: single-row append 37 -> 16 calls, 5-file scan 27 -> 13,
  `current_snapshot()` 4 -> 2; missing objects fail in milliseconds (404s are no longer
  retried for 3 s); data files are hashed before upload and stored through boto3 (pyarrow's
  S3 client is gone); the range reader learns the object size from a cached suffix-range
  read. OVH, single-row appends: 5.4 s -> 3.1 s p50 with one writer, 10.3 s -> 6.5 s p50
  with four. The remaining cost is ~16 sequential round trips at OVH's ~190 ms each: batch
  rows per commit, and use `scan(parallel=True)` for multi-file reads (#67).
- **Metadata growth**: `garbage_collect()` reclaims superseded `v*.metadata.json` files
  (current + `write.metadata.previous-versions-max`, now 10, kept) and `.tmp` leftovers;
  manifests compact automatically at `datashard.manifest.compaction-threshold` (64) so scan
  I/O stays bounded (300 tiny commits: 905 -> 51 storage calls); new
  `Table.set_properties()`, `Table.properties()`, `Table.expire_snapshots(older_than_ms,
  retain_last)` (5-day default) and `Table.compact_manifests()` (#68).
- **Write path**: one Arrow conversion and one row group per million rows (a 50k-row append
  was 50 row groups); `append_pandas()` uses `from_pandas` (was 6-11x slower via
  `to_dict('records')`); empty appends queue nothing (#69).

### Added

- Time-travel reads: `scan`, `to_pandas`, `scan_batches`, `iter_records`, `iter_pandas` and
  `row_count` accept `snapshot_id=` (#72).
- `decimal(P,S)` (exact `Decimal` values and bounds) and `timestamptz` (UTC) column types (#73).
- `TableMetadata.last_commit_id`, compared by the OCC check so two metadata-only commits in
  the same millisecond cannot both pass; a lost OCC attempt deletes the manifests it wrote;
  local writes loop until complete and verify the size (#74).
- `audit/evaluations/`: the live-probe harness (`run_all.sh`; external probes behind
  `AUDIT_ALLOW_EXTERNAL=1`).

### Changed - other

- Every module is under 500 lines: `transaction.py` split into `transaction.py`,
  `transaction_append.py`, `transaction_commit.py`, `table.py`, `table_scan.py`;
  `storage_backend.py` into `storage_backend.py`, `s3_backend.py`, `s3_range_file.py`; plus
  `data_io.py`, `arrow_types.py`, `exceptions.py`, `metadata_serde.py`, `version_hint.py`,
  `bounds.py`, `manifest_codec.py`, `lock_provider_polling.py`. Old import paths still
  resolve (#71).
- `load_table()` no longer creates `data/`, `metadata/` when the table does not exist;
  `__version__` is read from `pyproject.toml` in a development checkout (#72).
- Docs: time travel documents `snapshot_id=` reads; Avro/ORC removed as data-file formats;
  S3 locking documented; README states that tables are not readable by Iceberg engines and
  carries measured numbers instead of "~50 ms" (#72).
- `garbage_collect()` returns a fourth counter, `metadata_files`.

### Upgrade notes

- Set nothing for locking on S3 (auto-detect). If you had
  `DATASHARD_S3_USE_CONDITIONAL_WRITES=false`, remove it; keeping it now requires
  `DATASHARD_S3_ALLOW_UNSAFE_LOCK=1` and accepts possible lost commits.
- Tables written by 0.7.x read unchanged; manifests and snapshots written before 0.8.0 have
  no recorded sha256 (length is still checked) and their data files carry no page CRCs
  (page verification passes them through; use `verify_checksums="full"` for the old check).
- `garbage_collect(grace_period_ms=<5 min>)` now needs `allow_short_grace=True`.
- `delete_files()` of an unknown path and `create_table()` with a conflicting schema raise.

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
