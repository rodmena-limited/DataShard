# #94 — Make the per-commit metadata cost visible, and close the review's six other gaps (0.10.2)

Source: a production review by crypto-trader, running datashard since late August as the tape
store for a live market-making system — one local lake, 299 symbol-day tables, 7.8 GB, 8 streams,
~1 commit every 28 s per stream-symbol.

## Requirements (EARS)

### 1. The metadata cost model
- **When** a commit writes a metadata document larger than `datashard.metadata.warn-bytes`
  (default 1 MiB), datashard **shall** log a WARNING naming the size, the snapshot count, the cost
  model (every commit rewrites the whole document, so total metadata grows with the **square** of
  the commit count) and the two remedies (batch appends in one transaction; expire + collect). It
  **shall** warn again only after the size doubles.
- `Table.expire_snapshots()` **shall** fold manifest compaction into the **same** commit by
  default, and **shall** document what its `int` return counts.
- The README and docs **shall** state the cost model up front and signpost `new_transaction()`.

### 2. `row_count()` is not a liveness check
- `Table.verify()` **shall** exercise the real read path and return
  `{ok, snapshots, data_files, checked_files, rows, rows_read, deep, errors}`, **shall not** raise
  for a broken table, and `deep=True` **shall** additionally verify whole-file checksums.
- `row_count()`'s docstring **shall** say it is metadata-only and point at `verify()`.

### 3. The expire → collect two-step
- `garbage_collect()`'s docstring **shall** say that `expire_snapshots` frees no bytes, that this
  is the step that does, and that a file's age is its **mtime** — so a freshly copied table cannot
  be collected until the grace period passes.

### 4/5. Migration
- `migrate_table(dry_run=True)` **shall** report `metadata_bytes_now`, `metadata_bytes_added`
  (measured by encoding the new metadata in memory) and `peak_bytes`; the docs **shall** state the
  headroom requirement; every report key **shall** be documented.

### 6/7. Documentation
- The docs **shall** state that local and S3 take different read paths, and that filters remain
  **correct** without partition pruning.

## Evidence

One symbol-day of the reporter's recorder:

    *.parquet    3,027 files      9.5 MB   mean 3.3 KB   <- the data
    *.avro      12,104 files  1,508.6 MB
    *.json       3,029 files  1,962.0 MB   mean 648 KB   <- one per commit

365:1 metadata-to-data; 6.2 GB of a 7.8 GB store. During #93 a broken table reported a healthy
`row_count()`, so a health check on it read green while the table was unusable. Migration grew one
table 2.4 GB → 3.5 GB (+46 %) before GC.

## Synthesis (localised)

- **[CHOSEN]** Measure the cost where it is paid (warn on the document actually written) and make
  the remedy one call (fold compaction into expire). The user keeps control of retention.
- [REJECTED] Default snapshot retention: silently deleting history to save bytes is not the
  library's decision to make.
- [REJECTED] Documentation only: the reporter read the docs and still met this in production.

## Verification

`tests/test_operational_cost_and_health.py`: the warning carries the cost model, both remedies and
the property name, and fires at most 4 times in 40 commits; it is silent below the threshold and
with the property at `0`; expire collapses 6 manifests to 1 in one commit and `compact_manifests=False`
does not; `verify()` is green on a healthy table, reports (never raises) on a corrupted file, on a
missing file, and on a table with no metadata, while `row_count()` stays green on the same corrupted
table; `deep` and `limit` behave; the three docstrings carry their sentences; the migration dry run's
projection is within 5 % of, and never below, what the real run writes, and every documented report
key is present.

## Outcome
(filled at release)
