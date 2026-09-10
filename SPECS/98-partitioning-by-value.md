# #98 — Partitioning by value, pruning and compaction (0.11.0)

Requested by the crypto-trader platform, whose 429-table lake is partitioned by hour.

## Requirements (EARS)

- `create_table(partition_spec=...)` **shall** persist a spec and the writer **shall** apply it:
  rows grouped by partition tuple, one data file per partition per commit, under
  `data/<name>=<value>/`, with the partition struct (and the spec's field-ids) in the manifest and
  per-field summaries in the manifest list.
- Supported transforms **shall** be `identity`, `year`, `month`, `day`, `hour`, `bucket[N]` and
  `truncate[W]`, computed exactly as Iceberg defines them. Anything else **shall** be refused at
  create, never accepted and ignored.
- Filters **shall** prune whole partitions before column bounds are consulted, and a filtered scan
  **shall** return exactly what the same filter returns from an unpartitioned table.
- `Table.rewrite_data_files()` **shall** merge small files **within a partition only** and commit a
  single `replace` snapshot.
- Verification **shall** compare against DuckDB and pyiceberg (values, not counts), **shall** check
  each transform against pyiceberg's own implementation, **shall** include a negative control
  proving the readers consume our partition values, and **shall** run on a real S3 bucket.

## What the checks caught

Comparing each transform against **pyiceberg's implementation** rather than this repository's
reading of the spec found a real defect on the first run: decimal `truncate[W]` used the column's
declared scale where Iceberg uses the value's own, so a value like `1.5` in a `decimal(18,8)`
column came back unchanged whenever the trailing zeros made the unscaled value divisible by W.
289 value comparisons now cover bucket, temporal and truncate.

DuckDB additionally rejected the first partitioned manifests outright — `Manifest has 0
'field_summary'` — because the manifest list carried no per-field partition summaries. pyiceberg
read them happily, so a single-reader check would have shipped that.

## Measured

- 8 layouts (identity, year, month, day, hour, bucket[4], bucket[8], truncate+day), 18 rows each,
  two commits: DuckDB and pyiceberg return the same rows as `to_arrow()` for every one.
- Compaction: 24 files → 3, 40,816 → 5,205 bytes, rows unchanged, snapshot `replace`, both foreign
  readers unchanged.
- Pruning: an equality filter on the partition column opens 1 file of 3; a filter on a
  non-partition column opens all 3.
- Real OVH S3: a two-field spec (identity + day), 30 rows, pruning, compaction 4 → 2 files, and
  DuckDB reading the partitioned table over httpfs.

## Deliberately not done

- **Spec evolution** (`update_spec`): a table's spec is fixed at create. Changing it means
  multiple specs live in one table, each manifest bound to the spec it was written with; that is
  its own release.
- **Schema evolution** moved to 0.12 so partitioning and compaction could ship together, as the
  approved plan requires — partitioning alone multiplies small files.

## Outcome

**Shipped in 0.11.0 (2026-09-10):** https://pypi.org/project/datashard/0.11.0/ · tag v0.11.0.

Exercised: 362 unit tests (79 in the partitioning suite, including 289 transform comparisons
against pyiceberg's own implementations); 38/38 probes; the built wheel; and the served wheel from
PyPI against a real OVH bucket - a two-field spec, pruning, compaction 4 to 2 files, and DuckDB
reading the partitioned table over httpfs.

A pre-release adversarial review, asked for before this went to a live trading desk, found six
further defects - two of which abort a reader's process (DuckDB SIGABRTs on a decimal partition
value and on `local-timestamp-micros`), one that made pruning silently ineffective for `day` and
timestamp partitions, and one that could have merged two partitions into one file. All fixed, each
with a regression test, and recorded in the CHANGELOG.

Not exercised: Spark and Trino (only DuckDB and pyiceberg were run); spec evolution, which is
deliberately not implemented; a partitioned table larger than a few thousand rows.
