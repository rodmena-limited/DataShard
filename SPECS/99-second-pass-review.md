# #99 — Second-pass review of 0.11.0 (0.11.1)

Requested before the library settles for a couple of months ahead of a 1.0. The first pass (before
0.11.0 published) found six defects including two that abort a reader's process; this one went
after what that pass did not cover: many partitions in one commit, concurrency around compaction,
and pathological partition values.

## Requirements (EARS)

- A commit that writes N partition files **shall** register their GC-protection markers in ONE
  batched write, not one per partition.
- **When** a single commit spreads across many partitions (default 100), datashard **shall** log a
  WARNING naming the count, the spec, and the remedy.
- `identity` partitioning on a `float` or `double` column **shall** be refused.
- **When** a rewrite's inputs are removed between planning and committing, `rewrite_data_files`
  **shall** say that nothing changed, no rows were lost, and a re-run will work.
- A partitioned table's own Hive-style directories **shall not** leak into the data it returns.

## Measured on 0.11.0, before the fixes

| | before | after |
|---|---|---|
| marker registrations for a 200-partition commit | 201 | 2 batched |
| files for 5 NaN rows (double identity partition) | 5 | refused at create |
| losing rewrite's error | `FileNotFoundError: ... not part of the current snapshot` | names the cause and the remedy |

At OVH's ~190 ms per request, 200 sequential marker writes is ~38 s of round trips before the
first data file is written.

## Checked and found sound (no change needed)

- **A rewrite racing an append**: both committed, every row present, no duplicates, `verify()`
  green.
- **Two rewrites racing**: one won, the loser failed loudly with the table intact — only the
  message needed work.
- **Partition value round trip and path escaping**: already covered by the first pass's fixes.

## Outcome

**Shipped in 0.11.1 (2026-09-10):** https://pypi.org/project/datashard/0.11.1/ · tag v0.11.1.

Exercised: 367 unit tests; 38/38 probes; the built wheel (one marker batch for 120 partitions, the
float refusal, the many-partition warning, DuckDB agreeing); and the served wheel from PyPI against
a real OVH bucket - a two-field spec, 48 rows, pruning, compaction 12 to 6 files, and DuckDB
reading it over httpfs.

Not exercised: Spark and Trino; a partitioned table beyond a few thousand rows; the many-partition
warning threshold against a real high-cardinality production spec.
