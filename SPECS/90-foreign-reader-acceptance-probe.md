# #90 — [0.10.0] Foreign-reader acceptance probe: DuckDB iceberg_scan and pyiceberg on fresh, mutated and migrated tables, local and S3

EARS SPEC:
- audit/evaluations/probe_v0100_foreign_readers.py shall compare full rows (not counts) between Table.to_arrow() and each of DuckDB iceberg_scan and pyiceberg StaticTable after create, append (decimal, timestamptz, strings, nulls), delete_files, compact_manifests, expire_snapshots + garbage_collect, time travel, a table migrated from 0.9.1, an append_files parquet without field ids, and S3 via moto (httpfs); a NEGATIVE case shall prove bounds are consumed (a deliberately corrupted bound changes the foreign result) so a bounds-encoding regression cannot pass silently.

Design input: SPECS/83-iceberg-v2-spike.md; plan /home/farshid/.claude/plans/ok-let-s-plan-for-greedy-rainbow.md (0.10.0).

## Outcome

**Outcome (2026-09-06):** shipped in 0.10.0 (https://pypi.org/project/datashard/0.10.0/), commit 7f9faec, tag v0.10.0.
Exercised: 252 unit tests; 36/36 probes including the external OVH set; the foreign-reader acceptance probe comparing
full rows against DuckDB 1.5.5 (iceberg extension) and pyiceberg 0.12.0 after create, appends, delete_files,
compact_manifests, expire_snapshots + garbage_collect, time travel, migration and S3 via httpfs, with a negative
control proving both readers consume our bounds; migration verified against tables written by the released 0.7.2 and
0.9.1 packages; the wheel installed back from PyPI and smoke-tested (write, read by both foreign readers, CLI migrate
of a real 0.9.1 table). Not exercised: Spark and Trino (only DuckDB and pyiceberg were run), writes from another
engine (documented as unsupported until the 1.0 catalog client), and merge-on-read delete files (refused, not read).
