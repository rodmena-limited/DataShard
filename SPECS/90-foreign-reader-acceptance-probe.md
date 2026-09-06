# #90 — [0.10.0] Foreign-reader acceptance probe: DuckDB iceberg_scan and pyiceberg on fresh, mutated and migrated tables, local and S3

EARS SPEC:
- audit/evaluations/probe_v0100_foreign_readers.py shall compare full rows (not counts) between Table.to_arrow() and each of DuckDB iceberg_scan and pyiceberg StaticTable after create, append (decimal, timestamptz, strings, nulls), delete_files, compact_manifests, expire_snapshots + garbage_collect, time travel, a table migrated from 0.9.1, an append_files parquet without field ids, and S3 via moto (httpfs); a NEGATIVE case shall prove bounds are consumed (a deliberately corrupted bound changes the foreign result) so a bounds-encoding regression cannot pass silently.

Design input: SPECS/83-iceberg-v2-spike.md; plan /home/farshid/.claude/plans/ok-let-s-plan-for-greedy-rainbow.md (0.10.0).

## Outcome
(filled at release)
