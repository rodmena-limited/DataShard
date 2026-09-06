# #89 — [0.10.0] datashard migrate CLI: legacy layout to Iceberg v2 in one commit, no downgrade; GC understands both layouts

EARS SPEC:
- 'datashard migrate <table> [--dry-run]' (and Table.migrate_to_iceberg()) shall, under the table lock, rewrite every snapshot's manifest list and manifests into the Iceberg form (same snapshot ids, sequence numbers, timestamps; bounds converted only where the encoding is exact; data files untouched), write v{N+1}.metadata.json exclusively, write the hint, then rename the legacy root hint to metadata.version-hint.text.migrated so 0.8.x/0.9.x fail closed.

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
