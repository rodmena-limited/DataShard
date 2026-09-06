# #84 — [0.10.0] Iceberg v2 metadata.json serialisation (kebab-case, schemas/specs/orders/snapshots/refs) with legacy reader

EARS SPEC:
- metadata_serde shall emit Iceberg v2 JSON: format-version 2, table-uuid, location (URI), last-sequence-number, last-updated-ms, last-column-id, current-schema-id, schemas[{type:struct,schema-id,fields[{id,name,required,type}]}], default-spec-id, partition-specs, last-partition-id, default-sort-order-id, sort-orders, properties, current-snapshot-id, snapshots[{snapshot-id,parent-snapshot-id,sequence-number,timestamp-ms,manifest-list,summary{operation,...},schema-id}], snapshot-log, metadata-log, refs{main}.

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
