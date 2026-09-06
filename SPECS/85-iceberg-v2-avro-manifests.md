# #85 — [0.10.0] Iceberg v2 Avro manifests and manifest lists with field-ids, binary bounds and datashard_sha256 extras

EARS SPEC:
- Manifests shall use the exact Iceberg v2 manifest_entry schema (field-ids 0,1,3,4,2; data_file 134,100,101,102,103,104,108,109,110,137,125,128,131,132,135,140) plus datashard_sha256 (string, field-id 9001) for the data-file checksum; manifest lists the manifest_file schema (500,501,502,517,515,516,503,504,505,506,512,513,514,507,519) plus datashard_sha256 (field-id 9002).

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
