# #85 — [0.10.0] Iceberg v2 Avro manifests and manifest lists with field-ids, binary bounds and datashard_sha256 extras

EARS SPEC:
- Manifests shall use the exact Iceberg v2 manifest_entry schema (field-ids 0,1,3,4,2; data_file 134,100,101,102,103,104,108,109,110,137,125,128,131,132,135,140) plus datashard_sha256 (string, field-id 9001) for the data-file checksum; manifest lists the manifest_file schema (500,501,502,517,515,516,503,504,505,506,512,513,514,507,519) plus datashard_sha256 (field-id 9002).

Design input: SPECS/83-iceberg-v2-spike.md; plan /home/farshid/.claude/plans/ok-let-s-plan-for-greedy-rainbow.md (0.10.0).

## Outcome
(filled at release)
