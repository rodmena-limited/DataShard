# #84 — [0.10.0] Iceberg v2 metadata.json serialisation (kebab-case, schemas/specs/orders/snapshots/refs) with legacy reader

EARS SPEC:
- metadata_serde shall emit Iceberg v2 JSON: format-version 2, table-uuid, location (URI), last-sequence-number, last-updated-ms, last-column-id, current-schema-id, schemas[{type:struct,schema-id,fields[{id,name,required,type}]}], default-spec-id, partition-specs, last-partition-id, default-sort-order-id, sort-orders, properties, current-snapshot-id, snapshots[{snapshot-id,parent-snapshot-id,sequence-number,timestamp-ms,manifest-list,summary{operation,...},schema-id}], snapshot-log, metadata-log, refs{main}.

Design input: SPECS/83-iceberg-v2-spike.md; plan /home/farshid/.claude/plans/ok-let-s-plan-for-greedy-rainbow.md (0.10.0).

## Outcome
(filled at release)
