# Issue #72 — [P3 docs] Documentation overclaims: time-travel READS, Avro/ORC formats, ~50 ms S3 writes, partition pruning, Iceberg interoperability; load_table creates directories

Found by adversarial audit #55 (2026-09-06). Priority: low. Probe convention: PASS = claim holds, FAIL = defect reproduced.

EARS SPEC:
- Docs shall not claim capabilities scan() lacks: either implement scan(snapshot_id=...)/scan(as_of_ms=...) or rewrite docs/time_travel.rst and the README ('Query data as it existed at any point in time') to say only snapshot METADATA is retrievable.
- docs/performance.rst shall drop Avro/ORC as supported data-file formats (DataFileWriter raises 'Unsupported file format') or the formats shall be implemented.
- The README performance table shall carry measured numbers (OVH: 5.4 s per single-row commit, 0.9 s row_count on 5 files) and drop '~50ms'.
- 'Partition Pruning' shall be described as column-statistics pruning (partition_values are stored but never used for pruning).
- The README shall state that datashard tables are NOT readable by Apache Iceberg engines (snake_case metadata keys, custom Avro manifest schemas).
- load_table() shall not create data/, metadata/, metadata/manifests/ on a failed open (move makedirs into initialize_table).
- Verification: probe_house_rules_and_claims.py doc lines and probe_load_table_side_effects.py shall PASS.
