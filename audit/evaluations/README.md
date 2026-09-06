# datashard audit probes (issuedb #55)

Live-reproduction harness from the 2026-09-06 adversarial audit. Every probe drives
datashard through its public API (`create_table`, `load_table`, `Table`,
`Transaction`, `garbage_collect`) or its storage backends against a real
counterparty (local filesystem, a moto S3 server, or the configured OVH endpoint).

Convention: `PASS <claim>` = the claim holds; `FAIL <claim>` = the defect reproduces;
`SKIP` = the environment cannot exercise it. Each probe exits non-zero on any FAIL.
After a fix lands, its probe must PASS; a probe that FAILs again reopens the finding.

```
./run_all.sh                       # safe set: tmp dirs + local moto server (auto-started)
AUDIT_ALLOW_EXTERNAL=1 ./run_all.sh   # also the probe_external_* probes (real endpoint)
PYTHON=/path/to/python ./run_all.sh   # default: ../../.venv/bin/python
```

External probes read `DATASHARD_S3_*` from the environment (falling back to the
repo's `.env`), write only under a unique `audit-probe-<uuid>/` prefix and delete it.
Tunables: `AUDIT_MOTO_PORT` (5599), `AUDIT_PROCS`, `AUDIT_APPENDS`, `AUDIT_COMMITS`.

| Probe | Claim falsified | Ticket |
|---|---|---|
| probe_gc_table_path_prefix_collision | GC never deletes live data, whatever the table is called | see SPECS/*gc-table-path* |
| probe_gc_race_commit_during_slow_reachability | GC never deletes a file committed while GC runs | SPECS/*gc-race* |
| probe_truncated_manifest_silent_partial | corrupt manifests are detected, not read as shorter lists | SPECS/*truncated-manifest* |
| probe_s3_polling_lock_lost_commit | a commit that returned True is visible (polling vs CAS lock) | SPECS/*polling-lock* |
| probe_external_ovh_conditional_writes | provider capability facts (If-None-Match / If-Match) | SPECS/*polling-lock* |
| probe_external_ovh_cas_commit_e2e | multi-writer commits with CAS on the real endpoint | SPECS/*polling-lock* |
| probe_hint_recovery_picks_uncommitted_metadata | hint recovery selects the committed version | SPECS/*hint-recovery* |
| probe_delete_files_silent_noop | no public API silently no-ops | SPECS/*delete-files* |
| probe_schema_field_order_bricks_scan | divergent schemas are rejected before they brick scans | SPECS/*schema-field-order* |
| probe_s3_gc_prefix_overreach | GC touches only datashard's own prefixes | SPECS/*s3-gc-prefix* |
| probe_create_table_conflicting_schema | conflicting schema raises or warns | SPECS/*create-table* |
| probe_verify_checksums_defeats_projection | projection / pushdown / streaming with default settings | SPECS/*checksum-verification* |
| probe_s3_request_count, probe_s3_missing_key_latency | S3 request budget, fast 404s | SPECS/*s3-request-amplification* |
| probe_metadata_growth, probe_snapshot_retention_and_expiry | bounded metadata, reclaimable files, compaction | SPECS/*unbounded-metadata* |
| probe_row_group_layout, probe_append_pandas_path | write-path layout and pandas path | SPECS/*write-path* |
| probe_disk_threshold_blocks_writes_with_free_space | disk guard uses free bytes | SPECS/*disk-threshold* |
| probe_house_rules_and_claims, probe_load_table_side_effects | house rules, doc claims | SPECS/*house-rules*, SPECS/*docs-overclaim* |
| probe_types_decimal_timestamptz | decimal / timestamptz | SPECS/*types-decimal* |
| probe_multiprocess_local_contention | local multi-process OCC (PASSES on 0.7.2) | - |
| probe_s3_cas_lock_lease_both_directions | CAS lock blocks and releases (PASSES on 0.7.2) | - |
