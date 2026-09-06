# Issue #63 — [P1] S3 GC deletes sibling-prefix objects: list_files('data') matches '<table>/data_export/...' (no '/' delimiter)

Found by adversarial audit #55 (2026-09-06). Priority: high. Probe convention: PASS = claim holds, FAIL = defect reproduced.

EARS SPEC:
- S3StorageBackend.list_files(prefix) shall list exactly the keys under prefix.rstrip('/') + '/' and shall never return keys of sibling prefixes.
- The same delimiter rule shall apply to exists() for directory-like paths and to MetadataManager._recover_version_from_files.
- Verification: audit/evaluations/probe_s3_gc_prefix_overreach.py shall PASS.

EVIDENCE (CONFIRMED live, moto): objects '<table>/data_export/report.csv' and '<table>/metadata/manifests_archive/old.avro' were deleted by garbage_collect(): gc_stats data_files=1 manifest_files=1. Root cause: storage_backend.py:756-786 list_files paginates Prefix=<prefix>/<path> without a trailing '/'.
