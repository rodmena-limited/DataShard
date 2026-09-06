# Issue #59 — [P0 lost commit] Polling S3 lock admits two holders -> a commit that returned True is lost; production runs it on OVH, which DOES support conditional writes

Found by adversarial audit #55 (2026-09-06). Priority: critical. Probe convention: PASS = claim holds, FAIL = defect reproduced.

EARS SPEC:
- When DATASHARD_S3_USE_CONDITIONAL_WRITES is unset, S3StorageBackend shall probe the provider at initialisation with a conditional PUT (If-None-Match:* on a per-table probe key) and shall enable CAS when the provider answers 412 on the second write.
- If the provider does not honour conditional writes and DATASHARD_S3_ALLOW_UNSAFE_LOCK is not "1", then S3StorageBackend shall refuse to construct (fail closed) with a message naming the risk: two concurrent writers can both commit and one snapshot is silently lost.
- While the polling provider is explicitly forced, every commit shall log a WARNING and the documentation shall state the residual risk.
- The comments/docs stating that OVH Object Storage lacks If-None-Match (storage_backend.py, lock_provider.py, docs/S3_STORAGE.md, .env) shall be corrected: verified FALSE against s3.uk.io.cloud.ovh.net on 2026-09-06.
- Operations (immediate, no code change): production deployments shall set DATASHARD_S3_USE_CONDITIONAL_WRITES=true.
- Verification: probe_s3_polling_lock_lost_commit.py PASS (moto); probe_external_ovh_conditional_writes.py and probe_external_ovh_cas_commit_e2e.py PASS (real endpoint, AUDIT_ALLOW_EXTERNAL=1).

EVIDENCE (CONFIRMED live):
- Lock level: two S3PollingLockProviders both acquired when one PUT landed after the other's read-back (a few hundred ms of latency on one request).
- Commit level (real Transaction/MetadataManager code, timing injected via a client proxy): A and B both returned True from append_records(); rows visible 2 of 3; B's data file orphaned (GC would delete it). The identical orchestration with conditional writes ON kept all 3 rows.
- Real counterparty: PUT If-None-Match:* on an existing key -> 412 PreconditionFailed; PUT If-Match:<current etag> -> 200; PUT If-Match:<stale> -> 412. End to end with CAS ON: 4 processes x 5 appends -> 20/20 rows, 20 snapshots, 0 errors.
- Production .env: DATASHARD_S3_USE_CONDITIONAL_WRITES=false (the unsafe provider) on that very endpoint.

TECHNICAL PROBLEMS: 1. Mutual exclusion on an object store without a coordination service; 2. Provider capability detection (belief encoded in a comment, never tested against the provider - the 'mocked counterparty' failure class).
SOLUTION DOMAINS: S3 conditional writes (AWS API: If-None-Match / If-Match on PutObject); Iceberg catalogs (external CAS) for providers without it.
ALTERNATIVES: keep polling default + warning [REJECTED: the warning was ignored in production and data loss is silent]; external lock service (PostgreSQL advisory lock / Redis) for non-CAS providers [DEFERRED: only needed for providers that truly lack CAS]; CAS auto-detect + fail closed [CHOSEN].
