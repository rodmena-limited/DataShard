# Issue #68 — [P2 performance] Unbounded metadata: linear per-commit metadata, never-reclaimed metadata files, no manifest compaction, retention property unreachable

Found by adversarial audit #55 (2026-09-06). Priority: medium. Probe convention: PASS = claim holds, FAIL = defect reproduced.

EARS SPEC:
- garbage_collect() shall delete superseded v*.metadata.json files that are not the current version, not referenced by the current metadata_log, and older than the grace period.
- Table shall expose set_properties() so operators can set datashard.snapshot.retention-count / write.metadata.previous-versions-max; a documented default snapshot-retention policy (Iceberg-style max snapshot age, e.g. 5 days) shall be applied by an explicit maintenance call, never silently.
- When the current snapshot's manifest count exceeds a threshold (default 64), the next commit or Table.compact_manifests() shall rewrite small manifests into one so scan I/O is O(data files), not O(commits).
- Quantified: at 300 single-row commits, scan() shall issue < 100 storage calls (currently 905) and metadata/ shall hold < 1 MB of v*.metadata.json (currently 21.9 MB); current metadata size shall not grow with commits once retention applies.
- Verification: probe_metadata_growth.py and probe_snapshot_retention_and_expiry.py shall PASS.

EVIDENCE (CONFIRMED live): after 50/150/300 commits: current metadata 28/76/141 KiB (linear; ~46 MiB at 100k commits, re-read 4+ times per commit); 51/151/301 metadata files totalling 0.73/5.98/21.9 MiB (quadratic; ~2.4 TiB at 100k commits) and garbage_collect() removes none; 100/300/600 manifests; scan 155/455/905 storage calls (3 per commit ever made). The only retention lever (snapshot_manager.py SNAPSHOT_RETENTION_PROPERTY) is a table property no public API can set. expire_snapshots()+GC do work when called.
