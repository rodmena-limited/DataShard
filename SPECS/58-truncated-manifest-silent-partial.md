# Issue #58 — [P0 data loss] Truncated manifest read as a shorter list: partial scans, wrong row_count, GC deletes the files that fell off

Found by adversarial audit #55 (2026-09-06). Priority: critical. Probe convention: PASS = claim holds, FAIL = defect reproduced.

EARS SPEC:
- When reading a manifest, FileManager shall verify that the bytes consumed equal the manifest_length recorded in the manifest list, and shall raise CorruptDataError (never return a partial list) on mismatch or on an Avro block ending at EOF.
- The manifest list shall record a sha256 per manifest and the snapshot summary shall record the manifest list's length and sha256; readers shall verify both before trusting the entry list.
- If any reachable manifest or manifest list fails verification, then garbage_collect shall abort without deleting anything, and scan()/row_count() shall raise.
- Verification: audit/evaluations/probe_truncated_manifest_silent_partial.py shall PASS.

EVIDENCE (CONFIRMED live): a 3000-entry manifest (340788 B) truncated at its first Avro block boundary (17395 B) read back as 142 entries with no error; row_count() reported 1420 instead of 30000; garbage_collect() then deleted 2858 of 3000 live data files. Multi-block manifests are produced by append_files() with many files and by delete rewrites, i.e. exactly the large tables of a data lake. Manifests, manifest lists and metadata carry no integrity check today although manifest_length is already recorded.

TECHNICAL PROBLEMS: 1. Integrity of variable-length container files whose parser stops cleanly at EOF.
SOLUTION DOMAINS: Iceberg manifest_length field (already stored); content hashing (integrity.py IntegrityChecker already exists for data files).
ALTERNATIVES: rely on fastavro errors [REJECTED: block-boundary truncation is a clean EOF]; length check only [REJECTED: same-length corruption invisible]; length + sha256 for manifests and lists [CHOSEN: one hash per manifest read, negligible next to S3 latency].
