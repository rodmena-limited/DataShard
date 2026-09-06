# Issue #66 — [P2 performance] Default checksum verification reads whole files: projection, predicate pushdown and streaming degrade to full-file reads

Found by adversarial audit #55 (2026-09-06). Priority: medium. Probe convention: PASS = claim holds, FAIL = defect reproduced.

EARS SPEC:
- In the default integrity mode, scan(columns=...) shall read at most the projected columns' byte ranges plus footer: a 1-column projection over a 32 MB, 6-file table shall read < 15% of the table bytes (currently 100%).
- The default integrity mode shall use parquet page-level CRCs (write with write_page_checksum=True, read with page_checksum_verification=True); whole-file sha256 verification shall be an explicit mode (verify_checksums='full').
- scan_batches()/iter_records()/iter_pandas() shall never materialise a whole data file in memory in the default mode.
- Verification: audit/evaluations/probe_verify_checksums_defeats_projection.py shall PASS (its corruption-detection control must keep passing).

EVIDENCE (CONFIRMED live, local): scan(columns=['id']) read 31.9 MB of a 31.9 MB table (6 whole-file reads); point lookup after pruning read the whole 5.3 MB file (24-28 ms vs 4-8 ms with verification off); the first 1000-row scan_batches batch forced a 5.3 MB whole-file read. On S3 this is the dominant cost of every read (bytes transferred). Root cause: transaction.py:919-935 and 1137-1144 read the entire file via storage.read_file() before parsing.

TECHNICAL PROBLEMS: 1. Integrity verification granularity vs columnar access.
SOLUTION DOMAINS: Parquet page CRC32 (format feature, supported by pyarrow >= 12); Iceberg relies on parquet checksums, not whole-file hashes.
ALTERNATIVES: whole-file sha256 default [REJECTED: O(file) per read]; page CRC default + full-hash opt-in [CHOSEN]; per-column-chunk hashes in the manifest [REJECTED for now: custom format].
