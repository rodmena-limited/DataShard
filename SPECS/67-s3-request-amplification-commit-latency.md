# Issue #67 — [P2 performance] ~37 S3 calls per single-row append, 404s retried for 3.1 s; 5.4 s per commit on OVH (README claims ~50 ms)

Found by adversarial audit #55 (2026-09-06). Priority: medium. Probe convention: PASS = claim holds, FAIL = defect reproduced.

EARS SPEC:
- A single-row append_records() on S3 shall issue at most 12 requests from datashard's clients (currently 37 boto3 calls + pyarrow's PUTs), measured by audit/evaluations/probe_s3_request_count.py.
- The MetadataManager shall read the current metadata at most once per commit attempt (cached within the transaction) and shall not precede reads with exists() HEAD calls.
- FileNotFoundError / 404 shall not be retried (S3 is strongly consistent); a missing object shall surface in < 500 ms (currently 3.1 s: s3_consistency.py RETRYABLE_EXCEPTIONS includes OSError).
- Data files shall be written through the backend's own boto3 client with the checksum computed before upload (no read-back download, one S3 client instead of two); in-flight markers shall be removed with one batched DeleteObjects.
- Quantified targets on the OVH endpoint: single-writer single-row commit p50 < 1.5 s (measured 5.4 s); 4-writer p50 < 4 s (measured 10.3 s, max 53 s); current_snapshot() <= 2 calls (measured 4).

EVIDENCE (CONFIRMED live): moto: append = 37 calls {HeadObject 12, GetObject 13, PutObject 8, DeleteObject 4}; scan of 5 files = 27 calls; garbage_collect on 5 files = 37 calls; get_size/read_file/open_parquet_source of a missing key = 3.12-3.13 s each. OVH (real): 1 proc x 5 appends: p50 5.41 s, max 5.82 s; 4 procs x 5 appends: p50 10.34 s, max 53.24 s; row_count on 5 files 0.94 s.

ALTERNATIVES: parallel/async requests [DEFERRED]; remove redundant round trips first [CHOSEN].
