# Issue #80 — 0.9.0: issue independent object writes of a commit concurrently on S3 (marker PUTs), measure OVH commit latency

Part of the uplift plan (0.9.0). 

EARS SPEC:
- Where the ordering invariant allows (a file must never exist before its GC marker), the commit shall issue independent object writes concurrently: both in-flight markers for the manifest and the manifest list before writing them, and the marker deletes already batched.
- The commit shall not reorder marker-before-file for data files.
- Quantified target: single-row commit p50 on OVH below 2.5 s (measured 3.1 s on 0.8.1) with probe_external_ovh_cas_commit_e2e.py; the S3 call count per append shall not increase (probe_s3_request_count.py).
- The CHANGELOG shall carry the measured before/after numbers.

Synthesis (localised): concurrency = ThreadPoolExecutor over boto3 calls (boto3 clients are thread-safe); alternative async client [REJECTED: dependency and API churn for a 1-round-trip gain].
