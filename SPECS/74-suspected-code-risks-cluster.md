# Issue #74 — [P3 SUSPECTED] Unreproduced code-level risks: partial os.write, same-ms OCC window, stacked retry layers, temp/multipart leftovers, retry-orphan manifests

Found by adversarial audit #55 (2026-09-06). Priority: low. Probe convention: PASS = claim holds, FAIL = defect reproduced.

Not reproduced live; each item shall get a probe before it is fixed. EARS SPEC:
- LocalStorageBackend.write_file shall write all bytes (loop on os.write or use a file object) and verify the written length before os.replace (storage_backend.py:269 ignores the return value; a short write yields a truncated metadata file that passes fsync+rename).
- The OCC check shall compare a commit identity that cannot collide within one millisecond (the previous metadata file name / a commit uuid) in addition to last_updated_ms, so two metadata-only commits (expire_snapshots) in the same ms cannot both pass (metadata_manager.py:168-180).
- The boto3 client shall be created with a bounded retry config coordinated with with_s3_retry (today botocore's default 4 attempts x datashard's 6 = up to 24 attempts per operation).
- garbage_collect shall reclaim '.tmp.*' leftovers under metadata/ and abort incomplete multipart uploads left by pyarrow's S3 writer after a crash; OCC retries shall not leave orphan manifests/manifest lists for every failed attempt (or GC shall be documented as their cleanup).
