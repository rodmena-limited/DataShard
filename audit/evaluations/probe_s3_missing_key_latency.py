"""Claim (s3_consistency.py): retries exist for 'eventual consistency'; S3 has been
strongly consistent since 2020.

Suspect: FileNotFoundError is an OSError and OSError is in RETRYABLE_EXCEPTIONS, so a
genuinely missing object costs 5 retries with exponential backoff (~3 s) before the
error surfaces - on every get_size / mtime / open_seekable of a missing key.
"""
import time

import _harness as H

H.quiet_logs()
bucket = "audit-missing"
s3 = H.s3_env(bucket)
if s3 is None:
    H.skip("s3-missing-key-latency", "moto_server unavailable")
    H.finish()
from datashard import create_table  # noqa: E402

t = create_table("misstable", H.simple_schema())
for name, fn in (
    ("get_size", lambda: t.storage.get_size("data/nope.parquet")),
    ("open_parquet_source", lambda: t.file_manager.data_file_manager.open_parquet_source("/data/nope.parquet")),
    ("read_file", lambda: t.storage.read_file("data/nope.parquet")),
):
    t0 = time.perf_counter()
    try:
        fn()
        outcome = "no error"
    except FileNotFoundError:
        outcome = "FileNotFoundError"
    dt = time.perf_counter() - t0
    H.report(f"missing-key-{name}-fails-fast", dt < 0.5, f"{outcome} after {dt:.2f} s")
H.finish()
