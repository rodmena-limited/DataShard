"""Claim: GC only touches datashard's own data/ and metadata/manifests/ objects.

Suspect: S3StorageBackend.list_files('data') lists the key prefix '<table>/data' with no
trailing slash, so sibling keys such as '<table>/data_export/...' are listed, look
unreachable, and are deleted.
"""
import time
import uuid

import _harness as H

H.quiet_logs()
bucket = f"audit-overreach-{uuid.uuid4().hex[:8]}"
s3 = H.s3_env(bucket)
if s3 is None:
    H.skip("s3-gc-prefix-overreach", "moto_server unavailable")
    H.finish()
from datashard import create_table  # noqa: E402

t = create_table("ovr", H.simple_schema())
t.append_records([{"id": 1, "name": "a", "value": 1.0}], H.simple_schema())
for k in ("ovr/data_export/report.csv", "ovr/metadata/manifests_archive/old.avro"):
    s3.put_object(Bucket=bucket, Key=k, Body=b"operator file")
time.sleep(1.1)
stats = t.garbage_collect(grace_period_ms=0, allow_short_grace=True)
left = [k for k in ("ovr/data_export/report.csv", "ovr/metadata/manifests_archive/old.avro")
        if H.count_s3_keys(s3, bucket, k) == 1]
H.report("gc-does-not-delete-sibling-prefix-objects", len(left) == 2, f"survivors={left} gc_stats={stats}")
H.finish()
