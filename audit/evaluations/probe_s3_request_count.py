"""Claim (README S3 table): 'Write (1000 records) ~50ms' on S3.

Counts the S3 API calls datashard's boto3 client issues per operation against a moto
server. Budget (0.8.0, CAS backend): a single-row append <= 20 calls (was 37 + pyarrow's
own PUTs), a 5-file scan <= 15, current_snapshot() <= 2. Markers (3 PUT + 1 bulk
DELETE) and the lock (PUT / GET+DELETE) are the deliberate remainder.
"""
import collections
import time
import uuid

import _harness as H

H.quiet_logs()
bucket = "audit-reqcount"
s3 = H.s3_env(bucket, conditional=True)
if s3 is None:
    H.skip("s3-request-count", "moto_server unavailable")
    H.finish()
from datashard import create_table  # noqa: E402

schema = H.simple_schema()
counts = collections.Counter()


def hook(model, params, **kw):
    counts[model.name] += 1


t0 = time.perf_counter()
t = create_table(f"reqtable_{uuid.uuid4().hex[:8]}", schema)
t.storage.s3.meta.events.register("before-call.s3", hook)
t_create = time.perf_counter() - t0


def measure(fn):
    counts.clear()
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0, dict(counts), sum(counts.values())


dt, by_op, n_append = measure(lambda: t.append_records([{"id": 1, "name": "a", "value": 1.0}], schema))
H.report(
    "single-row-append-costs-at-most-20-s3-calls",
    n_append <= 20,
    f"{n_append} boto3 calls in {dt * 1000:.0f} ms on a local moto server: {by_op}",
)
for i in range(2, 6):
    t.append_records([{"id": i, "name": "a", "value": 1.0}], schema)
dt, by_op, n_scan = measure(lambda: t.scan())
H.report(
    "scan-of-5-files-costs-at-most-3-calls-per-file",
    n_scan <= 15,
    f"{n_scan} boto3 calls in {dt * 1000:.0f} ms: {by_op}",
)
dt, by_op, n_cur = measure(lambda: t.current_snapshot())
H.report("current_snapshot-costs-at-most-2-calls", n_cur <= 2, f"{n_cur} calls: {by_op}")
dt, by_op, n_gc = measure(lambda: t.garbage_collect())
print(f"  info: garbage_collect() on a 5-file table = {n_gc} calls in {dt * 1000:.0f} ms: {by_op}")
H.finish()
