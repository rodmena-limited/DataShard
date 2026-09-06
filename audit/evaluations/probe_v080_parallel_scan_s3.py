"""Claim (0.8.0): S3 reads are single-threaded per file, so cross-file parallelism via
scan(parallel=...) must work and return the same rows as a sequential scan.
"""
import time
import uuid

import _harness as H

H.quiet_logs()
bucket = f"audit-par-{uuid.uuid4().hex[:8]}"
s3 = H.s3_env(bucket, conditional=None)
if s3 is None:
    H.skip("s3-parallel-scan", "moto_server unavailable")
    H.finish()
from datashard import create_table  # noqa: E402

schema = H.simple_schema()
t = create_table("partable", schema)
for f in range(6):
    t.append_records([{"id": f * 1000 + i, "name": "n", "value": float(i)} for i in range(2000)], schema)
t0 = time.perf_counter()
seq = sorted(r["id"] for r in t.scan())
d_seq = time.perf_counter() - t0
t0 = time.perf_counter()
par = sorted(r["id"] for r in t.scan(parallel=4))
d_par = time.perf_counter() - t0
df = t.to_pandas(parallel=True, filter={"value": ("<", 10.0)}, columns=["id"])
H.report("s3-parallel-scan-matches-sequential", seq == par and len(seq) == 12000 and len(df) == 60,
         f"rows seq={len(seq)} par={len(par)} equal={seq == par}; filtered frame={len(df)} (expected 60); "
         f"sequential {d_seq * 1000:.0f} ms, parallel(4) {d_par * 1000:.0f} ms")
H.finish()
