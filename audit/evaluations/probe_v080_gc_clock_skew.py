"""Claim (0.8.0, #57): GC measures age from the instant it started, so nothing written
after GC started can be deleted.

Suspect: on S3 the object times come from the SERVER clock while the GC start instant
comes from the CLIENT clock. Reachable files are never candidates, so the exposed window
is a commit that lands DURING the GC run (unreachable in GC's view, its marker already
removed): with a client clock ahead of the server by more than the grace period that
fresh object looks older than the cutoff and is deleted. Deterministic: a commit is
injected during reachability and the clock the garbage collector consults is moved 2 h
ahead; grace is the 1 h default.
"""
import time
import uuid

import _harness as H

H.quiet_logs()
bucket = f"audit-skew-{uuid.uuid4().hex[:8]}"
s3 = H.s3_env(bucket, conditional=None)
if s3 is None:
    H.skip("gc-immune-to-client-clock-skew", "moto_server unavailable")
    H.finish()
import datashard.garbage_collector as gcmod  # noqa: E402
from datashard import create_table, load_table  # noqa: E402
from datashard.garbage_collector import GarbageCollector  # noqa: E402

schema = H.simple_schema()
t = create_table("skewtable", schema)
for i in range(3):
    t.append_records([{"id": i, "name": "r", "value": 1.0}], schema)
time.sleep(1.1)
gc = GarbageCollector(t.table_path, t.metadata_manager, t.file_manager)
orig, state = gc.file_manager.read_manifest_file, {"done": False}


def commit_during_gc(*a, **k):
    if not state["done"]:
        state["done"] = True
        load_table("skewtable").append_records([{"id": 99, "name": "late", "value": 1.0}], schema)
        time.sleep(1.1)  # S3 timestamps have 1 s resolution
    return orig(*a, **k)


gc.file_manager.read_manifest_file = commit_during_gc
real_time = gcmod.time.time
gcmod.time.time = lambda: real_time() + 7200  # the GC host's clock runs 2 h fast
try:
    stats = gc.collect(grace_period_ms=3600000)
finally:
    gcmod.time.time = real_time
try:
    rows = load_table("skewtable").row_count()
    err = None
except Exception as e:  # noqa: BLE001
    rows, err = None, f"{type(e).__name__}: {str(e)[:100]}"
H.report(
    "gc-immune-to-client-clock-skew",
    stats["data_files"] == 0 and stats["manifest_files"] == 0 and rows == 4,
    f"client clock +2h, grace 1h, one commit during GC: gc_stats={stats}, rows readable={rows} (expected 4) {err or ''}",
)
H.finish()
