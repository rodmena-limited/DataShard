"""Claim: a commit that completes while GC is running is never deleted by that GC.

Two scenarios, both real on large S3 tables where the reachability phase (3 requests
per manifest) outlasts the grace period:
 A. a transaction entirely inside the GC run: file written + committed after GC read
    the metadata -> must survive because nothing written after GC STARTED may be
    deleted (cutoff relative to the start instant);
 B. a long-running transaction: file + marker written BEFORE GC starts, older than the
    grace, committed (marker removed) during GC's reachability phase -> must survive
    because markers are loaded before the metadata read.
Historic behaviour (0.7.2): markers loaded after reachability, cutoff at listing time
-> both files deleted, table unreadable.
"""
import os
import tempfile
import time

import _harness as H

H.local_env()
H.quiet_logs()
from datashard import create_table, load_table  # noqa: E402
from datashard.garbage_collector import GarbageCollector  # noqa: E402

GRACE_MS = 2000


def run_scenario(name, long_running):
    tmp = tempfile.mkdtemp(prefix="audit_gcrace_")
    path = os.path.join(tmp, "t")
    schema = H.simple_schema()
    t = create_table(path, schema)
    t.append_records([{"id": 0, "name": "base", "value": 0.0}], schema)
    writer = load_table(path)
    tx = None
    if long_running:
        tx = writer.new_transaction().begin()
        tx.append_data([{"id": 1, "name": "long-tx", "value": 1.0}], schema)  # file + marker exist
        old = time.time() - 60
        for f in os.listdir(os.path.join(path, "data")):
            fp = os.path.join(path, "data", f)
            os.utime(fp, (old, old))  # the transaction has been open far longer than the grace
    gc = GarbageCollector(path, t.metadata_manager, t.file_manager)
    orig_read = gc.file_manager.read_manifest_file
    state = {"done": False}

    def slow_reachability(*a, **k):
        # First manifest read = GC is inside its reachability phase, metadata already read.
        if not state["done"]:
            state["done"] = True
            if tx is not None:
                tx.commit()
            else:
                writer.append_records([{"id": 1, "name": "committed-during-gc", "value": 1.0}], schema)
            time.sleep(GRACE_MS / 1000 + 0.5)  # GC stays slow past the grace period
        return orig_read(*a, **k)

    gc.file_manager.read_manifest_file = slow_reachability
    try:
        stats = gc.collect(grace_period_ms=GRACE_MS, allow_short_grace=True)
    finally:
        gc.file_manager.read_manifest_file = orig_read
    try:
        n = len(load_table(path).scan())
        err = None
    except Exception as e:  # noqa: BLE001
        n, err = None, f"{type(e).__name__}: {str(e)[:120]}"
    H.report(
        f"gc-never-deletes-file-committed-during-gc ({name})",
        n == 2 and stats["data_files"] == 0 and stats["manifest_files"] == 0,
        f"expected 2 rows readable after GC; got rows={n} gc_stats={stats} scan_error={err}",
    )


run_scenario("commit entirely during GC", long_running=False)
run_scenario("long transaction committing during GC", long_running=True)
H.finish()
