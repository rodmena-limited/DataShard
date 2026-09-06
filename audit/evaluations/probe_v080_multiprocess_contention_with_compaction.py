"""Claim (0.8.0): the rewritten commit path (cached OCC base, attempt cleanup, automatic
manifest compaction) keeps every commit under multi-process contention. Compaction
threshold is lowered to 8 so it fires dozens of times during the run.
Tunables: AUDIT_PROCS (8), AUDIT_APPENDS (30).
"""
import glob
import multiprocessing as mp
import os
import sys
import tempfile
import time

import _harness as H

H.local_env()
H.quiet_logs()
N_PROCS = int(os.environ.get("AUDIT_PROCS", "8"))
N_APPENDS = int(os.environ.get("AUDIT_APPENDS", "30"))


def worker(args):
    path, wid, n = args
    import logging

    logging.getLogger("datashard").setLevel(logging.ERROR)
    if H.SRC not in sys.path:
        sys.path.insert(0, H.SRC)
    from datashard import load_table

    t = load_table(path)
    schema = H.simple_schema()
    lat, errors = [], []
    for i in range(n):
        t0 = time.perf_counter()
        try:
            t.append_records([{"id": wid * 100000 + i, "name": f"w{wid}", "value": float(i)}], schema)
        except Exception as e:  # noqa: BLE001
            errors.append(f"{type(e).__name__}: {str(e)[:80]}")
        lat.append(time.perf_counter() - t0)
    return wid, lat, errors


if __name__ == "__main__":
    from datashard import create_table, load_table

    tmp = tempfile.mkdtemp(prefix="audit_mpc_")
    path = os.path.join(tmp, "t")
    t = create_table(path, H.simple_schema())
    t.set_properties({"datashard.manifest.compaction-threshold": "8"})
    ctx = mp.get_context("spawn")
    t0 = time.perf_counter()
    with ctx.Pool(N_PROCS) as pool:
        results = pool.map(worker, [(path, w, N_APPENDS) for w in range(N_PROCS)])
    elapsed = time.perf_counter() - t0
    lat = sorted(x for _, l_, _ in results for x in l_)
    errors = [e for _, _, errs in results for e in errs]
    t = load_table(path)
    expected = N_PROCS * N_APPENDS
    ids = sorted(r["id"] for r in t.scan())
    active = len(t.file_manager.read_manifest_list_file(t.current_snapshot().manifest_list))
    on_disk = len(glob.glob(os.path.join(path, "metadata", "manifests", "manifest_1*.avro")))
    H.report(
        "no-lost-or-duplicated-commits-under-contention-with-compaction",
        len(ids) == expected and len(set(ids)) == expected and t.row_count() == expected and not errors,
        f"{N_PROCS} procs x {N_APPENDS}: rows={len(ids)} unique={len(set(ids))} snapshots={len(t.snapshots())} "
        f"errors={len(errors)} {errors[:2]}; active manifests={active} (threshold 8), manifests on disk={on_disk}; "
        f"elapsed {elapsed:.1f}s p50={lat[len(lat) // 2] * 1000:.0f}ms p99={lat[int(len(lat) * 0.99)] * 1000:.0f}ms",
    )
    stats = t.garbage_collect(grace_period_ms=0, allow_short_grace=True)
    H.report("gc-after-contention-keeps-all-rows", load_table(path).row_count() == expected and stats["data_files"] == 0,
             f"gc_stats={stats}")
    H.finish()
