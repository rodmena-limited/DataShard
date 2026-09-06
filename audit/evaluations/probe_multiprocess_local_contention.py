"""Claim (README): 'Multiple processes can write without corruption'; OCC 'scales
linearly'. Tests the metadata lock in BOTH directions: writers are excluded while
another holds it (no lost update) AND every writer eventually completes (no
deadlock / lock timeout). Also measures commit latency under contention.
Tunables: AUDIT_PROCS (default 8), AUDIT_APPENDS per process (default 25).
"""
import multiprocessing as mp
import os
import sys
import tempfile
import time

import _harness as H

H.local_env()
H.quiet_logs()

N_PROCS = int(os.environ.get("AUDIT_PROCS", "8"))
N_APPENDS = int(os.environ.get("AUDIT_APPENDS", "25"))


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

    tmp = tempfile.mkdtemp(prefix="audit_mp_")
    path = os.path.join(tmp, "t")
    create_table(path, H.simple_schema())
    ctx = mp.get_context("spawn")
    t0 = time.perf_counter()
    with ctx.Pool(N_PROCS) as pool:
        results = pool.map(worker, [(path, w, N_APPENDS) for w in range(N_PROCS)])
    elapsed = time.perf_counter() - t0
    all_lat = sorted(x for _, lat, _ in results for x in lat)
    errors = [e for _, _, errs in results for e in errs]
    t = load_table(path)
    rows, snaps, scanned = t.row_count(), len(t.snapshots()), len(t.scan())
    expected = N_PROCS * N_APPENDS
    p50 = all_lat[len(all_lat) // 2]
    p99 = all_lat[min(len(all_lat) - 1, int(len(all_lat) * 0.99))]
    H.report(
        "no-lost-commits-under-multiprocess-contention",
        rows == expected and snaps == expected and scanned == expected and not errors,
        f"{N_PROCS} procs x {N_APPENDS} appends: rows={rows} snapshots={snaps} scanned={scanned} "
        f"expected={expected} errors={len(errors)} {errors[:2]}",
    )
    H.report(
        "commit-latency-under-contention",
        p99 < 5.0,
        f"elapsed={elapsed:.1f}s throughput={expected / elapsed:.1f} commits/s p50={p50 * 1000:.0f}ms "
        f"p99={p99 * 1000:.0f}ms max={all_lat[-1] * 1000:.0f}ms",
    )
    H.finish()
