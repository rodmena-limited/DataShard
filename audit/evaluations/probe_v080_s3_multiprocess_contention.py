"""Claim (0.8.0, #59/#67): on S3 with CAS locking and the reduced-round-trip commit path,
concurrent processes never lose or duplicate a commit. Runs against the moto server.
Tunables: AUDIT_PROCS (4), AUDIT_APPENDS (15).
"""
import multiprocessing as mp
import os
import sys
import time
import uuid

import _harness as H

H.quiet_logs()
N_PROCS = int(os.environ.get("AUDIT_PROCS", "4"))
N_APPENDS = int(os.environ.get("AUDIT_APPENDS", "15"))


def worker(args):
    env, tpath, wid, n = args
    os.environ.update(env)
    import logging

    logging.getLogger("datashard").setLevel(logging.ERROR)
    if H.SRC not in sys.path:
        sys.path.insert(0, H.SRC)
    from datashard import load_table

    t = load_table(tpath)
    schema = H.simple_schema()
    lat, errors = [], []
    for i in range(n):
        t0 = time.perf_counter()
        try:
            t.append_records([{"id": wid * 1000 + i, "name": f"w{wid}", "value": float(i)}], schema)
        except Exception as e:  # noqa: BLE001
            errors.append(f"{type(e).__name__}: {str(e)[:80]}")
        lat.append(time.perf_counter() - t0)
    return wid, lat, errors


if __name__ == "__main__":
    bucket = f"audit-s3mp-{uuid.uuid4().hex[:8]}"
    s3 = H.s3_env(bucket, conditional=None)
    if s3 is None:
        H.skip("s3-multiprocess-contention", "moto_server unavailable")
        H.finish()
    from datashard import create_table, load_table

    env = {k: v for k, v in os.environ.items() if k.startswith(("DATASHARD_", "AWS_"))}
    tpath = "mptable"
    t = create_table(tpath, H.simple_schema())
    t.set_properties({"datashard.manifest.compaction-threshold": "8"})
    ctx = mp.get_context("spawn")
    t0 = time.perf_counter()
    with ctx.Pool(N_PROCS) as pool:
        results = pool.map(worker, [(env, tpath, w, N_APPENDS) for w in range(N_PROCS)])
    elapsed = time.perf_counter() - t0
    lat = sorted(x for _, l_, _ in results for x in l_)
    errors = [e for _, _, errs in results for e in errs]
    t = load_table(tpath)
    expected = N_PROCS * N_APPENDS
    ids = sorted(r["id"] for r in t.scan(parallel=4))
    H.report(
        "s3-cas-no-lost-or-duplicated-commits-under-contention",
        len(ids) == expected and len(set(ids)) == expected and t.row_count() == expected and not errors,
        f"{N_PROCS} procs x {N_APPENDS}: rows={len(ids)} unique={len(set(ids))} snapshots={len(t.snapshots())} "
        f"errors={len(errors)} {errors[:2]}; elapsed {elapsed:.1f}s p50={lat[len(lat) // 2]:.2f}s p99={lat[int(len(lat) * 0.99)]:.2f}s",
    )
    H.finish()
