"""EXTERNAL probe - end-to-end multi-writer commits on the configured S3 endpoint with
conditional writes ON (the configuration this audit recommends for OVH).
Refuses to run unless AUDIT_ALLOW_EXTERNAL=1. Blast radius: creates one table under a
unique 'audit-probe-<uuid>/' prefix in the configured bucket, runs AUDIT_PROCS x
AUDIT_APPENDS single-row appends from separate processes, then deletes every object
under that prefix.
"""
import multiprocessing as mp
import os
import sys
import time
import uuid

import _harness as H

if os.environ.get("AUDIT_ALLOW_EXTERNAL") != "1":
    print("SKIP external-ovh-cas-commit-e2e: set AUDIT_ALLOW_EXTERNAL=1 to run against the configured endpoint")
    sys.exit(0)


def load_env():
    envfile = os.path.join(H.ROOT, ".env")
    if os.path.exists(envfile):
        for line in open(envfile):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k, v)
    os.environ["DATASHARD_STORAGE_TYPE"] = "s3"
    os.environ["DATASHARD_S3_USE_CONDITIONAL_WRITES"] = "true"
    os.environ.setdefault("AWS_ACCESS_KEY_ID", os.environ.get("DATASHARD_S3_ACCESS_KEY", ""))
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", os.environ.get("DATASHARD_S3_SECRET_KEY", ""))
    os.environ.setdefault("AWS_DEFAULT_REGION", os.environ.get("DATASHARD_S3_REGION", "us-east-1"))
    os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")


def worker(args):
    tpath, wid, n = args
    import logging

    logging.getLogger("datashard").setLevel(logging.ERROR)
    if H.SRC not in sys.path:
        sys.path.insert(0, H.SRC)
    load_env()
    from datashard import load_table

    t = load_table(tpath)
    schema = H.simple_schema()
    lat, errors = [], []
    for i in range(n):
        t0 = time.perf_counter()
        try:
            t.append_records([{"id": wid * 1000 + i, "name": f"w{wid}", "value": float(i)}], schema)
        except Exception as e:  # noqa: BLE001
            errors.append(f"{type(e).__name__}: {str(e)[:90]}")
        lat.append(time.perf_counter() - t0)
    return wid, lat, errors


if __name__ == "__main__":
    load_env()
    H.quiet_logs()
    import boto3

    from datashard import create_table, load_table

    N_PROCS = int(os.environ.get("AUDIT_PROCS", "4"))
    N_APPENDS = int(os.environ.get("AUDIT_APPENDS", "5"))
    bucket = os.environ["DATASHARD_S3_BUCKET"]
    tpath = f"audit-probe-{uuid.uuid4().hex}"
    host = os.environ.get("DATASHARD_S3_ENDPOINT", "").split("//")[-1]
    print(f"  info: endpoint={host} bucket={bucket} table={tpath} conditional_writes=true")
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("DATASHARD_S3_ENDPOINT"),
        aws_access_key_id=os.environ.get("DATASHARD_S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("DATASHARD_S3_SECRET_KEY"),
        region_name=os.environ.get("DATASHARD_S3_REGION", "us-east-1"),
    )
    try:
        t0 = time.perf_counter()
        t = create_table(tpath, H.simple_schema())
        H.report("cas-create_table-on-provider", t.storage.supports_cas, f"created in {time.perf_counter() - t0:.2f}s; supports_cas={t.storage.supports_cas}")
        ctx = mp.get_context("spawn")
        t0 = time.perf_counter()
        with ctx.Pool(N_PROCS) as pool:
            results = pool.map(worker, [(tpath, w, N_APPENDS) for w in range(N_PROCS)])
        elapsed = time.perf_counter() - t0
        lat = sorted(x for _, lats, _ in results for x in lats)
        errors = [e for _, _, errs in results for e in errs]
        t = load_table(tpath)
        t1 = time.perf_counter()
        rows = t.row_count()
        d_rc = time.perf_counter() - t1
        t1 = time.perf_counter()
        snaps = len(t.snapshots())
        d_sn = time.perf_counter() - t1
        t1 = time.perf_counter()
        scanned = len(t.scan())
        d_sc = time.perf_counter() - t1
        print(f"  info: read latency on provider: row_count {d_rc:.2f}s, snapshots {d_sn:.2f}s, scan({rows} rows/{snaps} files) {d_sc:.2f}s")
        expected = N_PROCS * N_APPENDS
        H.report(
            "cas-multiwriter-no-lost-commits-on-provider",
            rows == expected and snaps == expected and scanned == expected and not errors,
            f"{N_PROCS} procs x {N_APPENDS} appends in {elapsed:.1f}s: rows={rows} snapshots={snaps} scanned={scanned} "
            f"expected={expected} errors={len(errors)} {errors[:2]}; commit latency p50={lat[len(lat) // 2]:.2f}s max={lat[-1]:.2f}s",
        )
    finally:
        keys = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=tpath + "/"):
            keys += [{"Key": o["Key"]} for o in page.get("Contents", [])]
        for i in range(0, len(keys), 1000):
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys[i : i + 1000], "Quiet": True})
        left = H.count_s3_keys(s3, bucket, tpath + "/")
        print(f"  info: cleanup deleted {len(keys)} objects under {tpath}/ ; remaining={left}")
    H.finish()
