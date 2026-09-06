"""Claim: garbage_collect() never deletes a file referenced by the current snapshot,
whatever the table is called.

Suspect: GarbageCollector._normalize_path strips `table_path` as a plain STRING
prefix. A table whose path is a prefix of "data" / "metadata" (a table literally
called "data" is the natural name for a data lake) mis-normalises every listed path,
so nothing matches the reachable set and every live file looks like an orphan.
"""
import os
import tempfile
import time
import uuid

import _harness as H

H.local_env()
H.quiet_logs()
from datashard import create_table, load_table  # noqa: E402


def run_local(table_name):
    tmp = tempfile.mkdtemp(prefix="audit_gc_")
    cwd = os.getcwd()
    os.chdir(tmp)
    try:
        schema = H.simple_schema()
        t = create_table(table_name, schema)
        for i in range(3):
            t.append_records([{"id": i, "name": f"r{i}", "value": float(i)}], schema)
        before = t.row_count()
        data_dir = os.path.join(tmp, table_name, "data")
        files_before = [f for f in os.listdir(data_dir) if f.endswith(".parquet")]
        time.sleep(0.05)
        stats = t.garbage_collect(grace_period_ms=0, allow_short_grace=True)
        files_after = [f for f in os.listdir(data_dir) if f.endswith(".parquet")]
        try:
            after = len(load_table(table_name).scan())
            err = None
        except Exception as e:  # noqa: BLE001
            after, err = None, f"{type(e).__name__}: {str(e)[:120]}"
        ok = stats["data_files"] == 0 and after == before
        H.report(
            f"gc-keeps-live-data-local-table-named-{table_name!r}",
            ok,
            f"rows before={before} after={after} gc_stats={stats} parquet files "
            f"{len(files_before)}->{len(files_after)} scan_error={err}",
        )
    finally:
        os.chdir(cwd)


def run_s3(table_name):
    bucket = f"audit-gc-prefix-{uuid.uuid4().hex[:8]}"  # fresh bucket per run
    s3 = H.s3_env(bucket)
    if s3 is None:
        H.skip(f"gc-keeps-live-data-s3-table-named-{table_name!r}", "moto_server unavailable")
        return
    schema = H.simple_schema()
    t = create_table(table_name, schema)
    for i in range(3):
        t.append_records([{"id": i, "name": f"r{i}", "value": float(i)}], schema)
    before = t.row_count()
    n_before = H.count_s3_keys(s3, bucket, f"{table_name}/data/")
    time.sleep(1.1)  # S3 LastModified has 1 s resolution
    stats = t.garbage_collect(grace_period_ms=0, allow_short_grace=True)
    n_after = H.count_s3_keys(s3, bucket, f"{table_name}/data/")
    try:
        after = len(load_table(table_name).scan())
        err = None
    except Exception as e:  # noqa: BLE001
        after, err = None, f"{type(e).__name__}: {str(e)[:120]}"
    ok = stats["data_files"] == 0 and after == before
    H.report(
        f"gc-keeps-live-data-s3-table-named-{table_name!r}",
        ok,
        f"rows before={before} after={after} gc_stats={stats} data keys {n_before}->{n_after} scan_error={err}",
    )


run_local("data")   # suspect class: table path is a string prefix of 'data/...'
run_local("sales")  # control: unrelated name must PASS (the check can go green)
run_s3("data")
run_s3("trades")    # control
H.finish()
