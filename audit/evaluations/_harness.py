"""Shared helpers for the datashard audit probes (issuedb #55).

Every probe prints one line per claim: ``PASS <name>: <evidence>`` when the claim
holds, ``FAIL <name>: <evidence>`` when the defect reproduces, ``SKIP`` when the
environment cannot exercise it. ``finish()`` exits non-zero on any FAIL.
"""
import atexit
import os
import shutil
import socket
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

_results = []


def report(name, ok, evidence):
    print(f"{'PASS' if ok else 'FAIL'} {name}: {evidence}", flush=True)
    _results.append(bool(ok))
    return ok


def skip(name, why):
    print(f"SKIP {name}: {why}", flush=True)


def finish():
    sys.exit(0 if all(_results) else 1)


def local_env():
    """Force the local backend regardless of the caller's shell (.env may say s3)."""
    os.environ["DATASHARD_STORAGE_TYPE"] = "local"
    for k in list(os.environ):
        if k.startswith("DATASHARD_S3_"):
            os.environ.pop(k)


def quiet_logs(level="ERROR"):
    import logging

    logging.getLogger("datashard").setLevel(getattr(logging, level))
    logging.getLogger("datashard.lock_provider").setLevel(getattr(logging, level))
    logging.getLogger("datashard.garbage_collector").setLevel(getattr(logging, level))


def simple_schema():
    from datashard import Schema

    return Schema(
        schema_id=1,
        fields=[
            {"id": 1, "name": "id", "type": "long", "required": True},
            {"id": 2, "name": "name", "type": "string", "required": False},
            {"id": 3, "name": "value", "type": "double", "required": False},
        ],
    )


MOTO_PORT = int(os.environ.get("AUDIT_MOTO_PORT", "5599"))
_moto_proc = None


def _port_open(port):
    with socket.socket() as s:
        s.settimeout(0.3)
        return s.connect_ex(("127.0.0.1", port)) == 0


def _stop_moto():
    if _moto_proc is not None and _moto_proc.poll() is None:
        _moto_proc.terminate()  # by PID of the process we started - never pkill


def ensure_moto():
    """Endpoint URL of a moto S3 server; starts one (terminated at exit) if none listens."""
    global _moto_proc
    if _port_open(MOTO_PORT):
        return f"http://127.0.0.1:{MOTO_PORT}"
    exe = os.path.join(os.path.dirname(sys.executable), "moto_server")
    if not os.path.exists(exe):
        exe = shutil.which("moto_server")
    if not exe:
        return None
    _moto_proc = subprocess.Popen(
        [exe, "-p", str(MOTO_PORT)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    atexit.register(_stop_moto)
    for _ in range(60):
        if _port_open(MOTO_PORT):
            return f"http://127.0.0.1:{MOTO_PORT}"
        time.sleep(0.1)
    return None


def s3_env(bucket, conditional=True, prefix=""):
    """Point datashard at the moto server; returns a boto3 client or None if unavailable."""
    ep = ensure_moto()
    if ep is None:
        return None
    os.environ.update(
        {
            "DATASHARD_STORAGE_TYPE": "s3",
            "DATASHARD_S3_ENDPOINT": ep,
            "DATASHARD_S3_ACCESS_KEY": "testing",
            "DATASHARD_S3_SECRET_KEY": "testing",
            "DATASHARD_S3_BUCKET": bucket,
            "DATASHARD_S3_REGION": "us-east-1",
            "DATASHARD_S3_PREFIX": prefix,
            "DATASHARD_S3_USE_CONDITIONAL_WRITES": "true" if conditional else "false",
            # pyarrow's S3FileSystem (still used on the write path) reads these
            "AWS_ACCESS_KEY_ID": "testing",
            "AWS_SECRET_ACCESS_KEY": "testing",
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_EC2_METADATA_DISABLED": "true",
        }
    )
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=ep,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        region_name="us-east-1",
    )
    try:
        s3.create_bucket(Bucket=bucket)
    except Exception:
        pass
    return s3


def count_s3_keys(s3, bucket, prefix):
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        n += len(page.get("Contents", []))
    return n
