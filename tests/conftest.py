"""Shared fixtures.

S3 integration tests run against the endpoint configured through DATASHARD_S3_*
when one is set (a real provider), and otherwise against a moto_server started
once per session - so the S3 code path is exercised in every CI run, not only
when someone exports credentials (#65).
"""
import os
import shutil
import socket
import subprocess
import sys
import time
import uuid

import pytest

_S3_KEYS = (
    "DATASHARD_S3_ENDPOINT",
    "DATASHARD_S3_ACCESS_KEY",
    "DATASHARD_S3_SECRET_KEY",
    "DATASHARD_S3_BUCKET",
    "DATASHARD_S3_REGION",
)


@pytest.fixture(autouse=True)
def force_local_storage_by_default(monkeypatch):
    """
    Ensure that tests default to using local storage, even if the
    external environment is configured for S3.

    Tests that require S3 request the `s3_env` fixture.
    """
    monkeypatch.setenv("DATASHARD_STORAGE_TYPE", "local")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _port_open(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.2)
        return s.connect_ex(("127.0.0.1", port)) == 0


@pytest.fixture(scope="session")
def s3_test_endpoint():
    """Connection details for the S3 endpoint tests should use.

    Uses the operator's DATASHARD_S3_* configuration when a bucket is set;
    otherwise starts a moto_server for the session (skipping if moto is not
    installed: `pip install datashard[dev]`).
    """
    if os.environ.get("DATASHARD_S3_BUCKET"):
        yield {k: os.environ[k] for k in _S3_KEYS if os.environ.get(k)}
        return

    exe = os.path.join(os.path.dirname(sys.executable), "moto_server")
    if not os.path.exists(exe):
        exe = shutil.which("moto_server") or ""
    if not exe:
        pytest.skip("moto_server not installed (pip install datashard[dev]) and no DATASHARD_S3_* endpoint configured")

    port = _free_port()
    proc = subprocess.Popen([exe, "-p", str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 15
    while not _port_open(port):
        if time.time() > deadline or proc.poll() is not None:
            proc.terminate()
            pytest.fail("moto_server did not start")
        time.sleep(0.1)
    endpoint = f"http://127.0.0.1:{port}"
    import boto3

    boto3.client(
        "s3", endpoint_url=endpoint, aws_access_key_id="testing",
        aws_secret_access_key="testing", region_name="us-east-1",
    ).create_bucket(Bucket="datashard-tests")
    try:
        yield {
            "DATASHARD_S3_ENDPOINT": endpoint,
            "DATASHARD_S3_ACCESS_KEY": "testing",
            "DATASHARD_S3_SECRET_KEY": "testing",
            "DATASHARD_S3_BUCKET": "datashard-tests",
            "DATASHARD_S3_REGION": "us-east-1",
        }
    finally:
        proc.terminate()  # the process we started, by handle - never pkill
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


class S3TestContext:
    def __init__(self, client, bucket):
        self.client = client
        self.bucket = bucket
        self.created = []

    def table_name(self) -> str:
        name = f"test_table_{uuid.uuid4().hex[:8]}"
        self.created.append(name)
        return name

    def keys_under(self, prefix: str):
        keys = []
        for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            keys += [o["Key"] for o in page.get("Contents", [])]
        return keys

    def cleanup(self) -> None:
        for name in self.created:
            keys = self.keys_under(name + "/")
            for i in range(0, len(keys), 1000):
                self.client.delete_objects(
                    Bucket=self.bucket,
                    Delete={"Objects": [{"Key": k} for k in keys[i : i + 1000]], "Quiet": True},
                )


@pytest.fixture
def s3_env(s3_test_endpoint, monkeypatch):
    """Point datashard at the S3 test endpoint for one test.

    Leaves DATASHARD_S3_USE_CONDITIONAL_WRITES unset so datashard probes the
    endpoint (#59). Every table created through `table_name()` is deleted on
    teardown, so a real bucket is not littered with test_table_* prefixes.
    """
    import boto3

    for k, v in s3_test_endpoint.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("DATASHARD_STORAGE_TYPE", "s3")
    monkeypatch.delenv("DATASHARD_S3_USE_CONDITIONAL_WRITES", raising=False)
    monkeypatch.delenv("DATASHARD_S3_ALLOW_UNSAFE_LOCK", raising=False)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    client = boto3.client(
        "s3",
        endpoint_url=s3_test_endpoint.get("DATASHARD_S3_ENDPOINT"),
        aws_access_key_id=s3_test_endpoint.get("DATASHARD_S3_ACCESS_KEY"),
        aws_secret_access_key=s3_test_endpoint.get("DATASHARD_S3_SECRET_KEY"),
        region_name=s3_test_endpoint.get("DATASHARD_S3_REGION", "us-east-1"),
    )
    ctx = S3TestContext(client, s3_test_endpoint["DATASHARD_S3_BUCKET"])
    try:
        yield ctx
    finally:
        ctx.cleanup()
