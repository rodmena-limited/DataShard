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
import tempfile
import time
import uuid

import pytest

from datashard import Schema, create_table

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


# ---------------------------------------------------------------- scan-test fixtures
@pytest.fixture
def temp_table():
    """Create a temporary table for testing"""
    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "test_table")
        table = create_table(table_path)
        yield table


@pytest.fixture
def table_with_data():
    """Create a table with test data"""
    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "test_table")
        table = create_table(table_path)

        schema = Schema(
            schema_id=0,
            fields=[
                {"id": 1, "name": "id", "type": "long", "required": True},
                {"id": 2, "name": "name", "type": "string", "required": False},
                {"id": 3, "name": "age", "type": "int", "required": False},
                {"id": 4, "name": "status", "type": "string", "required": False},
                {"id": 5, "name": "score", "type": "double", "required": False},
            ],
        )

        # Insert test records
        records = [
            {"id": 1, "name": "Alice", "age": 30, "status": "active", "score": 95.5},
            {"id": 2, "name": "Bob", "age": 25, "status": "inactive", "score": 82.0},
            {"id": 3, "name": "Charlie", "age": 35, "status": "active", "score": 88.5},
            {"id": 4, "name": "Diana", "age": 28, "status": "pending", "score": 91.0},
            {"id": 5, "name": "Eve", "age": 32, "status": "active", "score": 77.5},
        ]

        table.append_records(records, schema)
        yield table


@pytest.fixture
def table_with_multiple_files():
    """Create a table with multiple parquet files for parallel/pruning tests"""
    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "test_table")
        table = create_table(table_path)

        schema = Schema(
            schema_id=0,
            fields=[
                {"id": 1, "name": "id", "type": "long", "required": True},
                {"id": 2, "name": "value", "type": "int", "required": False},
                {"id": 3, "name": "category", "type": "string", "required": False},
            ],
        )

        # Create multiple batches (each becomes a separate file)
        for batch_num in range(5):
            records = [
                {
                    "id": batch_num * 100 + i,
                    "value": batch_num * 10 + i,
                    "category": f"cat_{batch_num}",
                }
                for i in range(20)
            ]
            table.append_records(records, schema)

        yield table
