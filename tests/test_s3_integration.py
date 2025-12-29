"""
Integration tests for S3-compatible storage backend (MinIO, AWS S3, OVH)

These tests require S3 credentials to be set via environment variables:
- DATASHARD_STORAGE_TYPE=s3
- DATASHARD_S3_ENDPOINT=<your S3 endpoint>
- DATASHARD_S3_ACCESS_KEY=<your access key>
- DATASHARD_S3_SECRET_KEY=<your secret key>
- DATASHARD_S3_BUCKET=<your bucket>
- DATASHARD_S3_REGION=<your region>
- DATASHARD_S3_USE_CONDITIONAL_WRITES=false (for OVH and other non-AWS providers)
"""

import os
import uuid

import pytest

from datashard import Schema, create_table, load_table

# Store original env vars at module load time
_ORIGINAL_S3_ENV = {
    "DATASHARD_STORAGE_TYPE": os.getenv("DATASHARD_STORAGE_TYPE"),
    "DATASHARD_S3_ENDPOINT": os.getenv("DATASHARD_S3_ENDPOINT"),
    "DATASHARD_S3_ACCESS_KEY": os.getenv("DATASHARD_S3_ACCESS_KEY"),
    "DATASHARD_S3_SECRET_KEY": os.getenv("DATASHARD_S3_SECRET_KEY"),
    "DATASHARD_S3_BUCKET": os.getenv("DATASHARD_S3_BUCKET"),
    "DATASHARD_S3_REGION": os.getenv("DATASHARD_S3_REGION"),
    "DATASHARD_S3_USE_CONDITIONAL_WRITES": os.getenv("DATASHARD_S3_USE_CONDITIONAL_WRITES"),
}


def _setup_s3_env():
    """Setup S3 env vars from saved original values."""
    for key, value in _ORIGINAL_S3_ENV.items():
        if value is not None:
            os.environ[key] = value
    # Ensure storage type is s3
    os.environ["DATASHARD_STORAGE_TYPE"] = "s3"


# Skip all tests if S3 credentials are not configured
def _s3_configured():
    return bool(_ORIGINAL_S3_ENV.get("DATASHARD_S3_BUCKET"))


pytestmark = pytest.mark.skipif(
    not _s3_configured(),
    reason="S3 credentials not configured (set DATASHARD_S3_* env vars)"
)


def test_s3_storage_create_table():
    """Test creating a table with S3 storage backend"""
    _setup_s3_env()

    # Create unique table name
    table_name = f"test_table_{uuid.uuid4().hex[:8]}"

    try:
        # Create schema
        schema = Schema(
            schema_id=1,
            fields=[
                {"id": 1, "name": "id", "type": "long", "required": True},
                {"id": 2, "name": "name", "type": "string", "required": False},
                {"id": 3, "name": "value", "type": "double", "required": False},
            ],
        )

        # Create table on S3
        table = create_table(table_name, schema)
        assert table is not None
        assert table.storage.__class__.__name__ == "S3StorageBackend"
        print(f"✅ Created S3 table: {table_name}")

        # Verify metadata exists
        assert table.storage.exists("metadata")
        print("✅ Metadata directory exists on S3")

    finally:
        # Cleanup env vars
        for key in [
            "DATASHARD_STORAGE_TYPE",
            "DATASHARD_S3_ENDPOINT",
            "DATASHARD_S3_ACCESS_KEY",
            "DATASHARD_S3_SECRET_KEY",
            "DATASHARD_S3_BUCKET",
            "DATASHARD_S3_REGION",
            "DATASHARD_S3_USE_CONDITIONAL_WRITES",
        ]:
            if key in os.environ:
                del os.environ[key]


def test_s3_storage_write_and_read():
    """Test writing and reading data with S3 storage"""
    _setup_s3_env()

    # Create unique table name
    table_name = f"test_table_{uuid.uuid4().hex[:8]}"

    try:
        # Create schema
        schema = Schema(
            schema_id=1,
            fields=[
                {"id": 1, "name": "id", "type": "long", "required": True},
                {"id": 2, "name": "message", "type": "string", "required": False},
                {"id": 3, "name": "count", "type": "long", "required": False},
            ],
        )

        # Create table
        table = create_table(table_name, schema)
        print(f"✅ Created table: {table_name}")

        # Write data
        records = [
            {"id": 1, "message": "Hello S3", "count": 100},
            {"id": 2, "message": "MinIO test", "count": 200},
            {"id": 3, "message": "DataShard rocks", "count": 300},
        ]

        with table.transaction_manager.begin_transaction() as txn:
            txn.append_data(records, schema)

        print(f"✅ Wrote {len(records)} records to S3")

        # Read back
        loaded = load_table(table_name)
        snapshots = loaded.snapshot_manager.get_all_snapshots()
        print(f"✅ Read table back, found {len(snapshots)} snapshot(s)")

        # We should have at least 1 snapshot
        assert len(snapshots) >= 1, f"Expected at least 1 snapshot, got {len(snapshots)}"

        # Verify data files exist
        if snapshots:
            snapshot = snapshots[-1]  # Get latest snapshot
            assert snapshot.manifest_list is not None
            print(f"✅ Manifest list exists: {snapshot.manifest_list}")

        # Check data path exists
        assert loaded.storage.exists("data")
        print("✅ Data directory exists on S3")

    finally:
        # Cleanup env vars
        for key in [
            "DATASHARD_STORAGE_TYPE",
            "DATASHARD_S3_ENDPOINT",
            "DATASHARD_S3_ACCESS_KEY",
            "DATASHARD_S3_SECRET_KEY",
            "DATASHARD_S3_BUCKET",
            "DATASHARD_S3_REGION",
            "DATASHARD_S3_USE_CONDITIONAL_WRITES",
        ]:
            if key in os.environ:
                del os.environ[key]


def test_s3_multiple_transactions():
    """Test multiple concurrent transactions with S3"""
    _setup_s3_env()

    # Create unique table name
    table_name = f"test_table_{uuid.uuid4().hex[:8]}"

    try:
        # Create schema
        schema = Schema(
            schema_id=1,
            fields=[
                {"id": 1, "name": "batch_id", "type": "long", "required": True},
                {"id": 2, "name": "item_id", "type": "long", "required": True},
                {"id": 3, "name": "data", "type": "string", "required": False},
            ],
        )

        # Create table
        create_table(table_name, schema)
        print(f"✅ Created table: {table_name}")

        # Write multiple batches
        for batch in range(3):
            records = [
                {"batch_id": batch, "item_id": i, "data": f"batch_{batch}_item_{i}"}
                for i in range(5)
            ]

            # Reload table each time to ensure we see cumulative snapshots
            loaded = load_table(table_name)
            with loaded.transaction_manager.begin_transaction() as txn:
                txn.append_data(records, schema)

            print(f"✅ Wrote batch {batch} ({len(records)} records)")

        # Verify all snapshots
        loaded = load_table(table_name)
        snapshots = loaded.snapshot_manager.get_all_snapshots()
        print(f"✅ Found {len(snapshots)} snapshot(s)")

        # We should have all 3 batches
        assert len(snapshots) >= 3, f"Expected at least 3 snapshots, got {len(snapshots)}"

    finally:
        # Cleanup env vars
        for key in [
            "DATASHARD_STORAGE_TYPE",
            "DATASHARD_S3_ENDPOINT",
            "DATASHARD_S3_ACCESS_KEY",
            "DATASHARD_S3_SECRET_KEY",
            "DATASHARD_S3_BUCKET",
            "DATASHARD_S3_REGION",
            "DATASHARD_S3_USE_CONDITIONAL_WRITES",
        ]:
            if key in os.environ:
                del os.environ[key]


if __name__ == "__main__":
    print("=" * 60)
    print("DataShard v0.2.2 - S3 Integration Tests")
    print("=" * 60)
    print()

    print("Test 1: Create table on S3")
    print("-" * 60)
    test_s3_storage_create_table()
    print()

    print("Test 2: Write and read data from S3")
    print("-" * 60)
    test_s3_storage_write_and_read()
    print()

    print("Test 3: Multiple transactions on S3")
    print("-" * 60)
    test_s3_multiple_transactions()
    print()

    print("=" * 60)
    print("✅ ALL S3 INTEGRATION TESTS PASSED")
    print("=" * 60)
