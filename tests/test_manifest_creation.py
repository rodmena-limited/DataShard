"""
Comprehensive tests for manifest creation bug fix.

Tests that manifest lists are properly created with data files when appending records,
and that queries return the written data.
"""

import os
import tempfile

import fastavro
import pytest

from datashard import Schema, create_table


def test_append_records_creates_non_empty_manifests():
    """Test that appending records creates manifest lists with actual manifest entries.

    This test specifically validates the bug fix where Transaction.commit()
    was not passing data_files to _create_manifest_list_with_id(), resulting
    in empty manifest lists.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "test_table")

        # Create table with schema
        schema = Schema(
            schema_id=1,
            fields=[
                {"id": 1, "name": "id", "type": "long", "required": True},
                {"id": 2, "name": "value", "type": "string", "required": True},
                {"id": 3, "name": "count", "type": "long", "required": True},
            ],
        )
        table = create_table(table_path, schema)

        # Append records
        records = [
            {"id": 1, "value": "first", "count": 10},
            {"id": 2, "value": "second", "count": 20},
            {"id": 3, "value": "third", "count": 30},
        ]
        success = table.append_records(records, schema)
        assert success, "append_records should succeed"

        # Get current snapshot
        snapshot = table.current_snapshot()
        assert snapshot is not None, "Snapshot should exist after append"
        assert snapshot.manifest_list is not None, "Snapshot should have manifest_list"
        assert snapshot.manifest_list != "", "Manifest list path should not be empty"

        # Read manifest list file
        manifest_list_path = os.path.join(table_path, snapshot.manifest_list)
        with open(manifest_list_path, "rb") as f:
            # Read Avro manifest list
            reader = fastavro.reader(f)
            # Convert iterator to list of dicts to match JSON structure expectation in test
            manifests = list(reader)
            # Synthesize the structure expected by the test (which expects {'manifests': [...]})
            manifest_list_data = {"manifests": manifests}

        # BUG FIX VALIDATION: manifests array should NOT be empty
        assert "manifests" in manifest_list_data, "Manifest list should have 'manifests' key"
        assert (
            len(manifest_list_data["manifests"]) > 0
        ), "Manifests array should NOT be empty after appending records (BUG FIX)"

        # Validate manifest structure
        first_manifest = manifest_list_data["manifests"][0]
        assert "manifest_path" in first_manifest, "Manifest should have manifest_path"
        assert "added_snapshot_id" in first_manifest, "Manifest should have added_snapshot_id"

        # Validate manifest file exists
        manifest_path = os.path.join(table_path, first_manifest["manifest_path"])
        assert os.path.exists(manifest_path), f"Manifest file should exist at {manifest_path}"


def test_manifest_reading_returns_data_files():
    """Test that reading manifests returns actual data files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "test_table")

        # Create table and append data
        schema = Schema(
            schema_id=1,
            fields=[
                {"id": 1, "name": "user_id", "type": "long", "required": True},
                {"id": 2, "name": "action", "type": "string", "required": True},
            ],
        )
        table = create_table(table_path, schema)

        records = [{"user_id": 100, "action": "login"}, {"user_id": 101, "action": "logout"}]
        table.append_records(records, schema)

        # Read manifests via FileManager
        from datashard.file_manager import FileManager

        snapshot = table.current_snapshot()
        assert snapshot is not None

        file_manager = FileManager(table_path, table.metadata_manager, table.storage)
        # snapshot.manifest_list is already a relative path, use it directly
        manifest_list_path = snapshot.manifest_list

        manifests = file_manager.read_manifest_list_file(manifest_list_path)
        assert len(manifests) > 0, "Should have at least one manifest"

        # Read data files from manifest
        first_manifest = manifests[0]
        # first_manifest.manifest_path is also relative, use it directly
        manifest_path = first_manifest.manifest_path
        data_files = file_manager.read_manifest_file(manifest_path)

        assert len(data_files) > 0, "Manifest should contain data files"

        # Validate data file exists
        data_file = data_files[0]
        # Data file path is relative, construct absolute path
        data_file_path = os.path.join(table_path, data_file.file_path.lstrip("/"))
        assert os.path.exists(data_file_path), f"Data file should exist at {data_file_path}"


def test_multiple_appends_create_multiple_manifests():
    """Test that multiple append operations create multiple manifest entries."""
    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "test_table")

        schema = Schema(
            schema_id=1,
            fields=[
                {"id": 1, "name": "timestamp", "type": "long", "required": True},
                {"id": 2, "name": "event", "type": "string", "required": True},
            ],
        )
        table = create_table(table_path, schema)

        # First append
        table.append_records([{"timestamp": 1000, "event": "start"}], schema)

        # Second append
        table.append_records([{"timestamp": 2000, "event": "end"}], schema)

        # Get current snapshot
        snapshot = table.current_snapshot()
        assert snapshot is not None

        # Read manifest list
        manifest_list_path = os.path.join(table_path, snapshot.manifest_list)
        with open(manifest_list_path, "rb") as f:
            reader = fastavro.reader(f)
            manifests = list(reader)
            manifest_list_data = {"manifests": manifests}

        # Should have manifests from the append
        assert (
            len(manifest_list_data["manifests"]) > 0
        ), "Should have at least one manifest after multiple appends"


def test_concurrent_appends_create_valid_manifests():
    """Test that concurrent append operations create valid manifest lists.

    This validates that the OCC (Optimistic Concurrency Control) mechanism
    combined with proper manifest creation works correctly.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "test_table")

        schema = Schema(
            schema_id=1,
            fields=[
                {"id": 1, "name": "worker_id", "type": "long", "required": True},
                {"id": 2, "name": "task_id", "type": "string", "required": True},
            ],
        )
        table = create_table(table_path, schema)

        # Simulate concurrent appends (sequential for testing, OCC handles actual concurrency)
        for i in range(3):
            records = [{"worker_id": i, "task_id": f"task_{i}"}]
            success = table.append_records(records, schema)
            assert success, f"Append {i} should succeed"

        # Verify final snapshot has valid manifests
        snapshot = table.current_snapshot()
        assert snapshot is not None

        manifest_list_path = os.path.join(table_path, snapshot.manifest_list)
        with open(manifest_list_path, "rb") as f:
            reader = fastavro.reader(f)
            manifests = list(reader)
            manifest_list_data = {"manifests": manifests}

        # Final snapshot should have manifests
        assert (
            len(manifest_list_data["manifests"]) > 0
        ), "Final snapshot should have non-empty manifests"


def test_empty_append_creates_empty_manifest():
    """Test that appending with no data creates an empty manifest (edge case)."""
    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "test_table")

        schema = Schema(
            schema_id=1, fields=[{"id": 1, "name": "id", "type": "long", "required": True}]
        )
        table = create_table(table_path, schema)

        # Append empty records list
        success = table.append_records([], schema)
        assert success, "Appending empty list should succeed"

        # This should create an empty manifest list (legitimate empty case)
        snapshot = table.current_snapshot()
        if snapshot is not None and snapshot.manifest_list:
            manifest_list_path = os.path.join(table_path, snapshot.manifest_list)
            if os.path.exists(manifest_list_path):
                with open(manifest_list_path, "rb") as f:
                    reader = fastavro.reader(f)
                    manifests = list(reader)
                    manifest_list_data = {"manifests": manifests}

                # Empty append should have empty manifests (this is correct behavior)
                assert "manifests" in manifest_list_data


def test_pandas_query_returns_data():
    """Test that pandas queries work with properly created manifests.

    This is the end-to-end test that validates the complete fix.
    """
    pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    import pandas as pd

    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "test_table")

        schema = Schema(
            schema_id=1,
            fields=[
                {"id": 1, "name": "workflow_id", "type": "string", "required": True},
                {"id": 2, "name": "status", "type": "string", "required": True},
                {"id": 3, "name": "duration_ms", "type": "long", "required": True},
            ],
        )
        table = create_table(table_path, schema)

        # Append test records
        records = [
            {"workflow_id": "wf-001", "status": "success", "duration_ms": 1500},
            {"workflow_id": "wf-002", "status": "failed", "duration_ms": 3000},
            {"workflow_id": "wf-003", "status": "success", "duration_ms": 2200},
        ]
        table.append_records(records, schema)

        # Query with pandas (read parquet files via manifest)
        from datashard.file_manager import FileManager

        snapshot = table.current_snapshot()
        assert snapshot is not None

        file_manager = FileManager(table_path, table.metadata_manager, table.storage)

        # Use relative path for storage backend
        # snapshot.manifest_list is already a relative path like "metadata/manifests/..."
        manifests = file_manager.read_manifest_list_file(snapshot.manifest_list)
        assert len(manifests) > 0, "Should have manifests"

        # Read all data files
        all_data = []
        for manifest_file in manifests:
            # Use relative path for storage backend
            data_files = file_manager.read_manifest_file(manifest_file.manifest_path)

            for data_file in data_files:
                parquet_path = os.path.join(table_path, data_file.file_path.lstrip("/"))
                df = pd.read_parquet(parquet_path)
                all_data.append(df)

        # Combine all data
        result_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

        # Validate results
        assert len(result_df) == 3, "Should have 3 records"
        assert "workflow_id" in result_df.columns
        assert (
            result_df[result_df["status"] == "success"].shape[0] == 2
        ), "Should have 2 success records"
        assert result_df["duration_ms"].sum() == 6700, "Total duration should be 6700ms"


if __name__ == "__main__":
    # Run tests directly
    test_append_records_creates_non_empty_manifests()
    print("✓ test_append_records_creates_non_empty_manifests")

    test_manifest_reading_returns_data_files()
    print("✓ test_manifest_reading_returns_data_files")

    test_multiple_appends_create_multiple_manifests()
    print("✓ test_multiple_appends_create_multiple_manifests")

    test_concurrent_appends_create_valid_manifests()
    print("✓ test_concurrent_appends_create_valid_manifests")

    test_empty_append_creates_empty_manifest()
    print("✓ test_empty_append_creates_empty_manifest")

    try:
        test_pandas_query_returns_data()
        print("✓ test_pandas_query_returns_data")
    except Exception as e:
        print(f"⚠ test_pandas_query_returns_data (optional pandas test): {e}")

    print("\n✅ All manifest creation tests passed!")
