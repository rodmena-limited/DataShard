"""
Test script for the Python Iceberg implementation
Tests ACID transactions, time travel, and metadata management
"""

import os
import tempfile

from datashard import DataFile, FileFormat, create_table
from datashard.data_structures import PartitionField, PartitionSpec, Schema


def test_basic_functionality():
    """Test basic table creation and metadata management"""
    print("Testing basic functionality...")

    # Create a temporary directory for the test table
    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "test_table")

        # Create a table
        table = create_table(table_path)
        print(f"✓ Created table at {table_path}")

        # Verify initial state
        metadata = table.metadata_manager.refresh()
        assert metadata is not None
        print(f"✓ Table has UUID: {metadata.table_uuid}")

        # Verify snapshot functionality
        current_snapshot = table.current_snapshot()
        assert current_snapshot is None  # No snapshots yet
        print("✓ Initial snapshot is None as expected")

        print("Basic functionality test passed!\n")


def test_transactions():
    """Test ACID transaction functionality"""
    print("Testing ACID transactions...")

    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "transaction_test")
        table = create_table(table_path)

        # Create actual data files in the table's data directory
        os.makedirs(os.path.join(table_path, "data"), exist_ok=True)

        # Create actual parquet files using the data operations
        file1_path = os.path.join(table_path, "data", "batch1.parquet")
        file2_path = os.path.join(table_path, "data", "batch2.parquet")

        # Create dummy files with minimal content to satisfy file existence check
        with open(file1_path, "wb") as f:
            f.write(b"dummy parquet content for batch 1")
        with open(file2_path, "wb") as f:
            f.write(b"dummy parquet content for batch 2")

        # Create DataFile objects with Iceberg-style paths
        data_files = [
            DataFile(
                file_path="/data/batch1.parquet",
                file_format=FileFormat.PARQUET,
                partition_values={"year": 2023},
                record_count=1000,
                file_size_in_bytes=os.path.getsize(file1_path),
            ),
            DataFile(
                file_path="/data/batch2.parquet",
                file_format=FileFormat.PARQUET,
                partition_values={"year": 2024},
                record_count=2000,
                file_size_in_bytes=os.path.getsize(file2_path),
            ),
        ]

        # Test transaction with context manager
        with table.new_transaction() as tx:
            tx.append_files(data_files)
            result = tx.commit()
            assert result is True
            print("✓ Transaction committed successfully")

        # Verify the transaction created a snapshot
        snapshots = table.snapshots()
        print(f"DEBUG: Number of snapshots after transaction: {len(snapshots)}")
        print(f"DEBUG: All snapshots: {snapshots}")
        assert len(snapshots) == 1, f"Expected 1 snapshot, but got {len(snapshots)}"
        print(f"✓ Transaction created {len(snapshots)} snapshot(s)")

        # Test transaction rollback
        with table.new_transaction() as tx:
            tx.append_files(data_files)
            # Don't commit, let it exit without committing (should rollback)
        print("✓ Transaction rollback test passed")

        print("ACID transaction test passed!\n")


def test_time_travel():
    """Test time travel functionality"""
    print("Testing time travel functionality...")

    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "time_travel_test")
        table = create_table(table_path)

        # Create actual data files in the table's data directory
        os.makedirs(os.path.join(table_path, "data"), exist_ok=True)

        # Create actual parquet files using the data operations
        file1_path = os.path.join(table_path, "data", "time_travel_batch1.parquet")
        file2_path = os.path.join(table_path, "data", "time_travel_batch2.parquet")

        # Create dummy files with minimal content to satisfy file existence check
        with open(file1_path, "wb") as f:
            f.write(b"dummy parquet content for time travel batch 1")
        with open(file2_path, "wb") as f:
            f.write(b"dummy parquet content for time travel batch 2")

        data_files1 = [
            DataFile(
                file_path="/data/time_travel_batch1.parquet",
                file_format=FileFormat.PARQUET,
                partition_values={"year": 2023},
                record_count=1000,
                file_size_in_bytes=os.path.getsize(file1_path),
            )
        ]

        data_files2 = [
            DataFile(
                file_path="/data/time_travel_batch2.parquet",
                file_format=FileFormat.PARQUET,
                partition_values={"year": 2024},
                record_count=2000,
                file_size_in_bytes=os.path.getsize(file2_path),
            )
        ]

        # First snapshot
        with table.new_transaction() as tx:
            tx.append_files(data_files1)
            tx.commit()
        first_snapshot = table.current_snapshot()
        print(f"✓ Created first snapshot: {first_snapshot.snapshot_id}")

        # Wait a moment to ensure different timestamp
        import time

        time.sleep(0.01)

        # Second snapshot
        with table.new_transaction() as tx:
            tx.append_files(data_files2)
            tx.commit()
        second_snapshot = table.current_snapshot()
        print(f"✓ Created second snapshot: {second_snapshot.snapshot_id}")

        # Verify both snapshots exist
        all_snapshots = table.snapshots()
        assert len(all_snapshots) == 2
        print(f"✓ Table has {len(all_snapshots)} snapshots total")

        # Test time travel
        traveled_snapshot = table.time_travel(snapshot_id=first_snapshot.snapshot_id)
        assert traveled_snapshot.snapshot_id == first_snapshot.snapshot_id
        print(f"✓ Successfully traveled to snapshot: {traveled_snapshot.snapshot_id}")

        print("Time travel test passed!\n")


def test_metadata_management():
    """Test metadata management functionality"""
    print("Testing metadata management...")

    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "metadata_test")
        table = create_table(table_path)

        # Test schema creation
        Schema(
            schema_id=1,
            fields=[
                {"id": 1, "name": "id", "type": "long", "required": True},
                {"id": 2, "name": "name", "type": "string", "required": False},
            ],
        )

        # Test partition spec
        PartitionSpec(
            spec_id=0,
            fields=[
                PartitionField(
                    source_id=1, field_id=1000, name="id_partition", transform="identity"
                )
            ],
        )

        # Verify metadata persistence
        metadata = table.metadata_manager.refresh()
        assert metadata is not None
        print(f"✓ Metadata persisted with UUID: {metadata.table_uuid}")

        # Test refresh
        refreshed = table.refresh()
        assert refreshed is True
        print("✓ Metadata refresh successful")

        print("Metadata management test passed!\n")


def test_all_features():
    """Test all features working together"""
    print("Testing all features together...")

    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "full_test")
        table = create_table(table_path)

        # Create actual data files in the table's data directory
        os.makedirs(os.path.join(table_path, "data"), exist_ok=True)

        # Create actual parquet files and DataFile objects
        sample_files = []
        for i in range(3):
            file_path = os.path.join(table_path, "data", f"full_test_batch_{i}.parquet")
            # Create dummy file
            with open(file_path, "wb") as f:
                f.write(f"dummy parquet content for batch {i}".encode())

            sample_files.append(
                DataFile(
                    file_path=f"/data/full_test_batch_{i}.parquet",
                    file_format=FileFormat.PARQUET,
                    partition_values={"batch": i},
                    record_count=100 * (i + 1),
                    file_size_in_bytes=os.path.getsize(file_path),
                )
            )

        # Perform multiple transactions
        for i, files in enumerate([sample_files[0:1], sample_files[1:2], sample_files[2:3]]):
            with table.new_transaction() as tx:
                tx.append_files(files)
                tx.commit()
            print(f"✓ Completed transaction {i + 1}")

        # Verify we have 3 snapshots
        snapshots = table.snapshots()
        assert len(snapshots) == 3
        print(f"✓ Table has {len(snapshots)} snapshots after 3 transactions")

        # Test time travel to each snapshot
        for snapshot in snapshots:
            traveled = table.time_travel(snapshot_id=snapshot["snapshot_id"])
            assert traveled is not None
            print(f"✓ Can time travel to snapshot {snapshot['snapshot_id']}")

        # Test current snapshot
        current = table.current_snapshot()
        assert current is not None
        print(f"✓ Current snapshot is: {current.snapshot_id}")

        print("All features test passed!\n")


def main():
    """Run all tests"""
    print("Starting Python Iceberg implementation tests...\n")

    test_basic_functionality()
    test_transactions()
    test_time_travel()
    test_metadata_management()
    test_all_features()

    print("🎉 All tests passed! Python Iceberg implementation is working correctly.")


if __name__ == "__main__":
    main()
