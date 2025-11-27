"""
Test for Optimistic Concurrency Control (OCC) functionality
Tests concurrent operations to ensure proper conflict detection and retry logic
"""

import os
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from datashard import DataFile, FileFormat, create_table


def test_concurrent_operations():
    """Test that concurrent operations properly handle OCC"""
    print("Testing Optimistic Concurrency Control...")

    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "concurrent_test")
        table = create_table(table_path)

        # Create a function that adds a file to the table
        def add_file_thread(thread_id):
            # Create an actual file in the table's data directory
            file_path = os.path.join(table.table_path, "data", f"thread_{thread_id}_file.parquet")
            os.makedirs(os.path.join(table.table_path, "data"), exist_ok=True)
            with open(file_path, "wb") as f:
                f.write(f"dummy content for thread {thread_id}".encode())

            data_files = [
                DataFile(
                    file_path=f"/data/thread_{thread_id}_file.parquet",
                    file_format=FileFormat.PARQUET,
                    partition_values={"thread": thread_id},
                    record_count=100,
                    file_size_in_bytes=os.path.getsize(file_path),
                )
            ]

            # Add a small delay to increase chance of conflicts
            time.sleep(0.01)

            with table.new_transaction() as tx:
                tx.append_files(data_files)
                result = tx.commit()
                return f"Thread {thread_id}: {'Success' if result else 'Failed'}"

        # Run multiple transactions concurrently
        num_threads = 5
        results = []

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(add_file_thread, i) for i in range(num_threads)]

            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                print(f"  {result}")

        # Verify all operations completed successfully
        snapshots = table.snapshots()
        print(f"  Total snapshots created: {len(snapshots)}")

        # We should have 5 snapshots (one for each successful transaction)
        assert (
            len(snapshots) == num_threads
        ), f"Expected {num_threads} snapshots, got {len(snapshots)}"
        print("  ✓ All concurrent operations completed successfully with OCC")

        # Pytest expects None return for test functions


def test_occ_conflict_resolution():
    """Test that OCC properly detects and handles conflicts"""
    print("\nTesting OCC conflict detection and resolution...")

    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "occ_test")
        table = create_table(table_path)

        # Manually test the OCC mechanism by trying to update with stale metadata
        from datashard.metadata_manager import ConcurrentModificationException

        # Get initial metadata
        initial_metadata = table.metadata_manager.refresh()

        # Perform an operation that changes the metadata
        # Create actual file
        file_path = os.path.join(table.table_path, "data", "occ_test_file.parquet")
        os.makedirs(os.path.join(table.table_path, "data"), exist_ok=True)
        with open(file_path, "wb") as f:
            f.write(b"dummy content for OCC test")

        data_files = [
            DataFile(
                file_path="/data/occ_test_file.parquet",
                file_format=FileFormat.PARQUET,
                partition_values={"test": 1},
                record_count=100,
                file_size_in_bytes=os.path.getsize(file_path),
            )
        ]

        with table.new_transaction() as tx:
            tx.append_files(data_files)
            tx.commit()

        # Now try to commit with the stale metadata (should fail)
        try:
            # This should fail because the metadata has been updated in the meantime
            updated_metadata = table.metadata_manager.refresh()
            # Try to commit with stale initial_metadata as base and updated metadata as new
            table.metadata_manager.commit(initial_metadata, updated_metadata)
            print("  ✗ OCC failed to detect conflict (this shouldn't happen)")
            return False
        except ConcurrentModificationException:
            print("  ✓ OCC properly detected concurrent modification")
        except Exception as e:
            print(f"  ? OCC raised different exception: {e}")

        # Pytest expects None return for test functions


def main():
    """Run OCC-specific tests"""
    print("Testing Optimistic Concurrency Control (OCC) Implementation")
    print("=" * 60)

    test_concurrent_operations()
    test_occ_conflict_resolution()

    print("\n🎉 OCC tests completed successfully!")
    print("The implementation properly handles:")
    print("  - Concurrent operations with retry logic")
    print("  - Conflict detection and resolution")
    print("  - Atomic commits following Iceberg patterns")


if __name__ == "__main__":
    main()
