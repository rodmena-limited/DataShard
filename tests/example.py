"""
Example usage of the Python Iceberg implementation
"""
import os
import sys
import tempfile

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import time

from iceberg import DataFile, FileFormat, create_table


def main():
    print("Python Iceberg Implementation Example")
    print("=====================================")

    # Create a temporary table for the example
    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "example_table")
        print(f"\n1. Creating table at: {table_path}")

        # Create the table
        table = create_table(table_path)
        print("✓ Table created successfully")

        # Show initial state
        print("\n2. Initial table metadata:")
        metadata = table.metadata_manager.refresh()
        print(f"   - Table UUID: {metadata.table_uuid}")
        print(f"   - Current snapshot ID: {metadata.current_snapshot_id}")

        print("\n3. Adding data to the table")
        # Create actual data files in the table's data directory
        os.makedirs(os.path.join(table_path, "data"), exist_ok=True)

        # Create actual parquet files using the data operations
        file1_path = os.path.join(table_path, "data", "users_2023.parquet")
        file2_path = os.path.join(table_path, "data", "users_2024.parquet")

        # Create dummy files with minimal content to satisfy file existence check
        with open(file1_path, 'wb') as f:
            f.write(b'dummy parquet content for users 2023')
        with open(file2_path, 'wb') as f:
            f.write(b'dummy parquet content for users 2024')

        # Create DataFile objects with Iceberg-style paths
        data_files = [
            DataFile(
                file_path="/data/users_2023.parquet",
                file_format=FileFormat.PARQUET,
                partition_values={"year": 2023},
                record_count=5000,
                file_size_in_bytes=os.path.getsize(file1_path)
            ),
            DataFile(
                file_path="/data/users_2024.parquet",
                file_format=FileFormat.PARQUET,
                partition_values={"year": 2024},
                record_count=7500,
                file_size_in_bytes=os.path.getsize(file2_path)
            )
        ]

        print("   Adding 2 data files to the table...")
        with table.new_transaction() as tx:
            tx.append_files(data_files)
            tx.commit()
        print("✓ Transaction committed successfully")

        # Show snapshot information
        print("\n4. Snapshot information after first transaction:")
        snapshots = table.snapshots()
        print(f"   - Total snapshots: {len(snapshots)}")
        for snap in snapshots:
            print(f"   - Snapshot ID: {snap['snapshot_id']}")
            print(f"     Operation: {snap['operation']}")
            print(f"     Timestamp: {snap['timestamp']}")
            print(f"     Parent ID: {snap['parent_id']}")

        # Add more data to create another snapshot
        print("\n5. Adding more data to create a second snapshot")
        file3_path = os.path.join(table_path, "data", "users_2025.parquet")
        with open(file3_path, 'wb') as f:
            f.write(b'dummy parquet content for users 2025')

        more_files = [
            DataFile(
                file_path="/data/users_2025.parquet",
                file_format=FileFormat.PARQUET,
                partition_values={"year": 2025},
                record_count=10000,
                file_size_in_bytes=os.path.getsize(file3_path)
            )
        ]

        time.sleep(0.01)  # Small delay to ensure different timestamp
        with table.new_transaction() as tx:
            tx.append_files(more_files)
            tx.commit()
        print("✓ Second transaction committed")

        print(f"\n6. Table now has {len(table.snapshots())} snapshots")

        # Demonstrate time travel
        print("\n7. Time travel demonstration:")
        first_snapshot = snapshots[0]
        print(f"   Traveling to first snapshot: {first_snapshot['snapshot_id']}")
        traveled = table.time_travel(snapshot_id=first_snapshot['snapshot_id'])
        print(f"   ✓ Traveled to snapshot: {traveled.snapshot_id}")

        # Show current state
        current = table.current_snapshot()
        print(f"\n8. Current snapshot: {current.snapshot_id}")
        print(f"   - Timestamp: {current.timestamp_ms}")
        print(f"   - Operation: {current.operation}")

        print("\n🎉 Example completed successfully!")
        print("\nKey features demonstrated:")
        print("  - Table creation and metadata management")
        print("  - ACID transactions with commit/rollback")
        print("  - Multiple snapshots creation")
        print("  - Time travel functionality")
        print("  - Snapshot history tracking")


if __name__ == "__main__":
    main()
