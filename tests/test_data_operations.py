"""
Test script for data operations functionality
"""

import os
import tempfile

from datashard import create_table
from datashard.data_operations import DataFileManager
from datashard.data_structures import Schema
from datashard.file_manager import FileManager
from datashard.metadata_manager import MetadataManager


def test_data_reader_writer():
    """Test data file reading and writing operations"""
    print("Testing data reader/writer operations...")

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create sample data
        sample_records = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
            {"id": 3, "name": "Charlie", "age": 35},
        ]

        # Create an Iceberg schema
        iceberg_schema = Schema(
            schema_id=1,
            fields=[
                {"id": 1, "name": "id", "type": "long", "required": True},
                {"id": 2, "name": "name", "type": "string", "required": True},
                {"id": 3, "name": "age", "type": "int", "required": True},
            ],
        )

        # Create data file path
        parquet_file = os.path.join(temp_dir, "test.parquet")

        # Test writing data
        from datashard.storage_backend import create_storage_backend

        storage = create_storage_backend(temp_dir)
        metadata_manager = MetadataManager(temp_dir, storage)
        file_manager = FileManager(temp_dir, metadata_manager, storage)
        data_manager = DataFileManager(file_manager, storage)

        data_file = data_manager.write_data_file(
            file_path=parquet_file, records=sample_records, iceberg_schema=iceberg_schema
        )

        print(f"✓ Data file written: {parquet_file}")
        print(f"  - Record count: {data_file.record_count}")
        print(f"  - File size: {data_file.file_size_in_bytes} bytes")

        # Verify file exists and has correct data
        assert os.path.exists(parquet_file)
        assert data_file.record_count == 3

        # Test reading data back
        read_records = data_manager.read_data_file(parquet_file)
        assert len(read_records) == 3
        assert read_records[0]["name"] == "Alice"
        assert read_records[1]["age"] == 25
        assert read_records[2]["id"] == 3

        print(f"✓ Data read back matches original: {len(read_records)} records")

        # Test reading with specific columns
        read_records_subset = data_manager.read_data_file(parquet_file, columns=["id", "name"])
        assert "age" not in read_records_subset[0]  # age should not be present
        assert "id" in read_records_subset[0]  # id should be present
        assert len(read_records_subset[0]) == 2  # only id and name
        print(f"✓ Column selection works: {len(read_records_subset[0])} columns read")

        print("Data reader/writer test passed!")


def test_data_file_manager_integration():
    """Test DataFileManager integration with FileManager"""
    print("\nTesting DataFileManager integration...")

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create table
        table_path = os.path.join(temp_dir, "test_table")
        table = create_table(table_path)

        # Sample data
        sample_records = [
            {"id": 1, "name": "Alice", "department": "Engineering"},
            {"id": 2, "name": "Bob", "department": "Marketing"},
            {"id": 3, "name": "Charlie", "department": "Engineering"},
        ]

        # Create schema
        schema = Schema(
            schema_id=1,
            fields=[
                {"id": 1, "name": "id", "type": "long", "required": True},
                {"id": 2, "name": "name", "type": "string", "required": True},
                {"id": 3, "name": "department", "type": "string", "required": True},
            ],
        )

        # Test append_records method
        result = table.append_records(
            records=sample_records,
            schema=schema,
            partition_values={"department": "Engineering"},  # Example partition
        )
        assert result, "Append records should succeed"
        print("✓ append_records worked successfully")

        # Verify data was written and committed
        current_snapshot = table.current_snapshot()
        assert current_snapshot is not None
        print(f"✓ Transaction created snapshot: {current_snapshot.snapshot_id}")

        # Check that there are files in the data directory
        data_dir = os.path.join(table_path, "data")
        data_files = [f for f in os.listdir(data_dir) if f.endswith(".parquet")]
        assert len(data_files) > 0, "Should have created at least one data file"
        print(f"✓ Created {len(data_files)} data files")

        # Read back the data to verify
        created_file = os.path.join(data_dir, data_files[0])
        read_back = table.file_manager.data_file_manager.read_data_file(created_file)
        assert len(read_back) == 3, "Should have read back all 3 records"
        print(f"✓ Read back {len(read_back)} records from created file")

        print("Data file manager integration test passed!")


def test_schema_compatibility():
    """Test schema validation and compatibility"""
    print("\nTesting schema compatibility...")

    with tempfile.TemporaryDirectory() as temp_dir:
        from datashard.storage_backend import create_storage_backend

        storage = create_storage_backend(temp_dir)
        metadata_manager = MetadataManager(temp_dir, storage)
        file_manager = FileManager(temp_dir, metadata_manager, storage)
        data_manager = DataFileManager(file_manager, storage)

        # Valid records
        valid_records = [{"id": 1, "name": "Alice", "score": 95.5}]

        # Schema with proper types
        schema = Schema(
            schema_id=1,
            fields=[
                {"id": 1, "name": "id", "type": "long", "required": True},
                {"id": 2, "name": "name", "type": "string", "required": True},
                {"id": 3, "name": "score", "type": "double", "required": True},
            ],
        )

        # Test compatibility check
        is_compatible = data_manager.validate_data_compatibility(valid_records, schema)
        assert is_compatible, "Valid records should be compatible with schema"
        print("✓ Schema compatibility validation works")

        # Test data file creation with this schema
        parquet_file = os.path.join(temp_dir, "compat_test.parquet")
        data_manager.write_data_file(
            file_path=parquet_file, records=valid_records, iceberg_schema=schema
        )

        assert os.path.exists(parquet_file), "File should be created"
        print("✓ Data file created with validated schema")

        print("Schema compatibility test passed!")


def main():
    """Run all data operations tests"""
    print("Testing Data Operations Functionality")
    print("=" * 42)

    test_data_reader_writer()
    test_data_file_manager_integration()
    test_schema_compatibility()

    print("\n🎉 All data operations tests passed!")
    print("The implementation now properly handles:")
    print("  - Data file reading and writing (Parquet)")
    print("  - Schema conversion between Iceberg and PyArrow")
    print("  - Integration with table transactions")
    print("  - Schema validation and compatibility checks")


if __name__ == "__main__":
    main()
