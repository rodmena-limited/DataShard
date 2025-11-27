"""
Test script for file management functionality
"""

import os
import sys
import tempfile

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from datashard import DataFile, FileFormat, create_table


def test_file_validation():
    """Test file validation functionality"""
    print("Testing file validation...")

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a table first
        table_path = os.path.join(temp_dir, "test_table")
        table = create_table(table_path)

        # Create a temporary data file within the table structure
        data_subdir = os.path.join(table_path, "data")
        os.makedirs(data_subdir, exist_ok=True)
        data_file_path = os.path.join(data_subdir, "test.parquet")

        # Write some sample data
        with open(data_file_path, "wb") as f:
            f.write(b"sample parquet data")

        # Test file validation using Iceberg-style path (relative to table)
        iceberg_style_path = "/data/test.parquet"  # relative to table location
        assert table.file_manager.validate_file_exists(iceberg_style_path)
        print("✓ File validation works for existing files")

        # Test non-existent file
        fake_path = "/data/nonexistent.parquet"
        assert not table.file_manager.validate_file_exists(fake_path)
        print("✓ File validation correctly identifies non-existent files")

        print("File validation test passed!")


def test_manifest_creation():
    """Test manifest file creation"""
    print("\nTesting manifest creation...")

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a table first
        table_path = os.path.join(temp_dir, "test_table")
        table = create_table(table_path)

        # Create data files within the table structure
        data_subdir = os.path.join(table_path, "data")
        os.makedirs(data_subdir, exist_ok=True)

        file1_path = os.path.join(data_subdir, "file1.parquet")
        with open(file1_path, "wb") as f:
            f.write(b"data for file 1")

        file2_path = os.path.join(data_subdir, "file2.parquet")
        with open(file2_path, "wb") as f:
            f.write(b"data for file 2")

        # Create data file objects using Iceberg-style paths (relative to table)
        data_files = [
            DataFile(
                file_path="/data/file1.parquet",  # relative to table location
                file_format=FileFormat.PARQUET,
                partition_values={"year": 2023, "month": 1},
                record_count=100,
                file_size_in_bytes=1024,
            ),
            DataFile(
                file_path="/data/file2.parquet",
                file_format=FileFormat.PARQUET,
                partition_values={"year": 2023, "month": 2},
                record_count=200,
                file_size_in_bytes=2048,
            ),
        ]

        # Validate data files exist (this should pass)
        table.file_manager.validate_data_files(data_files)
        print("✓ Data file validation passed")

        # Create manifest file
        manifest = table.file_manager.create_manifest_file(data_files, snapshot_id=12345)
        print(f"✓ Manifest created: {manifest.manifest_path}")
        manifest_abs_path = os.path.join(table_path, manifest.manifest_path)
        assert os.path.exists(
            manifest_abs_path
        ), f"Manifest file should exist at {manifest_abs_path}"

        # Read manifest back
        read_files = table.file_manager.read_manifest_file(manifest.manifest_path)
        print(f"✓ Manifest read back: {len(read_files)} files")
        assert len(read_files) == 2

        print("Manifest creation test passed!")


def test_full_transaction_with_files():
    """Test a full transaction with actual file management"""
    print("\nTesting full transaction with file management...")

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create data files in the proper location
        table_path = os.path.join(temp_dir, "test_table")
        os.makedirs(os.path.join(table_path, "data"), exist_ok=True)

        # Create data files
        file1_path = os.path.join(table_path, "data", "batch1.parquet")
        with open(file1_path, "wb") as f:
            f.write(b"test data batch 1")

        file2_path = os.path.join(table_path, "data", "batch2.parquet")
        with open(file2_path, "wb") as f:
            f.write(b"test data batch 2")

        # Create table
        table = create_table(table_path)

        # Create DataFile objects using relative paths
        data_files = [
            DataFile(
                file_path="/data/batch1.parquet",  # relative to table path
                file_format=FileFormat.PARQUET,
                partition_values={"batch": 1},
                record_count=1000,
                file_size_in_bytes=10240,
            ),
            DataFile(
                file_path="/data/batch2.parquet",
                file_format=FileFormat.PARQUET,
                partition_values={"batch": 2},
                record_count=2000,
                file_size_in_bytes=20480,
            ),
        ]

        # Perform transaction
        with table.new_transaction() as tx:
            tx.append_files(data_files)
            result = tx.commit()
            assert result
            print("✓ Transaction with files committed successfully")

        # Verify the snapshot was created with proper manifest
        snapshots = table.snapshots()
        assert len(snapshots) == 1
        print(f"✓ Snapshot created with ID: {snapshots[0]['snapshot_id']}")

        # Verify current snapshot
        current = table.current_snapshot()
        assert current is not None
        print("✓ Current snapshot is accessible")

        print("Full transaction test passed!")


def test_integrity_verification():
    """Test integrity verification functionality"""
    print("\nTesting integrity verification...")

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create table
        table_path = os.path.join(temp_dir, "test_table")
        os.makedirs(os.path.join(table_path, "data"), exist_ok=True)
        table = create_table(table_path)

        # Create some data files
        file1_path = os.path.join(table_path, "data", "data1.parquet")
        with open(file1_path, "wb") as f:
            f.write(b"data file 1")

        data_files = [
            DataFile(
                file_path="/data/data1.parquet",
                file_format=FileFormat.PARQUET,
                partition_values={"id": 1},
                record_count=100,
                file_size_in_bytes=1024,
            )
        ]

        # Create manifest
        manifest = table.file_manager.create_manifest_file(data_files, snapshot_id=99999)

        # Verify integrity
        integrity_report = table.file_manager.verify_integrity([manifest])
        print(
            f"✓ Integrity check: {integrity_report['existing_files']}/{integrity_report['total_files']} files exist"
        )
        assert integrity_report["existing_files"] == integrity_report["total_files"] == 1
        assert len(integrity_report["missing_files"]) == 0

        # Create a manifest with a non-existent file to test missing file detection
        missing_file = DataFile(
            file_path="/data/missing.parquet",
            file_format=FileFormat.PARQUET,
            partition_values={"id": 2},
            record_count=200,
            file_size_in_bytes=2048,
        )

        missing_manifest = table.file_manager.create_manifest_file(
            [missing_file], snapshot_id=99998
        )
        integrity_report2 = table.file_manager.verify_integrity([missing_manifest])
        assert len(integrity_report2["missing_files"]) == 1
        assert integrity_report2["missing_files"][0] == "/data/missing.parquet"
        print("✓ Integrity check correctly identified missing file")

        print("Integrity verification test passed!")


def main():
    """Run all file management tests"""
    print("Testing File Management Functionality")
    print("=" * 45)

    test_file_validation()
    test_manifest_creation()
    test_full_transaction_with_files()
    test_integrity_verification()

    print("\n🎉 All file management tests passed!")
    print("The implementation now properly handles:")
    print("  - File validation and existence checks")
    print("  - Manifest file creation and reading")
    print("  - Full transaction workflow with file management")
    print("  - Data integrity verification")


if __name__ == "__main__":
    main()
