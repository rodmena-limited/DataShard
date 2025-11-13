"""
Basic tests for the datashard package
"""
import pytest
import tempfile
import os
from src.datashard.iceberg import create_table


def test_package_imports():
    """Test that the package can be imported"""
    from src.datashard import create_table, DataFile, FileFormat
    assert create_table is not None
    assert DataFile is not None
    assert FileFormat is not None


def test_table_creation():
    """Test that a table can be created"""
    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "test_table")
        table = create_table(table_path)
        
        # Basic checks
        assert table is not None
        assert hasattr(table, 'current_snapshot')
        assert hasattr(table, 'new_transaction')


if __name__ == "__main__":
    test_package_imports()
    test_table_creation()
    print("Basic tests passed!")