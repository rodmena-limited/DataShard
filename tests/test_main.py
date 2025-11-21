"""
Main test suite for pytest
"""

import sys
from pathlib import Path

import pytest

# Add tests directory to path for imports
tests_dir = Path(__file__).parent
if str(tests_dir) not in sys.path:
    sys.path.insert(0, str(tests_dir))

import clear_demo
import test_data_operations as data_ops_module
import test_file_management as file_mgmt_module
import test_iceberg
import test_occ


def test_iceberg_functionality():
    """Test core iceberg functionality"""
    try:
        test_iceberg.main()
        assert True
    except Exception as e:
        pytest.fail(f"Core functionality test failed: {e}")


def test_occ_functionality():
    """Test optimistic concurrency control"""
    try:
        test_occ.main()
        assert True
    except Exception as e:
        pytest.fail(f"OCC functionality test failed: {e}")


def test_file_management():
    """Test file management functionality"""
    try:
        file_mgmt_module.main()
        assert True
    except Exception as e:
        pytest.fail(f"File management test failed: {e}")


def test_data_operations():
    """Test data operations functionality"""
    try:
        data_ops_module.main()
        assert True
    except Exception as e:
        pytest.fail(f"Data operations test failed: {e}")


def test_concurrency_demo():
    """Test the concurrency safety demo"""
    try:
        clear_demo.main()
        assert True
    except Exception as e:
        pytest.fail(f"Concurrency demo failed: {e}")


if __name__ == "__main__":
    # Run all tests when executed directly
    test_iceberg_functionality()
    test_occ_functionality()
    test_file_management()
    test_data_operations()
    test_concurrency_demo()
    print("All tests passed!")
