"""
Main test suite for pytest
"""

import os
import sys

import pytest

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Get current directory
tests_dir = os.path.dirname(os.path.abspath(__file__))
if str(tests_dir) not in sys.path:
    sys.path.insert(0, str(tests_dir))

import clear_demo  # noqa: E402
import test_data_operations as data_ops_module  # noqa: E402
import test_file_management as file_mgmt_module  # noqa: E402
import test_iceberg  # noqa: E402
import test_occ  # noqa: E402


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
