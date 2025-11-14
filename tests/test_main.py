"""
Main test suite for pytest
"""
import pytest

from tests.clear_demo import main as demo_main
from tests.test_data_operations import main as test_data_operations_main
from tests.test_file_management import main as test_file_management_main
from tests.test_iceberg import main as test_iceberg_main
from tests.test_occ import main as test_occ_main


def test_iceberg_functionality():
    """Test core iceberg functionality"""
    try:
        test_iceberg_main()
        assert True
    except Exception as e:
        pytest.fail(f"Core functionality test failed: {e}")


def test_occ_functionality():
    """Test optimistic concurrency control"""
    try:
        test_occ_main()
        assert True
    except Exception as e:
        pytest.fail(f"OCC functionality test failed: {e}")


def test_file_management():
    """Test file management functionality"""
    try:
        test_file_management_main()
        assert True
    except Exception as e:
        pytest.fail(f"File management test failed: {e}")


def test_data_operations():
    """Test data operations functionality"""
    try:
        test_data_operations_main()
        assert True
    except Exception as e:
        pytest.fail(f"Data operations test failed: {e}")


def test_concurrency_demo():
    """Test the concurrency safety demo"""
    try:
        demo_main()
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
