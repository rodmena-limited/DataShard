"""
Comprehensive tests for scan features:
- Issue #1: Predicate pushdown
- Issue #2: Partition pruning
- Issue #3: Lazy evaluation / streaming
- Issue #4: Parallel reading
"""

import os
import tempfile

from datashard import Schema, create_table
from datashard.data_structures import DataFile, FileFormat
from datashard.filters import (
    FilterExpression,
    FilterOp,
    parse_filter_dict,
    prune_files_by_bounds,
    to_pyarrow_compute_expression,
)

# ============================================================================
# Issue #1: Predicate Pushdown Tests
# ============================================================================


class TestFilterParsing:
    """Test filter expression parsing"""

    def test_parse_equality_filter(self):
        """Test parsing simple equality filter"""
        filters = parse_filter_dict({"status": "active"})
        assert len(filters) == 1
        assert filters[0].column == "status"
        assert filters[0].op == FilterOp.EQ
        assert filters[0].value == "active"

    def test_parse_comparison_filter(self):
        """Test parsing comparison operators"""
        filters = parse_filter_dict({"age": (">", 30)})
        assert len(filters) == 1
        assert filters[0].column == "age"
        assert filters[0].op == FilterOp.GT
        assert filters[0].value == 30

    def test_parse_multiple_filters(self):
        """Test parsing multiple filters"""
        filters = parse_filter_dict({"age": (">=", 25), "status": "active"})
        assert len(filters) == 2

    def test_parse_between_filter(self):
        """Test parsing between filter (expands to 2 conditions)"""
        filters = parse_filter_dict({"age": ("between", (20, 40))})
        assert len(filters) == 2
        # Should have GE and LE conditions
        ops = {f.op for f in filters}
        assert FilterOp.GE in ops
        assert FilterOp.LE in ops

    def test_parse_in_filter(self):
        """Test parsing IN filter"""
        filters = parse_filter_dict({"status": ("in", ["active", "pending"])})
        assert len(filters) == 1
        assert filters[0].op == FilterOp.IN
        assert filters[0].value == ["active", "pending"]

    def test_parse_all_operators(self):
        """Test all supported operators"""
        test_cases = [
            ({"x": ("==", 1)}, FilterOp.EQ),
            ({"x": ("!=", 1)}, FilterOp.NE),
            ({"x": ("<", 1)}, FilterOp.LT),
            ({"x": ("<=", 1)}, FilterOp.LE),
            ({"x": (">", 1)}, FilterOp.GT),
            ({"x": (">=", 1)}, FilterOp.GE),
            ({"x": ("in", [1, 2])}, FilterOp.IN),
            ({"x": ("not_in", [1, 2])}, FilterOp.NOT_IN),
        ]
        for filter_dict, expected_op in test_cases:
            filters = parse_filter_dict(filter_dict)
            assert filters[0].op == expected_op, f"Failed for {filter_dict}"


class TestPyArrowComputeConversion:
    """Test conversion to the PyArrow compute expression used by every scan API"""

    def test_equality_and_comparisons_convert(self):
        expressions = [
            FilterExpression("status", FilterOp.EQ, "active"),
            FilterExpression("age", FilterOp.GT, 30),
            FilterExpression("score", FilterOp.LE, 90.0),
        ]
        assert to_pyarrow_compute_expression(expressions) is not None

    def test_null_ops_convert(self):
        """IS_NULL / IS_NOT_NULL must survive conversion (the removed
        to_pyarrow_filter silently dropped them)."""
        assert to_pyarrow_compute_expression(
            [FilterExpression("name", FilterOp.IS_NULL, None)]
        ) is not None
        assert to_pyarrow_compute_expression(
            [FilterExpression("name", FilterOp.IS_NOT_NULL, None)]
        ) is not None

    def test_empty_expression_list(self):
        assert to_pyarrow_compute_expression([]) is None


class TestPredicatePushdown:
    """Test predicate pushdown in scan()"""

    def test_scan_with_equality_filter(self, table_with_data):
        """Test scan with equality filter"""
        results = table_with_data.scan(filter={"status": "active"})
        assert len(results) == 3
        assert all(r["status"] == "active" for r in results)

    def test_scan_with_comparison_filter(self, table_with_data):
        """Test scan with comparison filter"""
        results = table_with_data.scan(filter={"age": (">", 30)})
        assert len(results) == 2
        assert all(r["age"] > 30 for r in results)

    def test_scan_with_in_filter(self, table_with_data):
        """Test scan with IN filter"""
        results = table_with_data.scan(filter={"status": ("in", ["active", "pending"])})
        assert len(results) == 4
        assert all(r["status"] in ["active", "pending"] for r in results)

    def test_scan_with_between_filter(self, table_with_data):
        """Test scan with between filter"""
        results = table_with_data.scan(filter={"age": ("between", (26, 32))})
        assert all(26 <= r["age"] <= 32 for r in results)

    def test_scan_with_multiple_filters(self, table_with_data):
        """Test scan with multiple filters (AND)"""
        results = table_with_data.scan(filter={"status": "active", "age": (">=", 30)})
        assert all(r["status"] == "active" and r["age"] >= 30 for r in results)

    def test_scan_with_column_projection_and_filter(self, table_with_data):
        """Test scan with both column projection and filter"""
        results = table_with_data.scan(
            columns=["id", "name", "status"], filter={"status": "active"}
        )
        assert len(results) == 3
        # Should only have projected columns
        for r in results:
            assert "id" in r
            assert "name" in r
            assert "status" in r
            # age and score should not be present
            assert "age" not in r
            assert "score" not in r

    def test_scan_no_filter_returns_all(self, table_with_data):
        """Test that scan without filter returns all records"""
        results = table_with_data.scan()
        assert len(results) == 5


class TestToPandasWithFilter:
    """Test to_pandas() with filter parameter"""

    def test_to_pandas_with_filter(self, table_with_data):
        """Test to_pandas with filter"""
        df = table_with_data.to_pandas(filter={"status": "active"})
        assert len(df) == 3
        assert all(df["status"] == "active")

    def test_to_pandas_with_column_and_filter(self, table_with_data):
        """Test to_pandas with column projection and filter"""
        df = table_with_data.to_pandas(
            columns=["id", "name"], filter={"age": (">", 28)}
        )
        assert "id" in df.columns
        assert "name" in df.columns
        assert "age" not in df.columns


# ============================================================================
# Issue #2: Partition Pruning Tests
# ============================================================================


class TestColumnBoundsComputation:
    """Test that column bounds are computed during write"""

    def test_bounds_stored_in_manifest(self, table_with_data):
        """Test that lower/upper bounds are stored in manifest"""
        data_files = table_with_data._get_data_files_from_manifest()
        assert len(data_files) > 0

        # At least one file should have bounds
        has_bounds = any(
            df.lower_bounds is not None and df.upper_bounds is not None
            for df in data_files
        )
        assert has_bounds, "No files have column bounds computed"

    def test_bounds_values_correct(self):
        """Test that bounds values are correct"""
        with tempfile.TemporaryDirectory() as temp_dir:
            table_path = os.path.join(temp_dir, "test_table")
            table = create_table(table_path)

            schema = Schema(
                schema_id=0,
                fields=[
                    {"id": 1, "name": "id", "type": "long", "required": True},
                    {"id": 2, "name": "value", "type": "int", "required": False},
                ],
            )

            records = [
                {"id": 1, "value": 10},
                {"id": 2, "value": 50},
                {"id": 3, "value": 30},
            ]
            table.append_records(records, schema)

            data_files = table._get_data_files_from_manifest()
            assert len(data_files) == 1

            df = data_files[0]
            if df.lower_bounds and df.upper_bounds:
                # field_id 2 is 'value'
                assert df.lower_bounds.get(2) == 10
                assert df.upper_bounds.get(2) == 50


class TestFilePruning:
    """Test file pruning based on column bounds"""

    def test_prune_files_by_equality(self):
        """Test pruning files by equality filter"""
        schema = Schema(
            schema_id=0,
            fields=[{"id": 1, "name": "value", "type": "int", "required": False}],
        )

        # Create mock data files with different bounds
        files = [
            DataFile(
                file_path="/data/f1.parquet",
                file_format=FileFormat.PARQUET,
                partition_values={},
                record_count=100,
                file_size_in_bytes=1000,
                lower_bounds={1: 0},
                upper_bounds={1: 50},
            ),
            DataFile(
                file_path="/data/f2.parquet",
                file_format=FileFormat.PARQUET,
                partition_values={},
                record_count=100,
                file_size_in_bytes=1000,
                lower_bounds={1: 51},
                upper_bounds={1: 100},
            ),
        ]

        # Filter for value == 75 should prune first file
        expressions = [FilterExpression("value", FilterOp.EQ, 75)]
        pruned = prune_files_by_bounds(files, expressions, schema)

        assert len(pruned) == 1
        assert pruned[0].file_path == "/data/f2.parquet"

    def test_prune_files_by_range(self):
        """Test pruning files by range filter"""
        schema = Schema(
            schema_id=0,
            fields=[{"id": 1, "name": "value", "type": "int", "required": False}],
        )

        files = [
            DataFile(
                file_path="/data/f1.parquet",
                file_format=FileFormat.PARQUET,
                partition_values={},
                record_count=100,
                file_size_in_bytes=1000,
                lower_bounds={1: 0},
                upper_bounds={1: 30},
            ),
            DataFile(
                file_path="/data/f2.parquet",
                file_format=FileFormat.PARQUET,
                partition_values={},
                record_count=100,
                file_size_in_bytes=1000,
                lower_bounds={1: 31},
                upper_bounds={1: 60},
            ),
            DataFile(
                file_path="/data/f3.parquet",
                file_format=FileFormat.PARQUET,
                partition_values={},
                record_count=100,
                file_size_in_bytes=1000,
                lower_bounds={1: 61},
                upper_bounds={1: 100},
            ),
        ]

        # Filter for value > 50 should prune first file
        expressions = [FilterExpression("value", FilterOp.GT, 50)]
        pruned = prune_files_by_bounds(files, expressions, schema)

        assert len(pruned) == 2
        paths = {f.file_path for f in pruned}
        assert "/data/f2.parquet" in paths
        assert "/data/f3.parquet" in paths

    def test_prune_files_no_bounds(self):
        """Test that files without bounds are not pruned"""
        schema = Schema(
            schema_id=0,
            fields=[{"id": 1, "name": "value", "type": "int", "required": False}],
        )

        files = [
            DataFile(
                file_path="/data/f1.parquet",
                file_format=FileFormat.PARQUET,
                partition_values={},
                record_count=100,
                file_size_in_bytes=1000,
                lower_bounds=None,
                upper_bounds=None,
            ),
        ]

        expressions = [FilterExpression("value", FilterOp.EQ, 999)]
        pruned = prune_files_by_bounds(files, expressions, schema)

        # File without bounds should not be pruned (conservative)
        assert len(pruned) == 1
