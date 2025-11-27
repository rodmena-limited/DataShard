"""
Comprehensive tests for scan features:
- Issue #1: Predicate pushdown
- Issue #2: Partition pruning
- Issue #3: Lazy evaluation / streaming
- Issue #4: Parallel reading
"""

import os
import tempfile
import time

import pytest

from datashard import Schema, create_table
from datashard.data_structures import DataFile, FileFormat
from datashard.filters import (
    FilterExpression,
    FilterOp,
    parse_filter_dict,
    prune_files_by_bounds,
    to_pyarrow_compute_expression,
    to_pyarrow_filter,
)

# ============================================================================
# Test Fixtures
# ============================================================================


@pytest.fixture
def temp_table():
    """Create a temporary table for testing"""
    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "test_table")
        table = create_table(table_path)
        yield table


@pytest.fixture
def table_with_data():
    """Create a table with test data"""
    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "test_table")
        table = create_table(table_path)

        schema = Schema(
            schema_id=0,
            fields=[
                {"id": 1, "name": "id", "type": "long", "required": True},
                {"id": 2, "name": "name", "type": "string", "required": False},
                {"id": 3, "name": "age", "type": "int", "required": False},
                {"id": 4, "name": "status", "type": "string", "required": False},
                {"id": 5, "name": "score", "type": "double", "required": False},
            ],
        )

        # Insert test records
        records = [
            {"id": 1, "name": "Alice", "age": 30, "status": "active", "score": 95.5},
            {"id": 2, "name": "Bob", "age": 25, "status": "inactive", "score": 82.0},
            {"id": 3, "name": "Charlie", "age": 35, "status": "active", "score": 88.5},
            {"id": 4, "name": "Diana", "age": 28, "status": "pending", "score": 91.0},
            {"id": 5, "name": "Eve", "age": 32, "status": "active", "score": 77.5},
        ]

        table.append_records(records, schema)
        yield table


@pytest.fixture
def table_with_multiple_files():
    """Create a table with multiple parquet files for parallel/pruning tests"""
    with tempfile.TemporaryDirectory() as temp_dir:
        table_path = os.path.join(temp_dir, "test_table")
        table = create_table(table_path)

        schema = Schema(
            schema_id=0,
            fields=[
                {"id": 1, "name": "id", "type": "long", "required": True},
                {"id": 2, "name": "value", "type": "int", "required": False},
                {"id": 3, "name": "category", "type": "string", "required": False},
            ],
        )

        # Create multiple batches (each becomes a separate file)
        for batch_num in range(5):
            records = [
                {
                    "id": batch_num * 100 + i,
                    "value": batch_num * 10 + i,
                    "category": f"cat_{batch_num}",
                }
                for i in range(20)
            ]
            table.append_records(records, schema)

        yield table


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


class TestPyArrowFilterConversion:
    """Test conversion to PyArrow filter format"""

    def test_to_pyarrow_filter_equality(self):
        """Test conversion of equality filter"""
        expressions = [FilterExpression("status", FilterOp.EQ, "active")]
        pa_filters = to_pyarrow_filter(expressions)
        assert pa_filters == [("status", "==", "active")]

    def test_to_pyarrow_filter_comparison(self):
        """Test conversion of comparison filters"""
        expressions = [
            FilterExpression("age", FilterOp.GT, 30),
            FilterExpression("score", FilterOp.LE, 90.0),
        ]
        pa_filters = to_pyarrow_filter(expressions)
        assert ("age", ">", 30) in pa_filters
        assert ("score", "<=", 90.0) in pa_filters

    def test_to_pyarrow_filter_empty(self):
        """Test empty filter list"""
        pa_filters = to_pyarrow_filter([])
        assert pa_filters is None


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


class TestPartitionPruningIntegration:
    """Integration tests for partition pruning in scan()"""

    def test_scan_prunes_files(self, table_with_multiple_files):
        """Test that scan prunes files based on bounds"""
        # Get all records to establish baseline
        all_records = table_with_multiple_files.scan()

        # Filter for very high values (should filter out most records)
        # Each batch has values: batch_num * 10 + i for i in 0..19
        # So batch 0: 0-19, batch 1: 10-29, batch 2: 20-39, batch 3: 30-49, batch 4: 40-59
        # Filter value >= 55 should only match batch 4 records (55, 56, 57, 58, 59)
        filtered = table_with_multiple_files.scan(filter={"value": (">=", 55)})

        # Filtered results should be fewer than all records
        assert len(filtered) < len(all_records)
        assert all(r["value"] >= 55 for r in filtered)
        # Should only have 5 records (55, 56, 57, 58, 59 from batch 4)
        assert len(filtered) == 5


# ============================================================================
# Issue #3: Lazy Evaluation / Streaming Tests
# ============================================================================


class TestScanBatches:
    """Test scan_batches() for streaming reads"""

    def test_scan_batches_yields_batches(self, table_with_data):
        """Test that scan_batches yields batches"""
        batches = list(table_with_data.scan_batches(batch_size=2))
        assert len(batches) > 0

        # Each batch should be a list of dicts
        for batch in batches:
            assert isinstance(batch, list)
            for record in batch:
                assert isinstance(record, dict)

    def test_scan_batches_total_records(self, table_with_data):
        """Test that scan_batches returns all records"""
        total_records = 0
        for batch in table_with_data.scan_batches(batch_size=2):
            total_records += len(batch)

        all_records = table_with_data.scan()
        assert total_records == len(all_records)

    def test_scan_batches_with_filter(self, table_with_data):
        """Test scan_batches with filter"""
        filtered_records = []
        for batch in table_with_data.scan_batches(
            batch_size=2, filter={"status": "active"}
        ):
            filtered_records.extend(batch)

        assert all(r["status"] == "active" for r in filtered_records)

    def test_scan_batches_with_columns(self, table_with_data):
        """Test scan_batches with column projection"""
        for batch in table_with_data.scan_batches(batch_size=2, columns=["id", "name"]):
            for record in batch:
                assert "id" in record
                assert "name" in record
                assert "age" not in record


class TestIterRecords:
    """Test iter_records() for row-by-row iteration"""

    def test_iter_records_yields_dicts(self, table_with_data):
        """Test that iter_records yields individual dicts"""
        count = 0
        for record in table_with_data.iter_records():
            assert isinstance(record, dict)
            count += 1

        assert count == 5

    def test_iter_records_with_filter(self, table_with_data):
        """Test iter_records with filter"""
        records = list(table_with_data.iter_records(filter={"status": "active"}))
        assert len(records) == 3
        assert all(r["status"] == "active" for r in records)


class TestIterPandas:
    """Test iter_pandas() for DataFrame chunk iteration"""

    def test_iter_pandas_yields_dataframes(self, table_with_data):
        """Test that iter_pandas yields DataFrames"""
        import pandas as pd

        chunks = list(table_with_data.iter_pandas(chunksize=2))
        assert len(chunks) > 0

        for chunk in chunks:
            assert isinstance(chunk, pd.DataFrame)

    def test_iter_pandas_with_filter(self, table_with_data):
        """Test iter_pandas with filter"""
        import pandas as pd

        all_chunks = list(
            table_with_data.iter_pandas(chunksize=10, filter={"status": "active"})
        )
        combined = pd.concat(all_chunks, ignore_index=True) if all_chunks else pd.DataFrame()

        assert len(combined) == 3
        assert all(combined["status"] == "active")


# ============================================================================
# Issue #4: Parallel Reading Tests
# ============================================================================


class TestParallelScan:
    """Test parallel reading in scan()"""

    def test_scan_parallel_same_results(self, table_with_multiple_files):
        """Test that parallel scan returns same results as sequential"""
        sequential = sorted(
            table_with_multiple_files.scan(parallel=False), key=lambda r: r["id"]
        )
        parallel = sorted(
            table_with_multiple_files.scan(parallel=True), key=lambda r: r["id"]
        )

        assert len(sequential) == len(parallel)
        assert sequential == parallel

    def test_scan_parallel_with_worker_count(self, table_with_multiple_files):
        """Test parallel scan with specific worker count"""
        results = table_with_multiple_files.scan(parallel=2)
        all_results = table_with_multiple_files.scan()

        assert len(results) == len(all_results)

    def test_scan_parallel_with_filter(self, table_with_multiple_files):
        """Test parallel scan with filter"""
        sequential = sorted(
            table_with_multiple_files.scan(filter={"value": (">=", 20)}, parallel=False),
            key=lambda r: r["id"],
        )
        parallel = sorted(
            table_with_multiple_files.scan(filter={"value": (">=", 20)}, parallel=True),
            key=lambda r: r["id"],
        )

        assert sequential == parallel


class TestParallelToPandas:
    """Test parallel reading in to_pandas()"""

    def test_to_pandas_parallel_same_results(self, table_with_multiple_files):
        """Test that parallel to_pandas returns same results as sequential"""
        sequential = (
            table_with_multiple_files.to_pandas(parallel=False)
            .sort_values("id")
            .reset_index(drop=True)
        )
        parallel = (
            table_with_multiple_files.to_pandas(parallel=True)
            .sort_values("id")
            .reset_index(drop=True)
        )

        assert len(sequential) == len(parallel)
        assert sequential.equals(parallel)

    def test_to_pandas_parallel_with_filter(self, table_with_multiple_files):
        """Test parallel to_pandas with filter"""
        df = table_with_multiple_files.to_pandas(
            filter={"value": ("<", 30)}, parallel=True
        )
        assert all(df["value"] < 30)


# ============================================================================
# PyArrow Compute Expression Tests
# ============================================================================


class TestPyArrowComputeExpression:
    """Test PyArrow compute expression generation"""

    def test_compute_expression_equality(self):
        """Test compute expression for equality"""
        expressions = [FilterExpression("x", FilterOp.EQ, 10)]
        expr = to_pyarrow_compute_expression(expressions)
        assert expr is not None

    def test_compute_expression_comparison(self):
        """Test compute expression for comparison"""
        expressions = [
            FilterExpression("x", FilterOp.GT, 5),
            FilterExpression("x", FilterOp.LT, 15),
        ]
        expr = to_pyarrow_compute_expression(expressions)
        assert expr is not None

    def test_compute_expression_in(self):
        """Test compute expression for IN"""
        expressions = [FilterExpression("x", FilterOp.IN, [1, 2, 3])]
        expr = to_pyarrow_compute_expression(expressions)
        assert expr is not None


# ============================================================================
# Edge Cases and Error Handling
# ============================================================================


class TestEdgeCases:
    """Test edge cases and error handling"""

    def test_scan_empty_table(self, temp_table):
        """Test scan on empty table"""
        results = temp_table.scan()
        assert results == []

    def test_scan_with_filter_no_matches(self, table_with_data):
        """Test scan with filter that matches nothing"""
        results = table_with_data.scan(filter={"age": (">", 1000)})
        assert results == []

    def test_scan_batches_empty_table(self, temp_table):
        """Test scan_batches on empty table"""
        batches = list(temp_table.scan_batches())
        assert batches == []

    def test_iter_records_empty_table(self, temp_table):
        """Test iter_records on empty table"""
        records = list(temp_table.iter_records())
        assert records == []

    def test_to_pandas_empty_table(self, temp_table):
        """Test to_pandas on empty table"""
        df = temp_table.to_pandas()
        assert len(df) == 0


# ============================================================================
# Performance Sanity Checks
# ============================================================================


class TestPerformance:
    """Basic performance sanity checks"""

    def test_parallel_not_slower_than_sequential(self, table_with_multiple_files):
        """Verify parallel reading isn't significantly slower than sequential"""
        # Run sequential
        start = time.time()
        _ = table_with_multiple_files.scan(parallel=False)
        sequential_time = time.time() - start

        # Run parallel
        start = time.time()
        _ = table_with_multiple_files.scan(parallel=True)
        parallel_time = time.time() - start

        # Parallel shouldn't be more than 3x slower (accounting for thread overhead on small data)
        assert parallel_time < sequential_time * 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
