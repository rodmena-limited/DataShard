"""
Scan feature tests, part 2 (split from test_scan_features.py for the 500-line cap):
partition-pruning integration, streaming (scan_batches / iter_*), parallel reads,
compute-expression conversion, edge cases and performance sanity checks.
Fixtures live in conftest.py.
"""

import time

import pytest

from datashard.filters import (
    FilterExpression,
    FilterOp,
    to_pyarrow_compute_expression,
)


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
