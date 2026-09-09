"""
Streaming reads: scan_batches / iter_records / iter_pandas (split out of table_scan.py
for the 500-line file cap).

These iterate a table without materialising it: a batch at a time out of each data
file, in the same integrity mode a scan uses.
"""

from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

from .data_structures import DataFile, Schema, TableMetadata
from .logging_config import get_logger

logger = get_logger(__name__)


class _StreamMixin:
    """Mixed into Table (with _ScanMixin, which provides the helpers below)."""

    metadata_manager: Any
    file_manager: Any
    storage: Any

    if TYPE_CHECKING:  # provided by Table / _ScanMixin

        def _get_all_data_files(
            self, metadata: Optional[TableMetadata] = None, snapshot_id: Optional[int] = None
        ) -> List[DataFile]: ...

        def _get_current_schema(self, metadata: Optional[TableMetadata] = None) -> Optional[Schema]: ...

        @staticmethod
        def _resolve_verify_mode(param: Any) -> str: ...

        @staticmethod
        def _as_corruption(data_file: DataFile, exc: BaseException) -> Optional[Exception]: ...

        @staticmethod
        def _prune_by_partition(
            data_files: List[DataFile], expressions: List[Any], metadata: Any, schema: Schema
        ) -> List[DataFile]: ...

    def scan_batches(
        self,
        batch_size: int = 10000,
        columns: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        verify_checksums: Optional[Union[bool, str]] = None,
        snapshot_id: Optional[int] = None,
    ) -> Iterator[List[Dict[str, Any]]]:
        """Scan data in batches for memory-efficient processing.

        Yields batches of records, processing one parquet file at a time
        using PyArrow's iter_batches for memory efficiency. Uses the same
        filter engine (and error semantics) as scan().

        Args:
            batch_size: Approximate number of records per batch
            columns: Optional column projection
            filter: Optional predicate pushdown filter
            verify_checksums: As in scan().

        Yields:
            List of records (dicts) per batch
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        from .filters import (
            parse_filter_dict,
            prune_files_by_bounds,
            to_pyarrow_compute_expression,
        )

        metadata = self.metadata_manager.refresh()
        data_files = self._get_all_data_files(metadata, snapshot_id)

        expressions = parse_filter_dict(filter) if filter else []
        if expressions and data_files:
            schema = self._get_current_schema(metadata)
            if schema:
                data_files = prune_files_by_bounds(data_files, expressions, schema)

        if not data_files:
            return

        compute_expr = to_pyarrow_compute_expression(expressions) if expressions else None
        mode = self._resolve_verify_mode(verify_checksums)

        yield from self._iter_file_batches(
            data_files, batch_size, columns, compute_expr, mode, pa, pq
        )

    def _iter_file_batches(
        self,
        data_files: List[DataFile],
        batch_size: int,
        columns: Optional[List[str]],
        compute_expr: Any,
        verify: Any,
        pa: Any,
        pq: Any,
    ) -> Iterator[List[Dict[str, Any]]]:
        """Iterate over batches from data files. Read errors propagate.

        In the default "page" mode a file is streamed and each page's CRC is
        checked as it is read - nothing is materialised whole (#66). Only "full"
        mode has to download a file completely to hash it.
        """
        from io import BytesIO

        from .integrity import CorruptDataError, IntegrityChecker

        mode = verify if isinstance(verify, str) else self._resolve_verify_mode(verify)
        data_file_manager = self.file_manager.data_file_manager

        # When filtering, read every column (the predicate may reference one not
        # in `columns`); project down to `columns` only after filtering.
        read_columns = None if compute_expr is not None else columns

        def batches(pf: Any, threads: bool) -> Iterator[List[Dict[str, Any]]]:
            for batch in pf.iter_batches(batch_size=batch_size, columns=read_columns, use_threads=threads):
                table = pa.Table.from_batches([batch])
                if compute_expr is not None:
                    table = table.filter(compute_expr)
                    if columns is not None:
                        table = table.select(columns)
                if table.num_rows > 0:
                    yield table.to_pylist()

        for data_file in data_files:
            try:
                if mode == "full" and data_file.checksum:
                    rel_path = data_file.file_path.lstrip("/")
                    raw = self.storage.read_file(rel_path)
                    if not IntegrityChecker.verify_checksum(raw, data_file.checksum):
                        raise CorruptDataError(
                            f"Checksum mismatch for data file {data_file.file_path}"
                        )
                    yield from batches(pq.ParquetFile(BytesIO(raw)), False)
                    continue
                with data_file_manager.parquet_source(data_file.file_path) as (src, threads):
                    pf = pq.ParquetFile(src, page_checksum_verification=mode != "off")
                    yield from batches(pf, threads)
            except (pa.ArrowException, OSError) as e:
                corrupt = self._as_corruption(data_file, e)
                if corrupt is not None:
                    raise corrupt from e
                raise

    def iter_records(
        self,
        columns: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        verify_checksums: Optional[Union[bool, str]] = None,
        snapshot_id: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Iterate over records one at a time.

        Memory efficient - only one batch in memory at a time.
        Ideal for row-by-row processing of large tables.

        Args:
            columns: Optional column projection
            filter: Optional predicate pushdown filter
            verify_checksums: As in scan().

        Yields:
            Individual records as dicts
        """
        for batch in self.scan_batches(
            batch_size=1000, columns=columns, filter=filter, verify_checksums=verify_checksums,
            snapshot_id=snapshot_id,
        ):
            for record in batch:
                yield record

    def iter_pandas(
        self,
        chunksize: int = 50000,
        columns: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        verify_checksums: Optional[Union[bool, str]] = None,
        snapshot_id: Optional[int] = None,
    ) -> Iterator[Any]:
        """Iterate over data as pandas DataFrame chunks.

        Memory efficient - only one chunk in memory at a time.
        Ideal for processing large tables with pandas operations.

        Args:
            chunksize: Approximate rows per chunk
            columns: Optional column projection
            filter: Optional predicate pushdown filter
            verify_checksums: As in scan().

        Yields:
            pandas DataFrame chunks

        Raises:
            ImportError: If pandas is not installed.
        """
        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError(
                "pandas is required for iter_pandas(). Install with: pip install pandas"
            ) from e

        for batch in self.scan_batches(
            batch_size=chunksize, columns=columns, filter=filter, verify_checksums=verify_checksums,
            snapshot_id=snapshot_id,
        ):
            yield pd.DataFrame(batch)

