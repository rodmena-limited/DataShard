"""
Read path of Table: scan / to_pandas / scan_batches / iter_* and their integrity
modes (mixed into Table; split out of transaction.py for the 500-line file cap, #71).
"""

import os
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from .data_io import read_parquet_table
from .data_structures import DataFile, Schema, TableMetadata
from .file_manager import FileManager
from .logging_config import get_logger
from .metadata_manager import MetadataManager
from .storage_backend import StorageBackend
from .uuid_columns import (
    decode_columns as decode_uuid_columns,
    split_expressions as split_uuid_expressions,
)

logger = get_logger(__name__)

_VERIFY_MODES = ("page", "full", "off")


class _ScanMixin:
    """Everything that reads data files."""

    metadata_manager: MetadataManager
    file_manager: FileManager
    storage: StorageBackend

    if TYPE_CHECKING:  # provided by Table

        def _get_all_data_files(
            self, metadata: Optional[TableMetadata] = None, snapshot_id: Optional[int] = None
        ) -> List[DataFile]: ...

        def _get_current_schema(self, metadata: Optional[TableMetadata] = None) -> Optional[Schema]: ...

    @staticmethod
    def _resolve_verify_mode(param: Any) -> str:
        """Integrity mode for reads.

        - "page" (default): parquet page CRCs, verified on the bytes a read
          actually touches - projection and pushdown keep their I/O savings (#66).
        - "full": the whole-file sha256 recorded at write time; downloads every
          byte of every file. True and the legacy env values true/1/yes/on mean this.
        - "off": no verification (False, or false/0/no/off).
        The default comes from DATASHARD_VERIFY_CHECKSUMS.
        """
        if param is None:
            param = os.getenv("DATASHARD_VERIFY_CHECKSUMS", "page")
        if param is True:
            return "full"
        if param is False:
            return "off"
        mode = str(param).strip().lower()
        mode = {
            "1": "full", "true": "full", "yes": "full", "on": "full",
            "0": "off", "false": "off", "no": "off", "none": "off",
        }.get(mode, mode)
        if mode not in _VERIFY_MODES:
            raise ValueError(f"verify_checksums must be one of {_VERIFY_MODES}, True or False; got {param!r}")
        return mode

    @staticmethod
    def _resolve_verify_checksums(param: Optional[bool]) -> bool:
        """Compatibility shim: True when whole-file verification is selected."""
        return _ScanMixin._resolve_verify_mode(param) == "full"

    @staticmethod
    def _as_corruption(data_file: DataFile, exc: BaseException) -> Optional[Exception]:
        """Map pyarrow's CRC / decompression failures to CorruptDataError."""
        from .integrity import CorruptDataError

        msg = str(exc).lower()
        markers = (
            "crc", "checksum", "corrupt", "decompress", "lz4", "thrift", "magic",
            "footer", "truncated", "unexpected end", "file size is",
        )
        if any(k in msg for k in markers):
            return CorruptDataError(
                f"Data file {data_file.file_path} failed integrity verification: {exc}"
            )
        return None

    def _read_datafile_table(
        self,
        data_file: DataFile,
        columns: Optional[List[str]],
        compute_expr: Any,
        verify: Any,
        pa: Any,
        pq: Any,
    ) -> Any:
        """Read one data file as a pyarrow Table under the given integrity mode.

        Errors propagate: a data file that is referenced by the current snapshot
        but unreadable/corrupt is a table integrity failure, not something to
        silently skip (silent skips make partial results indistinguishable from
        complete ones).
        """
        from io import BytesIO

        from .integrity import CorruptDataError, IntegrityChecker

        mode = verify if isinstance(verify, str) else self._resolve_verify_mode(verify)
        data_file_manager = self.file_manager.data_file_manager

        try:
            if mode == "full" and data_file.checksum:
                rel_path = data_file.file_path.lstrip("/")
                raw = self.storage.read_file(rel_path)
                if not IntegrityChecker.verify_checksum(raw, data_file.checksum):
                    raise CorruptDataError(
                        f"Checksum mismatch for data file {data_file.file_path}: "
                        f"stored data does not match the checksum recorded at write time"
                    )
                # Read all columns, filter, THEN project: the filter may reference
                # a column not in `columns`.
                table = read_parquet_table(BytesIO(raw), use_threads=False)
                if compute_expr is not None:
                    table = table.filter(compute_expr)
                if columns is not None:
                    table = table.select(columns)
                return table

            with data_file_manager.parquet_source(data_file.file_path) as (src, threads):
                kwargs: Dict[str, Any] = {
                    "columns": columns,
                    "use_threads": threads,
                    "page_checksum_verification": mode != "off",
                }
                if compute_expr is not None:
                    # pyarrow applies `filters` against all needed columns during
                    # the scan and returns only `columns`, so pushdown is correct.
                    kwargs["filters"] = compute_expr
                return read_parquet_table(src, **kwargs)
        except (pa.ArrowException, OSError) as e:
            corrupt = self._as_corruption(data_file, e)
            if corrupt is not None:
                raise corrupt from e
            raise

    def _scan_table(
        self,
        columns: Optional[List[str]],
        filter_dict: Optional[Dict[str, Any]],
        parallel: Union[bool, int],
        verify_checksums: Optional[Union[bool, str]],
        snapshot_id: Optional[int] = None,
    ) -> Any:
        """Shared scan core: returns a pyarrow Table, or None for an empty table.

        All filters (including is_null / is_not_null) are applied through a
        single compute-expression engine, so every scan API returns identical
        results for identical filters.
        """
        from concurrent.futures import ThreadPoolExecutor

        import pyarrow as pa
        import pyarrow.parquet as pq

        from .filters import (
            parse_filter_dict,
            prune_files_by_bounds,
            to_pyarrow_compute_expression,
        )

        # ONE metadata read per scan: snapshot and schema come from the same view (#67).
        metadata = self.metadata_manager.refresh()
        data_files = self._get_all_data_files(metadata, snapshot_id)
        if not data_files:
            return None

        schema = self._get_current_schema(metadata)
        expressions = parse_filter_dict(filter_dict) if filter_dict else []
        # A predicate on a uuid column is applied after that column is decoded (#92); the
        # rest is pushed into the parquet reader as before.
        pushdown, after_decode = split_uuid_expressions(expressions, schema)
        compute_expr = to_pyarrow_compute_expression(pushdown) if pushdown else None
        post_expr = to_pyarrow_compute_expression(after_decode) if after_decode else None

        # File-level pruning: whole partitions first (a partitioned table can skip a
        # directory without opening anything), then column bounds within what is left.
        if expressions and schema:
            expressions = pushdown + after_decode
            data_files = self._prune_by_partition(data_files, expressions, metadata, schema)
            data_files = prune_files_by_bounds(data_files, expressions, schema)
        if not data_files:
            return None

        mode = self._resolve_verify_mode(verify_checksums)

        # A column the post-decode predicate needs must be read even when it is not
        # projected, and dropped again afterwards.
        read_columns = columns
        if columns is not None and after_decode:
            read_columns = list(columns) + [
                e.column for e in after_decode if e.column not in columns
            ]

        def read_one(df: DataFile) -> Any:
            table = self._read_datafile_table(df, read_columns, compute_expr, mode, pa, pq)
            # A uuid column is 16 bytes on disk since 0.11.2 and a string before it; both
            # become strings here, per file, so one scan can span both (#92).
            table = decode_uuid_columns(table, schema)
            if post_expr is not None:
                table = table.filter(post_expr)
                if columns is not None:
                    table = table.select(columns)
            return table

        if parallel:
            n_workers = parallel if isinstance(parallel, int) else (os.cpu_count() or 4)
            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                tables = list(executor.map(read_one, data_files))
        else:
            tables = [read_one(df) for df in data_files]

        return self._concat_aligned(tables, pa)

    @staticmethod
    def _prune_by_partition(
        data_files: List[DataFile], expressions: List[Any], metadata: Any, schema: Schema
    ) -> List[DataFile]:
        """Skip whole partitions a filter cannot match (#98). No-op for an unpartitioned
        table, and never removes a file it cannot prove unmatched."""
        from .partition_pruning import prune_files_by_partition
        from .partitioning import spec_field_types

        spec = next(
            (p for p in (metadata.partition_specs if metadata else []) if p.spec_id == metadata.default_spec_id),
            None,
        )
        if spec is None or not spec.fields:
            return data_files
        column_of = {int(f["id"]): str(f["name"]) for f in schema.fields}
        return prune_files_by_partition(data_files, expressions, spec_field_types(spec, schema), column_of)

    @staticmethod
    def _concat_aligned(tables: List[Any], pa: Any) -> Any:
        """Concatenate per-file tables, aligning column ORDER to the first table.

        Files written by 0.7.2 and earlier could carry the table's fields in the
        caller's order (#62); pa.concat_tables treats that as a different schema.
        Same-named columns are reordered, then types are unified permissively
        where pyarrow supports it.
        """
        if len(tables) > 1:
            names = tables[0].column_names
            aligned = [tables[0]]
            for t in tables[1:]:
                if t.column_names != names and set(t.column_names) == set(names):
                    t = t.select(names)
                aligned.append(t)
            tables = aligned
            # Files written before 0.10 carry no PARQUET:field_id metadata; files
            # written since do. Field metadata is not data - drop it before concat.
            tables = [
                pa.Table.from_arrays(t.columns, schema=pa.schema([f.remove_metadata() for f in t.schema]))
                for t in tables
            ]
        try:
            return pa.concat_tables(tables, promote_options="permissive")
        except TypeError:  # pyarrow < 14 has no promote_options
            return pa.concat_tables(tables)

    def to_arrow(
        self,
        columns: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        parallel: Union[bool, int] = False,
        verify_checksums: Optional[Union[bool, str]] = None,
        snapshot_id: Optional[int] = None,
    ) -> Any:
        """Read the table as a pyarrow.Table - the interchange for DuckDB, Polars and
        friends (#78). Same filter, integrity and snapshot semantics as scan(). An
        empty result has zero rows and the table's (projected) schema.
        """
        import pyarrow as pa

        combined = self._scan_table(columns, filter, parallel, verify_checksums, snapshot_id)
        if combined is not None:
            return combined
        schema = self._get_current_schema()
        if schema is None or not schema.fields:
            return pa.schema([]).empty_table()
        arrow_schema = self.file_manager.data_file_manager.create_arrow_schema(schema)
        if columns is not None:
            arrow_schema = pa.schema([arrow_schema.field(c) for c in columns])
        return arrow_schema.empty_table()

    def scan(
        self,
        columns: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        parallel: Union[bool, int] = False,
        verify_checksums: Optional[Union[bool, str]] = None,
        snapshot_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Scan the table (current snapshot, or `snapshot_id` for time travel) and return records.

        Args:
            columns: Optional list of column names to read. If None, reads all columns.
            filter: Optional filter dict for predicate pushdown.
                Examples:
                    {"status": "failed"}              # status == "failed"
                    {"age": (">", 18)}                # age > 18
                    {"id": ("in", [1, 2, 3])}         # id in [1, 2, 3]
                    {"ts": ("between", (t1, t2))}     # t1 <= ts <= t2
                    {"name": ("is_null", True)}       # name IS NULL
                Null handling follows SQL semantics: comparison operators and
                in/not_in never match NULL values; use is_null / is_not_null.
            parallel: Enable parallel reading.
                - False: Sequential reading (default)
                - True: Use all CPU cores
                - int: Use specified number of threads
            verify_checksums: Integrity mode: "page" (default; parquet page CRCs on
                the bytes actually read), "full" (whole-file sha256 recorded at
                write time - downloads every byte) or "off". True/False select
                full/off. Defaults to the DATASHARD_VERIFY_CHECKSUMS env var.
            snapshot_id: Read the table as of this snapshot (see snapshots(),
                time_travel()). Default: the current snapshot. Raises ValueError
                for an unknown or expired snapshot.

        Returns:
            List of dictionaries, each representing a record.

        Raises:
            RuntimeError / OSError: If any referenced manifest or data file
                cannot be read - errors are never swallowed into partial results.
            CorruptDataError: If checksum verification fails.
        """
        combined = self._scan_table(columns, filter, parallel, verify_checksums, snapshot_id)
        if combined is None:
            return []
        result: List[Dict[str, Any]] = combined.to_pylist()
        return result

    def to_pandas(
        self,
        columns: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        parallel: Union[bool, int] = False,
        verify_checksums: Optional[Union[bool, str]] = None,
        snapshot_id: Optional[int] = None,
    ) -> Any:
        """Read the table (current snapshot, or `snapshot_id`) as a pandas DataFrame.

        Same filtering, error, and checksum semantics as scan().

        Raises:
            ImportError: If pandas is not installed.
        """
        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError(
                "pandas is required for to_pandas(). Install with: pip install pandas"
            ) from e

        combined = self._scan_table(columns, filter, parallel, verify_checksums, snapshot_id)
        if combined is None:
            return pd.DataFrame()
        return combined.to_pandas()
