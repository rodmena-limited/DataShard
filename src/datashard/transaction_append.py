"""
Append half of Transaction: schema resolution and the append_* / queue methods
(mixed into Transaction; split out of transaction.py for the 500-line file cap, #71).
"""

import json
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, cast

from .data_structures import DataFile, FileFormat, Schema, TableMetadata
from .file_manager import FileManager
from .logging_config import get_logger
from .metadata_manager import MetadataManager

if TYPE_CHECKING:
    from .transaction import Transaction

logger = get_logger(__name__)


class _AppendMixin:
    """append_files / append_data / append_pandas and their schema plumbing."""

    metadata_manager: MetadataManager
    file_manager: FileManager
    _operations: List[Dict[str, Any]]
    _written_files: List[str]
    _schema_cache: Optional[Schema]
    _schema_cache_set: bool
    _base_metadata_cache: Optional[TableMetadata]
    _adopted_schema: Optional[Schema]

    if TYPE_CHECKING:  # provided by Transaction

        def is_active(self) -> bool: ...

        def _register_inflight(self, file_path: str) -> None: ...

    def append_files(self, files: List[DataFile]) -> "Transaction":
        """Queue pre-built data files to append to the table.

        Each file must exist AND (for parquet, on a table that has a persisted
        schema) carry a schema identical to the table's - see
        _validate_file_schema for why divergence is rejected here rather than
        discovered at scan time.
        """
        return self._queue_files(files, validate_schema=True)

    def _queue_files(
        self, files: List[DataFile], validate_schema: bool, trusted: bool = False
    ) -> "Transaction":
        """Shared tail of append_files / append_data / append_pandas.

        `trusted` files were written by this transaction a moment ago: their
        existence and schema are known, so the HEAD and footer reads are skipped
        (#67). Caller-provided files get both checks.
        """
        if not self.is_active():
            raise RuntimeError("Transaction is not active")

        # Validate that the files exist in the file system
        # This is a critical check in production systems
        table_schema = self._resolve_table_schema() if validate_schema else None
        for data_file in files:
            if not trusted and not self.file_manager.validate_file_exists(data_file.file_path):
                raise FileNotFoundError(f"Data file does not exist: {data_file.file_path}")
            if table_schema is not None:
                self._validate_file_schema(data_file, table_schema)
            # Caller-provided files are unreachable until commit, exactly like the
            # files append_data writes: protect them from GC for the transaction's
            # lifetime (#57). Idempotent per path.
            self._register_inflight(data_file.file_path)

        self._operations.append({"type": "append_files", "files": files})

        return cast("Transaction", self)

    def _validate_file_schema(self, data_file: DataFile, table_schema: Schema) -> None:
        """Reject a pre-built data file whose stored schema diverges from the table's.

        pa.concat_tables requires identical schemas (field names, order, types
        AND nullability), so a divergent file commits happily and then makes
        every subsequent full scan fail - the file is accepted, the table is
        bricked. append_data(records) already enforces this via
        _validate_schema_against_table; this closes the same hole on the
        file-level API (#49).

        Only parquet files can be checked (the schema lives in the footer);
        other formats are queued unchecked, as before.
        """
        fmt = data_file.file_format
        fmt_name = fmt.value if isinstance(fmt, FileFormat) else str(fmt)
        if fmt_name.lower() != FileFormat.PARQUET.value:
            return

        import pyarrow.parquet as pq

        dfm = self.file_manager.data_file_manager
        expected = dfm.create_arrow_schema(table_schema)
        try:
            # Read through OUR backend, not pyarrow's S3 client (#54). Seekable,
            # so this fetches the footer only — not the whole data file.
            with dfm.open_parquet_source(data_file.file_path) as src:
                actual = pq.ParquetFile(src).schema_arrow
        except Exception as e:
            raise ValueError(
                f"Cannot read the parquet schema of '{data_file.file_path}': {e}. "
                f"Refusing to append a file whose schema cannot be verified against the "
                f"table schema - an unverifiable file can make every later scan fail."
            ) from e

        if not actual.equals(expected, check_metadata=False):
            raise ValueError(
                f"Data file '{data_file.file_path}' has a schema that does not match the "
                f"table's persisted schema. Appending it would make table scans fail. "
                f"Table schema: {expected}; file schema: {actual}"
            )

    def append_pandas(
        self,
        df: Any,
        schema: Optional["Schema"] = None,
    ) -> "Transaction":
        """Append a pandas DataFrame to the table.

        Converted with pyarrow's native from_pandas - no per-row dict round trip
        (#69). Columns absent from the schema are an error; required columns must
        be present and free of nulls. An empty frame queues nothing.

        Args:
            df: pandas DataFrame to append
            schema: Optional Schema. If None, uses the table's current schema.

        Returns:
            Self for chaining
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for append_pandas") from None

        if not isinstance(df, pd.DataFrame):
            raise ValueError("Expected a pandas DataFrame")
        if not self.is_active():
            raise RuntimeError("Transaction is not active")
        if len(df) == 0:
            logger.warning("append_pandas: empty DataFrame - nothing queued (no data file, no snapshot)")
            return cast("Transaction", self)

        schema = self._schema_for_append(schema)
        file_path = self._new_data_file_path()
        self._register_inflight(file_path)
        data_file = self.file_manager.data_file_manager.write_pandas_file(
            file_path=file_path,
            df=df,
            iceberg_schema=schema,
            file_format=FileFormat.PARQUET,
            partition_values={},
        )
        return self._queue_written_file(file_path, data_file)

    def append_arrow(
        self,
        table: Any,
        schema: Optional["Schema"] = None,
        partition_values: Optional[Dict[str, Any]] = None,
    ) -> "Transaction":
        """Append a pyarrow.Table as one data file (#79).

        The natural ingestion path for DuckDB / Polars / Arrow producers: no
        per-row dict round trip. Columns are conformed to the table's schema (see
        DataFileManager.write_arrow_file). An empty table queues nothing.
        """
        import pyarrow as pa

        if not isinstance(table, pa.Table):
            raise ValueError("Expected a pyarrow.Table")
        if not self.is_active():
            raise RuntimeError("Transaction is not active")
        if table.num_rows == 0:
            logger.warning("append_arrow: empty table - nothing queued (no data file, no snapshot)")
            return cast("Transaction", self)

        schema = self._schema_for_append(schema)
        file_path = self._new_data_file_path()
        self._register_inflight(file_path)
        data_file = self.file_manager.data_file_manager.write_arrow_file(
            file_path=file_path,
            table=table,
            iceberg_schema=schema,
            file_format=FileFormat.PARQUET,
            partition_values=partition_values or {},
        )
        return self._queue_written_file(file_path, data_file)

    def _resolve_table_schema(self) -> Optional[Schema]:
        """Resolve the table's persisted current schema, or None if the table
        has no usable (non-empty) schema. Cached for the transaction's lifetime."""
        if self._schema_cache_set:
            return self._schema_cache
        result: Optional[Schema] = None
        # Write-path read: no probe past the hint (the commit point detects a lag, #86)
        metadata = self.metadata_manager.refresh(probe=False)
        self._base_metadata_cache = metadata
        if metadata and metadata.schemas:
            for s in metadata.schemas:
                if s.schema_id == metadata.current_schema_id and s.fields:
                    result = s
                    break
            else:
                # Fallback: any non-empty schema
                for s in metadata.schemas:
                    if s.fields:
                        result = s
                        break
        self._schema_cache, self._schema_cache_set = result, True
        return result

    @staticmethod
    def _schema_signature(schema: Schema) -> Set[Any]:
        """Comparable signature of a schema's fields (name, type, required)."""
        sig = set()
        for f in schema.fields:
            f_type = f.get("type")
            type_key = json.dumps(f_type, sort_keys=True) if isinstance(f_type, (dict, list)) else f_type
            sig.add((f.get("name"), type_key, bool(f.get("required", False))))
        return sig

    def _validate_schema_against_table(self, schema: Schema) -> Optional[Schema]:
        """Reject appends whose schema diverges from the table's persisted schema.

        A divergent append would write parquet files whose schema differs from
        the rest of the table, making every subsequent full scan fail on
        concat - effectively bricking reads for the whole table.

        Returns the table's persisted schema (None for a legacy table without
        one). Callers write files with THAT schema, so the on-disk column order
        always follows the table even when the caller listed the same fields in
        a different order (#62).
        """
        table_schema = self._resolve_table_schema()
        if table_schema is None:
            return None  # No persisted schema (legacy table): nothing to enforce
        if self._schema_signature(schema) != self._schema_signature(table_schema):
            raise ValueError(
                "Provided schema does not match the table's persisted schema. "
                "Appending with a divergent schema would make table scans fail. "
                f"Table fields: {table_schema.fields}; provided fields: {schema.fields}"
            )
        return table_schema

    def append_data(
        self,
        records: List[Dict[str, Any]],
        schema: Optional["Schema"] = None,
        partition_values: Optional[Dict[str, Any]] = None,
    ) -> "Transaction":
        """Append actual data records to the table by creating new data files.

        The schema is resolved from the table metadata when not provided; a
        table without a usable schema raises instead of silently writing
        zero-column files. Files are written in the TABLE's field order (#62).
        An empty record list queues nothing - no empty data file, no snapshot (#69).
        """
        if not self.is_active():
            raise RuntimeError("Transaction is not active")
        if not records:
            logger.warning("append_data: no records - nothing queued (no data file, no snapshot)")
            return cast("Transaction", self)

        schema = self._schema_for_append(schema)
        file_path = self._new_data_file_path()

        # Register a GC-protection marker BEFORE writing the data file: the file
        # is unreachable until commit, and without the marker a long-running
        # transaction's files could exceed the GC grace period and be deleted
        # out from under the commit. Marker write failure aborts the append
        # (fail closed - never write unprotected files).
        self._register_inflight(file_path)

        data_file = self.file_manager.data_file_manager.write_data_file(
            file_path=file_path,
            records=records,
            iceberg_schema=schema,
            file_format=FileFormat.PARQUET,
            partition_values=partition_values or {},
        )
        return self._queue_written_file(file_path, data_file)

    def _schema_for_append(self, schema: Optional[Schema]) -> Schema:
        """The schema a new data file is written with: the table's persisted one
        (validated against the caller's when one is given), or the caller's for a
        legacy table without a persisted schema."""
        if schema is None:
            resolved = self._resolve_table_schema()
            if resolved is None:
                raise ValueError(
                    "No schema available: the table has no persisted schema and none was "
                    "provided. Create the table with create_table(path, schema=...) or pass "
                    "schema= explicitly - appending without a schema would silently discard "
                    "all record fields."
                )
            return resolved
        persisted = self._validate_schema_against_table(schema)
        if persisted is None:
            # A table created without a schema adopts the first append's schema in
            # the same commit, so the metadata (and every foreign reader) knows its
            # columns instead of showing an empty struct (#84).
            self._adopted_schema = schema
            return schema
        return persisted

    @staticmethod
    def _new_data_file_path() -> str:
        """Table-relative path for a new data file (UUID-unique)."""
        return f"data/auto_{uuid.uuid4().hex[:16]}.parquet"

    def _queue_written_file(self, file_path: str, data_file: DataFile) -> "Transaction":
        """Track a file this transaction wrote and queue it (Iceberg-style '/data/...' path)."""
        self._written_files.append(file_path)
        updated_data_file = DataFile(
            file_path="/" + file_path,
            file_format=data_file.file_format,
            partition_values=data_file.partition_values,
            record_count=data_file.record_count,
            file_size_in_bytes=data_file.file_size_in_bytes,
            column_sizes=data_file.column_sizes,
            value_counts=data_file.value_counts,
            null_value_counts=data_file.null_value_counts,
            lower_bounds=data_file.lower_bounds,
            upper_bounds=data_file.upper_bounds,
            checksum=data_file.checksum,
        )
        # Written with the table's own schema a moment ago: no HEAD, no footer read.
        return self._queue_files([updated_data_file], validate_schema=False, trusted=True)

