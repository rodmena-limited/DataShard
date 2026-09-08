"""
Data file operations and readers/writers for the Python Iceberg implementation
"""

import os
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple, Union

import pyarrow as pa
import pyarrow.parquet as pq

from .arrow_types import iceberg_type_to_arrow
from .column_stats import compute_column_bounds
from .data_io import (
    PANDAS_AVAILABLE,
    DataFileReader,
    DataFileWriter,
    pd,
    read_parquet_table,
)
from .data_structures import DataFile, FileFormat, Schema
from .integrity import IntegrityChecker
from .local_backend import LocalStorageBackend
from .logging_config import get_logger
from .s3_backend import S3StorageBackend
from .schema_validation import (
    check_cast_family,
    type_family,
    validate_arrow_table_strict,
    validate_records_strict,
)
from .storage_backend import StorageBackend

if TYPE_CHECKING:
    from .file_manager import FileManager

logger = get_logger(__name__)

# Exceptions PyArrow raises when data genuinely does not fit a schema. Anything
# else is a bug in our conversion code and must not be reported as "incompatible".
_SCHEMA_MISMATCH_ERRORS = (
    pa.ArrowInvalid,
    pa.ArrowTypeError,
    pa.ArrowNotImplementedError,
    TypeError,
    ValueError,
    KeyError,
)


class DataFileManager:
    """Manages data file operations including reading/writing with proper schema management"""

    def __init__(self, file_manager: "FileManager", storage: "StorageBackend"):
        self.file_manager = file_manager
        self.storage = storage
        # Keyed by a fingerprint of the FIELDS (ids, names, types, required, order),
        # never by schema_id alone: two schemas can share an id and differ (#62).
        self._arrow_schema_cache: Dict[str, pa.Schema] = {}
        # pyarrow's own S3 client is no longer used for reads (#54) or writes (#67):
        # data files are serialised in memory and stored through the storage backend.
        self._pyarrow_fs: Optional[Any] = None

    def _get_arrow_filesystem(self) -> Optional[Any]:
        """Get PyArrow filesystem for S3 or None for local filesystem"""
        if isinstance(self.storage, S3StorageBackend):
            try:
                import pyarrow.fs as pafs

                return pafs.S3FileSystem(
                    access_key=self.storage.access_key,
                    secret_key=self.storage.secret_key,
                    endpoint_override=self.storage.endpoint_url,
                    region=self.storage.region,
                )
            except ImportError as e:
                raise ImportError(
                    "pyarrow with S3 support is required for S3 storage backend. "
                    "Install with: pip install pyarrow"
                ) from e
        return None  # Local filesystem

    def open_parquet_source(self, file_path: str) -> Any:
        """A source for pq.ParquetFile / pq.read_table that avoids pyarrow's S3 client.

        Returns an open, SEEKABLE file object read through our own storage
        backend. Callers pass it instead of a (path, filesystem=) pair.

        pyarrow's S3FileSystem sends an ``x-amz-checksum-mode`` header on
        GetObject that OVH Object Storage rejects outright (#54), so every
        parquet read failed there while writes succeeded — the table was
        created, then the first append died validating its own output. boto3
        reads the same object fine, so the read goes through the backend.

        Still seekable, so pyarrow fetches only the footer to learn a schema
        and only the column chunks a scan needs. The caller owns the object and
        must close it.
        """
        # THE TRAVERSAL GUARD MUST RUN FIRST (#47). _get_arrow_path is what
        # raises ValueError("Path traversal...") for a manifest entry pointing
        # outside the table, e.g. a tampered '/etc/passwd'. Reading straight
        # through the backend would still CONTAIN the path (it resolves under
        # the table root) but would report a bland FileNotFoundError instead of
        # refusing — and containment that reports "not found" invites someone to
        # "fix" it by loosening the resolver. Called for the check, not the value.
        validated = self._get_arrow_path(file_path)

        if isinstance(self.storage, S3StorageBackend):
            # open_seekable takes the manifest-relative path; the backend adds
            # the bucket prefix itself.
            return self.storage.open_seekable(file_path.lstrip("/"))

        # Local: _get_arrow_path already returned a RESOLVED absolute path, so
        # open it directly. Passing it back through the backend would resolve it
        # a second time against the table root and produce /base/base/file.
        return open(validated, "rb")

    @contextmanager
    def parquet_source(self, file_path: str) -> Iterator[Tuple[Any, bool]]:
        """Yield (source, use_threads) for pq.read_table / pq.ParquetFile.

        Local: the resolved PATH - pyarrow reads it natively and may use its thread
        pool. S3: our seekable range reader (a Python file object), which pyarrow
        must read SINGLE-threaded: its threaded readers over a Python file object
        make CPython 3.13 abort at interpreter exit ('terminate called without an
        active exception', pyarrow 22, reproduced with a 10-line script - #74).
        Cross-file parallelism is scan(parallel=...)'s job. The traversal guard
        runs first (#47).
        """
        validated = self._get_arrow_path(file_path)
        if isinstance(self.storage, S3StorageBackend):
            src = self.storage.open_seekable(file_path.lstrip("/"))
            try:
                yield src, False
            finally:
                src.close()
        else:
            yield validated, True

    def _get_arrow_path(self, path: str) -> str:
        """Convert a manifest path to a PyArrow path (bucket/key for S3, absolute for local).

        Every path handed to PyArrow - for reads as well as writes - is treated
        as relative to the table root and routed through the storage backend's
        boundary check. Returning true absolute paths unchanged (as this used
        to) let a tampered or corrupt manifest entry such as '/etc/passwd' make
        the reader open any file on the host, bypassing the traversal guard the
        write/GC path enforces (#47).
        """
        if isinstance(self.storage, S3StorageBackend):
            # For PyArrow S3FileSystem, path should be bucket/key
            key = self.storage._get_s3_key(path)
            return f"{self.storage.bucket}/{key}"
        if isinstance(self.storage, LocalStorageBackend):
            base_path = self.storage._real_base_path()
            components = path.split("/")
            first_component = components[1] if path.startswith("/") and len(components) > 1 else ""

            if not os.path.isabs(path) or first_component in ("data", "metadata"):
                # Iceberg-style ('/data/x.parquet') or plain relative
                # ('data/x.parquet'): table-relative, resolved and
                # boundary-checked by the storage backend.
                return self.storage._resolve_path(path)

            # A true absolute path (e.g. from a caller holding the real
            # location of a file it just wrote) is honoured ONLY if it lies
            # inside the table root; anything else is a traversal attempt or a
            # tampered manifest entry and must not be opened.
            resolved = os.path.realpath(path)
            try:
                inside = os.path.commonpath([base_path, resolved]) == base_path
            except ValueError:
                inside = False
            if not inside:
                raise ValueError(
                    f"Security Error: Path traversal attempt detected. Resolved path "
                    f"'{resolved}' is outside table root '{base_path}'"
                )
            return resolved

        # Unknown backend: fall back to a table-root join (no escape either).
        table_path = self.file_manager.table_path
        return os.path.join(table_path, path.lstrip("/"))

    @staticmethod
    def schema_fingerprint(iceberg_schema: Schema) -> str:
        """Order-preserving identity of a schema's fields."""
        import json

        return json.dumps(iceberg_schema.fields, sort_keys=True, default=str)

    def create_arrow_schema(self, iceberg_schema: Schema) -> pa.Schema:
        """Convert Iceberg schema to PyArrow schema"""
        cache_key = self.schema_fingerprint(iceberg_schema)
        if cache_key in self._arrow_schema_cache:
            return self._arrow_schema_cache[cache_key]

        import pyarrow as pa

        fields = []
        for field_dict in iceberg_schema.fields:
            field_id = field_dict.get("id", 0)
            field_name = field_dict.get("name", f"field_{field_id}")
            field_type_str = field_dict.get("type", "string")

            # Map Iceberg types to PyArrow types
            arrow_type = self._iceberg_type_to_arrow(field_type_str)

            # Check if field is required
            is_nullable = not field_dict.get("required", False)

            # Iceberg readers resolve columns by field id (#88); without it DuckDB
            # returns all-NULL rows silently (spike #83).
            fields.append(pa.field(
                field_name, arrow_type, nullable=is_nullable,
                metadata={b"PARQUET:field_id": str(field_id).encode("utf-8")},
            ))

        schema = pa.schema(fields)
        self._arrow_schema_cache[cache_key] = schema
        return schema

    def _iceberg_type_to_arrow(self, iceberg_type: Union[str, Dict[str, Any]]) -> pa.DataType:
        """Convert an Iceberg type to a PyArrow type (see arrow_types)."""
        return iceberg_type_to_arrow(iceberg_type)

    def validate_records_strict(
        self, records: List[Dict[str, Any]], iceberg_schema: Schema
    ) -> None:
        """Records must not lose or coerce data silently (see schema_validation)."""
        validate_records_strict(records, iceberg_schema)

    def validate_arrow_table_strict(self, table: pa.Table, iceberg_schema: Schema) -> None:
        """Arrow twin of validate_records_strict (see schema_validation)."""
        validate_arrow_table_strict(table, iceberg_schema)

    def _write_arrow_table(self, file_path: str, table: pa.Table) -> Tuple[int, str]:
        """Serialise `table` to parquet once, in memory, and store it via the backend.

        - one row group per (up to) 1 Mi rows, lz4, page CRCs on (#66, #69);
        - the bytes are hashed BEFORE upload, so no read-back download and one
          client for data and metadata alike (#67);
        - the traversal guard runs first (#47).
        Returns (size_in_bytes, sha256).
        """
        rel_path = self._table_relative(file_path)  # raises on a path outside the table root
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, compression="lz4", write_page_checksum=True)
        data = sink.getvalue().to_pybytes()
        self.storage.write_file(rel_path, data)
        return len(data), IntegrityChecker.compute_checksum(data)

    def _table_relative(self, file_path: str) -> str:
        """Table-relative form of a data-file path accepted by the writer: Iceberg-style
        ('/data/x'), plain relative ('data/x'), or - local only - a true absolute path
        inside the table root (callers that hold the real location of a file)."""
        validated = self._get_arrow_path(file_path)  # traversal guard (#47)
        if isinstance(self.storage, LocalStorageBackend):
            return os.path.relpath(validated, self.storage._real_base_path()).replace(os.sep, "/")
        return file_path.replace("\\", "/").lstrip("/")

    _type_family = staticmethod(type_family)
    _check_cast_family = staticmethod(check_cast_family)

    def write_data_file(
        self,
        file_path: str,
        records: List[Dict[str, Any]],
        iceberg_schema: Schema,
        file_format: FileFormat = FileFormat.PARQUET,
        partition_values: Optional[Dict[str, Any]] = None,
    ) -> DataFile:
        """Write data records to a file and return DataFile metadata"""
        if file_format != FileFormat.PARQUET:
            raise ValueError(f"Unsupported file format: {file_format}")

        if records:
            self.validate_records_strict(records, iceberg_schema)

        arrow_schema = self.create_arrow_schema(iceberg_schema)
        # ONE conversion: the same table yields the statistics and the bytes (#69).
        table = pa.Table.from_pylist(records, schema=arrow_schema)
        lower_bounds, upper_bounds = (
            self._compute_column_bounds(table, iceberg_schema) if records else (None, None)
        )
        file_size, checksum = self._write_arrow_table(file_path, table)

        return DataFile(
            file_path=file_path,
            file_format=file_format,
            partition_values=partition_values or {},
            record_count=table.num_rows,
            file_size_in_bytes=file_size,
            lower_bounds=lower_bounds,
            upper_bounds=upper_bounds,
            checksum=checksum,
        )

    def _compute_column_bounds(
        self, table: pa.Table, iceberg_schema: Schema
    ) -> Tuple[Optional[Dict[int, Any]], Optional[Dict[int, Any]]]:
        """Min/max per column for file pruning (see column_stats)."""
        return compute_column_bounds(table, iceberg_schema)

    def write_arrow_file(
        self,
        file_path: str,
        table: pa.Table,
        iceberg_schema: Schema,
        file_format: FileFormat = FileFormat.PARQUET,
        partition_values: Optional[Dict[str, Any]] = None,
    ) -> DataFile:
        """Write an Arrow table as one data file and return its DataFile metadata (#79).

        The table is conformed to the persisted schema: columns not in the schema
        are an error (never silently dropped), optional columns that are absent are
        filled with nulls, columns are reordered to the table's order and cast to
        the table's Arrow types (so an int64 column lands in a `long` field and a
        DuckDB result can be appended as is); required columns must be null-free.
        """
        if file_format != FileFormat.PARQUET:
            raise ValueError(f"Unsupported file format: {file_format}")
        arrow_schema = self.create_arrow_schema(iceberg_schema)
        unknown = set(table.column_names) - set(arrow_schema.names)
        if unknown:
            raise ValueError(
                f"Table has columns not in the table schema: {sorted(unknown)}. "
                f"Schema fields: {sorted(arrow_schema.names)}. Refusing to silently drop data."
            )
        for field in arrow_schema:
            if field.name not in table.column_names:
                table = table.append_column(field.name, pa.nulls(table.num_rows, type=field.type))
            else:
                check_cast_family(field.name, table.schema.field(field.name).type, field.type)
        try:
            table = table.select(arrow_schema.names).cast(arrow_schema)
        except _SCHEMA_MISMATCH_ERRORS as e:
            raise ValueError(f"Table is not compatible with the table schema: {e}") from e
        self.validate_arrow_table_strict(table, iceberg_schema)
        lower_bounds, upper_bounds = self._compute_column_bounds(table, iceberg_schema)
        file_size, checksum = self._write_arrow_table(file_path, table)
        return DataFile(
            file_path=file_path,
            file_format=file_format,
            partition_values=partition_values or {},
            record_count=table.num_rows,
            file_size_in_bytes=file_size,
            lower_bounds=lower_bounds,
            upper_bounds=upper_bounds,
            checksum=checksum,
        )

    def write_pandas_file(
        self,
        file_path: str,
        df: "pd.DataFrame",
        iceberg_schema: Schema,
        file_format: FileFormat = FileFormat.PARQUET,
        partition_values: Optional[Dict[str, Any]] = None,
    ) -> DataFile:
        """Write a pandas DataFrame to a file and return DataFile metadata (requires pandas).

        Native pa.Table.from_pandas conversion - no per-row dict round trip (#69);
        the rest is write_arrow_file's conformance and validation.
        """
        if not PANDAS_AVAILABLE:
            raise ImportError(
                "pandas is not available. Install with: pip install datashard[pandas]"
            )
        allowed = {str(f["name"]) for f in iceberg_schema.fields}
        unknown = {str(c) for c in df.columns} - allowed
        if unknown:
            raise ValueError(
                f"DataFrame has columns not in the table schema: {sorted(unknown)}. "
                f"Schema fields: {sorted(allowed)}. Refusing to silently drop data."
            )
        arrow_schema = self.create_arrow_schema(iceberg_schema)
        try:
            table = pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)
        except _SCHEMA_MISMATCH_ERRORS as e:
            raise ValueError(f"DataFrame is not compatible with the table schema: {e}") from e
        return self.write_arrow_file(file_path, table, iceberg_schema, file_format, partition_values)

    def read_data_file(
        self,
        file_path: str,
        file_format: FileFormat = FileFormat.PARQUET,
        columns: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Read data from a file and return as list of records"""

        if file_format != FileFormat.PARQUET:
            raise ValueError(f"Unsupported file format: {file_format}")
        with self.parquet_source(file_path) as (source, threads):
            table = read_parquet_table(source, columns=columns or None, use_threads=threads)
        result: List[Dict[str, Any]] = table.to_pylist()
        return result

    def read_pandas_file(
        self,
        file_path: str,
        file_format: FileFormat = FileFormat.PARQUET,
        columns: Optional[List[str]] = None,
    ) -> "pd.DataFrame":
        """Read data from a file as pandas DataFrame (requires pandas)"""
        if not PANDAS_AVAILABLE:
            raise ImportError(
                "pandas is not available. Install with: pip install datashard[pandas]"
            )

        if file_format != FileFormat.PARQUET:
            raise ValueError(f"Unsupported file format: {file_format}")
        with self.parquet_source(file_path) as (source, threads):
            table = read_parquet_table(source, columns=columns or None, use_threads=threads)
        return table.to_pandas()

    def validate_pandas_compatibility(self, df: "pd.DataFrame", iceberg_schema: Schema) -> bool:
        """Validate that pandas DataFrame is compatible with the schema (requires pandas)"""
        if not PANDAS_AVAILABLE:
            raise ImportError(
                "pandas is not available. Install with: pip install datashard[pandas]"
            )

        try:
            # Convert the DataFrame to Arrow Table using the schema
            arrow_schema = self.create_arrow_schema(iceberg_schema)
            pa.Table.from_pandas(df, schema=arrow_schema)
            return True
        except _SCHEMA_MISMATCH_ERRORS as e:
            logger.debug(f"DataFrame is not compatible with the schema: {e}")
            return False

    def validate_data_compatibility(
        self, records: List[Dict[str, Any]], iceberg_schema: Schema
    ) -> bool:
        """Validate that records are compatible with the schema"""
        try:
            arrow_schema = self.create_arrow_schema(iceberg_schema)

            # Try to create a table with the records and schema
            pa.Table.from_pylist(records, schema=arrow_schema)
            return True
        except _SCHEMA_MISMATCH_ERRORS as e:
            logger.debug(f"Records are not compatible with the schema: {e}")
            return False


__all__ = ["DataFileManager", "DataFileReader", "DataFileWriter", "PANDAS_AVAILABLE"]
