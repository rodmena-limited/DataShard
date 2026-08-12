"""
Data file operations and readers/writers for the Python Iceberg implementation
"""

import os
import tempfile
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple, Union

import pyarrow as pa
import pyarrow.parquet as pq

# Try to import pandas as optional dependency
try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None  # Define as None to avoid reference errors

from .data_structures import DataFile, FileFormat, Schema
from .integrity import IntegrityChecker
from .logging_config import get_logger
from .storage_backend import LocalStorageBackend, S3StorageBackend, StorageBackend

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


class DataFileReader:
    """Reader for Iceberg data files"""

    def __init__(
        self,
        file_path: Any,
        file_format: FileFormat,
        schema: Optional[pa.Schema] = None,
        filesystem: Optional[Any] = None,
    ):
        """`file_path` is a path, or an open seekable binary file object.

        Passing a file object is how S3 reads avoid pyarrow's own S3 client,
        which OVH rejects (#54); pyarrow accepts either as a parquet source.
        """
        self.file_path = file_path
        self.file_format = file_format
        self._schema = schema  # Use private attribute to not conflict with schema method
        self._filesystem = filesystem
        self._reader: Optional[Any] = None

    def __enter__(self) -> "DataFileReader":
        self.open()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def open(self) -> None:
        """Open the data file for reading"""
        if self.file_format == FileFormat.PARQUET:
            if self._filesystem:
                # Use PyArrow filesystem (S3)
                self._reader = pq.ParquetFile(self.file_path, filesystem=self._filesystem)
            else:
                # Use local filesystem
                self._reader = pq.ParquetFile(self.file_path)
        else:
            raise ValueError(f"Unsupported file format: {self.file_format}")

    def close(self) -> None:
        """Close the data file"""
        if self._reader:
            # ParquetFile doesn't need explicit closing in pyarrow
            self._reader = None

    def read_all(self) -> pa.Table:
        """Read all data from the file"""
        if not self._reader:
            self.open()

        assert self._reader is not None
        return self._reader.read()

    def read_batches(self, batch_size: int = 1000) -> Iterator[pa.RecordBatch]:
        """Read data in batches"""
        if not self._reader:
            self.open()

        assert self._reader is not None
        for batch in self._reader.iter_batches(batch_size=batch_size):
            yield batch

    def read_columns(self, column_names: List[str]) -> pa.Table:
        """Read specific columns from the file"""
        if not self._reader:
            self.open()

        assert self._reader is not None
        return self._reader.read(columns=column_names)

    def read_pandas(self) -> Optional["pd.DataFrame"]:
        """Read data as pandas DataFrame (requires pandas)"""
        if not PANDAS_AVAILABLE:
            raise ImportError(
                "pandas is not available. Install with: pip install datashard[pandas]"
            )

        if not self._reader:
            self.open()

        assert self._reader is not None
        table = self._reader.read()
        return table.to_pandas()

    def read_batches_pandas(self, batch_size: int = 1000) -> Iterator["pd.DataFrame"]:
        """Read data in pandas DataFrame batches (requires pandas)"""
        if not PANDAS_AVAILABLE:
            raise ImportError(
                "pandas is not available. Install with: pip install datashard[pandas]"
            )

        if not self._reader:
            self.open()

        assert self._reader is not None
        for batch in self._reader.iter_batches(batch_size=batch_size):
            yield batch.to_pandas()

    def read_columns_pandas(self, column_names: List[str]) -> Optional["pd.DataFrame"]:
        """Read specific columns from the file as pandas DataFrame (requires pandas)"""
        if not PANDAS_AVAILABLE:
            raise ImportError(
                "pandas is not available. Install with: pip install datashard[pandas]"
            )

        if not self._reader:
            self.open()

        assert self._reader is not None
        table = self._reader.read(columns=column_names)
        return table.to_pandas()

    def schema(self) -> Optional[pa.Schema]:
        """Get the schema of the file"""
        # If no reader opened yet, return the stored schema
        if not self._reader:
            self.open()

        assert self._reader is not None
        return self._reader.schema


class DataFileWriter:
    """Writer for Iceberg data files"""

    def __init__(
        self,
        file_path: str,
        file_format: FileFormat,
        schema: pa.Schema,
        metadata: Optional[Dict[str, Any]] = None,
        filesystem: Optional[Any] = None,
    ):
        self.file_path = file_path
        self.file_format = file_format
        self._schema = schema  # Use private attribute to not conflict with any method
        self.metadata = metadata or {}
        self._filesystem = filesystem
        self._writer: Optional[Any] = None
        self._temp_file: Optional[Any] = None
        self._row_count = 0

    def __enter__(self) -> "DataFileWriter":
        self.open()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def open(self) -> None:
        """Open the file for writing"""
        if self.file_format == FileFormat.PARQUET:
            # Add our metadata to the schema
            schema_metadata = self._schema.metadata if self._schema.metadata else {}
            for key, value in self.metadata.items():
                schema_metadata[key.encode("utf-8")] = str(value).encode("utf-8")

            modified_schema = self._schema.with_metadata(schema_metadata)

            if self._filesystem:
                # For S3, write directly using PyArrow filesystem
                self._writer = pq.ParquetWriter(
                    self.file_path,
                    modified_schema,
                    compression="lz4",
                    filesystem=self._filesystem,
                )
            else:
                # For local filesystem, use temp file pattern for safety
                temp_dir = os.path.dirname(self.file_path)
                # Ensure the directory exists before creating temp file
                os.makedirs(temp_dir, exist_ok=True)
                self._temp_file = tempfile.NamedTemporaryFile(
                    delete=False, dir=temp_dir, suffix=".parquet"
                )
                try:
                    self._writer = pq.ParquetWriter(
                        self._temp_file.name, modified_schema, compression="lz4"
                    )
                except BaseException:
                    # Never leave the temp file behind when the writer could not
                    # be constructed: nothing will ever close or rename it.
                    temp_name = self._temp_file.name
                    self._temp_file.close()
                    self._temp_file = None
                    try:
                        os.remove(temp_name)
                    except OSError:
                        logger.warning(f"Could not remove temp file {temp_name}")
                    raise
        else:
            raise ValueError(f"Unsupported file format: {self.file_format}")

    def write_batch(self, batch: Union[pa.RecordBatch, pa.Table]) -> None:
        """Write a batch of data to the file"""
        if not self._writer:
            self.open()

        assert self._writer is not None
        # Convert to RecordBatch if it's a Table
        if isinstance(batch, pa.Table):
            for record_batch in batch.to_batches():
                self._writer.write_batch(record_batch)
                self._row_count += record_batch.num_rows
        else:
            self._writer.write_batch(batch)
            self._row_count += batch.num_rows

    def write_records(self, records: List[Dict[str, Any]]) -> None:
        """Write a list of records to the file"""
        if not self._writer:
            self.open()

        assert self._writer is not None
        if records:
            # Convert records to Arrow Table
            table = pa.Table.from_pylist(records, schema=self._schema)
            self.write_batch(table)

    def write_pandas(self, df: "pd.DataFrame") -> None:
        """Write a pandas DataFrame to the file (requires pandas)"""
        if not PANDAS_AVAILABLE:
            raise ImportError(
                "pandas is not available. Install with: pip install datashard[pandas]"
            )

        if not self._writer:
            self.open()

        assert self._writer is not None
        # Convert pandas DataFrame to Arrow Table
        table = pa.Table.from_pandas(df, schema=self._schema)
        self.write_batch(table)

    def close(self) -> None:
        """Close the writer and finalize the file atomically"""
        if not self._writer:
            return

        try:
            assert self._writer is not None
            self._writer.close()

            # Move temp file to final location (only for local filesystem)
            if self._temp_file:
                temp_name = self._temp_file.name
                try:
                    # Durability: ParquetWriter.close() only flushes to the page
                    # cache; fsync the file contents before the rename so a
                    # committed snapshot can never reference data lost in a
                    # power failure (metadata writes are already fsync'd).
                    fsync_fd = os.open(temp_name, os.O_RDONLY)
                    try:
                        os.fsync(fsync_fd)
                    finally:
                        os.close(fsync_fd)

                    # Atomic rename on both POSIX and Windows
                    os.replace(temp_name, self.file_path)

                    # Sync directory to persist rename
                    try:
                        dir_path = os.path.dirname(self.file_path)
                        dir_fd = os.open(dir_path, os.O_RDONLY)
                        try:
                            os.fsync(dir_fd)
                        finally:
                            os.close(dir_fd)
                    except (OSError, AttributeError):
                        # Directory fsync not supported - acceptable
                        pass

                except Exception:
                    # Clean up temp file if rename failed
                    try:
                        if os.path.exists(temp_name):
                            os.remove(temp_name)
                    except Exception:
                        pass  # Ignore cleanup errors
                    raise

                self._temp_file = None

        finally:
            self._writer = None

    @property
    def row_count(self) -> int:
        """Get the number of rows written to the file"""
        return self._row_count


class DataFileManager:
    """Manages data file operations including reading/writing with proper schema management"""

    def __init__(self, file_manager: "FileManager", storage: "StorageBackend"):
        self.file_manager = file_manager
        self.storage = storage
        self._arrow_schema_cache: Dict[int, pa.Schema] = {}
        self._pyarrow_fs = self._get_arrow_filesystem()

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

    def create_arrow_schema(self, iceberg_schema: Schema) -> pa.Schema:
        """Convert Iceberg schema to PyArrow schema"""
        if iceberg_schema.schema_id in self._arrow_schema_cache:
            return self._arrow_schema_cache[iceberg_schema.schema_id]

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

            fields.append(pa.field(field_name, arrow_type, nullable=is_nullable))

        schema = pa.schema(fields)
        self._arrow_schema_cache[iceberg_schema.schema_id] = schema
        return schema

    def _iceberg_type_to_arrow(self, iceberg_type: Union[str, Dict[str, Any]]) -> pa.DataType:
        """Convert Iceberg type string to PyArrow type"""
        import pyarrow as pa

        if isinstance(iceberg_type, dict):
            iceberg_type = iceberg_type.get("type", "string")

        type_mapping = {
            "boolean": pa.bool_(),
            "int": pa.int32(),
            "long": pa.int64(),
            "float": pa.float32(),
            "double": pa.float64(),
            "date": pa.date32(),
            "time": pa.time64("us"),
            "timestamp": pa.timestamp("us"),
            "string": pa.string(),
            "uuid": pa.string(),  # For UUID handling
            "binary": pa.binary(),
            "fixed": pa.binary(),
        }

        # Handle complex types
        if isinstance(iceberg_type, str):
            if iceberg_type.startswith("list<"):
                # Extract element type and map it
                element_type = iceberg_type[5:-1]  # Remove 'list<>' wrapper
                return pa.list_(self._iceberg_type_to_arrow(element_type))
            elif iceberg_type.startswith("map<"):
                # For now, treat as string - in real implementation would need key/value types
                return pa.string()
            elif iceberg_type.startswith("struct<"):
                return pa.string()  # For now, treat as string

        return type_mapping.get(str(iceberg_type), pa.string())  # Default to string

    def validate_records_strict(
        self, records: List[Dict[str, Any]], iceberg_schema: Schema
    ) -> None:
        """Validate records against the schema, raising on silent-loss hazards.

        - Unknown/misnamed fields raise instead of being silently dropped by
          pyarrow's schema projection.
        - Required (non-nullable) fields must be present and non-None; pyarrow's
          from_pylist does not enforce nullability, so we must.
        Type mismatches are left to pyarrow, which raises on incompatible values.
        """
        # Schema.__post_init__ guarantees every field has a "name".
        allowed = {str(f["name"]) for f in iceberg_schema.fields}
        required = {
            str(f["name"]) for f in iceberg_schema.fields if f.get("required", False)
        }

        for i, record in enumerate(records):
            unknown = {str(k) for k in record.keys()} - allowed
            if unknown:
                raise ValueError(
                    f"Record {i} has fields not in the table schema: {sorted(unknown)}. "
                    f"Schema fields: {sorted(allowed)}. Refusing to silently drop data."
                )
            for name in required:
                if record.get(name) is None:
                    raise ValueError(
                        f"Record {i} is missing required field '{name}' (or it is None)"
                    )

    def write_data_file(
        self,
        file_path: str,
        records: List[Dict[str, Any]],
        iceberg_schema: Schema,
        file_format: FileFormat = FileFormat.PARQUET,
        partition_values: Optional[Dict[str, Any]] = None,
    ) -> DataFile:
        """Write data records to a file and return DataFile metadata"""

        if records:
            self.validate_records_strict(records, iceberg_schema)

        arrow_schema = self.create_arrow_schema(iceberg_schema)

        # Convert path for PyArrow (adds bucket prefix for S3)
        arrow_path = self._get_arrow_path(file_path)

        # Convert records to Arrow table to compute statistics before writing
        lower_bounds = None
        upper_bounds = None
        if records:
            table = pa.Table.from_pylist(records, schema=arrow_schema)
            lower_bounds, upper_bounds = self._compute_column_bounds(table, iceberg_schema)

        with DataFileWriter(arrow_path, file_format, arrow_schema, {}, self._pyarrow_fs) as writer:
            if records:
                # Write in batches to handle large datasets efficiently
                batch_size = 1000
                for i in range(0, len(records), batch_size):
                    batch_records = records[i : i + batch_size]
                    writer.write_records(batch_records)

        # Create and return the DataFile object with statistics
        # For S3, use storage backend to get size
        if isinstance(self.storage, S3StorageBackend):
            clean_path = file_path.lstrip("/")
            file_size = self.storage.get_size(clean_path)
            # Compute checksum by reading file content (S3) using streaming
            # This avoids loading the entire file into memory
            with self.storage.open_file(clean_path) as stream:
                checksum = IntegrityChecker.compute_checksum_from_stream(stream)
        else:
            # For local filesystem, use the absolute arrow_path
            file_size = os.path.getsize(arrow_path)
            checksum = IntegrityChecker.compute_file_checksum(arrow_path)

        return DataFile(
            file_path=file_path,
            file_format=file_format,
            partition_values=partition_values or {},
            record_count=writer.row_count,
            file_size_in_bytes=file_size,
            lower_bounds=lower_bounds,
            upper_bounds=upper_bounds,
            checksum=checksum,
        )

    def _compute_column_bounds(
        self,
        table: pa.Table,
        iceberg_schema: Schema,
    ) -> Tuple[Optional[Dict[int, Any]], Optional[Dict[int, Any]]]:
        """Compute min/max bounds for each column.

        These bounds are used for partition pruning - allowing scan() to
        skip files that cannot contain matching records.

        Args:
            table: PyArrow Table with the data
            iceberg_schema: Iceberg schema for field ID mapping

        Returns:
            Tuple of (lower_bounds, upper_bounds) dicts mapping field ID to value
        """
        import pyarrow.compute as pc

        lower_bounds: Dict[int, Any] = {}
        upper_bounds: Dict[int, Any] = {}

        for field_dict in iceberg_schema.fields:
            field_id = field_dict.get("id")
            field_name = field_dict.get("name")
            field_type = field_dict.get("type", "string")

            if field_id is None or field_name is None:
                continue

            if field_name not in table.column_names:
                continue

            # Skip complex types and binary - can't compute meaningful bounds
            if field_type in ("binary", "fixed", "list", "map", "struct"):
                continue

            column = table.column(field_name)

            try:
                # Compute min/max using PyArrow compute
                min_scalar = pc.min(column)
                max_scalar = pc.max(column)

                min_val = min_scalar.as_py()
                max_val = max_scalar.as_py()

                if min_val is not None:
                    lower_bounds[field_id] = min_val
                if max_val is not None:
                    upper_bounds[field_id] = max_val
            except pa.ArrowNotImplementedError:
                # min/max is not defined for this column type: no bounds, hence
                # no pruning for it (correct, just less selective).
                logger.debug(
                    f"No min/max support for column '{field_name}' "
                    f"({column.type}); skipping bounds"
                )
                continue

        return lower_bounds if lower_bounds else None, upper_bounds if upper_bounds else None

    def write_pandas_file(
        self,
        file_path: str,
        df: "pd.DataFrame",
        iceberg_schema: Schema,
        file_format: FileFormat = FileFormat.PARQUET,
        partition_values: Optional[Dict[str, Any]] = None,
    ) -> DataFile:
        """Write a pandas DataFrame to a file and return DataFile metadata (requires pandas)"""
        if not PANDAS_AVAILABLE:
            raise ImportError(
                "pandas is not available. Install with: pip install datashard[pandas]"
            )

        # Validate that the DataFrame is compatible with the schema
        if not self.validate_pandas_compatibility(df, iceberg_schema):
            raise ValueError("DataFrame is not compatible with the provided schema")

        arrow_schema = self.create_arrow_schema(iceberg_schema)

        # Convert path for PyArrow (adds bucket prefix for S3)
        arrow_path = self._get_arrow_path(file_path)

        # Compute column bounds before writing (parity with write_data_file so
        # pandas-written files participate in pruning)
        arrow_table = pa.Table.from_pandas(df, schema=arrow_schema)
        lower_bounds, upper_bounds = self._compute_column_bounds(arrow_table, iceberg_schema)

        with DataFileWriter(arrow_path, file_format, arrow_schema, {}, self._pyarrow_fs) as writer:
            writer.write_batch(arrow_table)

        # Size + checksum via the RESOLVED path (file_path is table-relative and
        # only resolves from the table root; arrow_path is the real location)
        if isinstance(self.storage, S3StorageBackend):
            clean_path = file_path.lstrip("/")
            file_size = self.storage.get_size(clean_path)
            with self.storage.open_file(clean_path) as stream:
                checksum = IntegrityChecker.compute_checksum_from_stream(stream)
        else:
            file_size = os.path.getsize(arrow_path)
            checksum = IntegrityChecker.compute_file_checksum(arrow_path)

        return DataFile(
            file_path=file_path,
            file_format=file_format,
            partition_values=partition_values or {},
            record_count=writer.row_count,
            file_size_in_bytes=file_size,
            lower_bounds=lower_bounds,
            upper_bounds=upper_bounds,
            checksum=checksum,
        )

    def read_data_file(
        self,
        file_path: str,
        file_format: FileFormat = FileFormat.PARQUET,
        columns: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Read data from a file and return as list of records"""

        # Read through OUR storage backend, not pyarrow's S3 filesystem (#54):
        # pyarrow's S3 client sends a header OVH rejects on every GetObject.
        # The object is seekable, so parquet still reads only what it needs.
        with self.open_parquet_source(file_path) as source, \
                DataFileReader(source, file_format) as reader:
            if columns:
                table = reader.read_columns(columns)
            else:
                table = reader.read_all()

            # Convert to list of dictionaries
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

        # Read through OUR storage backend, not pyarrow's S3 filesystem (#54):
        # pyarrow's S3 client sends a header OVH rejects on every GetObject.
        # The object is seekable, so parquet still reads only what it needs.
        with self.open_parquet_source(file_path) as source, \
                DataFileReader(source, file_format) as reader:
            if columns:
                return reader.read_columns_pandas(columns)
            else:
                return reader.read_pandas()

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
