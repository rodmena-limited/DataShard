"""
Legacy per-file reader / writer classes (DataFileReader, DataFileWriter). The hot
path (DataFileManager) serialises tables in memory instead; these remain for API
compatibility. Split out of data_operations.py for the 500-line file cap (#71).
"""

import os
import tempfile
from typing import Any, Dict, Iterator, List, Optional, Union

import pyarrow as pa
import pyarrow.parquet as pq

# Try to import pandas as optional dependency
try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None  # Define as None to avoid reference errors

from .data_structures import FileFormat
from .logging_config import get_logger

logger = get_logger(__name__)

__all__ = ["DataFileReader", "DataFileWriter", "PANDAS_AVAILABLE", "pd"]


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
                    write_page_checksum=True,
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
                        self._temp_file.name, modified_schema, compression="lz4",
                        write_page_checksum=True,
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


