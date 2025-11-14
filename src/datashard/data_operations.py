"""
Data file operations and readers/writers for the Python Iceberg implementation
"""

import os
import tempfile
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

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


if TYPE_CHECKING:
    from .file_manager import FileManager


class DataFileReader:
    """Reader for Iceberg data files"""

    def __init__(self, file_path: str, file_format: FileFormat, schema: Optional[pa.Schema] = None):
        self.file_path = file_path
        self.file_format = file_format
        self._schema = schema  # Use private attribute to not conflict with schema method
        self._reader = None

    def __enter__(self) -> "DataFileReader":
        self.open()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def open(self) -> None:
        """Open the data file for reading"""
        if self.file_format == FileFormat.PARQUET:
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

        return self._reader.read()

    def read_batches(self, batch_size: int = 1000) -> Iterator[pa.RecordBatch]:
        """Read data in batches"""
        if not self._reader:
            self.open()

        for batch in self._reader.iter_batches(batch_size=batch_size):
            yield batch

    def read_columns(self, column_names: List[str]) -> pa.Table:
        """Read specific columns from the file"""
        if not self._reader:
            self.open()

        return self._reader.read(columns=column_names)

    def read_pandas(self) -> Optional["pd.DataFrame"]:
        """Read data as pandas DataFrame (requires pandas)"""
        if not PANDAS_AVAILABLE:
            raise ImportError(
                "pandas is not available. Install with: pip install datashard[pandas]"
            )

        if not self._reader:
            self.open()

        if self._reader:
            table = self._reader.read()
            return table.to_pandas()
        return None

    def read_batches_pandas(self, batch_size: int = 1000) -> Iterator["pd.DataFrame"]:
        """Read data in pandas DataFrame batches (requires pandas)"""
        if not PANDAS_AVAILABLE:
            raise ImportError(
                "pandas is not available. Install with: pip install datashard[pandas]"
            )

        if not self._reader:
            self.open()

        if self._reader:
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

        if self._reader:
            table = self._reader.read(columns=column_names)
            return table.to_pandas()
        else:
            return pd.DataFrame() if pd else None

    def schema(self) -> Optional[pa.Schema]:
        """Get the schema of the file"""
        # If no reader opened yet, return the stored schema
        if not self._reader:
            self.open()

        if self._reader:
            return self._reader.schema
        else:
            return self._schema  # Return stored schema if reader not available


class DataFileWriter:
    """Writer for Iceberg data files"""

    def __init__(
        self,
        file_path: str,
        file_format: FileFormat,
        schema: pa.Schema,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.file_path = file_path
        self.file_format = file_format
        self._schema = schema  # Use private attribute to not conflict with any method
        self.metadata = metadata or {}
        self._writer = None
        self._temp_file = None
        self._row_count = 0

    def __enter__(self) -> "DataFileWriter":
        self.open()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def open(self) -> None:
        """Open the file for writing"""
        if self.file_format == FileFormat.PARQUET:
            # Create a temporary file first
            temp_dir = os.path.dirname(self.file_path)
            self._temp_file = tempfile.NamedTemporaryFile(
                delete=False, dir=temp_dir, suffix=".parquet"
            )

            # Add our metadata to the schema
            schema_metadata = self._schema.metadata if self._schema.metadata else {}
            for key, value in self.metadata.items():
                schema_metadata[key.encode("utf-8")] = str(value).encode("utf-8")

            modified_schema = self._schema.with_metadata(schema_metadata)

            self._writer = pq.ParquetWriter(
                self._temp_file.name, modified_schema, compression="snappy"
            )
        else:
            raise ValueError(f"Unsupported file format: {self.file_format}")

    def write_batch(self, batch: Union[pa.RecordBatch, pa.Table]) -> None:
        """Write a batch of data to the file"""
        if not self._writer:
            self.open()

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

        # Convert pandas DataFrame to Arrow Table
        table = pa.Table.from_pandas(df, schema=self._schema)
        self.write_batch(table)

    def close(self) -> None:
        """Close the writer and finalize the file"""
        if self._writer:
            self._writer.close()
            # Move temp file to final location
            if self._temp_file:
                os.rename(self._temp_file.name, self.file_path)
                self._temp_file = None
            self._writer = None

    @property
    def row_count(self) -> int:
        """Get the number of rows written to the file"""
        return self._row_count


class DataFileManager:
    """Manages data file operations including reading/writing with proper schema management"""

    def __init__(self, file_manager: "FileManager"):
        self.file_manager = file_manager
        self._arrow_schema_cache: Dict[int, pa.Schema] = {}

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

    def write_data_file(
        self,
        file_path: str,
        records: List[Dict[str, Any]],
        iceberg_schema: Schema,
        file_format: FileFormat = FileFormat.PARQUET,
        partition_values: Optional[Dict[str, Any]] = None,
    ) -> DataFile:
        """Write data records to a file and return DataFile metadata"""

        arrow_schema = self.create_arrow_schema(iceberg_schema)

        with DataFileWriter(file_path, file_format, arrow_schema, {}) as writer:
            if records:
                # Write in batches to handle large datasets efficiently
                batch_size = 1000
                for i in range(0, len(records), batch_size):
                    batch_records = records[i : i + batch_size]
                    writer.write_records(batch_records)

        # Create and return the DataFile object with statistics
        return DataFile(
            file_path=file_path,
            file_format=file_format,
            partition_values=partition_values or {},
            record_count=writer.row_count,
            file_size_in_bytes=os.path.getsize(file_path),
        )

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

        with DataFileWriter(file_path, file_format, arrow_schema, {}) as writer:
            writer.write_pandas(df)

        # Create and return the DataFile object with statistics
        return DataFile(
            file_path=file_path,
            file_format=file_format,
            partition_values=partition_values or {},
            record_count=writer.row_count,
            file_size_in_bytes=os.path.getsize(file_path),
        )

    def read_data_file(
        self,
        file_path: str,
        file_format: FileFormat = FileFormat.PARQUET,
        columns: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Read data from a file and return as list of records"""

        with DataFileReader(file_path, file_format) as reader:
            if columns:
                table = reader.read_columns(columns)
            else:
                table = reader.read_all()

            # Convert to list of dictionaries
            return table.to_pylist()

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

        with DataFileReader(file_path, file_format) as reader:
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
        except Exception:
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
        except Exception:
            return False
