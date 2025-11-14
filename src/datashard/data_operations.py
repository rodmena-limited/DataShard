"""
Data file operations and readers/writers for the Python Iceberg implementation
"""
import os
import tempfile
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

import pyarrow as pa
import pyarrow.parquet as pq

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

    def schema(self) -> pa.Schema:
        """Get the schema of the file"""
        if not self._reader:
            self.open()

        return self._reader.schema


class DataFileWriter:
    """Writer for Iceberg data files"""

    def __init__(self, file_path: str, file_format: FileFormat,
                 schema: pa.Schema, metadata: Optional[Dict[str, Any]] = None):
        self.file_path = file_path
        self.file_format = file_format
        self._schema = schema  # Use private attribute to not conflict with schema method
        self.metadata = metadata or {}
        self._writer = None
        self._temp_file = None
        self._row_count = 0

    def __enter__(self) -> "DataFileReader":
        self.open()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def open(self) -> None:
        """Open the data file for writing"""
        if self.file_format == FileFormat.PARQUET:
            # Create a temporary file first
            temp_dir = os.path.dirname(self.file_path)
            self._temp_file = tempfile.NamedTemporaryFile(
                delete=False, dir=temp_dir, suffix='.parquet'
            )

            # Add our metadata to the schema
            schema_metadata = self._schema.metadata if self._schema.metadata else {}
            for key, value in self.metadata.items():
                schema_metadata[key.encode('utf-8')] = str(value).encode('utf-8')

            modified_schema = self._schema.with_metadata(schema_metadata)

            self._writer = pq.ParquetWriter(
                self._temp_file.name,
                modified_schema,
                compression='snappy'
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

    def file_size(self) -> int:
        """Get the size of the written file"""
        return os.path.getsize(self.file_path) if os.path.exists(self.file_path) else 0


class DataFileManager:
    """Manages data file operations, reading and writing"""

    def __init__(self, file_manager: "FileManager") -> None:
        self.file_manager = file_manager
        self._arrow_schema_cache: Dict[int, pa.Schema] = {}

    def create_arrow_schema(self, iceberg_schema: Schema) -> pa.Schema:
        """Convert Iceberg schema to PyArrow schema"""
        if iceberg_schema.schema_id in self._arrow_schema_cache:
            return self  # type: ignore[generic-type]._arrow_schema_cache[iceberg_schema.schema_id]

        fields = []
        for field in iceberg_schema.fields:
            field_id = field.get('id', 0)
            field_name = field.get('name', f'field_{field_id}')
            field_type = field.get('type', 'string')

            # Map Iceberg types to PyArrow types
            arrow_type = self._iceberg_type_to_arrow(field_type)

            # Check if field is required
            is_nullable = not field.get('required', False)

            fields.append(pa.field(field_name, arrow_type, nullable=is_nullable))

        schema = pa.schema(fields)
        self._arrow_schema_cache[iceberg_schema.schema_id] = schema
        return schema

    def _iceberg_type_to_arrow(self, iceberg_type: Union[str, Dict[str, Any]]) -> pa.DataType:
        """Convert Iceberg type string to PyArrow type"""
        if isinstance(iceberg_type, dict):
            iceberg_type = iceberg_type.get('type', 'string')

        type_mapping = {
            'boolean': pa.bool_(),
            'int': pa.int32(),
            'long': pa.int64(),
            'float': pa.float32(),
            'double': pa.float64(),
            'date': pa.date32(),
            'time': pa.time64('us'),
            'timestamp': pa.timestamp('us'),
            'string': pa.string(),
            'uuid': pa.string(),  # For UUID handling
            'binary': pa.binary(),
            'fixed': pa.binary(),
        }

        # Handle complex types
        if isinstance(iceberg_type, str):
            if iceberg_type.startswith('list<'):
                element_type = iceberg_type[5:-1]  # Remove 'list<>' wrapper
                return pa.list_(self._iceberg_type_to_arrow(element_type))
            elif iceberg_type.startswith('map<'):
                # For now, treat as string - in real implementation would need key/value types
                return pa.string()
            elif iceberg_type.startswith('struct<'):
                return pa.string()  # For now, treat as string

        return type_mapping.get(str(iceberg_type), pa.string())  # Default ...string

    def write_data_file(self,
                       file_path: str,
                       records: List[Dict[str, Any]],
                       iceberg_schema: Schema,
                       file_format: FileFormat = FileFormat.PARQUET,
                       partition_values: Optional[Dict[str, Any]] = None) -> DataFile:
        """Write data records to a file and return DataFile metadata"""

        arrow_schema = self.create_arrow_schema(iceberg_schema)

        # Prepare metadata
        import time
        metadata = {
            'iceberg_schema_id': str(iceberg_schema.schema_id),
            'created_at': str(int(time.time() * 1000000))  # microseconds since epoch
        }
        if partition_values:
            metadata.update({f'partition_{k}': str(v) for k, v in partition_values.items()})

        # Create the writer
        with DataFileWriter(file_path, file_format, arrow_schema, metadata) as writer:
            if records:
                # Write in batches to handle large datasets
                batch_size = 1000
                for i in range(0, len(records), batch_size):
                    batch_records = records[i:i + batch_size]
                    writer.write_records(batch_records)

        # Create and return the DataFile object with statistics
        record_count = writer.row_count
        file_size = os.path.getsize(file_path)

        return DataFile(
            file_path=file_path,
            file_format=file_format,
            partition_values=partition_values or {},
            record_count=record_count,
            file_size_in_bytes=file_size
        )

    def read_data_file(self,
                      file_path: str,
                      file_format: FileFormat = FileFormat.PARQUET,
                      columns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Read data from a file and return as list of records"""

        with DataFileReader(file_path, file_format) as reader:
            if columns:
                table = reader.read_columns(columns)
            else:
                table = reader.read_all()

            # Convert to list of dictionaries
            records = table.to_pylist()
            return records

    def append_to_data_file(self,
                           file_path: str,
                           new_records: List[Dict[str, Any]],
                           iceberg_schema: Schema,
                           file_format: FileFormat = FileFormat.PARQUET) -> DataFile:
        """Append records to an existing data file"""

        # For Parquet files, we need to read the existing data, append, and write back
        # In a real implementation, we'd use append functionality or create a new file with both datasets

        if file_format == FileFormat.PARQUET:
            if os.path.exists(file_path):
                # Read existing data
                existing_records = self.read_data_file(file_path, file_format)
                # Combine with new records
                all_records = existing_records + new_records
            else:
                all_records = new_records

            # For efficiency in real systems, we'd typically create new files rather than appending to existing ones
            # This is a simplification for this implementation
            arrow_schema = self.create_arrow_schema(iceberg_schema)
            temp_path = f"{file_path}.tmp"

            with DataFileWriter(temp_path, file_format, arrow_schema) as writer:
                if all_records:
                    batch_size = 1000
                    for i in range(0, len(all_records), batch_size):
                        batch_records = all_records[i:i + batch_size]
                        writer.write_records(batch_records)

            # Replace original file
            os.replace(temp_path, file_path)

            return DataFile(
                file_path=file_path,
                file_format=file_format,
                partition_values={},  # Would be extracted from path in real implementation
                record_count=writer.row_count,
                file_size_in_bytes=os.path.getsize(file_path)
            )

    def validate_data_compatibility(self,
                                   records: List[Dict[str, Any]],
                                   iceberg_schema: Schema) -> bool:
        """Validate that records are compatible with the schema"""
        try:
            arrow_schema = self.create_arrow_schema(iceberg_schema)

            # Try to create a table with the records and schema
            pa.Table.from_pylist(records, schema=arrow_schema)
            return True
        except Exception:
            return False
