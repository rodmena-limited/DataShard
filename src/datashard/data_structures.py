"""
Core data structures for the Python Iceberg implementation
"""

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class FileFormat(Enum):
    """Supported file formats"""

    PARQUET = "parquet"
    AVRO = "avro"
    ORC = "orc"


class ManifestContent(Enum):
    """Type of content in manifest files"""

    DATA = 0
    DELETES = 1


@dataclass
class Schema:
    """Table schema definition"""

    schema_id: int
    fields: List[Dict[str, Any]]
    schema_string: str = ""

    def __post_init__(self) -> None:
        if not self.schema_string:
            self.schema_string = json.dumps(self.fields)

        # Validate fields
        valid_primitive_types = {
            "boolean", "int", "long", "float", "double",
            "date", "time", "timestamp", "string",
            "uuid", "fixed", "binary"
        }

        for field_def in self.fields:
            if "id" not in field_def:
                 raise ValueError(f"Invalid schema: Field missing required property 'id': {field_def}")
            if "name" not in field_def:
                 raise ValueError(f"Invalid schema: Field missing required property 'name': {field_def}")
            if "type" not in field_def:
                 raise ValueError(f"Invalid schema: Field missing required property 'type': {field_def}")

            f_type = field_def["type"]
            if isinstance(f_type, str):
                if f_type not in valid_primitive_types:
                     raise ValueError(f"Invalid schema: Unknown field type '{f_type}' in field '{field_def['name']}'. Supported primitive types: {valid_primitive_types}")
            # We permit dict/list for complex types (struct, list, map) without deep validation for now


@dataclass
class PartitionField:
    """Partition field definition"""

    source_id: int
    field_id: int
    name: str
    transform: str  # e.g., "identity", "bucket[16]", "truncate[10]", etc.


@dataclass
class PartitionSpec:
    """Partition specification"""

    spec_id: int
    fields: List[PartitionField]


@dataclass
class SortField:
    """Sort field definition"""

    source_id: int
    field_id: int
    transform: str
    direction: str  # "asc" or "desc"


@dataclass
class SortOrder:
    """Sort order specification"""

    order_id: int
    fields: List[SortField]


@dataclass
class DataFile:
    """Represents a data file in the table"""

    file_path: str
    file_format: FileFormat
    partition_values: Dict[str, Any]
    record_count: int
    file_size_in_bytes: int
    column_sizes: Optional[Dict[int, int]] = None
    value_counts: Optional[Dict[int, int]] = None
    null_value_counts: Optional[Dict[int, int]] = None
    lower_bounds: Optional[Dict[int, Any]] = None
    upper_bounds: Optional[Dict[int, Any]] = None
    key_metadata: Optional[bytes] = None
    checksum: Optional[str] = None
    split_offsets: Optional[List[int]] = None
    split_compressed_offsets: Optional[List[int]] = None
    equality_ids: Optional[List[int]] = None
    sort_order_id: Optional[int] = None


@dataclass
class DeleteFile:
    """Represents a delete file in the table"""

    file_path: str
    file_format: FileFormat
    partition_values: Dict[str, Any]
    record_count: int
    file_size_in_bytes: int
    content: ManifestContent = ManifestContent.DELETES


@dataclass
class ManifestFile:
    """Manifest file that lists data or delete files"""

    manifest_path: str
    manifest_length: int
    partition_spec_id: int
    added_snapshot_id: int
    added_data_files_count: int
    existing_data_files_count: int
    deleted_data_files_count: int
    partitions: List[Dict[str, Any]]  # partition summaries
    content: ManifestContent = ManifestContent.DATA
    sequence_number: Optional[int] = None
    min_sequence_number: Optional[int] = None


@dataclass
class Snapshot:
    """Represents a snapshot of the table at a point in time"""

    snapshot_id: int
    timestamp_ms: int  # milliseconds since epoch
    manifest_list: str  # path to manifest list file
    parent_snapshot_id: Optional[int] = None
    operation: Optional[str] = None  # "append", "replace", "delete", etc.
    summary: Optional[Dict[str, str]] = None
    schema_id: Optional[int] = None

    def __post_init__(self) -> None:
        if self.summary is None:
            self.summary = {}


@dataclass
class HistoryEntry:
    """History entry for tracking snapshot changes"""

    timestamp_ms: int
    snapshot_id: int


@dataclass
class TableMetadata:
    """Main metadata structure for an Iceberg table"""

    location: str
    table_uuid: str = field(default_factory=lambda: str(uuid.uuid4()))
    format_version: int = 2
    last_sequence_number: int = 0
    last_updated_ms: int = field(default_factory=lambda: int(datetime.now().timestamp() * 1000))
    last_column_id: int = 0
    schemas: List[Schema] = field(default_factory=list)
    current_schema_id: int = 0
    partition_specs: List[PartitionSpec] = field(default_factory=list)
    default_spec_id: int = 0
    sort_orders: List[SortOrder] = field(default_factory=list)
    default_sort_order_id: int = 1
    properties: Dict[str, str] = field(default_factory=dict)
    current_snapshot_id: Optional[int] = None
    snapshots: List[Snapshot] = field(default_factory=list)
    snapshot_log: List[HistoryEntry] = field(default_factory=list)
    metadata_log: List[Dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.schemas:
            # Create a default empty schema
            self.schemas = [Schema(schema_id=0, fields=[])]
        if self.current_schema_id == 0 and self.schemas:
            self.current_schema_id = self.schemas[0].schema_id
        if not self.partition_specs:
            self.partition_specs = [PartitionSpec(spec_id=0, fields=[])]
        if not self.sort_orders:
            self.sort_orders = [SortOrder(order_id=1, fields=[])]
