"""
datashard - Safe concurrent data operations for ML/AI workloads

A Python implementation of Apache Iceberg providing ACID transactions,
snapshotting, and safe concurrent access.

Supports both local filesystem and S3-compatible storage (AWS S3, MinIO, etc.)
"""

from ._version import __version__

__author__ = "RODMENA LIMITED"


# Import the main classes to make them available at package level
from .data_structures import (
    DeleteFile,
    HistoryEntry,
    ManifestContent,
    ManifestFile,
    PartitionField,
    PartitionSpec,
    Schema,
    Snapshot,
    SortField,
    SortOrder,
    TableMetadata,
)
from .filters import FilterExpression, FilterOp, parse_filter_dict
from .garbage_collector import GarbageCollectionAborted
from .iceberg import DataFile, FileFormat, create_table, load_table
from .integrity import CorruptDataError
from .metadata_manager import (
    AmbiguousCommitError,
    AmbiguousMetadataError,
    ConcurrentModificationException,
    LegacyLayoutError,
    MetadataManager,
    SchemaMismatchError,
    TableExistsError,
)
from .migrate import migrate_table
from .snapshot_manager import SnapshotManager
from .table import Table
from .transaction import Transaction
from .transaction_manager import TransactionManager

__all__ = [
    "create_table",
    "load_table",
    "migrate_table",
    "LegacyLayoutError",
    "DataFile",
    "FileFormat",
    "Schema",
    "PartitionSpec",
    "SortOrder",
    "DeleteFile",
    "ManifestFile",
    "Snapshot",
    "TableMetadata",
    "Table",
    "Transaction",
    # documented in the API reference, so they must be importable from the package
    "HistoryEntry",
    "ManifestContent",
    "PartitionField",
    "SortField",
    "MetadataManager",
    "SnapshotManager",
    "TransactionManager",
    "FilterOp",
    "FilterExpression",
    "parse_filter_dict",
    "ConcurrentModificationException",
    "AmbiguousCommitError",
    "AmbiguousMetadataError",
    "TableExistsError",
    "SchemaMismatchError",
    "CorruptDataError",
    "GarbageCollectionAborted",
    "__version__",
    "__author__",
]
