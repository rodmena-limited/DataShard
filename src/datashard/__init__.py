"""
datashard - Safe concurrent data operations for ML/AI workloads

A Python implementation of Apache Iceberg providing ACID transactions,
time travel, and safe concurrent access.

Supports both local filesystem and S3-compatible storage (AWS S3, MinIO, etc.)
"""

__version__ = "0.3.2"
__author__ = "RODMENA LIMITED"


# Import the main classes to make them available at package level
from .data_structures import (
    DeleteFile,
    ManifestFile,
    PartitionSpec,
    Schema,
    Snapshot,
    SortOrder,
    TableMetadata,
)
from .filters import FilterExpression, FilterOp, parse_filter_dict
from .iceberg import DataFile, FileFormat, create_table, load_table
from .metadata_manager import ConcurrentModificationException
from .transaction import Table, Transaction

__all__ = [
    "create_table",
    "load_table",
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
    "FilterOp",
    "FilterExpression",
    "parse_filter_dict",
    "ConcurrentModificationException",
    "__version__",
    "__author__",
]
