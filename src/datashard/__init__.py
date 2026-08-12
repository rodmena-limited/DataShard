"""
datashard - Safe concurrent data operations for ML/AI workloads

A Python implementation of Apache Iceberg providing ACID transactions,
snapshotting, and safe concurrent access.

Supports both local filesystem and S3-compatible storage (AWS S3, MinIO, etc.)
"""

# Single source of truth for the version is pyproject.toml; resolve it from
# package metadata so __version__ can never drift from the released version.
try:
    from importlib.metadata import PackageNotFoundError, version

    try:
        __version__ = version("datashard")
    except PackageNotFoundError:
        __version__ = "0.7.1"
except ImportError:  # pragma: no cover - Python < 3.8
    __version__ = "0.7.1"

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
from .garbage_collector import GarbageCollectionAborted
from .iceberg import DataFile, FileFormat, create_table, load_table
from .integrity import CorruptDataError
from .metadata_manager import (
    AmbiguousCommitError,
    ConcurrentModificationException,
    TableExistsError,
)
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
    "AmbiguousCommitError",
    "TableExistsError",
    "CorruptDataError",
    "GarbageCollectionAborted",
    "__version__",
    "__author__",
]
