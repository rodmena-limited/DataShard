"""
datashard - Safe concurrent data operations for ML/AI workloads

A Python implementation of Apache Iceberg providing ACID transactions,
time travel, and safe concurrent access.
"""
__version__ = "0.1.6"
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
from .iceberg import DataFile, FileFormat, create_table, load_table
from .transaction import Table, Transaction

__all__ = [
    'create_table',
    'load_table',
    'DataFile',
    'FileFormat',
    'Schema',
    'PartitionSpec',
    'SortOrder',
    'DeleteFile',
    'ManifestFile',
    'Snapshot',
    'TableMetadata',
    'Table',
    'Transaction',
    '__version__',
    '__author__'
]
