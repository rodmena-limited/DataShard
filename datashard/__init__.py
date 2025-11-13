"""
datashard - Safe concurrent data operations for ML/AI workloads

A Python implementation of Apache Iceberg providing ACID transactions,
time travel, and safe concurrent access.
"""
__version__ = "0.1.2"
__author__ = "RODMENA LIMITED"

# Import the main classes to make them available at package level
from .iceberg import create_table, load_table, DataFile
from .data_structures import FileFormat
from .transaction import Table


__all__ = [
    'create_table',
    'load_table',
    'DataFile', 
    'FileFormat',
    'Table',
    '__version__',
    '__author__'
]