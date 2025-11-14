"""
Main module for the Python Iceberg implementation
Provides the primary API for working with Iceberg tables
"""
import os
from typing import Optional

from .data_structures import DataFile, FileFormat, PartitionSpec, Schema
from .transaction import Table, Transaction


def create_table(table_path: str, schema: Optional[Schema] = None, partition_spec: Optional[PartitionSpec] = None) -> 'Table':
    """
    Create a new Iceberg table

    Args:
        table_path: Path where the table should be stored
        schema: Optional schema for the table
        partition_spec: Optional partition spec for the table

    Returns:
        Table instance
    """
    # Ensure the table directory exists
    os.makedirs(table_path, exist_ok=True)

    # Create the table instance
    table = Table(table_path, create_if_not_exists=True)

    # Initialize the table with default metadata if it doesn't exist
    current_metadata = table.metadata_manager.refresh()
    if current_metadata is None:
        from .data_structures import TableMetadata
        initial_metadata = TableMetadata(
            location=table_path
        )
        table.metadata_manager.initialize_table(initial_metadata)

    # If schema is provided, we'd update the table metadata
    # For now, we'll just return the table with default schema
    return table


def load_table(table_path: str) -> Table:
    """
    Load an existing Iceberg table

    Args:
        table_path: Path to the existing table

    Returns:
        Table instance
    """
    if not os.path.exists(os.path.join(table_path, 'metadata')):
        raise ValueError(f"No Iceberg table found at {table_path}")

    return Table(table_path, create_if_not_exists=False)


__all__ = [
    'Table', 'Transaction', 'create_table', 'load_table',
    'DataFile', 'FileFormat'
]
