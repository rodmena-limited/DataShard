"""
Main module for the Python Iceberg implementation
Provides the primary API for working with Iceberg tables
"""

from typing import Optional

from .data_structures import DataFile, FileFormat, PartitionSpec, Schema
from .transaction import Table, Transaction


def create_table(
    table_path: str,
    schema: Optional[Schema] = None,
    partition_spec: Optional[PartitionSpec] = None,
) -> "Table":
    """
    Create a new Iceberg table

    Args:
        table_path: Path where the table should be stored
        schema: Optional schema for the table
        partition_spec: Optional partition spec for the table

    Returns:
        Table instance
    """
    from .storage_backend import create_storage_backend

    # Create storage backend and ensure table directory exists
    storage = create_storage_backend(table_path)
    storage.makedirs(".", exist_ok=True)

    # Create the table instance (will create storage internally)
    table = Table(table_path, create_if_not_exists=True)

    # Initialize the table with default metadata if it doesn't exist
    current_metadata = table.metadata_manager.refresh()
    if current_metadata is None:
        from .data_structures import TableMetadata

        initial_metadata = TableMetadata(location=table_path)
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
    from .storage_backend import create_storage_backend

    # Check if table exists using storage backend
    storage = create_storage_backend(table_path)
    if not storage.exists("metadata"):
        raise ValueError(f"No Iceberg table found at {table_path}")

    return Table(table_path, create_if_not_exists=False)


__all__ = ["Table", "Transaction", "create_table", "load_table", "DataFile", "FileFormat"]
