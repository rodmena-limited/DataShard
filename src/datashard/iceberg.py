"""
Main module for the Python Iceberg implementation
Provides the primary API for working with Iceberg tables
"""

from typing import Optional

from .data_structures import DataFile, FileFormat, PartitionSpec, Schema
from .logging_config import get_logger
from .transaction import Table, Transaction

logger = get_logger(__name__)


def create_table(
    table_path: str,
    schema: Optional[Schema] = None,
    partition_spec: Optional[PartitionSpec] = None,
) -> "Table":
    """
    Create a new Iceberg table (or open the existing one at table_path).

    The provided schema and partition spec are PERSISTED into the table
    metadata, so subsequent appends without an explicit schema use them.
    Initialization is guarded and race-safe: an already-initialized table is
    never overwritten, and two concurrent creators cannot clobber each other.

    Args:
        table_path: Path where the table should be stored
        schema: Optional schema for the table (persisted as the current schema)
        partition_spec: Optional partition spec for the table (persisted)

    Returns:
        Table instance
    """
    table = Table(
        table_path,
        create_if_not_exists=True,
        schema=schema,
        partition_spec=partition_spec,
    )

    # If the table already existed, a provided schema is NOT applied - warn
    # loudly rather than silently ignoring correctness-relevant input.
    if schema is not None:
        current = table._get_current_schema()
        if current is None or not current.fields:
            logger.warning(
                f"create_table({table_path!r}): table already existed without a persisted "
                f"schema; the provided schema was NOT applied. Appends must pass schema= "
                f"explicitly."
            )

    return table


def load_table(table_path: str) -> Table:
    """
    Load an existing Iceberg table

    Args:
        table_path: Path to the existing table

    Returns:
        Table instance

    Raises:
        ValueError: If no initialized table exists at table_path.
    """
    table = Table(table_path, create_if_not_exists=False)

    # Verify actual metadata exists (a bare directory is not a table). Uses the
    # recovery-aware refresh, so a table with a lost version hint still loads.
    if table.metadata_manager.refresh() is None:
        raise ValueError(f"No Iceberg table found at {table_path}")

    return table


__all__ = ["Table", "Transaction", "create_table", "load_table", "DataFile", "FileFormat"]
