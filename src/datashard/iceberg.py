"""
Main module for the Python Iceberg implementation
Provides the primary API for working with Iceberg tables
"""

import json
from typing import Any, List, Optional, Tuple

from .data_structures import DataFile, FileFormat, PartitionSpec, Schema
from .logging_config import get_logger
from .metadata_manager import SchemaMismatchError
from .transaction import Table, Transaction

logger = get_logger(__name__)


def _schema_layout(schema: Schema) -> List[Tuple[Any, Any, str, bool]]:
    """Ordered (id, name, type, required) tuples - what a file's layout depends on."""
    return [
        (
            f.get("id"),
            f.get("name"),
            json.dumps(f.get("type"), sort_keys=True),
            bool(f.get("required", False)),
        )
        for f in schema.fields
    ]


def create_table(
    table_path: str,
    schema: Optional[Schema] = None,
    partition_spec: Optional[PartitionSpec] = None,
    if_exists: str = "error",
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
        if_exists: What to do when the table already exists with a DIFFERENT
            persisted schema than `schema`: "error" (default) raises
            SchemaMismatchError; "ignore" keeps the existing schema and logs a
            warning. A silent mismatch used to be the behaviour (#64).

    Returns:
        Table instance

    Raises:
        SchemaMismatchError: existing table, different schema, if_exists="error".
    """
    if if_exists not in ("error", "ignore"):
        raise ValueError(f"if_exists must be 'error' or 'ignore', got {if_exists!r}")

    table = Table(
        table_path,
        create_if_not_exists=True,
        schema=schema,
        partition_spec=partition_spec,
    )

    # If the table already existed, the provided schema was NOT applied. Say so
    # loudly - or refuse - rather than silently returning a different table
    # than the caller described.
    if schema is not None and not table.created:
        current = table._get_current_schema()
        if current is None or not current.fields:
            logger.warning(
                f"create_table({table_path!r}): table already existed without a persisted "
                f"schema; the provided schema was NOT applied. Appends must pass schema= "
                f"explicitly."
            )
        elif _schema_layout(current) != _schema_layout(schema):
            message = (
                f"create_table({table_path!r}): table already exists with a different "
                f"schema. Persisted fields: {[f.get('name') for f in current.fields]}; "
                f"requested fields: {[f.get('name') for f in schema.fields]}."
            )
            if if_exists == "ignore":
                logger.warning(message + " Keeping the persisted schema (if_exists='ignore').")
            else:
                raise SchemaMismatchError(
                    message + " Pass if_exists='ignore' to open it with its persisted schema, "
                    "or use load_table()."
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
