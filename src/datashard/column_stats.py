"""
Per-file column statistics (min/max bounds) used for file pruning. Split out of
data_operations.py for the 500-line file cap.
"""

from typing import Any, Dict, Optional, Tuple

import pyarrow as pa

from .data_structures import Schema
from .logging_config import get_logger

logger = get_logger(__name__)


def compute_column_bounds(
table: pa.Table,
iceberg_schema: Schema,
) -> Tuple[Optional[Dict[int, Any]], Optional[Dict[int, Any]]]:
    """Compute min/max bounds for each column.

    These bounds are used for partition pruning - allowing scan() to
    skip files that cannot contain matching records.

    Args:
        table: PyArrow Table with the data
        iceberg_schema: Iceberg schema for field ID mapping

    Returns:
        Tuple of (lower_bounds, upper_bounds) dicts mapping field ID to value
    """
    import pyarrow.compute as pc

    lower_bounds: Dict[int, Any] = {}
    upper_bounds: Dict[int, Any] = {}

    for field_dict in iceberg_schema.fields:
        field_id = field_dict.get("id")
        field_name = field_dict.get("name")
        field_type = field_dict.get("type", "string")

        if field_id is None or field_name is None:
            continue

        if field_name not in table.column_names:
            continue

        # Skip complex types and binary - can't compute meaningful bounds
        if field_type in ("binary", "fixed", "list", "map", "struct"):
            continue

        column = table.column(field_name)

        try:
            # Compute min/max using PyArrow compute
            min_scalar = pc.min(column)
            max_scalar = pc.max(column)

            min_val = min_scalar.as_py()
            max_val = max_scalar.as_py()

            if min_val is not None:
                lower_bounds[field_id] = min_val
            if max_val is not None:
                upper_bounds[field_id] = max_val
        except pa.ArrowNotImplementedError:
            # min/max is not defined for this column type: no bounds, hence
            # no pruning for it (correct, just less selective).
            logger.debug(
                f"No min/max support for column '{field_name}' "
                f"({column.type}); skipping bounds"
            )
            continue

    return lower_bounds if lower_bounds else None, upper_bounds if upper_bounds else None

