"""
Filter expression support for predicate pushdown and partition pruning.
Converts user-friendly filter syntax to PyArrow filter expressions.
"""

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc

if TYPE_CHECKING:
    from .data_structures import DataFile, Schema


class FilterOp(Enum):
    """Supported filter operations"""
    EQ = "=="
    NE = "!="
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="
    IN = "in"
    NOT_IN = "not_in"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


@dataclass
class FilterExpression:
    """Represents a single filter condition"""
    column: str
    op: FilterOp
    value: Any


def parse_filter_dict(filter_dict: Dict[str, Any]) -> List[FilterExpression]:
    """
    Parse user-friendly filter dict into FilterExpression list.

    Supported formats:
        {"column": value}                    -> column == value
        {"column": ("==", value)}            -> column == value
        {"column": (">", value)}             -> column > value
        {"column": ("in", [v1, v2])}         -> column in [v1, v2]
        {"column": ("between", (lo, hi))}    -> lo <= column <= hi
        {"column": ("is_null", True)}        -> column is null
        {"column": ("is_not_null", True)}    -> column is not null

    Args:
        filter_dict: Dictionary mapping column names to filter conditions

    Returns:
        List of FilterExpression objects
    """
    expressions = []
    for column, condition in filter_dict.items():
        if isinstance(condition, tuple) and len(condition) == 2:
            op_str, value = condition
            op_str_lower = op_str.lower() if isinstance(op_str, str) else op_str

            if op_str_lower == "between":
                # Expand to two conditions: lo <= column <= hi
                lo, hi = value
                expressions.append(FilterExpression(column, FilterOp.GE, lo))
                expressions.append(FilterExpression(column, FilterOp.LE, hi))
            elif op_str_lower in ("is_null", "isnull"):
                expressions.append(FilterExpression(column, FilterOp.IS_NULL, None))
            elif op_str_lower in ("is_not_null", "notnull", "isnotnull"):
                expressions.append(FilterExpression(column, FilterOp.IS_NOT_NULL, None))
            else:
                op = _parse_op(op_str)
                expressions.append(FilterExpression(column, op, value))
        else:
            # Simple equality: {"column": value}
            expressions.append(FilterExpression(column, FilterOp.EQ, condition))
    return expressions


def _parse_op(op_str: str) -> FilterOp:
    """Parse operator string to FilterOp enum"""
    mapping = {
        "==": FilterOp.EQ,
        "=": FilterOp.EQ,
        "eq": FilterOp.EQ,
        "!=": FilterOp.NE,
        "<>": FilterOp.NE,
        "ne": FilterOp.NE,
        "<": FilterOp.LT,
        "lt": FilterOp.LT,
        "<=": FilterOp.LE,
        "le": FilterOp.LE,
        ">": FilterOp.GT,
        "gt": FilterOp.GT,
        ">=": FilterOp.GE,
        "ge": FilterOp.GE,
        "in": FilterOp.IN,
        "not_in": FilterOp.NOT_IN,
        "not in": FilterOp.NOT_IN,
        "notin": FilterOp.NOT_IN,
    }
    return mapping.get(op_str.lower() if isinstance(op_str, str) else op_str, FilterOp.EQ)


def to_pyarrow_filter(
    expressions: List[FilterExpression],
) -> Optional[List[Tuple[str, str, Any]]]:
    """
    Convert FilterExpressions to PyArrow filter format.

    PyArrow filters use format: [("column", "op", value), ...]
    Multiple filters are ANDed together.

    Args:
        expressions: List of FilterExpression objects

    Returns:
        List of tuples in PyArrow filter format, or None if empty
    """
    if not expressions:
        return None

    pa_filters = []
    for expr in expressions:
        # Skip null checks - handled separately
        if expr.op in (FilterOp.IS_NULL, FilterOp.IS_NOT_NULL):
            continue

        pa_op = {
            FilterOp.EQ: "==",
            FilterOp.NE: "!=",
            FilterOp.LT: "<",
            FilterOp.LE: "<=",
            FilterOp.GT: ">",
            FilterOp.GE: ">=",
            FilterOp.IN: "in",
            FilterOp.NOT_IN: "not in",
        }.get(expr.op)

        if pa_op:
            pa_filters.append((expr.column, pa_op, expr.value))

    return pa_filters if pa_filters else None


def _build_condition(
    expr: FilterExpression,
    field: pc.Expression,
) -> pc.Expression:
    """Build a PyArrow compute condition for a single filter expression."""
    op_handlers: Dict[FilterOp, Any] = {
        FilterOp.EQ: lambda: field == expr.value,
        FilterOp.NE: lambda: field != expr.value,
        FilterOp.LT: lambda: field < expr.value,
        FilterOp.LE: lambda: field <= expr.value,
        FilterOp.GT: lambda: field > expr.value,
        FilterOp.GE: lambda: field >= expr.value,
        FilterOp.IN: lambda: pc.is_in(field, value_set=pa.array(expr.value)),
        FilterOp.NOT_IN: lambda: ~pc.is_in(field, value_set=pa.array(expr.value)),
        FilterOp.IS_NULL: lambda: field.is_null(),
        FilterOp.IS_NOT_NULL: lambda: field.is_valid(),
    }
    handler = op_handlers.get(expr.op)
    if handler is None:
        # Default to equality for unknown ops
        return field == expr.value
    return handler()


def to_pyarrow_compute_expression(
    expressions: List[FilterExpression],
) -> Optional[pc.Expression]:
    """
    Convert FilterExpressions to PyArrow compute expression.
    Used for more complex filtering with pyarrow.compute.

    Args:
        expressions: List of FilterExpression objects

    Returns:
        PyArrow compute Expression, or None if empty
    """
    if not expressions:
        return None

    combined: Optional[pc.Expression] = None
    for expr in expressions:
        field = pc.field(expr.column)
        condition = _build_condition(expr, field)

        if combined is None:
            combined = condition
        else:
            combined = combined & condition

    return combined


def prune_files_by_bounds(
    data_files: List["DataFile"],
    expressions: List[FilterExpression],
    schema: "Schema",
) -> List["DataFile"]:
    """
    Prune data files based on column bounds (min/max statistics).
    Returns files that MAY contain matching records.

    This is a key optimization - we can skip reading entire files
    if their column bounds prove no records can match the filter.

    Args:
        data_files: List of DataFile objects with lower_bounds/upper_bounds
        expressions: List of FilterExpression objects
        schema: Table schema for column name to ID mapping

    Returns:
        Filtered list of DataFile objects that may contain matches
    """
    if not expressions or not data_files:
        return data_files

    # Build column name to field ID mapping from schema
    col_name_to_id: Dict[str, int] = {}
    for field_dict in schema.fields:
        field_id = field_dict.get("id")
        field_name = field_dict.get("name")
        if field_id is not None and field_name:
            col_name_to_id[field_name] = field_id

    pruned = []
    for data_file in data_files:
        if _file_may_match(data_file, expressions, col_name_to_id):
            pruned.append(data_file)

    return pruned


def _file_may_match(
    data_file: "DataFile",
    expressions: List[FilterExpression],
    col_name_to_id: Dict[str, int],
) -> bool:
    """
    Check if a file MAY contain matching records based on column bounds.

    Uses the principle: if file's [min, max] range for a column doesn't
    overlap with the filter condition, the file can be skipped.

    Args:
        data_file: DataFile with lower_bounds and upper_bounds
        expressions: Filter conditions to check
        col_name_to_id: Column name to field ID mapping

    Returns:
        True if file may contain matches, False if it definitely doesn't
    """
    lower_bounds = data_file.lower_bounds or {}
    upper_bounds = data_file.upper_bounds or {}

    for expr in expressions:
        col_id = col_name_to_id.get(expr.column)
        if col_id is None:
            # Unknown column, can't prune - assume it may match
            continue

        file_min = lower_bounds.get(col_id)
        file_max = upper_bounds.get(col_id)

        if file_min is None or file_max is None:
            # No bounds available for this column, can't prune
            continue

        # Check if filter condition is impossible given bounds
        try:
            if expr.op == FilterOp.EQ:
                # For equality: file_min <= value <= file_max must be possible
                if expr.value < file_min or expr.value > file_max:
                    return False

            elif expr.op == FilterOp.NE:
                # For inequality: can only prune if entire file has same value
                if file_min == file_max == expr.value:
                    return False

            elif expr.op == FilterOp.GT:
                # Need at least one value > filter_value
                # Possible if file_max > value
                if file_max <= expr.value:
                    return False

            elif expr.op == FilterOp.GE:
                # Need at least one value >= filter_value
                # Possible if file_max >= value
                if file_max < expr.value:
                    return False

            elif expr.op == FilterOp.LT:
                # Need at least one value < filter_value
                # Possible if file_min < value
                if file_min >= expr.value:
                    return False

            elif expr.op == FilterOp.LE:
                # Need at least one value <= filter_value
                # Possible if file_min <= value
                if file_min > expr.value:
                    return False

            elif expr.op == FilterOp.IN:
                # For IN: at least one value in the list must be in [file_min, file_max]
                if expr.value:
                    has_possible_match = any(
                        file_min <= v <= file_max for v in expr.value
                    )
                    if not has_possible_match:
                        return False

        except TypeError:
            # Comparison failed (incompatible types), can't prune
            continue

    return True  # File may contain matches


def get_column_id_by_name(schema: "Schema", column_name: str) -> Optional[int]:
    """Get field ID for a column name from schema"""
    for field_dict in schema.fields:
        if field_dict.get("name") == column_name:
            return field_dict.get("id")
    return None
