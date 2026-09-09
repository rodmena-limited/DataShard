"""
Skipping whole partitions a filter cannot match (split out of partitioning.py for the
500-line file cap, #98).
"""

from typing import Any, Callable, Dict, List, Tuple

from .data_structures import PartitionField
from .partitioning import (
    TEMPORAL,
    TRUNCATE_RE,
    UnsupportedTransform,
    canonical,
    result_type,
    transform_function,
)


def _monotonic(transform: str) -> bool:
    """True when `t(a) <= t(b)` whenever `a <= b`, so an ordering predicate can be
    evaluated against the partition value. bucket[] is deliberately NOT monotonic."""
    return transform == "identity" or transform in TEMPORAL or bool(TRUNCATE_RE.match(transform))


def prune_files_by_partition(
    data_files: List[Any],
    expressions: List[Any],
    spec_fields: List[Tuple[PartitionField, str]],
    column_of: Dict[int, str],
) -> List[Any]:
    """Drop the files whose PARTITION proves they cannot hold a matching row.

    Only ever removes a file when the predicate is provably unsatisfiable for the whole
    partition; anything uncertain - an unknown transform, a null partition value, a
    predicate on a column that is not partitioned, values that do not compare - keeps the
    file. Over-keeping costs I/O; over-pruning would lose rows.
    """
    from .filters import FilterOp

    if not data_files or not expressions or not spec_fields:
        return data_files
    by_column: Dict[str, List[Tuple[PartitionField, str]]] = {}
    for pf, source_type in spec_fields:
        by_column.setdefault(column_of.get(int(pf.source_id), ""), []).append((pf, source_type))

    kept = []
    for data_file in data_files:
        values = data_file.partition_values or {}
        if not values or _may_match_partition(values, expressions, by_column, FilterOp):
            kept.append(data_file)
    return kept


def _may_match_partition(
    values: Dict[str, Any],
    expressions: List[Any],
    by_column: Dict[str, List[Tuple[PartitionField, str]]],
    FilterOp: Any,
) -> bool:
    for expr in expressions:
        for pf, source_type in by_column.get(expr.column, []):
            value = values.get(pf.name)
            if value is None:
                continue                      # a null partition tells us nothing
            try:
                fn = transform_function(pf.transform, source_type)
                value_type = result_type(pf.transform, source_type)
            except UnsupportedTransform:
                continue
            if not _partition_can_satisfy(value, expr, fn, pf.transform, FilterOp, value_type):
                return False
    return True


def _partition_can_satisfy(
    value: Any, expr: Any, fn: Callable[[Any], Any], transform: str, FilterOp: Any, value_type: str
) -> bool:
    """False only when NO row with this partition value can satisfy `expr`.

    Both sides go through `canonical` first: the stored value arrives as Avro decoded it
    (a date object, an aware datetime) while the transform computes ints, and comparing
    those raises rather than pruning.
    """
    try:
        ours = canonical(value, value_type)

        def theirs(v: Any) -> Any:
            return canonical(fn(v), value_type)

        if expr.op == FilterOp.EQ:
            return bool(ours == theirs(expr.value))
        if expr.op == FilterOp.IN:
            candidates = [v for v in (expr.value or []) if v is not None]
            return not candidates or any(ours == theirs(v) for v in candidates)
        if not _monotonic(transform):
            return True                       # bucket[] says nothing about ordering
        if expr.op in (FilterOp.LT, FilterOp.LE):
            return bool(ours <= theirs(expr.value))
        if expr.op in (FilterOp.GT, FilterOp.GE):
            return bool(ours >= theirs(expr.value))
    except (TypeError, ValueError, UnsupportedTransform):
        return True                           # cannot decide -> keep the file
    return True
