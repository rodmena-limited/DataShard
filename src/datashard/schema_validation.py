"""
Schema-conformance checks applied before a data file is written (split out of
data_operations.py for the 500-line file cap).

These are the guards that stop a silent loss or a silent coercion: an unknown
column dropped by a projection, a null in a required column, or a value quietly
cast across type families (the string "12" becoming the integer 12).
"""

from typing import Any, Dict, List

import pyarrow as pa

from .data_structures import Schema


def validate_records_strict(records: List[Dict[str, Any]], iceberg_schema: Schema) -> None:
    """Validate records against the schema, raising on silent-loss hazards.

    - Unknown/misnamed fields raise instead of being silently dropped by
      pyarrow's schema projection.
    - Required (non-nullable) fields must be present and non-None; pyarrow's
      from_pylist does not enforce nullability, so we must.
    Type mismatches are left to pyarrow, which raises on incompatible values.
    """
    # Schema.__post_init__ guarantees every field has a "name".
    allowed = {str(f["name"]) for f in iceberg_schema.fields}
    required = {str(f["name"]) for f in iceberg_schema.fields if f.get("required", False)}

    for i, record in enumerate(records):
        unknown = {str(k) for k in record.keys()} - allowed
        if unknown:
            raise ValueError(
                f"Record {i} has fields not in the table schema: {sorted(unknown)}. "
                f"Schema fields: {sorted(allowed)}. Refusing to silently drop data."
            )
        for name in required:
            if record.get(name) is None:
                raise ValueError(f"Record {i} is missing required field '{name}' (or it is None)")


def validate_arrow_table_strict(table: pa.Table, iceberg_schema: Schema) -> None:
    """Arrow-level twin of validate_records_strict: required columns present and free of
    nulls. (Unknown columns cannot survive from_pandas/from_pylist with an explicit
    schema, but callers check the DataFrame's columns before converting.)"""
    required = [str(f["name"]) for f in iceberg_schema.fields if f.get("required", False)]
    for name in required:
        if name not in table.column_names:
            raise ValueError(f"Required field '{name}' is missing")
        nulls = table.column(name).null_count
        if nulls:
            raise ValueError(f"Required field '{name}' has {nulls} null value(s)")


def type_family(t: pa.DataType) -> str:
    """The family a pyarrow type belongs to, for cross-family cast refusal."""
    pt = pa.types
    if pt.is_boolean(t):
        return "bool"
    if pt.is_integer(t) or pt.is_floating(t) or pt.is_decimal(t):
        return "number"
    if pt.is_timestamp(t):
        return "timestamp"
    if pt.is_date(t):
        return "date"
    if pt.is_time(t):
        return "time"
    if pt.is_string(t) or pt.is_large_string(t):
        return "string"
    if pt.is_binary(t) or pt.is_large_binary(t) or pt.is_fixed_size_binary(t):
        return "binary"
    if pt.is_null(t):
        return "null"
    return str(t)


def check_cast_family(name: str, source: pa.DataType, target: pa.DataType) -> None:
    """Refuse cross-family coercions pyarrow would perform silently: the string "12"
    must not become the integer 12, an int must not become text. Widening inside a
    family (int32 -> long, float -> double, decimal precision) and null columns are fine.
    """
    src, dst = type_family(source), type_family(target)
    if src != "null" and src != dst:
        raise ValueError(
            f"Column '{name}' is {source} but the table field is {target}; refusing to coerce "
            f"across type families - convert it explicitly before appending"
        )
