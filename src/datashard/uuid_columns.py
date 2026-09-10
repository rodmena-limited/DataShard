"""
uuid columns: 16 raw bytes on disk, strings in Python (#92).

Iceberg's `uuid` is a 16-byte fixed value. datashard wrote a parquet STRING for it until
0.11.2, which pyiceberg refuses to read ("Cannot promote an string to uuid"), so the type
was simply refused at create. It is now written as Iceberg specifies.

Callers still see strings: a value written as `"6ba7b810-9dad-11d1-80b4-00c04fd430c8"`
reads back as that same string, which keeps the API unchanged, keeps results JSON
serialisable, and keeps a table written before 0.11.2 - whose uuid column really is a
parquet string - readable in the same scan as one written after.
"""

import uuid as _uuid
from typing import Any, Dict, List, Optional

import pyarrow as pa

from .data_structures import UUID_BYTES, Schema


def uuid_field_names(schema: Optional[Schema]) -> List[str]:
    """The names of the schema's uuid columns (usually none, so callers can skip fast)."""
    if schema is None:
        return []
    return [str(f["name"]) for f in schema.fields if f.get("type") == "uuid"]


def to_bytes(value: Any) -> Optional[bytes]:
    """A uuid value as its 16 bytes, from a string, a UUID, or the bytes themselves."""
    if value is None:
        return None
    if isinstance(value, _uuid.UUID):
        return value.bytes
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        if len(raw) != UUID_BYTES:
            raise ValueError(f"a uuid value must be {UUID_BYTES} bytes, got {len(raw)}")
        return raw
    return _uuid.UUID(str(value)).bytes


def to_text(value: Any) -> Optional[str]:
    """The canonical string form of a uuid value."""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray, memoryview)):
        return str(_uuid.UUID(bytes=bytes(value)))
    return str(value)


def normalise_records(records: List[Dict[str, Any]], schema: Optional[Schema]) -> List[Dict[str, Any]]:
    """Records with every uuid field turned into its 16 bytes, ready for Arrow.

    Returns the input unchanged when the schema has no uuid column, so the common case
    copies nothing.
    """
    names = uuid_field_names(schema)
    if not names:
        return records
    out = []
    for i, record in enumerate(records):
        converted = dict(record)
        for name in names:
            if name in converted:
                try:
                    converted[name] = to_bytes(converted[name])
                except (ValueError, AttributeError, TypeError) as e:
                    raise ValueError(f"Record {i} field '{name}' is not a uuid: {e}") from e
        out.append(converted)
    return out


def encode_columns(table: pa.Table, schema: Optional[Schema]) -> pa.Table:
    """A table whose uuid columns hold 16-byte values, converting from strings if needed.

    Used on the WRITE path, so a caller may hand over either form (a DuckDB result is a
    string column; a pyarrow round trip is already binary).
    """
    for name in uuid_field_names(schema):
        if name not in table.column_names:
            continue
        column = table.column(name)
        if pa.types.is_fixed_size_binary(column.type):
            continue
        values = [to_bytes(v) for v in column.to_pylist()]
        table = table.set_column(
            table.column_names.index(name), name, pa.array(values, pa.binary(UUID_BYTES))
        )
    return table


def decode_columns(table: pa.Table, schema: Optional[Schema]) -> pa.Table:
    """A table whose uuid columns hold strings, whatever they hold on disk.

    Applied per FILE on the read path, before files are concatenated: a table written
    before 0.11.2 stores strings and one written after stores binary, and both must land
    in the same scan.
    """
    for name in uuid_field_names(schema):
        if name not in table.column_names:
            continue
        column = table.column(name)
        if not pa.types.is_fixed_size_binary(column.type) and not pa.types.is_binary(column.type):
            continue
        values = [to_text(v) for v in column.to_pylist()]
        table = table.set_column(table.column_names.index(name), name, pa.array(values, pa.string()))
    return table


def _canonical_value(value: Any) -> Any:
    """The canonical text of a uuid filter value, whatever form the caller used.

    A value that is not a uuid is returned untouched: a nonsense predicate then matches
    nothing, which is what a filter is for, instead of raising in the middle of a scan.
    """
    try:
        return to_text(to_bytes(value))
    except (ValueError, AttributeError, TypeError):
        return value


def split_expressions(expressions: List[Any], schema: Optional[Schema]) -> Any:
    """Split filters into (pushdown, after_decode).

    A predicate on a uuid column cannot be pushed into the parquet reader: what is stored
    is 16 bytes, what the caller filtered on is a string, and one table can hold both
    encodings at once. Those predicates are applied after `decode_columns`, where every
    file looks the same; everything else is pushed down as before.

    The uuid values are canonicalised on the way through, so a string, a `uuid.UUID` and
    the raw bytes all select the same rows, and so bounds pruning compares like with like.
    A table written before 0.11.2 that stored a NON-canonical string (upper case, braces)
    is the one case this does not match - its bytes never went through a uuid at all.
    """
    from .filters import FilterExpression, FilterOp

    names = set(uuid_field_names(schema))
    if not names or not expressions:
        return list(expressions), []
    pushdown, after_decode = [], []
    for expr in expressions:
        if expr.column not in names:
            pushdown.append(expr)
        elif expr.op in (FilterOp.IS_NULL, FilterOp.IS_NOT_NULL):
            after_decode.append(expr)
        elif expr.op in (FilterOp.IN, FilterOp.NOT_IN):
            values = [_canonical_value(v) for v in (expr.value or [])]
            after_decode.append(FilterExpression(expr.column, expr.op, values))
        else:
            after_decode.append(FilterExpression(expr.column, expr.op, _canonical_value(expr.value)))
    return pushdown, after_decode


def normalise_dataframe(df: Any, schema: Optional[Schema]) -> Any:
    """A DataFrame whose uuid columns hold 16 bytes, ready for Arrow.

    The pandas path builds an Arrow table against the table's schema, where a uuid column
    is fixed-width binary; a column of strings has to be converted first or pyarrow raises
    about a length-36 value. Returns the input untouched when there is nothing to convert.
    """
    names = [n for n in uuid_field_names(schema) if n in getattr(df, "columns", [])]
    if not names:
        return df
    df = df.copy()
    for name in names:
        try:
            df[name] = df[name].map(to_bytes)
        except (ValueError, AttributeError, TypeError) as e:
            raise ValueError(f"Column '{name}' holds a value that is not a uuid: {e}") from e
    return df
