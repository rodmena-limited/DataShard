"""
Iceberg partition transforms (#98).

A partition value datashard writes is pruned on by DuckDB, pyiceberg, Spark and Trino,
so a transform that disagrees with Iceberg's by one bucket makes those engines skip rows
that match. Every function here is checked against pyiceberg's own implementation in
tests/test_partitioning.py rather than against this module's reading of the spec.

Supported: identity, year, month, day, hour, bucket[N], truncate[W]. Anything else is
refused at create time - an unsupported transform accepted and ignored would silently
write a table whose layout contradicts its metadata.
"""

import re
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple

from .data_structures import PartitionField, PartitionSpec, parse_decimal_type

_EPOCH_DATE = date(1970, 1, 1)
_EPOCH_NAIVE = datetime(1970, 1, 1)
_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)

BUCKET_RE = re.compile(r"^bucket\[(\d+)\]$")
TRUNCATE_RE = re.compile(r"^truncate\[(\d+)\]$")
TEMPORAL = ("year", "month", "day", "hour")
SUPPORTED = ("identity", *TEMPORAL, "bucket[N]", "truncate[W]")


class UnsupportedTransform(ValueError):
    """Raised for a transform datashard cannot compute exactly as Iceberg defines it."""


# ---------------------------------------------------------------- murmur3

def murmur3_32(data: bytes) -> int:
    """Murmur3 x86 32-bit, the hash Iceberg's bucket transform is defined on.

    Implemented here rather than pulled in as a dependency: it runs once per distinct
    partition value, and datashard's dependency list is deliberately three packages long.
    """
    c1, c2 = 0xCC9E2D51, 0x1B873593
    h = 0
    rounded = (len(data) // 4) * 4
    for i in range(0, rounded, 4):
        k = int.from_bytes(data[i:i + 4], "little")
        k = (k * c1) & 0xFFFFFFFF
        k = ((k << 15) | (k >> 17)) & 0xFFFFFFFF
        k = (k * c2) & 0xFFFFFFFF
        h ^= k
        h = ((h << 13) | (h >> 19)) & 0xFFFFFFFF
        h = (h * 5 + 0xE6546B64) & 0xFFFFFFFF
    tail = data[rounded:]
    if tail:
        k = int.from_bytes(tail + b"\x00" * (4 - len(tail)), "little")
        k = (k * c1) & 0xFFFFFFFF
        k = ((k << 15) | (k >> 17)) & 0xFFFFFFFF
        k = (k * c2) & 0xFFFFFFFF
        h ^= k
    h ^= len(data)
    h ^= h >> 16
    h = (h * 0x85EBCA6B) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * 0xC2B2AE35) & 0xFFFFFFFF
    h ^= h >> 16
    return h


def bucket_bytes(value: Any, iceberg_type: str) -> bytes:
    """The bytes Iceberg hashes for `value`. NOT the same as the bounds encoding:
    ints and dates are promoted to 8-byte little-endian longs before hashing."""
    import struct

    if iceberg_type in ("int", "long"):
        return struct.pack("<q", int(value))
    if iceberg_type == "date":
        return struct.pack("<q", (value - _EPOCH_DATE).days)
    if iceberg_type == "time":
        micros = ((value.hour * 60 + value.minute) * 60 + value.second) * 1_000_000 + value.microsecond
        return struct.pack("<q", micros)
    if iceberg_type in ("timestamp", "timestamptz"):
        return struct.pack("<q", _micros(value))
    if iceberg_type == "string":
        return str(value).encode("utf-8")
    if iceberg_type in ("binary", "fixed") or iceberg_type.startswith("fixed"):
        return bytes(value)
    if iceberg_type == "uuid":
        from .uuid_columns import to_bytes as _uuid_bytes

        return _uuid_bytes(value) or b""      # the column holds 16 bytes; a caller a string
    if parse_decimal_type(iceberg_type) is not None:
        dec = value if isinstance(value, Decimal) else Decimal(str(value))
        sign, digits, exponent = dec.as_tuple()
        unscaled = int("".join(map(str, digits))) * (10 ** max(0, int(exponent)))
        if int(exponent) < 0:
            unscaled = int("".join(map(str, digits)))
        unscaled = -unscaled if sign else unscaled
        n = max(1, (unscaled.bit_length() + 8) // 8)
        return unscaled.to_bytes(n, "big", signed=True)
    raise UnsupportedTransform(f"bucket[] is not defined for type {iceberg_type!r}")


def _micros(value: datetime) -> int:
    base = _EPOCH_UTC if value.tzinfo is not None else _EPOCH_NAIVE
    delta = value - base
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


# ---------------------------------------------------------------- transforms

def _years(value: Any) -> int:
    return (value.year - 1970) if isinstance(value, (date, datetime)) else _unsupported(value, "year")


def _months(value: Any) -> int:
    if not isinstance(value, (date, datetime)):
        _unsupported(value, "month")
    return (value.year - 1970) * 12 + (value.month - 1)


def _days(value: Any) -> int:
    if isinstance(value, datetime):
        return (value.date() - _EPOCH_DATE).days
    if isinstance(value, date):
        return (value - _EPOCH_DATE).days
    return _unsupported(value, "day")


def _hours(value: Any) -> int:
    if not isinstance(value, datetime):
        _unsupported(value, "hour")
    return _micros(value) // 3_600_000_000


def _unsupported(value: Any, name: str) -> int:
    raise UnsupportedTransform(f"the {name} transform is not defined for {type(value).__name__}")


def truncate_value(value: Any, width: int, iceberg_type: str) -> Any:
    if value is None:
        return None
    if iceberg_type in ("int", "long"):
        return value - (value % width)          # Python's % is already floorMod
    if iceberg_type == "string":
        return str(value)[:width]
    if iceberg_type in ("binary", "fixed") or iceberg_type.startswith("fixed"):
        return bytes(value)[:width]
    if parse_decimal_type(iceberg_type) is not None:
        # Iceberg truncates the unscaled value at the VALUE's own scale, not the column
        # type's. Using the type's scale silently returned the value unchanged whenever
        # the extra zeros made it divisible by W - checked against pyiceberg, which is
        # the implementation every foreign reader agrees with.
        dec = value if isinstance(value, Decimal) else Decimal(str(value))
        exponent = dec.as_tuple().exponent
        if not isinstance(exponent, int):
            raise UnsupportedTransform("truncate[] is not defined for NaN or Infinity")
        scale = -exponent
        sign, digits, _ = dec.as_tuple()
        unscaled = int("".join(map(str, digits)) or "0")
        if sign:
            unscaled = -unscaled
        truncated = unscaled - (unscaled % width)
        return Decimal(f"{truncated}e-{scale}") if scale else Decimal(truncated)
    raise UnsupportedTransform(f"truncate[] is not defined for type {iceberg_type!r}")


def _reject_float_identity(transform: str, iceberg_type: str) -> None:
    """identity on a float or double is refused.

    NaN never equals itself, so every NaN row lands in its own partition - a file per row.
    Iceberg deprecates float and double as identity partition sources for the same reason.
    """
    if transform == "identity" and iceberg_type in ("float", "double"):
        raise UnsupportedTransform(
            f"identity partitioning on a {iceberg_type} column is refused: NaN never equals "
            f"itself, so every NaN row would become its own partition and its own file. Iceberg "
            f"deprecates float and double as identity partition sources, and defines no other "
            f"transform for them either - partition on a different column. The {iceberg_type} "
            f"stays queryable through its column statistics, which is what filters use."
        )


def transform_function(transform: str, iceberg_type: str) -> Callable[[Any], Any]:
    """The callable for `transform` applied to a column of `iceberg_type`.

    Raises UnsupportedTransform for anything datashard cannot compute exactly as Iceberg
    does - refusing beats writing a partition value a foreign reader disagrees with.
    """
    if transform == "identity":
        _reject_float_identity(transform, iceberg_type)
        return lambda v: v
    if transform in TEMPORAL:
        if iceberg_type not in ("date", "timestamp", "timestamptz"):
            raise UnsupportedTransform(
                f"the {transform} transform needs a date or timestamp column, not {iceberg_type!r}"
            )
        if transform == "hour" and iceberg_type == "date":
            raise UnsupportedTransform("the hour transform needs a timestamp column, not a date")
        return {"year": _years, "month": _months, "day": _days, "hour": _hours}[transform]
    m = BUCKET_RE.match(transform)
    if m:
        n = int(m.group(1))
        if n <= 0:
            raise UnsupportedTransform(f"bucket[{n}] must have a positive modulus")

        def bucket(v: Any, _n: int = n, _t: str = iceberg_type) -> Optional[int]:
            if v is None:
                return None
            return (murmur3_32(bucket_bytes(v, _t)) & 0x7FFFFFFF) % _n

        bucket_bytes(_probe_value(iceberg_type), iceberg_type)  # fail now, not at write time
        return bucket
    m = TRUNCATE_RE.match(transform)
    if m:
        w = int(m.group(1))
        if w <= 0:
            raise UnsupportedTransform(f"truncate[{w}] must have a positive width")
        truncate_value(_probe_value(iceberg_type), w, iceberg_type)  # fail now

        def truncate(v: Any, _w: int = w, _t: str = iceberg_type) -> Any:
            return None if v is None else truncate_value(v, _w, _t)

        return truncate
    raise UnsupportedTransform(
        f"unsupported partition transform {transform!r}; datashard supports {list(SUPPORTED)}"
    )


def _probe_value(iceberg_type: str) -> Any:
    """A representative value, used to reject an impossible transform/type pair at create."""
    if iceberg_type in ("int", "long"):
        return 1
    if iceberg_type == "string":
        return "x"
    if iceberg_type == "date":
        return _EPOCH_DATE
    if iceberg_type == "time":
        return dt_time(0, 0)
    if iceberg_type in ("timestamp", "timestamptz"):
        return _EPOCH_UTC if iceberg_type == "timestamptz" else _EPOCH_NAIVE
    if iceberg_type in ("binary", "fixed") or iceberg_type.startswith("fixed"):
        return b"x"
    if iceberg_type == "uuid":
        return "00000000-0000-0000-0000-000000000000"
    if parse_decimal_type(iceberg_type) is not None:
        return Decimal("1")
    return "x"


def result_type(transform: str, source_type: str) -> str:
    """The Iceberg type of the partition VALUE the transform produces."""
    if transform == "identity":
        return source_type
    if transform in ("year", "month", "hour"):
        return "int"
    if transform == "day":
        return "date"
    if BUCKET_RE.match(transform):
        return "int"
    if TRUNCATE_RE.match(transform):
        return source_type
    raise UnsupportedTransform(f"unsupported partition transform {transform!r}")


# ---------------------------------------------------------------- spec plumbing

def partition_key(
    row: Dict[str, Any], fields: List[Tuple[PartitionField, str]], column_of: Dict[int, str]
) -> Tuple[Any, ...]:
    """The partition tuple for one record."""
    return tuple(
        transform_function(pf.transform, src)(row.get(column_of[int(pf.source_id)]))
        for pf, src in fields
    )


def path_segment(name: str, value: Any) -> str:
    """`name=value` for the data-file path. Cosmetic - readers use the manifests - but it
    is what makes a partitioned lake browsable, so it follows Hive/Iceberg convention."""
    from urllib.parse import quote

    if value is None:
        return f"{name}=__HIVE_DEFAULT_PARTITION__"
    if isinstance(value, bool):
        text = "true" if value else "false"
    elif isinstance(value, (bytes, bytearray)):
        text = bytes(value).hex()
    else:
        text = str(value)
    return f"{name}={quote(text, safe='')}"


# ---------------------------------------------------------------- Avro plumbing

def avro_type(iceberg_type: str) -> Any:
    """The Avro type for a partition value of `iceberg_type`.

    Logical types are used rather than hand-encoded ints and bytes: fastavro then
    converts date / timestamp / decimal values itself, and every Iceberg reader
    understands the same logical types.
    """
    simple = {
        "boolean": "boolean", "int": "int", "long": "long", "float": "float",
        "double": "double", "string": "string", "uuid": "string",
        "binary": "bytes",
    }
    if iceberg_type in simple:
        return simple[iceberg_type]
    if iceberg_type == "date":
        return {"type": "int", "logicalType": "date"}
    if iceberg_type == "time":
        return {"type": "long", "logicalType": "time-micros"}
    if iceberg_type in ("timestamp", "timestamptz"):
        # BOTH use timestamp-micros, including the zoneless one. Avro's
        # local-timestamp-micros is the technically correct encoding for a timestamp
        # WITHOUT zone, and DuckDB's Avro reader does not implement it - it does not
        # raise, it ABORTS THE PROCESS ("Unknown Avro logical type"). Reading beats
        # pedantry: a zoneless partition value comes back UTC-annotated, which pruning
        # canonicalises away and which no reader misinterprets, because the underlying
        # micros are identical.
        return {"type": "long", "logicalType": "timestamp-micros"}
    if iceberg_type.startswith("fixed"):
        return "bytes"
    spec = parse_decimal_type(iceberg_type)
    if spec is not None:
        return {"type": "bytes", "logicalType": "decimal", "precision": spec[0], "scale": spec[1]}
    raise UnsupportedTransform(f"no Avro mapping for partition type {iceberg_type!r}")


def partition_avro_fields(spec_fields: List[Tuple[PartitionField, str]]) -> List[Dict[str, Any]]:
    """The Avro fields of the manifest entry's `partition` struct, with Iceberg field-ids.

    Optional (union with null) as Iceberg requires: a partition value can be null.
    """
    return [
        {
            "name": pf.name,
            "type": ["null", avro_type(result_type(pf.transform, source_type))],
            "field-id": int(pf.field_id),
            "default": None,
        }
        for pf, source_type in spec_fields
    ]


def spec_field_types(spec: PartitionSpec, schema: Any) -> List[Tuple[PartitionField, str]]:
    """[(field, SOURCE type)] for a persisted spec, without re-validating the schema."""
    by_id = {int(f["id"]): str(f["type"]) for f in schema.fields} if schema is not None else {}
    return [(pf, by_id.get(int(pf.source_id), "string")) for pf in spec.fields]


def partition_groups(
    table: Any,
    spec_fields: List[Tuple[PartitionField, str]],
    column_of: Dict[int, str],
) -> List[Tuple[Dict[str, Any], Any]]:
    """Split an Arrow table into [(partition values, rows)] - one entry per partition.

    Transforms are memoised per distinct source value, so a low-cardinality partition
    (the usual case: an hour, a day, a symbol) costs one transform call per value rather
    than one per row. Order within a partition is preserved, and the union of the groups
    is the input, so nothing is dropped or duplicated.
    """
    if not spec_fields:
        return [({}, table)]
    columns = {}
    for pf, source_type in spec_fields:
        name = column_of[int(pf.source_id)]
        fn = transform_function(pf.transform, source_type)
        memo: Dict[Any, Any] = {}

        def apply(v: Any, _fn: Callable[[Any], Any] = fn, _memo: Dict[Any, Any] = memo) -> Any:
            try:
                if v not in _memo:
                    _memo[v] = _fn(v)
                return _memo[v]
            except TypeError:      # unhashable source value: transform it directly
                return _fn(v)

        columns[pf.name] = [apply(v) for v in table.column(name).to_pylist()]

    order: List[Tuple[Any, ...]] = []
    rows_by_key: Dict[Tuple[Any, ...], List[int]] = {}
    names = [pf.name for pf, _ in spec_fields]
    for i in range(table.num_rows):
        key = tuple(_hashable(columns[n][i]) for n in names)
        if key not in rows_by_key:
            rows_by_key[key] = []
            order.append(key)
        rows_by_key[key].append(i)

    groups = []
    for key in order:
        indices = rows_by_key[key]
        values = {n: columns[n][indices[0]] for n in names}
        groups.append((values, table.take(indices)))
    return groups


def _hashable(value: Any) -> Any:
    return value.hex() if isinstance(value, (bytes, bytearray)) else value


def partition_path(base: str, values: Dict[str, Any]) -> str:
    """`data/<name>=<value>/.../` for a partitioned data file."""
    if not values:
        return base
    return "/".join([base] + [path_segment(n, v) for n, v in values.items()])


def canonical(value: Any, value_type: str) -> Any:
    """A comparable form of a partition value, independent of how Avro decoded it.

    The value in a manifest comes back through Avro's logical types - an Iceberg `date`
    as a `datetime.date`, a timestamp as a datetime - while a transform computes ints.
    Comparing the two forms raises TypeError, which the pruner treats as "cannot decide"
    and keeps the file, so pruning silently stopped working for `day` and timestamp
    partitions. Both sides go through here first.
    """
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, datetime):
        return _micros(value)
    if isinstance(value, date):
        return (value - _EPOCH_DATE).days
    if isinstance(value, dt_time):
        return ((value.hour * 60 + value.minute) * 60 + value.second) * 1_000_000 + value.microsecond
    if value_type == "date" and isinstance(value, int):
        return value
    return value
