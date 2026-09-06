"""
Iceberg single-value binary serialisation for column bounds (#85).

Foreign readers PRUNE files on these bytes (spike #83: a wrong bound makes DuckDB
and pyiceberg return wrong rows), so a value is encoded only when the encoding
for its type is exact; anything else yields None and the column simply has no
bound (no pruning, correct results).
"""

import struct
import uuid
from datetime import date, datetime, time as dt_time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from .data_structures import parse_decimal_type

_EPOCH_DATE = date(1970, 1, 1)
_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)
_EPOCH_NAIVE = datetime(1970, 1, 1)
_MICRO = timedelta(microseconds=1)


def _micros_since_epoch(value: datetime) -> int:
    if value.tzinfo is None:
        return (value - _EPOCH_NAIVE) // _MICRO
    return (value - _EPOCH_UTC) // _MICRO


def _exact_int(value: Any) -> Optional[int]:
    """`value` as an int, but ONLY when it already is one.

    A bound is not a cast: coercing 1.5 or "5" into 1 / 5 would claim a file's minimum
    is lower than it is, and a foreign reader pruning on that bound would skip rows
    that match. Anything that is not exactly an integer gets no bound at all.
    """
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _unscaled(dec: Decimal, scale: int) -> Optional[int]:
    """The unscaled integer of `dec` at `scale`, or None if it would not be exact.

    Computed from the digit tuple, never through quantize()/scaleb(): those honour the
    decimal context (28 significant digits by default), which silently dropped every
    bound of a decimal(38, s) column carrying more than 28 digits.
    """
    sign, digits, exponent = dec.as_tuple()
    if not isinstance(exponent, int):
        return None  # NaN / Infinity
    shift = exponent + scale
    if shift < 0:
        return None  # more fractional digits than the type holds: rounding, so no bound
    unscaled = int("".join(map(str, digits))) * (10 ** shift)
    return -unscaled if sign else unscaled


def encode_bound(value: Any, iceberg_type: Any) -> Optional[bytes]:
    """Encode `value` as Iceberg single-value binary for `iceberg_type`, or None.

    Little-endian int/long/float/double; date = int days since 1970-01-01; time,
    timestamp and timestamptz = long microseconds; decimal = minimal big-endian
    two's-complement of the unscaled value; string UTF-8; uuid 16 bytes; boolean
    one byte; binary/fixed as is.

    Returns None - i.e. NO bound, and therefore no pruning - for a complex type, or
    whenever `value` is not already the exact Python type the column holds. DuckDB and
    pyiceberg PRUNE files on these bytes, so a bound that is off by any amount makes
    them return wrong rows; omitting one only makes a scan read more files.
    """
    if value is None or not isinstance(iceberg_type, str):
        return None
    t = iceberg_type
    try:
        if t == "boolean":
            return (b"\x01" if value else b"\x00") if isinstance(value, bool) else None
        if t in ("int", "long"):
            n = _exact_int(value)
            return None if n is None else struct.pack("<i" if t == "int" else "<q", n)
        if t in ("float", "double"):
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                return None
            return struct.pack("<f" if t == "float" else "<d", float(value))
        if t == "date":
            if isinstance(value, datetime) or not isinstance(value, date):
                return None
            return struct.pack("<i", (value - _EPOCH_DATE).days)
        if t == "time":
            if not isinstance(value, dt_time):
                return None
            micros = ((value.hour * 60 + value.minute) * 60 + value.second) * 1_000_000 + value.microsecond
            return struct.pack("<q", micros)
        if t in ("timestamp", "timestamptz"):
            if not isinstance(value, datetime):
                return None
            return struct.pack("<q", _micros_since_epoch(value))
        if t == "string":
            return value.encode("utf-8") if isinstance(value, str) else None
        if t == "uuid":
            return uuid.UUID(str(value)).bytes if isinstance(value, (str, uuid.UUID)) else None
        if t == "binary" or t.startswith("fixed"):
            return bytes(value) if isinstance(value, (bytes, bytearray, memoryview)) else None
        spec = parse_decimal_type(t)
        if spec is not None:
            if not isinstance(value, Decimal):
                return None  # a float or a string is not an exact decimal
            unscaled = _unscaled(value, spec[1])
            if unscaled is None:
                return None
            n = (unscaled.bit_length() + 8) // 8  # +1 sign bit, rounded up to whole bytes
            return unscaled.to_bytes(max(n, 1), "big", signed=True)
    except (ValueError, TypeError, OverflowError, struct.error, InvalidOperation):
        return None
    return None


def decode_bound(raw: Any, iceberg_type: Any) -> Any:
    """Inverse of encode_bound. Tolerates int->long and float->double promotion by
    decoding on the byte length. Returns None when the bytes cannot be decoded."""
    if raw is None or not isinstance(iceberg_type, str):
        return None
    data = bytes(raw)
    t = iceberg_type
    try:
        if t == "boolean":
            return data[0] != 0
        if t in ("int", "long"):
            if len(data) == 4:
                return struct.unpack("<i", data)[0]
            if len(data) == 8:
                return struct.unpack("<q", data)[0]
            return None
        if t in ("float", "double"):
            if len(data) == 4:
                return struct.unpack("<f", data)[0]
            if len(data) == 8:
                return struct.unpack("<d", data)[0]
            return None
        if t == "date":
            return _EPOCH_DATE + timedelta(days=struct.unpack("<i", data)[0])
        if t == "time":
            micros = struct.unpack("<q", data)[0]
            return (_EPOCH_NAIVE + timedelta(microseconds=micros)).time()
        if t == "timestamp":
            return _EPOCH_NAIVE + timedelta(microseconds=struct.unpack("<q", data)[0])
        if t == "timestamptz":
            return _EPOCH_UTC + timedelta(microseconds=struct.unpack("<q", data)[0])
        if t == "string":
            return data.decode("utf-8")
        if t == "uuid":
            return str(uuid.UUID(bytes=data))
        if t == "binary" or t.startswith("fixed"):
            return data
        spec = parse_decimal_type(t)
        if spec is not None:
            unscaled = int.from_bytes(data, "big", signed=True)
            # From a string, not scaleb(): scaleb rounds to the decimal context's
            # 28 significant digits, which corrupts decimal(38, s) bounds.
            return Decimal(f"{unscaled}e-{spec[1]}")
    except (ValueError, TypeError, struct.error, IndexError, UnicodeDecodeError):
        return None
    return None
