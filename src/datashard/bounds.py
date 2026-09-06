"""
Type-faithful encoding of column bounds through Avro strings (split out of
file_manager.py, #71).
"""

import json
from datetime import date, datetime, time as dt_time
from decimal import Decimal, InvalidOperation
from typing import Any, Dict

# ------------------------------------------------------------------
# Bound value encoding: type-faithful round-trip through Avro strings
# ------------------------------------------------------------------

def encode_bound(value: Any) -> str:
    """Encode a bound value with an explicit type tag.

    Bounds travel through an Avro map<string>; encoding the type prevents
    the lossy stringify-then-guess round trip that could invert min/max or
    change comparison semantics (and thus wrongly prune files).
    """
    # NOTE: bool before int (bool subclasses int); datetime before date.
    if isinstance(value, bool):
        payload: Dict[str, Any] = {"t": "bool", "v": value}
    elif isinstance(value, int):
        payload = {"t": "int", "v": value}
    elif isinstance(value, float):
        payload = {"t": "float", "v": value}
    elif isinstance(value, Decimal):
        payload = {"t": "dec", "v": str(value)}  # exact; never via float
    elif isinstance(value, datetime):
        payload = {"t": "ts", "v": value.isoformat()}
    elif isinstance(value, date):
        payload = {"t": "date", "v": value.isoformat()}
    elif isinstance(value, dt_time):
        payload = {"t": "time", "v": value.isoformat()}
    elif isinstance(value, str):
        payload = {"t": "str", "v": value}
    else:
        payload = {"t": "str", "v": str(value)}
    return json.dumps(payload)

def decode_bound(raw: Any) -> Any:
    """Decode a bound value, supporting both tagged and legacy formats."""
    if not isinstance(raw, str):
        return raw
    try:
        payload = json.loads(raw)
    except (ValueError, TypeError):
        return infer_value_legacy(raw)
    if not isinstance(payload, dict) or "t" not in payload or "v" not in payload:
        return infer_value_legacy(raw)

    tag, v = payload["t"], payload["v"]
    try:
        if tag == "bool":
            return bool(v)
        if tag == "int":
            return int(v)
        if tag == "float":
            return float(v)
        if tag == "dec":
            return Decimal(v)
        if tag == "ts":
            return datetime.fromisoformat(v)
        if tag == "date":
            return date.fromisoformat(v)
        if tag == "time":
            return dt_time.fromisoformat(v)
        if tag == "str":
            return str(v)
    except (ValueError, TypeError, InvalidOperation):
        return v
    return v

def infer_value_legacy(value: str) -> Any:
    """Best-effort type inference for bounds written by older versions."""
    if value.isdigit():
        return int(value)
    try:
        return float(value)
    except ValueError:
        pass
    if value.lower() == 'true':
        return True
    if value.lower() == 'false':
        return False
    return value

