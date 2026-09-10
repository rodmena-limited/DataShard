"""
Iceberg -> PyArrow type mapping (split out of data_operations.py, #71).
"""

from typing import Any, Dict, Union

import pyarrow as pa

from .data_structures import UUID_BYTES, parse_decimal_type, parse_fixed_type


def iceberg_type_to_arrow(iceberg_type: Union[str, Dict[str, Any]]) -> pa.DataType:
    """Convert Iceberg type string to PyArrow type"""
    import pyarrow as pa

    if isinstance(iceberg_type, dict):
        iceberg_type = iceberg_type.get("type", "string")

    type_mapping = {
        "boolean": pa.bool_(),
        "int": pa.int32(),
        "long": pa.int64(),
        "float": pa.float32(),
        "double": pa.float64(),
        "date": pa.date32(),
        "time": pa.time64("us"),
        "timestamp": pa.timestamp("us"),
        "timestamptz": pa.timestamp("us", tz="UTC"),
        "string": pa.string(),
        # Iceberg's uuid is 16 raw bytes. datashard wrote a parquet STRING here until
        # 0.11.2, which pyiceberg refused to read as a uuid ("Cannot promote an string to
        # uuid"); scans convert it back to a str so callers see what they wrote (#92).
        "uuid": pa.binary(UUID_BYTES),
        "binary": pa.binary(),
        # A bare "fixed" has no width and is not a valid Iceberg type; it stays mapped
        # for tables written before 0.10, and is refused for new schemas.
        "fixed": pa.binary(),
    }

    # Handle complex types
    if isinstance(iceberg_type, str):
        decimal_spec = parse_decimal_type(iceberg_type)
        if decimal_spec is not None:
            return pa.decimal128(*decimal_spec)
        fixed_width = parse_fixed_type(iceberg_type)
        if fixed_width is not None:
            return pa.binary(fixed_width)
        if iceberg_type.startswith("list<"):
            # Extract element type and map it
            element_type = iceberg_type[5:-1]  # Remove 'list<>' wrapper
            return pa.list_(iceberg_type_to_arrow(element_type))
        elif iceberg_type.startswith("map<"):
            # For now, treat as string - in real implementation would need key/value types
            return pa.string()
        elif iceberg_type.startswith("struct<"):
            return pa.string()  # For now, treat as string

    return type_mapping.get(str(iceberg_type), pa.string())  # Default to string

