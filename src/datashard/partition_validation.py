"""
Checking a partition spec before a table exists (split out of partitioning.py for the
500-line file cap, #92).

A spec is validated at CREATE, not at first append: a table whose spec cannot be written
correctly should never exist, and a message at create names the alternative while the
caller can still act on it.
"""

from typing import Any, List, Optional, Tuple

from .data_structures import PartitionField, PartitionSpec, parse_decimal_type
from .partitioning import UnsupportedTransform, result_type, transform_function


def validate_spec(spec: Optional[PartitionSpec], schema: Any) -> List[Tuple[PartitionField, str]]:
    """[(field, source iceberg type)] for a spec, raising on anything unsupported.

    Called at create time so a bad spec fails before a table exists, not on first append.
    """
    if spec is None or not spec.fields:
        return []
    if schema is None or not schema.fields:
        # A table created without a schema adopts one at its first append; the spec is
        # checked against THAT schema then, with the same message.
        return []
    by_id = {int(f["id"]): f for f in schema.fields}
    by_name = {str(f["name"]): f for f in schema.fields} if schema is not None else {}
    out = []
    seen_names, seen_ids = set(), set()
    for pf in spec.fields:
        source = by_id.get(int(pf.source_id))
        if source is None:
            raise UnsupportedTransform(
                f"partition field {pf.name!r} has source-id {pf.source_id}, which is not a column "
                f"of this schema (columns: {sorted(by_name)})"
            )
        if pf.name in seen_names:
            raise UnsupportedTransform(f"duplicate partition field name {pf.name!r}")
        if int(pf.field_id) in seen_ids:
            raise UnsupportedTransform(f"duplicate partition field id {pf.field_id}")
        if int(pf.field_id) < 1000:
            raise UnsupportedTransform(
                f"partition field {pf.name!r} has field-id {pf.field_id}; Iceberg assigns "
                f"partition field ids from 1000"
            )
        seen_names.add(pf.name)
        seen_ids.add(int(pf.field_id))
        source_type = source["type"]
        transform_function(pf.transform, source_type)  # raises if unsupported for this type
        _check_readable_partition_value(pf, source_type)
        out.append((pf, source_type))
    return out


def _check_readable_partition_value(pf: PartitionField, source_type: str) -> None:
    """Refuse a partition whose VALUE type a major Iceberg reader cannot read.

    A partition value goes into the manifest's Avro partition struct, and DuckDB's Avro
    reader cannot decode an Avro `decimal` there: it does not raise, it ABORTS THE
    PROCESS (SIGABRT, "exception_type":"Conversion"). Measured against DuckDB 1.5.5 with
    identity and truncate on a decimal column; bucket[N] on the same column is fine
    because its value is an int.

    Writing a table that crashes a reader is worse than refusing to create it, so this
    fails at create with the alternative spelled out.
    """
    value_type = result_type(pf.transform, source_type)
    if value_type == "uuid" or value_type.startswith("fixed"):
        raise UnsupportedTransform(
            f"partition field {pf.name!r} would carry a {value_type} value, which datashard "
            f"does not write: Iceberg readers disagree on how a uuid or fixed partition value "
            f"is spelled in the manifest's Avro struct and in the path, and a partition value a "
            f"reader misreads is worse than no partitioning. Use bucket[N] on that column - its "
            f"value is an int, and Iceberg's bucket of a uuid is the hash of exactly the 16 bytes "
            f"datashard stores."
        )
    if parse_decimal_type(value_type) is not None:
        raise UnsupportedTransform(
            f"partition field {pf.name!r} would carry a {value_type} value, and DuckDB's Avro "
            f"reader crashes on a decimal partition value (verified against DuckDB 1.5.5) - "
            f"datashard will not write a table that crashes a reader. Partition that column with "
            f"bucket[N] instead, whose value is an int, or partition on a different column; the "
            f"decimal stays queryable either way through its column statistics."
        )
