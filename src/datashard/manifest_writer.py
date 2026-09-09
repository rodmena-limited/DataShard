"""
Writing Iceberg v2 manifests and manifest lists (#85). Split out of file_manager.py.

Records go out with absolute URIs (#87), binary bounds (iceberg_bounds) and the two
datashard_sha256 extras. The Avro header carries the table schema, so a manifest is
self-describing for our own decoder and for foreign readers alike.
"""

import json
from io import BytesIO
from typing import Any, Dict, List, Optional, Sequence, Tuple

import fastavro

from .data_structures import (
    DataFile,
    ManifestContent,
    ManifestFile,
    PartitionSpec,
    Schema,
)
from .iceberg_avro import MANIFEST_LIST_SCHEMA, manifest_entry_schema
from .iceberg_bounds import encode_bound
from .metadata_serde import _partition_spec_to_iceberg, schema_to_iceberg
from .table_paths import join_uri

ENTRY_STATUS_EXISTING = 0
ENTRY_STATUS_ADDED = 1
ENTRY_STATUS_DELETED = 2

# (status, snapshot_id, sequence_number, data_file)
ManifestEntry = Tuple[int, Optional[int], Optional[int], DataFile]


def _field_types(schema: Schema) -> Dict[int, Any]:
    return {int(f["id"]): f["type"] for f in schema.fields}


def _int_map(values: Optional[Dict[int, Any]]) -> Optional[List[Dict[str, int]]]:
    if not values:
        return None
    out = []
    for k, v in values.items():
        try:
            out.append({"key": int(k), "value": int(v)})
        except (TypeError, ValueError):
            continue
    return out or None


def _bounds_map(values: Optional[Dict[int, Any]], types: Dict[int, Any]) -> Optional[List[Dict[str, Any]]]:
    """Binary bounds for the columns whose encoding is exact; others are omitted
    (a wrong bound makes foreign readers return wrong rows, spike #83)."""
    if not values:
        return None
    out = []
    for k, v in values.items():
        try:
            field_id = int(k)
        except (TypeError, ValueError):
            continue
        encoded = encode_bound(v, types.get(field_id))
        if encoded is not None:
            out.append({"key": field_id, "value": encoded})
    return out or None


def _data_file_record(
    df: DataFile, location: str, types: Dict[int, Any], partition_names: Sequence[str] = ()
) -> Dict[str, Any]:
    fmt = df.file_format.value if hasattr(df.file_format, "value") else str(df.file_format)
    # Every partition field of the spec must appear, null when the row group had no value:
    # a reader resolves the struct positionally by field-id and a missing key is not "null".
    partition = {name: df.partition_values.get(name) for name in partition_names}
    return {
        "content": 0,
        "file_path": join_uri(location, df.file_path),
        "file_format": fmt.upper(),
        "partition": partition,
        "record_count": int(df.record_count),
        "file_size_in_bytes": int(df.file_size_in_bytes),
        "column_sizes": _int_map(df.column_sizes),
        "value_counts": _int_map(df.value_counts),
        "null_value_counts": _int_map(df.null_value_counts),
        "nan_value_counts": None,
        "lower_bounds": _bounds_map(df.lower_bounds, types),
        "upper_bounds": _bounds_map(df.upper_bounds, types),
        "key_metadata": None,
        "split_offsets": list(df.split_offsets) if df.split_offsets else None,
        "equality_ids": None,
        "sort_order_id": df.sort_order_id,
        "datashard_sha256": df.checksum,
        # Free-form labels from the pre-0.11 API, kept only for a table with NO spec:
        # with a spec the values live in the Iceberg partition struct above, which is
        # what foreign readers prune on.
        "datashard_partition_labels": (
            json.dumps(df.partition_values, default=str)
            if df.partition_values and not partition_names else None
        ),
    }


def encode_manifest(
    entries: Sequence[ManifestEntry],
    schema: Schema,
    spec: PartitionSpec,
    location: str,
) -> bytes:
    """Avro bytes of a data manifest holding `entries`.

    When the table has a partition spec, each entry carries the Iceberg partition struct -
    a field per spec field, with the spec's field-ids - which is what DuckDB, pyiceberg,
    Spark and Trino prune on (#98).
    """
    from .partitioning import partition_avro_fields, spec_field_types

    spec_fields = spec_field_types(spec, schema) if spec.fields else []
    partition_fields = partition_avro_fields(spec_fields)
    partition_names = [pf.name for pf, _ in spec_fields]
    types = _field_types(schema)
    records = [
        {
            "status": status,
            "snapshot_id": snapshot_id,
            "sequence_number": sequence_number,
            "file_sequence_number": sequence_number,
            "data_file": _data_file_record(df, location, types, partition_names),
        }
        for status, snapshot_id, sequence_number, df in entries
    ]
    header = {
        "schema": json.dumps(schema_to_iceberg(schema)),
        "schema-id": str(schema.schema_id),
        "partition-spec": json.dumps(_partition_spec_to_iceberg(spec)["fields"]),
        "partition-spec-id": str(spec.spec_id),
        "format-version": "2",
        "content": "data",
    }
    bio = BytesIO()
    fastavro.writer(
        bio, fastavro.parse_schema(manifest_entry_schema(partition_fields)), records, metadata=header
    )
    return bio.getvalue()


def partition_summaries(
    entries: Sequence[ManifestEntry], spec: PartitionSpec, schema: Schema
) -> List[Dict[str, Any]]:
    """One Iceberg `field_summary` per partition field, in SPEC ORDER.

    A manifest list for a partitioned table must carry these: DuckDB refuses a manifest
    whose summary count does not match the spec ("Manifest has 0 'field_summary'"), and
    every engine uses them to skip a whole manifest before opening it.
    """
    from .partitioning import result_type, spec_field_types

    if not spec.fields:
        return []
    summaries = []
    for pf, source_type in spec_field_types(spec, schema):
        value_type = result_type(pf.transform, source_type)
        values = [df.partition_values.get(pf.name) for _s, _sid, _seq, df in entries]
        present = [v for v in values if v is not None]
        lower = upper = None
        if present:
            try:
                lower = encode_bound(min(present), value_type)
                upper = encode_bound(max(present), value_type)
            except TypeError:      # values that do not order together: no bound, no pruning
                lower = upper = None
        summaries.append({
            "contains_null": len(present) != len(values),
            "contains_nan": None,
            "lower_bound": lower,
            "upper_bound": upper,
        })
    return summaries


def encode_manifest_list(
    manifests: Sequence[ManifestFile],
    snapshot_id: int,
    parent_snapshot_id: Optional[int],
    sequence_number: Optional[int],
    location: str,
) -> bytes:
    """Avro bytes of a manifest list naming `manifests`."""
    records = []
    for mf in manifests:
        content_val = int(mf.content.value) if isinstance(mf.content, ManifestContent) else int(mf.content)
        seq = mf.sequence_number if mf.sequence_number is not None else 0
        records.append({
            "manifest_path": join_uri(location, mf.manifest_path),
            "manifest_length": int(mf.manifest_length),
            "partition_spec_id": int(mf.partition_spec_id),
            "content": content_val,
            "sequence_number": seq,
            "min_sequence_number": mf.min_sequence_number if mf.min_sequence_number is not None else seq,
            "added_snapshot_id": int(mf.added_snapshot_id),
            "added_files_count": int(mf.added_data_files_count),
            "existing_files_count": int(mf.existing_data_files_count),
            "deleted_files_count": int(mf.deleted_data_files_count),
            "added_rows_count": int(mf.added_rows_count or 0),
            "existing_rows_count": int(mf.existing_rows_count or 0),
            "deleted_rows_count": 0,
            "partitions": list(mf.partitions) or None,
            "key_metadata": None,
            "datashard_sha256": mf.checksum,
        })
    header = {
        "snapshot-id": str(snapshot_id),
        "parent-snapshot-id": str(parent_snapshot_id) if parent_snapshot_id not in (None, -1) else "null",
        "sequence-number": str(sequence_number if sequence_number is not None else 0),
        "format-version": "2",
    }
    bio = BytesIO()
    fastavro.writer(bio, fastavro.parse_schema(MANIFEST_LIST_SCHEMA), records, metadata=header)
    return bio.getvalue()
