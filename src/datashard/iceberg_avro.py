"""
Iceberg v2 Avro schemas for manifests and manifest lists, with the spec's field-ids
(#85). Foreign readers resolve fields by id; the two datashard extras carry ids in
a private range and are ignored by DuckDB and pyiceberg (verified in spike #83).
"""

from typing import Any, Dict, List

# datashard extras (non-spec, ignored by foreign readers)
DATASHARD_SHA256_DATA_FILE_ID = 9001
DATASHARD_SHA256_MANIFEST_ID = 9002
# pre-0.10 free-form partition labels (not Iceberg partitioning; kept for callers)
DATASHARD_PARTITION_LABELS_ID = 9003

# Positional-delete file columns (Iceberg reserved ids), for 1.0
POS_DELETE_FILE_PATH_ID = 2147483546
POS_DELETE_POS_ID = 2147483545

_COUNT_NAMES = ("added_files_count", "existing_files_count", "deleted_files_count")


def _map_of(key_id: int, value_id: int, value_type: str) -> List[Any]:
    return [
        "null",
        {
            "type": "array",
            "logicalType": "map",
            "items": {
                "type": "record",
                "name": f"k{key_id}_v{value_id}",
                "fields": [
                    {"name": "key", "type": "int", "field-id": key_id},
                    {"name": "value", "type": value_type, "field-id": value_id},
                ],
            },
        },
    ]


def manifest_entry_schema(partition_fields: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Avro schema of one manifest (data_file entries). `partition_fields` are the
    Avro fields of the partition struct (empty for an unpartitioned table)."""
    data_file = {
        "type": "record",
        "name": "r2",
        "fields": [
            {"name": "content", "type": "int", "field-id": 134},
            {"name": "file_path", "type": "string", "field-id": 100},
            {"name": "file_format", "type": "string", "field-id": 101},
            {"name": "partition", "type": {"type": "record", "name": "r102", "fields": list(partition_fields)}, "field-id": 102},
            {"name": "record_count", "type": "long", "field-id": 103},
            {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
            {"name": "column_sizes", "type": _map_of(117, 118, "long"), "field-id": 108, "default": None},
            {"name": "value_counts", "type": _map_of(119, 120, "long"), "field-id": 109, "default": None},
            {"name": "null_value_counts", "type": _map_of(121, 122, "long"), "field-id": 110, "default": None},
            {"name": "nan_value_counts", "type": _map_of(138, 139, "long"), "field-id": 137, "default": None},
            {"name": "lower_bounds", "type": _map_of(126, 127, "bytes"), "field-id": 125, "default": None},
            {"name": "upper_bounds", "type": _map_of(129, 130, "bytes"), "field-id": 128, "default": None},
            {"name": "key_metadata", "type": ["null", "bytes"], "field-id": 131, "default": None},
            {"name": "split_offsets", "type": ["null", {"type": "array", "items": "long", "element-id": 133}], "field-id": 132, "default": None},
            {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int", "element-id": 136}], "field-id": 135, "default": None},
            {"name": "sort_order_id", "type": ["null", "int"], "field-id": 140, "default": None},
            {"name": "datashard_sha256", "type": ["null", "string"], "field-id": DATASHARD_SHA256_DATA_FILE_ID, "default": None},
            {"name": "datashard_partition_labels", "type": ["null", "string"], "field-id": DATASHARD_PARTITION_LABELS_ID, "default": None},
        ],
    }
    return {
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            {"name": "status", "type": "int", "field-id": 0},
            {"name": "snapshot_id", "type": ["null", "long"], "field-id": 1, "default": None},
            {"name": "sequence_number", "type": ["null", "long"], "field-id": 3, "default": None},
            {"name": "file_sequence_number", "type": ["null", "long"], "field-id": 4, "default": None},
            {"name": "data_file", "type": data_file, "field-id": 2},
        ],
    }


MANIFEST_LIST_SCHEMA: Dict[str, Any] = {
    "type": "record",
    "name": "manifest_file",
    "fields": [
        {"name": "manifest_path", "type": "string", "field-id": 500},
        {"name": "manifest_length", "type": "long", "field-id": 501},
        {"name": "partition_spec_id", "type": "int", "field-id": 502},
        {"name": "content", "type": "int", "field-id": 517},
        {"name": "sequence_number", "type": "long", "field-id": 515},
        {"name": "min_sequence_number", "type": "long", "field-id": 516},
        {"name": "added_snapshot_id", "type": "long", "field-id": 503},
        {"name": "added_files_count", "type": "int", "field-id": 504},
        {"name": "existing_files_count", "type": "int", "field-id": 505},
        {"name": "deleted_files_count", "type": "int", "field-id": 506},
        {"name": "added_rows_count", "type": "long", "field-id": 512},
        {"name": "existing_rows_count", "type": "long", "field-id": 513},
        {"name": "deleted_rows_count", "type": "long", "field-id": 514},
        {
            "name": "partitions",
            "type": [
                "null",
                {
                    "type": "array",
                    "element-id": 508,
                    "items": {
                        "type": "record",
                        "name": "r508",
                        "fields": [
                            {"name": "contains_null", "type": "boolean", "field-id": 509},
                            {"name": "contains_nan", "type": ["null", "boolean"], "field-id": 518, "default": None},
                            {"name": "lower_bound", "type": ["null", "bytes"], "field-id": 510, "default": None},
                            {"name": "upper_bound", "type": ["null", "bytes"], "field-id": 511, "default": None},
                        ],
                    },
                },
            ],
            "field-id": 507,
            "default": None,
        },
        {"name": "key_metadata", "type": ["null", "bytes"], "field-id": 519, "default": None},
        {"name": "datashard_sha256", "type": ["null", "string"], "field-id": DATASHARD_SHA256_MANIFEST_ID, "default": None},
    ],
}


def count_field_names(record: Dict[str, Any]) -> Dict[str, int]:
    """The three file counts of a manifest_file record, accepting the v1 names
    (added_data_files_count, ...) that older lists and some writers still use."""
    out: Dict[str, int] = {}
    for name in _COUNT_NAMES:
        legacy = name.replace("_files_count", "_data_files_count")
        value = record.get(name, record.get(legacy))
        out[name] = int(value) if value is not None else 0
    return out
