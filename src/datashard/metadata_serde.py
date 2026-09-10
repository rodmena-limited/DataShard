"""
TableMetadata <-> Iceberg v2 metadata.json (#84).

On the way out: the kebab-case Iceberg v2 document, with every path as an absolute
URI under `location`. On the way in: the Iceberg form (ours or a foreign writer's)
or the legacy snake_case form written before 0.10 (see metadata_serde_legacy); the
result's paths are table-relative, which is what the rest of datashard speaks.
"""

import json
from typing import Any, Dict, List, Optional

from .data_structures import (
    HistoryEntry,
    PartitionField,
    PartitionSpec,
    Schema,
    Snapshot,
    SortField,
    SortOrder,
    TableMetadata,
)
from .metadata_serde_legacy import legacy_dict_to_metadata
from .table_paths import is_uri, join_uri, to_relative

# Snapshot summary keys carrying datashard's manifest-list integrity data (#58),
# namespaced so foreign writers' snapshots are simply "unverified", never "corrupt".
SUMMARY_LIST_LENGTH = "datashard.manifest-list-length"
SUMMARY_LIST_SHA256 = "datashard.manifest-list-sha256"
LEGACY_SUMMARY_LIST_LENGTH = "manifest-list-length"
LEGACY_SUMMARY_LIST_SHA256 = "manifest-list-sha256"
NAME_MAPPING_PROPERTY = "schema.name-mapping.default"
NO_SNAPSHOT = -1


def is_legacy_document(doc: Dict[str, Any]) -> bool:
    """True for the snake_case form datashard wrote before 0.10."""
    return "format-version" not in doc and "format_version" in doc


# A bare "fixed" carries no width, and Iceberg has no such type - pyiceberg's parser
# rejects it ("Could not match fixed, expected format fixed[22]"). Since 0.11.2 `uuid`
# and `fixed[L]` ARE representable: they are written as fixed-width binary, which is
# what Iceberg specifies (#92).
NOT_ICEBERG_REPRESENTABLE = {"fixed": "fixed[L], giving the width, or binary"}


def unrepresentable_fields(schema: Schema) -> Dict[str, str]:
    """{field name: suggested type} for columns no Iceberg reader could read reliably."""
    return {
        str(f["name"]): NOT_ICEBERG_REPRESENTABLE[f["type"]]
        for f in schema.fields
        if isinstance(f.get("type"), str) and f["type"] in NOT_ICEBERG_REPRESENTABLE
    }


def legacy_unreadable_fields(schema: Schema) -> Dict[str, str]:
    """{field name: what to do} for columns of a table being MIGRATED whose existing data
    files a foreign reader cannot read.

    Wider than :func:`unrepresentable_fields`, which only refuses what cannot be written
    today: a table old enough to need migrating stores its `uuid` column as a parquet
    string, which pyiceberg refuses to promote ("Cannot promote an string to uuid"). Those
    files are not rewritten by a migration - the data is untouched - so the operator is
    told which columns foreign readers will still refuse, and how to fix them.
    """
    out = dict(unrepresentable_fields(schema))
    for f in schema.fields:
        if f.get("type") == "uuid" and str(f["name"]) not in out:
            out[str(f["name"])] = "rewrite_data_files() to convert the old string files"
    return out


def check_iceberg_representable(schema: Schema) -> None:
    """Refuse to CREATE a table whose columns would not be readable by Iceberg engines.

    Existing tables keep working - this is only checked when a schema is first persisted.
    """
    bad = unrepresentable_fields(schema)
    if bad:
        detail = ", ".join(f"'{n}' (use {t})" for n, t in bad.items())
        raise ValueError(
            f"datashard tables are Iceberg v2 tables, and these columns cannot be expressed as "
            f"Iceberg columns every engine reads: {detail}. A bare 'fixed' has no width, and "
            f"Iceberg has no such type - pyiceberg's parser rejects it outright."
        )


def schema_to_iceberg(schema: Schema) -> Dict[str, Any]:
    fields = []
    for f in schema.fields:
        entry: Dict[str, Any] = {
            "id": int(f["id"]),
            "name": str(f["name"]),
            "required": bool(f.get("required", False)),
            "type": f["type"],
        }
        if f.get("doc"):
            entry["doc"] = f["doc"]
        fields.append(entry)
    return {"type": "struct", "schema-id": schema.schema_id, "fields": fields}


def name_mapping_json(schema: Schema) -> str:
    """schema.name-mapping.default for parquet files that carry no field ids (#88)."""
    return json.dumps([{"field-id": int(f["id"]), "names": [str(f["name"])]} for f in schema.fields])


def _partition_spec_to_iceberg(spec: PartitionSpec) -> Dict[str, Any]:
    return {
        "spec-id": spec.spec_id,
        "fields": [
            {"source-id": pf.source_id, "field-id": pf.field_id, "name": pf.name, "transform": pf.transform}
            for pf in spec.fields
        ],
    }


def _sort_order_to_iceberg(order: SortOrder) -> Dict[str, Any]:
    # Iceberg's unsorted order is id 0; datashard's pre-0.10 default carried id 1.
    order_id = 0 if not order.fields else order.order_id
    return {
        "order-id": order_id,
        "fields": [
            {"source-id": sf.source_id, "transform": sf.transform, "direction": sf.direction, "null-order": "nulls-first"}
            for sf in order.fields
        ],
    }


def _snapshot_to_iceberg(snapshot: Snapshot, location: str) -> Dict[str, Any]:
    summary: Dict[str, str] = {"operation": snapshot.operation or "append"}
    for k, v in (snapshot.summary or {}).items():
        if k == "operation":
            continue
        # integrity keys written before 0.10 move into the datashard.* namespace
        key = {LEGACY_SUMMARY_LIST_LENGTH: SUMMARY_LIST_LENGTH, LEGACY_SUMMARY_LIST_SHA256: SUMMARY_LIST_SHA256}.get(k, k)
        summary[key] = str(v)
    doc: Dict[str, Any] = {
        "snapshot-id": snapshot.snapshot_id,
        "sequence-number": snapshot.sequence_number if snapshot.sequence_number is not None else 0,
        "timestamp-ms": snapshot.timestamp_ms,
        "manifest-list": join_uri(location, snapshot.manifest_list),
        "summary": summary,
    }
    if snapshot.parent_snapshot_id is not None and snapshot.parent_snapshot_id != NO_SNAPSHOT:
        doc["parent-snapshot-id"] = snapshot.parent_snapshot_id
    if snapshot.schema_id is not None:
        doc["schema-id"] = snapshot.schema_id
    return doc


def metadata_to_dict(metadata: TableMetadata) -> Dict[str, Any]:
    """Iceberg v2 metadata.json document for `metadata` (paths become URIs)."""
    location = metadata.location
    current = metadata.current_snapshot_id if metadata.current_snapshot_id is not None else NO_SNAPSHOT
    partition_ids = [pf.field_id for spec in metadata.partition_specs for pf in spec.fields]
    orders = [_sort_order_to_iceberg(o) for o in metadata.sort_orders] or [{"order-id": 0, "fields": []}]
    default_order = metadata.default_sort_order_id
    if not any(o["order-id"] == default_order for o in orders):
        default_order = orders[0]["order-id"]
    doc: Dict[str, Any] = {
        "format-version": 2,
        "table-uuid": metadata.table_uuid,
        "location": location,
        "last-sequence-number": metadata.last_sequence_number,
        "last-updated-ms": metadata.last_updated_ms,
        "last-column-id": metadata.last_column_id or max(
            (int(f["id"]) for s in metadata.schemas for f in s.fields), default=0
        ),
        "current-schema-id": metadata.current_schema_id,
        "schemas": [schema_to_iceberg(s) for s in metadata.schemas],
        "default-spec-id": metadata.default_spec_id,
        "partition-specs": [_partition_spec_to_iceberg(s) for s in metadata.partition_specs],
        "last-partition-id": max(partition_ids, default=999),
        "default-sort-order-id": default_order,
        "sort-orders": orders,
        "properties": {str(k): str(v) for k, v in metadata.properties.items()},
        "current-snapshot-id": current,
        "snapshots": [_snapshot_to_iceberg(s, location) for s in metadata.snapshots],
        "snapshot-log": [
            {"timestamp-ms": e.timestamp_ms, "snapshot-id": e.snapshot_id} for e in metadata.snapshot_log
        ],
        "metadata-log": [
            {
                "timestamp-ms": e["timestamp-ms"],
                # Entries are table-relative; one kept verbatim (a foreign writer's file
                # outside this table) is already a URI and must not be joined again.
                "metadata-file": e["metadata-file"] if is_uri(e["metadata-file"]) else join_uri(location, e["metadata-file"]),
            }
            for e in metadata.metadata_log
            if isinstance(e, dict) and e.get("metadata-file")
        ],
        "refs": {"main": {"snapshot-id": current, "type": "branch"}} if current != NO_SNAPSHOT else {},
    }
    return doc


def _schema_from_iceberg(doc: Dict[str, Any]) -> Schema:
    fields = []
    for f in doc.get("fields", []):
        entry = {"id": f["id"], "name": f["name"], "type": f["type"], "required": bool(f.get("required", False))}
        if f.get("doc"):
            entry["doc"] = f["doc"]
        fields.append(entry)
    return Schema(schema_id=int(doc.get("schema-id", 0)), fields=fields)


def _snapshot_from_iceberg(doc: Dict[str, Any], locations: List[Optional[str]]) -> Snapshot:
    summary = {str(k): str(v) for k, v in (doc.get("summary") or {}).items()}
    operation = summary.pop("operation", None)
    return Snapshot(
        snapshot_id=int(doc["snapshot-id"]),
        timestamp_ms=int(doc["timestamp-ms"]),
        manifest_list=to_relative(doc["manifest-list"], locations),
        parent_snapshot_id=doc.get("parent-snapshot-id"),
        operation=operation,
        summary=summary,
        schema_id=doc.get("schema-id"),
        sequence_number=doc.get("sequence-number"),
    )


def dict_to_metadata(doc: Dict[str, Any], actual_location: Optional[str] = None) -> TableMetadata:
    """Parse a metadata.json document of either form. `actual_location` is the URI
    of the root the table is opened at; paths under the recorded location OR the
    actual one resolve (a moved table keeps reading, #87)."""
    if is_legacy_document(doc):
        return legacy_dict_to_metadata(doc)
    location = str(doc["location"])
    locations: List[Optional[str]] = [location, actual_location]
    schemas = [_schema_from_iceberg(s) for s in doc.get("schemas", [])]
    if not schemas and "schema" in doc:  # v1 documents carry a single schema
        schemas = [_schema_from_iceberg(doc["schema"])]
    specs = [
        PartitionSpec(
            spec_id=int(s["spec-id"]),
            fields=[
                PartitionField(source_id=f["source-id"], field_id=f["field-id"], name=f["name"], transform=f["transform"])
                for f in s.get("fields", [])
            ],
        )
        for s in doc.get("partition-specs", [])
    ]
    orders = [
        SortOrder(
            order_id=int(o["order-id"]),
            fields=[
                SortField(source_id=f["source-id"], field_id=0, transform=f["transform"], direction=f["direction"])
                for f in o.get("fields", [])
            ],
        )
        for o in doc.get("sort-orders", [])
    ]
    current = doc.get("current-snapshot-id", NO_SNAPSHOT)
    return TableMetadata(
        location=location,
        table_uuid=str(doc["table-uuid"]),
        format_version=int(doc.get("format-version", 2)),
        last_sequence_number=int(doc.get("last-sequence-number", 0)),
        last_updated_ms=int(doc.get("last-updated-ms", 0)),
        last_column_id=int(doc.get("last-column-id", 0)),
        schemas=schemas,
        current_schema_id=int(doc.get("current-schema-id", schemas[0].schema_id if schemas else 0)),
        partition_specs=specs,
        default_spec_id=int(doc.get("default-spec-id", 0)),
        sort_orders=orders,
        default_sort_order_id=int(doc.get("default-sort-order-id", 0)),
        properties={str(k): str(v) for k, v in (doc.get("properties") or {}).items()},
        current_snapshot_id=int(current) if current is not None else NO_SNAPSHOT,
        snapshots=[_snapshot_from_iceberg(s, locations) for s in doc.get("snapshots", [])],
        snapshot_log=[
            HistoryEntry(timestamp_ms=int(e["timestamp-ms"]), snapshot_id=int(e["snapshot-id"]))
            for e in doc.get("snapshot-log", [])
        ],
        metadata_log=[
            {"timestamp-ms": e["timestamp-ms"], "metadata-file": _relative_or_none(e.get("metadata-file"), locations)}
            for e in doc.get("metadata-log", [])
            if isinstance(e, dict)
        ],
    )


def _relative_or_none(path: Optional[str], locations: List[Optional[str]]) -> Optional[str]:
    if not path:
        return None
    try:
        return to_relative(path, locations)
    except ValueError:
        return path  # a foreign writer's file elsewhere: kept verbatim, never deleted by GC
