"""
TableMetadata <-> JSON dict conversion (split out of metadata_manager.py, #71).
"""

from typing import Any, Dict

from .data_structures import Schema, TableMetadata


def metadata_to_dict(metadata: TableMetadata) -> Dict[str, Any]:
    """Convert TableMetadata to dictionary for JSON serialization"""
    return {
        "location": metadata.location,
        "table_uuid": metadata.table_uuid,
        "format_version": metadata.format_version,
        "last_sequence_number": metadata.last_sequence_number,
        "last_updated_ms": metadata.last_updated_ms,
        "last_column_id": metadata.last_column_id,
        "schemas": [
            {
                "schema_id": schema.schema_id,
                "fields": schema.fields,
                "schema_string": schema.schema_string,
            }
            for schema in metadata.schemas
        ],
        "current_schema_id": metadata.current_schema_id,
        "partition_specs": [
            {
                "spec_id": spec.spec_id,
                "fields": [
                    {
                        "source_id": field.source_id,
                        "field_id": field.field_id,
                        "name": field.name,
                        "transform": field.transform,
                    }
                    for field in spec.fields
                ],
            }
            for spec in metadata.partition_specs
        ],
        "default_spec_id": metadata.default_spec_id,
        "sort_orders": [
            {
                "order_id": order.order_id,
                "fields": [
                    {
                        "source_id": field.source_id,
                        "field_id": field.field_id,
                        "transform": field.transform,
                        "direction": field.direction,
                    }
                    for field in order.fields
                ],
            }
            for order in metadata.sort_orders
        ],
        "default_sort_order_id": metadata.default_sort_order_id,
        "properties": metadata.properties,
        "current_snapshot_id": metadata.current_snapshot_id,
        "snapshots": [
            {
                "snapshot_id": snapshot.snapshot_id,
                "timestamp_ms": snapshot.timestamp_ms,
                "manifest_list": snapshot.manifest_list,
                "parent_snapshot_id": snapshot.parent_snapshot_id,
                "operation": snapshot.operation,
                "summary": snapshot.summary,
                "schema_id": snapshot.schema_id,
                "sequence_number": snapshot.sequence_number,
            }
            for snapshot in metadata.snapshots
        ],
        "snapshot_log": [
            {"timestamp_ms": entry.timestamp_ms, "snapshot_id": entry.snapshot_id}
            for entry in metadata.snapshot_log
        ],
        "metadata_log": metadata.metadata_log,
        "last_commit_id": metadata.last_commit_id,
    }

def dict_to_metadata(metadata_dict: Dict[str, Any]) -> TableMetadata:
    """Convert dictionary back to TableMetadata"""
    from .data_structures import (
        HistoryEntry as HistoryEntryStruct,
        PartitionField,
        PartitionSpec,
        Snapshot as SnapshotStruct,
        SortField,
        SortOrder,
    )

    # Reconstruct schemas
    schemas = [
        Schema(
            schema_id=schema_dict["schema_id"],
            fields=schema_dict["fields"],
            schema_string=schema_dict.get("schema_string", ""),
        )
        for schema_dict in metadata_dict["schemas"]
    ]

    # Reconstruct partition specs
    partition_specs = []
    for spec_dict in metadata_dict["partition_specs"]:
        fields = [
            PartitionField(
                source_id=field_dict["source_id"],
                field_id=field_dict["field_id"],
                name=field_dict["name"],
                transform=field_dict["transform"],
            )
            for field_dict in spec_dict["fields"]
        ]
        partition_specs.append(PartitionSpec(spec_id=spec_dict["spec_id"], fields=fields))

    # Reconstruct sort orders
    sort_orders = []
    for order_dict in metadata_dict["sort_orders"]:
        sort_fields = [
            SortField(
                source_id=field_dict["source_id"],
                field_id=field_dict["field_id"],
                transform=field_dict["transform"],
                direction=field_dict["direction"],
            )
            for field_dict in order_dict["fields"]
        ]
        sort_orders.append(SortOrder(order_id=order_dict["order_id"], fields=sort_fields))

    # Reconstruct snapshots
    snapshots = [
        SnapshotStruct(
            snapshot_id=snapshot_dict["snapshot_id"],
            timestamp_ms=snapshot_dict["timestamp_ms"],
            manifest_list=snapshot_dict["manifest_list"],
            parent_snapshot_id=snapshot_dict.get("parent_snapshot_id"),
            operation=snapshot_dict.get("operation"),
            summary=snapshot_dict.get("summary", {}),
            schema_id=snapshot_dict.get("schema_id"),
            sequence_number=snapshot_dict.get("sequence_number"),
        )
        for snapshot_dict in metadata_dict["snapshots"]
    ]

    # Reconstruct history
    snapshot_log = [
        HistoryEntryStruct(
            timestamp_ms=entry_dict["timestamp_ms"], snapshot_id=entry_dict["snapshot_id"]
        )
        for entry_dict in metadata_dict["snapshot_log"]
    ]

    return TableMetadata(
        location=metadata_dict["location"],
        table_uuid=metadata_dict["table_uuid"],
        format_version=metadata_dict["format_version"],
        last_sequence_number=metadata_dict["last_sequence_number"],
        last_updated_ms=metadata_dict["last_updated_ms"],
        last_column_id=metadata_dict["last_column_id"],
        schemas=schemas,
        current_schema_id=metadata_dict["current_schema_id"],
        partition_specs=partition_specs,
        default_spec_id=metadata_dict["default_spec_id"],
        sort_orders=sort_orders,
        default_sort_order_id=metadata_dict["default_sort_order_id"],
        properties=metadata_dict["properties"],
        current_snapshot_id=metadata_dict["current_snapshot_id"],
        snapshots=snapshots,
        snapshot_log=snapshot_log,
        metadata_log=metadata_dict.get("metadata_log", []),
        last_commit_id=metadata_dict.get("last_commit_id", ""),
    )

