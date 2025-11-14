"""
Metadata management for the Python Iceberg implementation
"""
import json
import os
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

from .data_structures import HistoryEntry, Schema, Snapshot, TableMetadata


class ConcurrentModificationException(Exception):
    """Exception thrown when concurrent modifications are detected"""
    pass


class MetadataManager:
    """Manages table metadata persistence and updates"""

    def __init__(self, table_path: str):
        self.table_path = table_path
        self.metadata_path = os.path.join(table_path, 'metadata')
        self.current_version = 0
        self._lock = threading.RLock()  # For thread safety

        # Ensure metadata directory exists
        os.makedirs(self.metadata_path, exist_ok=True)

    def initialize_table(self, metadata: TableMetadata) -> TableMetadata:
        """Initialize a new table with the given metadata"""
        with self._lock:
            # Set initial values
            if metadata.current_snapshot_id is None:
                metadata.current_snapshot_id = -1  # No snapshot initially
            metadata.last_updated_ms = int(datetime.now().timestamp() * 1000)

            # Write the metadata file
            metadata_file = f"v{self.current_version}.metadata.json"
            metadata_path = os.path.join(self.metadata_path, metadata_file)

            self._write_metadata_file(metadata_path, metadata)

            # Create version hint file
            self._write_version_hint(self.current_version)

            return metadata

    def refresh(self) -> Optional[TableMetadata]:
        """Refresh metadata from the latest version"""
        with self._lock:
            version = self._read_version_hint()
            if version is None:
                return None

            metadata_file = f"v{version}.metadata.json"
            metadata_path = os.path.join(self.metadata_path, metadata_file)

            if not os.path.exists(metadata_path):
                return None

            return self._read_metadata_file(metadata_path)

    def commit(self, base_metadata: TableMetadata, new_metadata: TableMetadata) -> TableMetadata:
        """Commit new metadata with Optimistic Concurrency Control following Iceberg pattern"""
        # In true Iceberg style, we need to check that the underlying metadata hasn't changed
        # since we read it. This is done by comparing with current state during commit.

        # Read the current metadata to compare with the base that the caller had
        current = self.refresh()

        # Check UUID consistency
        if current and current.table_uuid != base_metadata.table_uuid:
            raise ValueError("Table UUID mismatch - concurrent modification detected")

        # The key OCC check: verify that the metadata hasn't changed since the caller read it
        # This implements the Iceberg atomic commit pattern
        if current and current.current_snapshot_id != base_metadata.current_snapshot_id:
            # A snapshot has been added while the operation was in progress
            raise ConcurrentModificationException(
                f"Cannot commit metadata: concurrent modification detected. "
                f"Expected current_snapshot_id: {base_metadata.current_snapshot_id}, "
                f"but found: {current.current_snapshot_id}"
            )

        if current and current.last_updated_ms != base_metadata.last_updated_ms:
            # The metadata was updated while this operation was in progress
            raise ConcurrentModificationException(
                f"Cannot commit metadata: concurrent modification detected. "
                f"Expected last_updated_ms: {base_metadata.last_updated_ms}, "
                f"but found: {current.last_updated_ms}"
            )

        # After validation passes, perform the atomic update
        with self._lock:
            # Double-check the state right before committing (double-checked locking pattern)
            fresh_current = self.refresh()
            if (fresh_current and
                (fresh_current.current_snapshot_id != base_metadata.current_snapshot_id or
                 fresh_current.last_updated_ms != base_metadata.last_updated_ms)):
                raise ConcurrentModificationException(
                    "Concurrent modification detected during commit - retry operation"
                )

            # Update sequence number and timestamp
            new_metadata.last_updated_ms = int(datetime.now().timestamp() * 1000)
            self.current_version += 1  # This creates the next version

            # Write new metadata file
            metadata_file = f"v{self.current_version}.metadata.json"
            metadata_path = os.path.join(self.metadata_path, metadata_file)

            self._write_metadata_file(metadata_path, new_metadata)

            # Atomically update the version hint to point to the new version
            self._write_version_hint(self.current_version)

            return new_metadata

    def get_snapshot_by_id(self, snapshot_id: int) -> Optional[Snapshot]:
        """Get a specific snapshot by ID"""
        metadata = self.refresh()
        if not metadata:
            return None

        for snapshot in metadata.snapshots:
            if snapshot.snapshot_id == snapshot_id:
                return snapshot
        return None

    def get_current_snapshot(self) -> Optional[Snapshot]:
        """Get the current snapshot"""
        metadata = self.refresh()
        if not metadata or metadata.current_snapshot_id is None:
            return None

        for snapshot in metadata.snapshots:
            if snapshot.snapshot_id == metadata.current_snapshot_id:
                return snapshot
        return None

    def get_all_snapshots(self) -> List[Snapshot]:
        """Get all snapshots"""
        metadata = self.refresh()
        if not metadata:
            return []
        return metadata.snapshots

    def get_snapshot_history(self) -> List[HistoryEntry]:
        """Get snapshot history"""
        metadata = self.refresh()
        if not metadata:
            return []
        return metadata.snapshot_log

    def _write_metadata_file(self, path: str, metadata: TableMetadata) -> None:
        """Write metadata to a JSON file"""
        metadata_dict = self._metadata_to_dict(metadata)

        with open(path, 'w', encoding='utf-8') as f:
            json.dump(metadata_dict, f, indent=2)

    def _read_metadata_file(self, path: str) -> TableMetadata:
        """Read metadata from a JSON file"""
        with open(path, 'r', encoding='utf-8') as f:
            metadata_dict = json.load(f)

        return self._dict_to_metadata(metadata_dict)

    def _metadata_to_dict(self, metadata: TableMetadata) -> Dict[str, Any]:
        """Convert TableMetadata to dictionary for JSON serialization"""
        return {
            'location': metadata.location,
            'table_uuid': metadata.table_uuid,
            'format_version': metadata.format_version,
            'last_sequence_number': metadata.last_sequence_number,
            'last_updated_ms': metadata.last_updated_ms,
            'last_column_id': metadata.last_column_id,
            'schemas': [
                {
                    'schema_id': schema.schema_id,
                    'fields': schema.fields,
                    'schema_string': schema.schema_string
                }
                for schema in metadata.schemas
            ],
            'current_schema_id': metadata.current_schema_id,
            'partition_specs': [
                {
                    'spec_id': spec.spec_id,
                    'fields': [
                        {
                            'source_id': field.source_id,
                            'field_id': field.field_id,
                            'name': field.name,
                            'transform': field.transform
                        }
                        for field in spec.fields
                    ]
                }
                for spec in metadata.partition_specs
            ],
            'default_spec_id': metadata.default_spec_id,
            'sort_orders': [
                {
                    'order_id': order.order_id,
                    'fields': [
                        {
                            'source_id': field.source_id,
                            'field_id': field.field_id,
                            'transform': field.transform,
                            'direction': field.direction
                        }
                        for field in order.fields
                    ]
                }
                for order in metadata.sort_orders
            ],
            'default_sort_order_id': metadata.default_sort_order_id,
            'properties': metadata.properties,
            'current_snapshot_id': metadata.current_snapshot_id,
            'snapshots': [
                {
                    'snapshot_id': snapshot.snapshot_id,
                    'timestamp_ms': snapshot.timestamp_ms,
                    'manifest_list': snapshot.manifest_list,
                    'parent_snapshot_id': snapshot.parent_snapshot_id,
                    'operation': snapshot.operation,
                    'summary': snapshot.summary,
                    'schema_id': snapshot.schema_id
                }
                for snapshot in metadata.snapshots
            ],
            'snapshot_log': [
                {
                    'timestamp_ms': entry.timestamp_ms,
                    'snapshot_id': entry.snapshot_id
                }
                for entry in metadata.snapshot_log
            ],
            'metadata_log': metadata.metadata_log
        }

    def _dict_to_metadata(self, metadata_dict: Dict[str, Any]) -> TableMetadata:
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
                schema_id=schema_dict['schema_id'],
                fields=schema_dict['fields'],
                schema_string=schema_dict.get('schema_string', '')
            )
            for schema_dict in metadata_dict['schemas']
        ]

        # Reconstruct partition specs
        partition_specs = []
        for spec_dict in metadata_dict['partition_specs']:
            fields = [
                PartitionField(
                    source_id=field_dict['source_id'],
                    field_id=field_dict['field_id'],
                    name=field_dict['name'],
                    transform=field_dict['transform']
                )
                for field_dict in spec_dict['fields']
            ]
            partition_specs.append(PartitionSpec(
                spec_id=spec_dict['spec_id'],
                fields=fields
            ))

        # Reconstruct sort orders
        sort_orders = []
        for order_dict in metadata_dict['sort_orders']:
            sort_fields = [
                SortField(
                    source_id=field_dict['source_id'],
                    field_id=field_dict['field_id'],
                    transform=field_dict['transform'],
                    direction=field_dict['direction']
                )
                for field_dict in order_dict['fields']
            ]
            sort_orders.append(SortOrder(
                order_id=order_dict['order_id'],
                fields=sort_fields
            ))

        # Reconstruct snapshots
        snapshots = [
            SnapshotStruct(
                snapshot_id=snapshot_dict['snapshot_id'],
                timestamp_ms=snapshot_dict['timestamp_ms'],
                manifest_list=snapshot_dict['manifest_list'],
                parent_snapshot_id=snapshot_dict.get('parent_snapshot_id'),
                operation=snapshot_dict.get('operation'),
                summary=snapshot_dict.get('summary', {}),
                schema_id=snapshot_dict.get('schema_id')
            )
            for snapshot_dict in metadata_dict['snapshots']
        ]

        # Reconstruct history
        snapshot_log = [
            HistoryEntryStruct(
                timestamp_ms=entry_dict['timestamp_ms'],
                snapshot_id=entry_dict['snapshot_id']
            )
            for entry_dict in metadata_dict['snapshot_log']
        ]

        return TableMetadata(
            location=metadata_dict['location'],
            table_uuid=metadata_dict['table_uuid'],
            format_version=metadata_dict['format_version'],
            last_sequence_number=metadata_dict['last_sequence_number'],
            last_updated_ms=metadata_dict['last_updated_ms'],
            last_column_id=metadata_dict['last_column_id'],
            schemas=schemas,
            current_schema_id=metadata_dict['current_schema_id'],
            partition_specs=partition_specs,
            default_spec_id=metadata_dict['default_spec_id'],
            sort_orders=sort_orders,
            default_sort_order_id=metadata_dict['default_sort_order_id'],
            properties=metadata_dict['properties'],
            current_snapshot_id=metadata_dict['current_snapshot_id'],
            snapshots=snapshots,
            snapshot_log=snapshot_log,
            metadata_log=metadata_dict['metadata_log']
        )

    def _write_version_hint(self, version: int) -> None:
        """Write the current version number to a hint file"""
        hint_path = os.path.join(self.table_path, 'metadata.version-hint.text')
        with open(hint_path, 'w', encoding='utf-8') as f:
            f.write(str(version))

    def _read_version_hint(self) -> Optional[int]:
        """Read the current version number from the hint file"""
        hint_path = os.path.join(self.table_path, 'metadata.version-hint.text')
        if not os.path.exists(hint_path):
            return None

        with open(hint_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            return int(content) if content.isdigit() else None
