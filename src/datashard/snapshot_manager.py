"""
Snapshotting and time travel functionality for the Python Iceberg implementation
"""
import os
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List, Optional

from .data_structures import HistoryEntry, Snapshot
from .metadata_manager import MetadataManager


class SnapshotManager:
    """Manages snapshots and time travel functionality"""

    def __init__(self, metadata_manager: MetadataManager):
        self.metadata_manager = metadata_manager
        self.snapshots_path = os.path.join(self.metadata_manager.table_path, 'snapshots')

        # Ensure snapshots directory exists
        os.makedirs(self.snapshots_path, exist_ok=True)

    def create_snapshot(self, manifest_list_path: str, operation: str = "append",
                       parent_snapshot_id: Optional[int] = None,
                       summary: Optional[Dict[str, str]] = None) -> Snapshot:
        """Create a new snapshot with proper OCC handling"""
        # Get the base metadata that will be used for comparison in commit
        base_metadata = self.metadata_manager.refresh()
        if base_metadata is None:
            raise ValueError("Cannot create snapshot: no current metadata")

        # Generate a new snapshot ID
        snapshot_id = int(datetime.now().timestamp() * 1000000)  # microseconds since epoch

        # Create new snapshot
        snapshot = Snapshot(
            snapshot_id=snapshot_id,
            timestamp_ms=int(datetime.now().timestamp() * 1000),
            manifest_list=manifest_list_path,
            parent_snapshot_id=parent_snapshot_id,
            operation=operation,
            summary=summary or {}
        )

        # Create new metadata based on base, but with modifications
        new_metadata = deepcopy(base_metadata)
        new_metadata.snapshots.append(snapshot)
        new_metadata.current_snapshot_id = snapshot_id

        # Add to snapshot log
        history_entry = HistoryEntry(
            timestamp_ms=snapshot.timestamp_ms,
            snapshot_id=snapshot.snapshot_id
        )
        new_metadata.snapshot_log.append(history_entry)

        # Commit the updated metadata using OCC (base and new metadata)
        # This will fail if the base doesn't match the current state
        self.metadata_manager.commit(base_metadata, new_metadata)

        # Return the snapshot that was created
        return snapshot

    def get_snapshot_by_id(self, snapshot_id: int) -> Optional[Snapshot]:
        """Get a snapshot by its ID"""
        return self.metadata_manager.get_snapshot_by_id(snapshot_id)

    def get_current_snapshot(self) -> Optional[Snapshot]:
        """Get the current snapshot"""
        return self.metadata_manager.get_current_snapshot()

    def get_all_snapshots(self) -> List[Snapshot]:
        """Get all snapshots"""
        return self.metadata_manager.get_all_snapshots()

    def get_snapshot_history(self) -> List[HistoryEntry]:
        """Get snapshot history log"""
        return self.metadata_manager.get_snapshot_history()

    def get_snapshot_by_timestamp(self, timestamp_ms: int) -> Optional[Snapshot]:
        """Get the most recent snapshot before or at the given timestamp"""
        snapshots = self.get_all_snapshots()

        # Find the most recent snapshot at or before the timestamp
        target_snapshot = None
        for snapshot in sorted(snapshots, key=lambda s: s.timestamp_ms):
            if snapshot.timestamp_ms <= timestamp_ms:
                target_snapshot = snapshot
            else:
                break

        return target_snapshot

    def get_snapshot_as_of_time(self, timestamp_ms: int) -> Optional[Snapshot]:
        """Get the snapshot as of a specific time (alias for time travel)"""
        return self.get_snapshot_by_timestamp(timestamp_ms)

    def list_snapshots(self) -> List[Dict[str, Any]]:
        """List all snapshots with their details"""
        snapshots = self.get_all_snapshots()
        return [
            {
                'snapshot_id': snapshot.snapshot_id,
                'timestamp_ms': snapshot.timestamp_ms,
                'timestamp': datetime.fromtimestamp(snapshot.timestamp_ms / 1000.0),
                'operation': snapshot.operation,
                'summary': snapshot.summary,
                'parent_id': snapshot.parent_snapshot_id
            }
            for snapshot in snapshots
        ]

    def rollback_to(self, snapshot_id: int) -> bool:
        """Rollback the table to a specific snapshot"""
        raise NotImplementedError("Rollback functionality requires more complex implementation")

    def time_travel_to(self, snapshot_id: int) -> Any:
        """Time travel to a specific snapshot"""
        # This would typically involve setting the current snapshot to the specified one
        # For our implementation, we'll return the snapshot data for the user to work with
        return self.get_snapshot_by_id(snapshot_id)

    def time_travel_to_timestamp(self, timestamp_ms: int) -> Any:
        """Time travel to a snapshot at or before a specific timestamp"""
        snapshot = self.get_snapshot_by_timestamp(timestamp_ms)
        if snapshot:
            return self.time_travel_to(snapshot.snapshot_id)
        return None

    def delete_snapshot(self, snapshot_id: int) -> bool:
        """Delete a specific snapshot (soft delete for now)"""
        current_metadata = self.metadata_manager.refresh()
        if current_metadata is None:
            return False

        # Find and remove the snapshot
        snapshot_to_remove = None
        for i, snapshot in enumerate(current_metadata.snapshots):
            if snapshot.snapshot_id == snapshot_id:
                snapshot_to_remove = i
                break

        if snapshot_to_remove is not None:
            del current_metadata.snapshots[snapshot_to_remove]

            # Update current snapshot if needed
            if current_metadata.current_snapshot_id == snapshot_id:
                # Set to the most recent snapshot
                valid_snapshots = [s for s in current_metadata.snapshots
                                 if s.snapshot_id != snapshot_id]
                if valid_snapshots:
                    current_metadata.current_snapshot_id = max(
                        s.snapshot_id for s in valid_snapshots
                    )
                else:
                    current_metadata.current_snapshot_id = None

            # Commit the changes
            self.metadata_manager.commit(current_metadata, current_metadata)
            return True

        return False
