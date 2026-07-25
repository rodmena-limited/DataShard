"""
Snapshotting and time travel functionality for the Python Iceberg implementation
"""

from copy import deepcopy
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from .data_structures import HistoryEntry, Snapshot, TableMetadata
from .logging_config import get_logger
from .metadata_manager import MetadataManager

logger = get_logger(__name__)

# Table property enabling opt-in snapshot retention. Iceberg's
# write.metadata.previous-versions-max governs METADATA FILES, not snapshots,
# so datashard uses its own explicit property and never prunes by default.
SNAPSHOT_RETENTION_PROPERTY = "datashard.snapshot.retention-count"


def repoint_parents_to_surviving_ancestors(
    all_snapshots: List[Snapshot], kept: List[Snapshot]
) -> None:
    """Rewrite kept snapshots' parent ids so none references a removed snapshot.

    When snapshots are expired or deleted, a survivor whose parent was removed
    would otherwise carry a dangling parent_snapshot_id - a reader walking the
    chain cannot tell that from corruption. Each survivor is repointed to its
    nearest SURVIVING ancestor (a true ancestor, so history is not invented),
    or to None when the whole ancestry was removed.

    Must be called with `all_snapshots` as the list BEFORE removal, since the
    ancestry of removed snapshots is what the walk follows.
    """
    parent_of = {s.snapshot_id: s.parent_snapshot_id for s in all_snapshots}
    kept_ids = {s.snapshot_id for s in kept}

    for snapshot in kept:
        parent = snapshot.parent_snapshot_id
        seen: set[int] = set()
        while parent is not None and parent != -1 and parent not in kept_ids:
            if parent in seen:  # cycle in corrupt metadata: stop, drop the link
                parent = None
                break
            seen.add(parent)
            parent = parent_of.get(parent)
        snapshot.parent_snapshot_id = parent


class SnapshotManager:
    """Manages snapshots and time travel functionality"""

    def __init__(self, metadata_manager: MetadataManager):
        self.metadata_manager = metadata_manager

    def create_snapshot(
        self,
        manifest_list_path: str,
        operation: str = "append",
        parent_snapshot_id: Optional[int] = None,
        summary: Optional[Dict[str, str]] = None,
        base_metadata: Optional[TableMetadata] = None,
        snapshot_id: Optional[int] = None,
        metadata_mutator: Optional[Callable[[TableMetadata], None]] = None,
        sequence_number: Optional[int] = None,
    ) -> Snapshot:
        """Create a new snapshot with proper OCC handling.

        Args:
            manifest_list_path: Path to the manifest list file
            operation: Type of operation (append, overwrite, etc.)
            parent_snapshot_id: ID of the parent snapshot
            summary: Optional summary metadata
            base_metadata: Optional base metadata for OCC. If provided, this will be
                used instead of refreshing. This is critical for proper retry handling
                in Transaction.commit() - passing the base ensures retries use fresh
                metadata read by the caller rather than an independent stale read.
            snapshot_id: Snapshot id to use. Callers that already stamped an id
                into manifests/manifest lists MUST pass it here so the committed
                Snapshot carries the same id (lineage integrity). If omitted, a
                new UUID-derived id is generated.
            metadata_mutator: Optional callback applied to the new metadata
                (after the snapshot is appended, before commit). Used e.g. by
                expire_snapshots to fold metadata changes into the same commit.
            sequence_number: Iceberg v2 sequence number for this snapshot.
                Callers that already stamped it into manifest entries MUST pass
                the same value here; otherwise it is derived from the base
                metadata's last_sequence_number.
        """
        import uuid

        # Use provided base_metadata or refresh if not provided
        if base_metadata is None:
            base_metadata = self.metadata_manager.refresh()
        if base_metadata is None:
            raise ValueError("Cannot create snapshot: no current metadata")

        # Use the caller's snapshot id when given (it is already embedded in the
        # manifest list filename and manifest entries); generate otherwise.
        # UUID-derived, masked to 63 bits for signed-long (Java/Avro) compatibility.
        if snapshot_id is None:
            snapshot_id = (uuid.uuid4().int & ((1 << 63) - 1))

        # Iceberg v2 sequence number: monotonic per commit. The caller stamps
        # the same value into this commit's manifest entries, so metadata and
        # manifests always agree on when a file entered the table.
        if sequence_number is None:
            sequence_number = base_metadata.last_sequence_number + 1

        # Create new snapshot
        snapshot = Snapshot(
            snapshot_id=snapshot_id,
            timestamp_ms=int(datetime.now().timestamp() * 1000),
            manifest_list=manifest_list_path,
            parent_snapshot_id=parent_snapshot_id,
            operation=operation,
            summary=summary or {},
            # Snapshots record the schema they were written against, so a
            # time-travel read knows how to interpret its files.
            schema_id=base_metadata.current_schema_id,
            sequence_number=sequence_number,
        )

        # Create new metadata based on base, but with modifications
        new_metadata = deepcopy(base_metadata)
        new_metadata.snapshots.append(snapshot)
        new_metadata.current_snapshot_id = snapshot_id
        new_metadata.last_sequence_number = max(
            base_metadata.last_sequence_number, sequence_number
        )

        # Add to snapshot log
        history_entry = HistoryEntry(
            timestamp_ms=snapshot.timestamp_ms, snapshot_id=snapshot.snapshot_id
        )
        new_metadata.snapshot_log.append(history_entry)

        # Apply caller-supplied metadata changes (e.g. snapshot expiry) so they
        # land in the same atomic commit.
        if metadata_mutator is not None:
            metadata_mutator(new_metadata)
            # The mutator must never remove the snapshot being committed.
            if all(s.snapshot_id != snapshot_id for s in new_metadata.snapshots):
                raise ValueError("metadata_mutator removed the snapshot being committed")

        # OPT-IN snapshot retention. Never prunes unless the table property is
        # explicitly set, and never prunes the current snapshot.
        self._apply_retention(new_metadata)

        # Commit the updated metadata using OCC (base and new metadata)
        # This will fail if the base doesn't match the current state
        self.metadata_manager.commit(base_metadata, new_metadata)

        # Return the snapshot that was created
        return snapshot

    def _apply_retention(self, metadata: TableMetadata) -> None:
        """Apply opt-in snapshot retention (SNAPSHOT_RETENTION_PROPERTY)."""
        raw = metadata.properties.get(SNAPSHOT_RETENTION_PROPERTY)
        if raw is None:
            return
        try:
            retention_count = int(raw)
        except (TypeError, ValueError):
            logger.warning(
                f"Ignoring invalid {SNAPSHOT_RETENTION_PROPERTY}={raw!r} (not an integer)"
            )
            return
        if retention_count < 1 or len(metadata.snapshots) <= retention_count:
            return

        current_id = metadata.current_snapshot_id
        sorted_snapshots = sorted(metadata.snapshots, key=lambda s: s.timestamp_ms)
        kept = sorted_snapshots[-retention_count:]
        kept_ids = {s.snapshot_id for s in kept}
        # Never drop the current snapshot, whatever its age.
        if current_id is not None and current_id not in kept_ids:
            current = next(
                (s for s in metadata.snapshots if s.snapshot_id == current_id), None
            )
            if current is not None:
                kept.append(current)
                kept_ids.add(current_id)

        surviving = [s for s in sorted_snapshots if s.snapshot_id in kept_ids]
        repoint_parents_to_surviving_ancestors(metadata.snapshots, surviving)
        metadata.snapshots = surviving
        metadata.snapshot_log = [
            e for e in metadata.snapshot_log if e.snapshot_id in kept_ids
        ]

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
                "snapshot_id": snapshot.snapshot_id,
                "timestamp_ms": snapshot.timestamp_ms,
                "timestamp": datetime.fromtimestamp(snapshot.timestamp_ms / 1000.0),
                "operation": snapshot.operation,
                "summary": snapshot.summary,
                "parent_id": snapshot.parent_snapshot_id,
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
        """Delete a specific snapshot (metadata-level removal).

        When the CURRENT snapshot is deleted, the table is repointed to the
        most recent remaining snapshot by commit recency (snapshot_log order,
        falling back to timestamp) - snapshot ids are random UUIDs, so max(id)
        would pick an arbitrary snapshot.
        """
        base_metadata = self.metadata_manager.refresh()
        if base_metadata is None:
            return False

        # Find and remove the snapshot
        snapshot_to_remove = None
        for i, snapshot in enumerate(base_metadata.snapshots):
            if snapshot.snapshot_id == snapshot_id:
                snapshot_to_remove = i
                break

        if snapshot_to_remove is not None:
            # Create a NEW metadata object for the updated state
            # CRITICAL: Must use deepcopy to create separate object for OCC
            new_metadata = deepcopy(base_metadata)
            before_removal = list(new_metadata.snapshots)
            del new_metadata.snapshots[snapshot_to_remove]
            # No survivor may keep a parent link to the removed snapshot.
            repoint_parents_to_surviving_ancestors(before_removal, new_metadata.snapshots)

            # Drop the deleted snapshot's history entries (no dangling log rows)
            new_metadata.snapshot_log = [
                e for e in new_metadata.snapshot_log if e.snapshot_id != snapshot_id
            ]

            # Update current snapshot if needed
            if new_metadata.current_snapshot_id == snapshot_id:
                new_metadata.current_snapshot_id = self._most_recent_snapshot_id(
                    new_metadata
                )

            # Commit the changes with proper OCC (base vs new)
            self.metadata_manager.commit(base_metadata, new_metadata)
            return True

        return False

    @staticmethod
    def _most_recent_snapshot_id(metadata: TableMetadata) -> Optional[int]:
        """Most recently committed remaining snapshot: latest snapshot_log entry
        that still exists, falling back to max timestamp_ms."""
        remaining_ids = {s.snapshot_id for s in metadata.snapshots}
        if not remaining_ids:
            return None

        for entry in reversed(metadata.snapshot_log):
            if entry.snapshot_id in remaining_ids:
                return entry.snapshot_id

        # No usable log - fall back to newest by timestamp (stable enough)
        newest = max(metadata.snapshots, key=lambda s: s.timestamp_ms)
        return newest.snapshot_id
