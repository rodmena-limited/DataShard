"""
Table: the main user-facing handle (transactions, snapshots, maintenance, GC).
Split out of transaction.py for the 500-line file cap (#71); the read path lives
in table_scan.py.
"""

import time
from typing import Any, Dict, List, Optional

from .data_structures import DataFile, Schema, Snapshot, TableMetadata
from .duckdb_bridge import _DuckDBMixin
from .file_manager import FileManager
from .logging_config import get_logger
from .metadata_manager import MetadataManager
from .snapshot_manager import SnapshotManager
from .table_scan import _ScanMixin
from .transaction import Transaction, TransactionManager

logger = get_logger(__name__)

# Default snapshot age for Table.expire_snapshots() when no policy is given.
DEFAULT_SNAPSHOT_MAX_AGE_MS = 5 * 24 * 3600 * 1000




class Table(_ScanMixin, _DuckDBMixin):
    """Main table interface with transaction support"""

    def __init__(
        self,
        table_path: str,
        create_if_not_exists: bool = True,
        schema: Optional[Schema] = None,
        partition_spec: Optional[Any] = None,
    ):
        from .storage_backend import create_storage_backend

        self.table_path = table_path
        # True when THIS constructor initialised the table (False = it existed).
        self.created = False

        # Create storage backend
        self.storage = create_storage_backend(table_path)

        # Create managers with storage backend
        self.metadata_manager = MetadataManager(table_path, self.storage)
        self.snapshot_manager = SnapshotManager(self.metadata_manager)
        self.file_manager = FileManager(table_path, self.metadata_manager, self.storage)
        self.transaction_manager = TransactionManager(
            self.metadata_manager, self.snapshot_manager, self.file_manager
        )

        # Initialize if needed. The check is "no readable metadata" (not a
        # directory-existence probe, which local backends satisfy trivially).
        if create_if_not_exists and self.metadata_manager.refresh() is None:
            self._initialize_table(schema, partition_spec)

    def _initialize_table(
        self, schema: Optional[Schema] = None, partition_spec: Optional[Any] = None
    ) -> None:
        """Initialize a new table, persisting the provided schema/partition spec."""
        from .metadata_manager import TableExistsError

        if schema is not None:
            initial_metadata = TableMetadata(
                location=self.table_path,
                schemas=[schema],
                current_schema_id=schema.schema_id,
                partition_specs=[partition_spec] if partition_spec is not None else [],
            )
        else:
            initial_metadata = TableMetadata(
                location=self.table_path,
                partition_specs=[partition_spec] if partition_spec is not None else [],
            )

        try:
            self.metadata_manager.initialize_table(initial_metadata)
            self.created = True
        except TableExistsError:
            # A concurrent creator won the race - their metadata is authoritative.
            logger.info(f"Table {self.table_path} was concurrently initialized; using existing metadata")

    def new_transaction(self) -> Transaction:
        """Create a new transaction"""
        return self.transaction_manager.begin_transaction()

    def current_snapshot(self) -> Optional[Snapshot]:
        """Get the current snapshot"""
        return self.snapshot_manager.get_current_snapshot()

    def snapshot_by_id(self, snapshot_id: int) -> Optional[Snapshot]:
        """Get a specific snapshot by ID"""
        return self.snapshot_manager.get_snapshot_by_id(snapshot_id)

    def snapshots(self) -> List[Dict[str, Any]]:
        """Get all snapshots"""
        return self.snapshot_manager.list_snapshots()

    def time_travel(
        self, snapshot_id: Optional[int] = None, timestamp: Optional[int] = None
    ) -> Any:
        """Look up a historical Snapshot by id or timestamp.

        Returns snapshot METADATA (id, timestamp, manifest list reference); it
        does not switch the table's state. To READ the data as of that snapshot,
        pass its id to scan(), to_pandas(), scan_batches(), iter_*() or
        row_count() as snapshot_id=... (#72).
        """
        if snapshot_id is not None:
            return self.snapshot_manager.time_travel_to(snapshot_id)
        elif timestamp is not None:
            return self.snapshot_manager.time_travel_to_timestamp(timestamp)
        else:
            return self.current_snapshot()

    def append_data(self, files: List[DataFile]) -> bool:
        """Append data files to the table (convenience method)"""
        with self.new_transaction() as tx:
            tx.append_files(files)
            result = tx.commit()
            return bool(result)

    def append_pandas(
        self,
        df: Any,
        schema: Optional["Schema"] = None,
    ) -> bool:
        """Append pandas DataFrame to table (convenience method)"""
        with self.new_transaction() as tx:
            tx.append_pandas(df, schema)
            result = tx.commit()
            return bool(result)

    def append_arrow(self, table: Any, schema: Optional["Schema"] = None) -> bool:
        """Append a pyarrow.Table in one transaction (convenience method, #79)."""
        with self.new_transaction() as tx:
            tx.append_arrow(table, schema)
            result = tx.commit()
            return bool(result)

    def append_records(
        self,
        records: List[Dict[str, Any]],
        schema: Optional["Schema"] = None,
        partition_values: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Append actual data records to the table by creating new data files (convenience method)"""
        with self.new_transaction() as tx:
            tx.append_data(records=records, schema=schema, partition_values=partition_values)
            result = tx.commit()
            return bool(result)

    def refresh(self) -> bool:
        """Refresh the table metadata from storage"""
        metadata = self.metadata_manager.refresh()
        return metadata is not None

    # ------------------------------------------------------------------
    # Maintenance (#68)
    # ------------------------------------------------------------------

    def properties(self) -> Dict[str, str]:
        """The table's properties (retention / compaction policy knobs)."""
        metadata = self.metadata_manager.refresh()
        return dict(metadata.properties) if metadata else {}

    def set_properties(self, properties: Dict[str, Optional[str]]) -> bool:
        """Set (or with None, remove) table properties in one metadata commit."""
        with self.new_transaction() as tx:
            tx.set_properties(properties)
            return bool(tx.commit())

    def expire_snapshots(
        self, older_than_ms: Optional[int] = None, retain_last: Optional[int] = None
    ) -> int:
        """Expire snapshots and return how many were removed.

        With no arguments, snapshots older than DEFAULT_SNAPSHOT_MAX_AGE_MS (5 days)
        are expired. The current snapshot and the `retain_last` most recent ones are
        always kept. Files owned only by expired snapshots are reclaimed by the next
        garbage_collect() once they exceed its grace period.
        """
        if older_than_ms is None and retain_last is None:
            older_than_ms = int(time.time() * 1000) - DEFAULT_SNAPSHOT_MAX_AGE_MS
        before = len(self.snapshots())
        with self.new_transaction() as tx:
            tx.expire_snapshots(older_than_ms=older_than_ms, retain_last=retain_last)
            tx.commit()
        return before - len(self.snapshots())

    def compact_manifests(self) -> bool:
        """Rewrite the active manifests into one. Returns True when a compaction
        snapshot was committed, False when there was nothing to compact. Commits
        also compact automatically once the manifest count reaches the
        datashard.manifest.compaction-threshold property (default 64)."""
        with self.new_transaction() as tx:
            tx.compact_manifests()
            tx.commit()
        return tx.did_commit_snapshot

    def repair_version_hint(self, metadata_file: str) -> None:
        """Operator action after AmbiguousMetadataError: declare which metadata file
        is the committed one (see MetadataManager.repair_version_hint)."""
        self.metadata_manager.repair_version_hint(metadata_file)

    def garbage_collect(
        self, grace_period_ms: int = 3600000, allow_short_grace: bool = False
    ) -> Dict[str, int]:
        """Delete orphaned files not referenced by any snapshot.

        Fail closed: if any reachable manifest cannot be read or verified, GC
        aborts (GarbageCollectionAborted) without deleting anything. Files
        belonging to in-flight transactions are protected via markers regardless
        of age, and nothing written after GC started is ever deleted.

        Args:
            grace_period_ms: Only delete orphaned files older than this age,
                measured from the start of the call (default 1 hour). Must exceed
                the longest transaction plus the longest GC run on this table.
            allow_short_grace: Accept a grace period below 5 minutes. Only safe
                when no other writer can be active.

        Returns:
            Dict with counts of deleted files by type.
        """
        from .garbage_collector import GarbageCollector
        gc = GarbageCollector(self.table_path, self.metadata_manager, self.file_manager)
        return gc.collect(grace_period_ms, allow_short_grace=allow_short_grace)

    def row_count(self, snapshot_id: Optional[int] = None) -> int:
        """Get total row count from manifest metadata without scanning data.

        This is a fast O(manifest_files) operation that reads only metadata,
        not the actual parquet data files. Use this for count-only queries
        instead of len(table.scan()).

        Args:
            snapshot_id: Count as of a historical snapshot (default: current).

        Returns:
            Total number of rows across all data files in the snapshot.
        """
        data_files = self._get_all_data_files(snapshot_id=snapshot_id)
        return sum(df.record_count for df in data_files)

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def _get_all_data_files(
        self, metadata: Optional[TableMetadata] = None, snapshot_id: Optional[int] = None
    ) -> List[DataFile]:
        """Get ALL data files referenced by a snapshot (current by default).

        Fail closed: a snapshot that references a missing or unreadable
        manifest (list) raises instead of returning partial/empty results -
        readers must be able to distinguish "empty table" from "broken table".
        Pass `metadata` to reuse a view already read (one read per scan, #67).
        """
        if metadata is None:
            metadata = self.metadata_manager.refresh()
        if metadata is None:
            return []
        if snapshot_id is not None:
            snapshot = next((s for s in metadata.snapshots if s.snapshot_id == snapshot_id), None)
            if snapshot is None:
                raise ValueError(
                    f"Snapshot {snapshot_id} does not exist in this table (expired, or never committed)"
                )
        else:
            current_id = metadata.current_snapshot_id
            snapshot = next((s for s in metadata.snapshots if s.snapshot_id == current_id), None)
            if snapshot is None:
                # An unset current_snapshot_id means "empty table". A SET id that
                # resolves to nothing means the metadata is inconsistent - returning
                # [] there would report a broken table as an empty one (#48).
                if current_id is not None and current_id != -1:
                    raise RuntimeError(
                        f"Table metadata is inconsistent: current_snapshot_id {current_id} "
                        f"does not match any snapshot in metadata.snapshots - refusing to "
                        f"report a broken table as an empty one"
                    )
                return []

        manifest_list_path = snapshot.manifest_list
        if manifest_list_path.startswith("/"):
            manifest_list_path = manifest_list_path.lstrip("/")

        # Reads are integrity-checked against the length/sha256 recorded at commit
        # (#58); a missing object raises FileNotFoundError from the read itself, so
        # no separate exists() round trip is needed (#67).
        exp_len, exp_sum = FileManager.snapshot_list_integrity(snapshot)
        try:
            manifest_files = self.file_manager.read_manifest_list_file(
                manifest_list_path, expected_length=exp_len, expected_checksum=exp_sum
            )
        except FileNotFoundError as e:
            raise RuntimeError(
                f"Current snapshot {snapshot.snapshot_id} references missing manifest "
                f"list '{snapshot.manifest_list}' - table metadata is inconsistent"
            ) from e

        all_data_files = []
        seen_paths = set()

        for manifest_ref in manifest_files:
            manifest_path = manifest_ref.manifest_path
            if not manifest_path:
                continue
            if manifest_path.startswith("/"):
                manifest_path = manifest_path.lstrip("/")

            try:
                manifest_data_files = self.file_manager.read_manifest_file(
                    manifest_path,
                    expected_length=manifest_ref.manifest_length,
                    expected_checksum=manifest_ref.checksum,
                )
            except FileNotFoundError as e:
                raise RuntimeError(
                    f"Manifest list references missing manifest '{manifest_ref.manifest_path}' "
                    f"- table metadata is inconsistent"
                ) from e

            for data_file in manifest_data_files:
                # Normalize before de-duplicating: the same file can appear as
                # '/data/x.parquet' in one manifest and 'data/x.parquet' in
                # another, and reading it twice would double every row.
                file_path = data_file.file_path.lstrip("/")
                if file_path in seen_paths:
                    continue
                seen_paths.add(file_path)
                all_data_files.append(data_file)

        return all_data_files

    def _get_data_files_from_manifest(self) -> List[DataFile]:
        """Get data files from the current snapshot's manifests.

        Retained for callers/tests that want the raw DataFile objects (e.g. to
        inspect column bounds). Same fail-closed semantics as _get_all_data_files
        but without cross-manifest path de-duplication.
        """
        return self._get_all_data_files()

    def _get_current_schema(self, metadata: Optional[TableMetadata] = None) -> Optional[Schema]:
        """Get the current schema from metadata (pass a view already read to avoid a re-read).

        Returns:
            Current Schema object, or None if the table has no schema.
        """
        if metadata is None:
            metadata = self.metadata_manager.refresh()
        if metadata and metadata.schemas:
            # Find current schema by ID
            for schema in metadata.schemas:
                if schema.schema_id == metadata.current_schema_id:
                    return schema
            # Fallback to first schema
            return metadata.schemas[0]
        return None

    def _resolve_file_path(self, file_path: str) -> str:
        """Resolve a manifest file path to an absolute path inside the table root.

        Delegates to the storage backend's boundary-checked resolver: every
        path in a manifest is table-relative, and an absolute one must never be
        opened as-is (#47).

        Args:
            file_path: File path (possibly Iceberg-style starting with '/')

        Returns:
            Absolute file path within the table root

        Raises:
            ValueError: If the path resolves outside the table root.
        """
        return str(self.file_manager.data_file_manager._get_arrow_path(file_path))
