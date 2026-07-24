"""
Metadata management for the Python Iceberg implementation
"""

import re
import threading
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from .data_structures import HistoryEntry, Schema, Snapshot, TableMetadata
from .logging_config import get_logger

if TYPE_CHECKING:
    from .storage_backend import StorageBackend

logger = get_logger(__name__)

# Matches both legacy (v3.metadata.json) and current (v3-1a2b3c4d.metadata.json) names
_METADATA_FILE_RE = re.compile(r"^v(\d+)(?:-[0-9a-f]{8})?\.metadata\.json$")


class ConcurrentModificationException(Exception):
    """Exception thrown when concurrent modifications are detected"""

    pass


class TableExistsError(Exception):
    """Raised when initializing a table over an already-initialized table."""

    pass


class AmbiguousCommitError(Exception):
    """Raised when the commit-point write failed in a way that may still have
    become visible (e.g. an S3 PUT that errored client-side after possibly
    succeeding server-side).

    Callers MUST NOT delete files written for this transaction: the commit may
    be durable and referencing them. Orphan cleanup is the garbage collector's
    job once the true outcome is observable.
    """

    pass


class MetadataManager:
    """Manages table metadata persistence and updates"""

    HINT_PATH = "metadata.version-hint.text"

    def __init__(self, table_path: str, storage: "StorageBackend"):
        self.table_path = table_path
        self.storage = storage
        self.metadata_path = "metadata"  # Relative to table_path
        self.current_version = 0
        self._lock = threading.RLock()  # For thread safety

        # Distributed lock for multi-process/multi-host safety
        # PHASE 2: Added distributed locking (FileLock for local, S3 Lock for cloud)
        self.lock_provider = self.storage.create_lock(".locks/metadata.lock", timeout=30.0)

        # Ensure metadata directory exists
        self.storage.makedirs(self.metadata_path, exist_ok=True)

    def initialize_table(self, metadata: TableMetadata) -> TableMetadata:
        """Initialize a new table with the given metadata.

        Guarded: refuses to run against an already-initialized table (any
        readable version hint or existing v*.metadata.json), and takes the
        metadata lock so two concurrent creators cannot both initialize.

        Raises:
            TableExistsError: If the table already has metadata.
        """
        with self._lock:
            self.lock_provider.acquire()
            try:
                # Refuse to clobber an existing table. This covers both a valid
                # hint AND hint-less tables recovered by scanning metadata files,
                # so a lost/corrupt hint can never lead to destructive re-init.
                if self._current_version_info() is not None:
                    raise TableExistsError(
                        f"Table at {self.table_path} is already initialized; "
                        f"refusing to overwrite its metadata"
                    )

                # Set initial values
                if metadata.current_snapshot_id is None:
                    metadata.current_snapshot_id = -1  # No snapshot initially
                metadata.last_updated_ms = int(datetime.now().timestamp() * 1000)

                # Write the metadata file
                metadata_file = self._new_metadata_filename(0)
                metadata_path = f"{self.metadata_path}/{metadata_file}"
                self._write_metadata_file(metadata_path, metadata)

                # Create version hint file. Where the backend supports CAS,
                # create-if-absent so a racing initializer loses loudly.
                if self.storage.supports_cas:
                    from .storage_backend import CASConflictError

                    try:
                        self.storage.write_file_cas(
                            self.HINT_PATH, metadata_file.encode("utf-8"), etag=None
                        )
                    except CASConflictError as e:
                        raise TableExistsError(
                            f"Table at {self.table_path} was concurrently initialized"
                        ) from e
                else:
                    self.storage.write_file(self.HINT_PATH, metadata_file.encode("utf-8"))

                self.current_version = 0
                return metadata
            finally:
                self._release_lock_safely()

    def refresh(self) -> Optional[TableMetadata]:
        """Refresh metadata from the latest version.

        The version hint is treated as a HINT, not the source of truth: if it
        is missing, unreadable, or points at a missing file, the latest
        version is recovered by scanning v*.metadata.json files.
        """
        with self._lock:
            info = self._current_version_info()
            if info is None:
                return None

            _version, metadata_file = info
            metadata_path = f"{self.metadata_path}/{metadata_file}"
            return self._read_metadata_file(metadata_path)

    def commit(self, base_metadata: TableMetadata, new_metadata: TableMetadata) -> TableMetadata:
        """Commit new metadata with Optimistic Concurrency Control following Iceberg pattern.

        Protocol:
        1. Acquire thread lock + distributed lock.
        2. Validate base against current state (OCC check).
        3. Write the new metadata to a UNIQUE filename (version + random suffix)
           so concurrent committers can never overwrite each other's content.
        4. Fencing check: re-validate we still hold the distributed lock.
        5. Flip the version hint - the commit point. On CAS-capable backends
           this is a conditional PUT keyed to the hint's ETag, so even a fully
           broken lock cannot produce a silent lost update.

        Raises:
            ConcurrentModificationException: Clean conflict - safe to retry.
            AmbiguousCommitError: The commit-point write failed but may have
                succeeded server-side. Callers must NOT delete data files.
        """
        # Acquire thread lock for thread safety within same process
        with self._lock:
            # PHASE 2: Acquire distributed lock for multi-process safety
            self.lock_provider.acquire()

            try:
                # PHASE 1: Validation (inside lock to prevent races)
                current = self.refresh()

                # Check UUID consistency
                if current and current.table_uuid != base_metadata.table_uuid:
                    raise ValueError("Table UUID mismatch - concurrent modification detected")

                # The key OCC check: verify that the metadata hasn't changed since the caller read it
                if current and current.current_snapshot_id != base_metadata.current_snapshot_id:
                    raise ConcurrentModificationException(
                        f"Cannot commit metadata: concurrent modification detected. "
                        f"Expected current_snapshot_id: {base_metadata.current_snapshot_id}, "
                        f"but found: {current.current_snapshot_id}"
                    )

                if current and current.last_updated_ms != base_metadata.last_updated_ms:
                    raise ConcurrentModificationException(
                        f"Cannot commit metadata: concurrent modification detected. "
                        f"Expected last_updated_ms: {base_metadata.last_updated_ms}, "
                        f"but found: {current.last_updated_ms}"
                    )

                # PHASE 2: Prepare new version
                new_metadata.last_updated_ms = int(datetime.now().timestamp() * 1000)

                # Read current version (and, on CAS backends, the hint's ETag so
                # the commit point below can be a true compare-and-swap).
                hint_etag: Optional[str] = None
                filesystem_version: Optional[int] = None
                if self.storage.supports_cas:
                    try:
                        hint_bytes, hint_etag = self.storage.read_file_with_etag(self.HINT_PATH)
                        parsed = self._parse_hint_content(hint_bytes)
                        if parsed is not None:
                            filesystem_version = parsed[0]
                    except FileNotFoundError:
                        hint_etag = None
                if filesystem_version is None:
                    info = self._current_version_info()
                    filesystem_version = info[0] if info is not None else None
                if filesystem_version is None:
                    filesystem_version = 0
                next_version = filesystem_version + 1

                # PHASE 3: Write new metadata file (but don't make it visible yet).
                # The filename embeds a random suffix: two racing committers can
                # never write the same object, so the winner's hint always
                # references the winner's content.
                metadata_file = self._new_metadata_filename(next_version)
                metadata_path = f"{self.metadata_path}/{metadata_file}"
                self._write_metadata_file(metadata_path, new_metadata)

                # PHASE 3.5: Fencing - re-validate lock ownership immediately
                # before the commit point. A holder whose lease was broken (e.g.
                # after a long pause) must not flip the hint.
                if not self.lock_provider.is_held():
                    raise ConcurrentModificationException(
                        "Lost distributed lock before commit point; retrying"
                    )

                # PHASE 4: Atomically make new version visible.
                # This is the commit point - after this, the new metadata is visible.
                # If we crash before this, the new metadata file is orphaned but table is consistent.
                self._write_hint_at_commit_point(metadata_file, hint_etag)

                # Success - update in-memory version
                self.current_version = next_version

                return new_metadata
            finally:
                # PHASE 2: Always release lock - and never let a release failure
                # mask/poison the commit outcome (a durable commit must not be
                # reported as failed because unlock hiccuped).
                self._release_lock_safely()

    def _write_hint_at_commit_point(self, metadata_file: str, hint_etag: Optional[str]) -> None:
        """Flip the version hint (the commit point), classifying failures.

        - CAS backends: conditional PUT. Precondition failure = clean conflict
          (ConcurrentModificationException, retryable). Any other error is
          AMBIGUOUS (the PUT may have landed) -> AmbiguousCommitError.
        - Backends with atomic_write_failures (local temp+rename): an exception
          means the flip did not happen -> propagate as a clean failure.
        - Other backends: an exception is ambiguous -> AmbiguousCommitError.
        """
        from .storage_backend import CASConflictError

        content = metadata_file.encode("utf-8")

        if self.storage.supports_cas:
            try:
                self.storage.write_file_cas(self.HINT_PATH, content, hint_etag)
                return
            except CASConflictError as e:
                raise ConcurrentModificationException(
                    "Version hint changed under us (CAS conflict); retrying"
                ) from e
            except Exception as e:
                raise AmbiguousCommitError(
                    f"Version hint write failed ambiguously: {e}"
                ) from e

        try:
            self.storage.write_file(self.HINT_PATH, content)
        except Exception as e:
            if self.storage.atomic_write_failures:
                # Guaranteed not visible - clean failure, caller may roll back.
                raise
            raise AmbiguousCommitError(
                f"Version hint write failed ambiguously: {e}"
            ) from e

    def _release_lock_safely(self) -> None:
        """Release the distributed lock without ever raising."""
        try:
            self.lock_provider.release()
        except Exception as e:
            logger.warning(f"Failed to release metadata lock (will self-heal by lease expiry): {e}")

    @staticmethod
    def _new_metadata_filename(version: int) -> str:
        """Unique metadata filename: version + random suffix (Iceberg-style)."""
        return f"v{version}-{uuid.uuid4().hex[:8]}.metadata.json"

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
        self.storage.write_json(path, metadata_dict)

    def _read_metadata_file(self, path: str) -> TableMetadata:
        """Read metadata from a JSON file"""
        metadata_dict = self.storage.read_json(path)
        return self._dict_to_metadata(metadata_dict)

    def _metadata_to_dict(self, metadata: TableMetadata) -> Dict[str, Any]:
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
                }
                for snapshot in metadata.snapshots
            ],
            "snapshot_log": [
                {"timestamp_ms": entry.timestamp_ms, "snapshot_id": entry.snapshot_id}
                for entry in metadata.snapshot_log
            ],
            "metadata_log": metadata.metadata_log,
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
            metadata_log=metadata_dict["metadata_log"],
        )

    # ------------------------------------------------------------------
    # Version-hint handling (hint = pointer, metadata files = truth)
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_hint_content(content: bytes) -> Optional[Tuple[int, str]]:
        """Parse hint file content into (version, metadata_filename).

        Supports the legacy format (bare version number) and the current
        format (full metadata filename). Returns None if unparseable.
        """
        try:
            text = content.decode("utf-8").strip()
        except UnicodeDecodeError:
            return None
        if not text:
            return None
        if text.isdigit():
            # Legacy format: plain version number -> legacy filename
            return int(text), f"v{text}.metadata.json"
        m = _METADATA_FILE_RE.match(text)
        if m:
            return int(m.group(1)), text
        return None

    def _read_version_hint(self) -> Optional[Tuple[int, str]]:
        """Read (version, metadata_filename) from the hint file, or None."""
        if not self.storage.exists(self.HINT_PATH):
            return None
        content = self.storage.read_file(self.HINT_PATH)
        return self._parse_hint_content(content)

    def _recover_version_from_files(self) -> Optional[Tuple[int, str]]:
        """Recover the latest (version, filename) by scanning metadata files.

        Used when the hint is missing/corrupt/stale. Picks the highest version;
        among same-version files (possible after historical races) prefers the
        most recently modified.
        """
        try:
            all_files = self.storage.list_files(self.metadata_path)
        except Exception:
            return None

        best: Optional[Tuple[int, str]] = None
        best_mtime = -1.0
        for rel_path in all_files:
            basename = rel_path.replace("\\", "/").rsplit("/", 1)[-1]
            # Only consider files directly in metadata/ (not metadata/manifests/...)
            parent = rel_path.replace("\\", "/").rsplit("/", 1)[0] if "/" in rel_path.replace("\\", "/") else ""
            if parent not in ("", self.metadata_path):
                continue
            m = _METADATA_FILE_RE.match(basename)
            if not m:
                continue
            version = int(m.group(1))
            if best is None or version > best[0]:
                best = (version, basename)
                try:
                    best_mtime = self.storage.get_modified_time(f"{self.metadata_path}/{basename}")
                except Exception:
                    best_mtime = -1.0
            elif version == best[0]:
                try:
                    mtime = self.storage.get_modified_time(f"{self.metadata_path}/{basename}")
                except Exception:
                    mtime = -1.0
                if mtime > best_mtime:
                    best = (version, basename)
                    best_mtime = mtime

        if best is not None:
            logger.warning(
                f"Version hint missing or invalid for {self.table_path}; "
                f"recovered latest metadata {best[1]} by scanning"
            )
        return best

    def _current_version_info(self) -> Optional[Tuple[int, str]]:
        """Resolve the current (version, metadata_filename).

        Prefers a valid hint that points at an existing file; otherwise falls
        back to scanning metadata files (#22: the hint is only a hint).
        """
        hinted = self._read_version_hint()
        if hinted is not None:
            _version, filename = hinted
            if self.storage.exists(f"{self.metadata_path}/{filename}"):
                return hinted
        return self._recover_version_from_files()
