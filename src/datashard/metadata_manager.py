"""
Metadata management for the Python Iceberg implementation
"""

import threading
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from .data_structures import HistoryEntry, Snapshot, TableMetadata
from .exceptions import (
    AmbiguousCommitError,
    AmbiguousMetadataError,
    ConcurrentModificationException,
    SchemaMismatchError,
    TableExistsError,
)
from .logging_config import get_logger
from .metadata_serde import dict_to_metadata, metadata_to_dict
from .version_hint import _METADATA_FILE_RE, _VersionHintMixin

if TYPE_CHECKING:
    from .storage_backend import StorageBackend

logger = get_logger(__name__)

__all__ = [
    "MetadataManager",
    "AmbiguousCommitError",
    "AmbiguousMetadataError",
    "ConcurrentModificationException",
    "SchemaMismatchError",
    "TableExistsError",
    "_METADATA_FILE_RE",
    "_VersionHintMixin",
]


class MetadataManager(_VersionHintMixin):
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
            try:
                return self._read_metadata_file(f"{self.metadata_path}/{metadata_file}")
            except FileNotFoundError:
                # The hint points at a file that is gone (stale or corrupt hint):
                # recover by scanning, as for a missing hint (#22).
                recovered = self._recover_version_from_files()
                if recovered is None or recovered[1] == metadata_file:
                    raise
                logger.warning(
                    f"Version hint for {self.table_path} points at missing {metadata_file}; "
                    f"using recovered {recovered[1]}"
                )
                return self._read_metadata_file(f"{self.metadata_path}/{recovered[1]}")

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
                # PHASE 1: Validation (inside lock to prevent races). One hint read
                # (carrying the ETag on CAS backends) and one metadata read - the
                # earlier refresh() + separate ETag read cost 5 round trips (#67).
                current, hint_etag, filesystem_version, previous_metadata_file = (
                    self._read_current_for_commit()
                )

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

                # Commit identity: two metadata-only commits in the same millisecond
                # share last_updated_ms and the snapshot id; the commit id cannot
                # collide (#74). Empty on versions written before 0.8.0.
                if (
                    current
                    and current.last_commit_id
                    and current.last_commit_id != base_metadata.last_commit_id
                ):
                    raise ConcurrentModificationException(
                        f"Cannot commit metadata: concurrent modification detected. "
                        f"Expected commit {base_metadata.last_commit_id or '<none>'}, "
                        f"but found: {current.last_commit_id}"
                    )

                # PHASE 2: Prepare new version
                new_metadata.last_updated_ms = int(datetime.now().timestamp() * 1000)
                new_metadata.last_commit_id = uuid.uuid4().hex
                next_version = (filesystem_version or 0) + 1

                # Record the version this commit supersedes, so the metadata
                # file chain is auditable (Iceberg's metadata-log).
                if previous_metadata_file is not None:
                    self._append_metadata_log(
                        new_metadata, base_metadata, previous_metadata_file
                    )

                # PHASE 3: Write new metadata file (but don't make it visible yet).
                # The filename embeds a random suffix: two racing committers can
                # never write the same object, so the winner's hint always
                # references the winner's content.
                metadata_file = self._new_metadata_filename(next_version)
                metadata_path = f"{self.metadata_path}/{metadata_file}"
                self._write_metadata_file(metadata_path, new_metadata)

                try:
                    # PHASE 3.5: Fencing - re-validate lock ownership immediately
                    # before the commit point. A holder whose lease was broken
                    # (e.g. after a long pause) must not flip the hint. On CAS
                    # backends the conditional hint write below IS the fence (a
                    # stolen lock plus a foreign commit changes the hint's ETag),
                    # so the extra round trip is skipped there (#67).
                    if not self.storage.supports_cas and not self.lock_provider.is_held():
                        raise ConcurrentModificationException(
                            "Lost distributed lock before commit point; retrying"
                        )

                    # PHASE 4: Atomically make new version visible - the commit
                    # point. If we crash before this, the new metadata file is
                    # orphaned but the table is consistent.
                    self._write_hint_at_commit_point(metadata_file, hint_etag)
                except ConcurrentModificationException:
                    # Clean loss: our hint write definitely did not land, so the
                    # metadata file we wrote was never committed. Remove it - if it
                    # outlived us, hint recovery could not tell it from the real
                    # v{N} and might resurrect it (#60).
                    self._discard_uncommitted_metadata(metadata_path)
                    raise
                except AmbiguousCommitError:
                    raise  # the hint MAY point at this file: it must stay
                except Exception:
                    if self.storage.atomic_write_failures:
                        self._discard_uncommitted_metadata(metadata_path)
                    raise

                # Success - update in-memory version
                self.current_version = next_version

                return new_metadata
            finally:
                # PHASE 2: Always release lock - and never let a release failure
                # mask/poison the commit outcome (a durable commit must not be
                # reported as failed because unlock hiccuped).
                self._release_lock_safely()

    # Cap on retained metadata-log entries (Iceberg's property name). Superseded
    # metadata files outside the log are reclaimed by garbage_collect() (#68), so
    # this is also how many previous versions stay on disk; 10 keeps the chain
    # auditable without the quadratic growth 100 caused on busy tables.
    PREVIOUS_VERSIONS_MAX_PROPERTY = "write.metadata.previous-versions-max"
    DEFAULT_PREVIOUS_VERSIONS_MAX = 10

    def _append_metadata_log(
        self,
        new_metadata: TableMetadata,
        base_metadata: TableMetadata,
        previous_metadata_file: str,
    ) -> None:
        """Append the superseded metadata file to the metadata log.

        The log is what makes the metadata chain auditable: every committed
        version names the version it replaced, with the timestamp that version
        carried. Trimmed to write.metadata.previous-versions-max (oldest first)
        so it cannot grow without bound.
        """
        entry_path = f"{self.metadata_path}/{previous_metadata_file}"
        log = list(new_metadata.metadata_log)
        if log and log[-1].get("metadata-file") == entry_path:
            return  # already recorded (e.g. a retried commit)

        log.append({
            "timestamp-ms": base_metadata.last_updated_ms,
            "metadata-file": entry_path,
        })

        raw_max = new_metadata.properties.get(self.PREVIOUS_VERSIONS_MAX_PROPERTY)
        try:
            max_entries = int(raw_max) if raw_max is not None else self.DEFAULT_PREVIOUS_VERSIONS_MAX
        except (TypeError, ValueError):
            logger.warning(
                f"Ignoring invalid {self.PREVIOUS_VERSIONS_MAX_PROPERTY}={raw_max!r} "
                f"(not an integer)"
            )
            max_entries = self.DEFAULT_PREVIOUS_VERSIONS_MAX
        if max_entries >= 1 and len(log) > max_entries:
            log = log[-max_entries:]

        new_metadata.metadata_log = log

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

    def _read_current_for_commit(
        self,
    ) -> Tuple[Optional[TableMetadata], Optional[str], Optional[int], Optional[str]]:
        """(current metadata, hint ETag, current version, current metadata filename).

        The hint is read once - with its ETag on CAS backends, so the commit point
        can be a conditional PUT - and the metadata file it names once. A missing
        or unparseable hint falls back to scanning (hint = pointer, files = truth).
        """
        hint_etag: Optional[str] = None
        info: Optional[Tuple[int, str]] = None
        try:
            if self.storage.supports_cas:
                hint_bytes, hint_etag = self.storage.read_file_with_etag(self.HINT_PATH)
            else:
                hint_bytes = self.storage.read_file(self.HINT_PATH)
            info = self._parse_hint_content(hint_bytes)
        except FileNotFoundError:
            hint_etag = None
        if info is None:
            info = self._recover_version_from_files()
        if info is None:
            return None, hint_etag, None, None
        version, metadata_file = info
        try:
            current = self._read_metadata_file(f"{self.metadata_path}/{metadata_file}")
        except FileNotFoundError:
            recovered = self._recover_version_from_files()
            if recovered is None or recovered[1] == metadata_file:
                raise
            version, metadata_file = recovered
            current = self._read_metadata_file(f"{self.metadata_path}/{metadata_file}")
        return current, hint_etag, version, metadata_file

    def _discard_uncommitted_metadata(self, metadata_path: str) -> None:
        """Best-effort removal of a metadata file whose commit is known to have failed."""
        try:
            self.storage.delete_file(metadata_path)
        except Exception as e:
            logger.warning(f"Could not remove uncommitted metadata file {metadata_path}: {e}")

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
        return metadata_to_dict(metadata)

    def _dict_to_metadata(self, metadata_dict: Dict[str, Any]) -> TableMetadata:
        """Convert dictionary back to TableMetadata"""
        return dict_to_metadata(metadata_dict)
