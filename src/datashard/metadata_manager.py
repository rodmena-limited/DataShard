"""
Table metadata persistence and the commit protocol (#86).

Layout (Iceberg v2, readable by DuckDB / pyiceberg / Spark / Trino):

    <table>/metadata/v{N}.metadata.json   the commit point: created exclusively
    <table>/metadata/version-hint.text    "N" - a pointer readers start from
    <table>/metadata/snap-*.avro          manifest lists
    <table>/metadata/*-m0.avro            manifests
    <table>/data/                         parquet files

A commit is the exclusive creation of v{N+1}.metadata.json: S3 If-None-Match:*,
local temp+fsync+os.link. Two committers racing for N+1 cannot both succeed, so
the version number is the commit identity; the lock is only a contention reducer.
The hint is written afterwards with only-if-greater semantics and healed by readers
that find a newer version (version_hint.py).
"""

import threading
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from .data_structures import HistoryEntry, Snapshot, TableMetadata
from .exceptions import (
    AmbiguousCommitError,
    AmbiguousMetadataError,
    ConcurrentModificationException,
    LegacyLayoutError,
    SchemaMismatchError,
    TableExistsError,
)
from .logging_config import get_logger
from .metadata_serde import dict_to_metadata, is_legacy_document, metadata_to_dict
from .table_paths import location_uri
from .version_hint import _METADATA_FILE_RE, _VersionHintMixin, metadata_file_name

if TYPE_CHECKING:
    from .storage_backend import StorageBackend

logger = get_logger(__name__)

__all__ = [
    "MetadataManager",
    "AmbiguousCommitError",
    "AmbiguousMetadataError",
    "ConcurrentModificationException",
    "LegacyLayoutError",
    "SchemaMismatchError",
    "TableExistsError",
    "_METADATA_FILE_RE",
    "_VersionHintMixin",
]


class MetadataManager(_VersionHintMixin):
    """Manages table metadata persistence and updates"""

    HINT_PATH = "metadata/version-hint.text"

    # Cap on retained metadata-log entries (Iceberg's property name). Superseded
    # metadata files outside the log are reclaimed by garbage_collect() (#68).
    PREVIOUS_VERSIONS_MAX_PROPERTY = "write.metadata.previous-versions-max"
    DEFAULT_PREVIOUS_VERSIONS_MAX = 10

    # Every commit rewrites the WHOLE metadata document, and that document lists every
    # snapshot - so a table's total metadata bytes grow with the SQUARE of its commit
    # count. One production lake reached 3.47 GB of metadata carrying 9.5 MB of data
    # (365:1) at 3,027 single-row commits, with nothing in the library saying so (#94).
    # The warning fires on the document actually written, which is the cost as it is
    # paid, and then only once per doubling so a busy table is not spammed.
    METADATA_WARN_BYTES_PROPERTY = "datashard.metadata.warn-bytes"
    DEFAULT_METADATA_WARN_BYTES = 1 << 20  # 1 MiB

    def __init__(self, table_path: str, storage: "StorageBackend"):
        self.table_path = table_path
        self.storage = storage
        self.metadata_path = "metadata"  # Relative to table_path
        self.current_version = 0
        self._lock = threading.RLock()
        # URI of the root this table is opened at (what foreign readers see, #87)
        self.location_uri = location_uri(storage, table_path)
        # Location recorded in the last metadata read: differs from location_uri
        # only for a moved table (paths under either resolve).
        self.recorded_location: Optional[str] = None
        self._warned_moved = False
        # Size of the metadata document at the last size warning (0 = never warned)
        self._warned_metadata_bytes = 0
        # Distributed lock (flock locally, CAS lease on S3): contention reducer
        # only - correctness rests on the exclusive create of v{N}.metadata.json.
        self.lock_provider = self.storage.create_lock(".locks/metadata.lock", timeout=30.0)
        # Directories are created by initialize_table(), not here (#72).

    # ------------------------------------------------------------ lifecycle

    def initialize_table(self, metadata: TableMetadata) -> TableMetadata:
        """Create v1.metadata.json exclusively; a racing creator loses loudly.

        Raises:
            TableExistsError: the table already has metadata.
            LegacyLayoutError: a pre-0.10 table lives here (migrate it instead).
        """
        from .storage_backend import CASConflictError

        with self._lock:
            self.lock_provider.acquire()
            try:
                if self._current_version_info() is not None:  # raises LegacyLayoutError itself
                    raise TableExistsError(
                        f"Table at {self.table_path} is already initialized; refusing to overwrite its metadata"
                    )
                for directory in (self.metadata_path, "data"):
                    self.storage.makedirs(directory, exist_ok=True)

                metadata.location = self.location_uri
                if metadata.current_snapshot_id is None:
                    metadata.current_snapshot_id = -1
                metadata.last_updated_ms = int(datetime.now().timestamp() * 1000)
                try:
                    self._write_metadata_file_exclusive(1, metadata)
                except CASConflictError as e:
                    raise TableExistsError(f"Table at {self.table_path} was concurrently initialized") from e
                self._advance_hint(1)
                self.current_version = 1
                self.recorded_location = metadata.location
                return metadata
            finally:
                self._release_lock_safely()

    def refresh(self, probe: bool = True) -> Optional[TableMetadata]:
        """The current metadata, or None when no table exists here.

        Readers probe past the hint so a lagging hint never hides a committed
        version; probe=False (the write path) skips that round trip because a
        commit conflict reveals the same thing (see version_hint).
        """
        with self._lock:
            info = self._current_version_info(probe=probe)
            if info is None:
                return None
            version, metadata_file = info
            metadata = self._read_metadata_file(f"{self.metadata_path}/{metadata_file}")
            self.current_version = version
            return metadata

    def commit(
        self, base_metadata: TableMetadata, new_metadata: TableMetadata, relocate: bool = False
    ) -> TableMetadata:
        """Commit `new_metadata` on top of `base_metadata` (optimistic concurrency).

        1. Lock (contention reducer), re-read the current version and validate the
           base against it - a stale base is a clean ConcurrentModificationException.
        2. Write v{N+1}.metadata.json EXCLUSIVELY - the commit point. A conflict is a
           clean loss (someone else committed N+1; nothing of ours is visible).
        3. Advance the hint (best effort, only-if-greater).

        Raises:
            ConcurrentModificationException: clean conflict - safe to retry.
            AmbiguousCommitError: the exclusive write failed in a way that may still
                have landed (S3 error after the PUT). Callers must NOT delete data
                files; a retry could otherwise duplicate the commit.
        """
        from .storage_backend import CASConflictError

        with self._lock:
            self.lock_provider.acquire()
            try:
                current, version, hint_state = self._read_current_for_commit()
                if current is None or version is None:
                    raise RuntimeError(f"Cannot commit: table {self.table_path} has no metadata")
                if current.table_uuid != base_metadata.table_uuid:
                    raise ValueError("Table UUID mismatch - concurrent modification detected")
                if current.current_snapshot_id != base_metadata.current_snapshot_id:
                    raise ConcurrentModificationException(
                        f"Cannot commit metadata: concurrent modification detected. Expected "
                        f"current_snapshot_id: {base_metadata.current_snapshot_id}, but found: "
                        f"{current.current_snapshot_id}"
                    )
                if current.last_updated_ms != base_metadata.last_updated_ms:
                    raise ConcurrentModificationException(
                        f"Cannot commit metadata: concurrent modification detected. Expected "
                        f"last_updated_ms: {base_metadata.last_updated_ms}, but found: {current.last_updated_ms}"
                    )

                if not relocate:
                    new_metadata.location = current.location  # writers never mix prefixes (#87)
                new_metadata.last_updated_ms = int(datetime.now().timestamp() * 1000)
                if new_metadata.last_updated_ms <= base_metadata.last_updated_ms:
                    new_metadata.last_updated_ms = base_metadata.last_updated_ms + 1
                next_version = version + 1
                self._append_metadata_log(new_metadata, base_metadata, metadata_file_name(version))

                try:
                    self._write_metadata_file_exclusive(next_version, new_metadata)
                except CASConflictError as e:
                    # Either a concurrent committer took this version, or our hint
                    # lagged behind a durable commit. Both are resolved the same
                    # way: find the true latest version and move the hint there, so
                    # the retry starts from it instead of looping on a stale hint.
                    self.heal_hint(version)
                    raise ConcurrentModificationException(
                        f"Version {next_version} was committed by another writer; retrying"
                    ) from e
                except Exception as e:
                    if self.storage.atomic_write_failures:
                        raise  # guaranteed not visible: clean failure
                    raise AmbiguousCommitError(
                        f"Metadata write for version {next_version} failed ambiguously: {e}"
                    ) from e

                # ---- commit point passed: only best-effort work below ----
                self._advance_hint(next_version, known=hint_state)
                self.current_version = next_version
                return new_metadata
            finally:
                self._release_lock_safely()

    # ------------------------------------------------------------ helpers

    def _append_metadata_log(
        self, new_metadata: TableMetadata, base_metadata: TableMetadata, previous_metadata_file: str
    ) -> None:
        """Record the superseded metadata file (Iceberg's metadata-log), trimmed to
        write.metadata.previous-versions-max entries (oldest first)."""
        entry_path = f"{self.metadata_path}/{previous_metadata_file}"
        log = [e for e in new_metadata.metadata_log if isinstance(e, dict) and e.get("metadata-file")]
        if log and log[-1].get("metadata-file") == entry_path:
            new_metadata.metadata_log = log
            return
        log.append({"timestamp-ms": base_metadata.last_updated_ms, "metadata-file": entry_path})
        raw_max = new_metadata.properties.get(self.PREVIOUS_VERSIONS_MAX_PROPERTY)
        try:
            max_entries = int(raw_max) if raw_max is not None else self.DEFAULT_PREVIOUS_VERSIONS_MAX
        except (TypeError, ValueError):
            logger.warning(f"Ignoring invalid {self.PREVIOUS_VERSIONS_MAX_PROPERTY}={raw_max!r} (not an integer)")
            max_entries = self.DEFAULT_PREVIOUS_VERSIONS_MAX
        if max_entries >= 1 and len(log) > max_entries:
            log = log[-max_entries:]
        new_metadata.metadata_log = log

    def _read_current_for_commit(
        self,
    ) -> Tuple[Optional[TableMetadata], Optional[int], Optional[Tuple[Optional[int], Optional[str]]]]:
        """(current metadata, its version, the hint's (version, etag) as just read).

        The hint is read ONCE per commit - with its ETag on CAS backends, so the
        post-commit hint advance is a conditional PUT needing no second read - and
        without probing ahead: a lagging hint surfaces as a conflict at the commit
        point, which heals it (#86).
        """
        hint_state: Optional[Tuple[Optional[int], Optional[str]]] = None
        hinted: Optional[int] = None
        try:
            if self.storage.supports_cas:
                raw, etag = self.storage.read_file_with_etag(self.HINT_PATH)
                hinted = self._parse_hint_content(raw)
                hint_state = (hinted, etag)
            else:
                hinted = self._parse_hint_content(self.storage.read_file(self.HINT_PATH))
                hint_state = (hinted, None)
        except FileNotFoundError:
            hint_state = (None, None)
        if hinted is None:
            info = self._current_version_info(probe=False)
            if info is None:
                return None, None, None
            version, metadata_file = info
            hint_state = None  # the hint was rewritten while resolving; do not reuse it
        else:
            version, metadata_file = hinted, metadata_file_name(hinted)
        return self._read_metadata_file(f"{self.metadata_path}/{metadata_file}"), version, hint_state

    def _release_lock_safely(self) -> None:
        """Release the distributed lock without ever raising."""
        try:
            self.lock_provider.release()
        except Exception as e:
            logger.warning(f"Failed to release metadata lock (will self-heal by lease expiry): {e}")

    # ------------------------------------------------------------ snapshots

    def get_snapshot_by_id(self, snapshot_id: int) -> Optional[Snapshot]:
        metadata = self.refresh()
        if not metadata:
            return None
        return next((s for s in metadata.snapshots if s.snapshot_id == snapshot_id), None)

    def get_current_snapshot(self) -> Optional[Snapshot]:
        metadata = self.refresh()
        if not metadata or metadata.current_snapshot_id is None:
            return None
        return next((s for s in metadata.snapshots if s.snapshot_id == metadata.current_snapshot_id), None)

    def get_all_snapshots(self) -> List[Snapshot]:
        metadata = self.refresh()
        return metadata.snapshots if metadata else []

    def get_snapshot_history(self) -> List[HistoryEntry]:
        metadata = self.refresh()
        return metadata.snapshot_log if metadata else []

    # ------------------------------------------------------------ file I/O

    def _write_metadata_file_exclusive(self, version: int, metadata: TableMetadata) -> None:
        import json

        content = json.dumps(self._metadata_to_dict(metadata), indent=2).encode("utf-8")
        self._warn_if_metadata_is_expensive(content, metadata)
        self.storage.create_exclusive(f"{self.metadata_path}/{metadata_file_name(version)}", content)

    def _warn_if_metadata_is_expensive(self, content: bytes, metadata: TableMetadata) -> None:
        """Say out loud what a large metadata document costs, and how to stop paying it.

        Silent until the document passes datashard.metadata.warn-bytes (1 MiB by
        default; 0 disables), then again only when it has DOUBLED, so a table that
        commits every few seconds gets a handful of lines rather than a flood.
        """
        raw = metadata.properties.get(self.METADATA_WARN_BYTES_PROPERTY)
        try:
            threshold = int(raw) if raw is not None else self.DEFAULT_METADATA_WARN_BYTES
        except (TypeError, ValueError):
            threshold = self.DEFAULT_METADATA_WARN_BYTES
        size = len(content)
        if threshold <= 0 or size < threshold or size < self._warned_metadata_bytes * 2:
            return
        self._warned_metadata_bytes = size
        readable = (
            f"{size / (1 << 20):.1f} MiB" if size >= (1 << 20) else f"{size / 1024:.0f} KiB"
        )
        logger.warning(
            f"{self.table_path}: the table metadata document is now {readable} across "
            f"{len(metadata.snapshots)} snapshots, and EVERY commit writes a fresh copy of it - so the "
            f"metadata this table accumulates grows with the SQUARE of the number of commits. "
            f"Batch rows into one transaction (`with table.new_transaction() as tx: tx.append_records(...)`) "
            f"instead of one commit per row, and run "
            f"`table.expire_snapshots(retain_last=N)` then `table.garbage_collect()` to prune the chain "
            f"and reclaim the superseded files. Set the {self.METADATA_WARN_BYTES_PROPERTY!r} table "
            f"property to change or silence this threshold."
        )

    def _write_metadata_file(self, path: str, metadata: TableMetadata) -> None:
        """Unconditional write (tests / tooling); commits use the exclusive form."""
        self.storage.write_json(path, self._metadata_to_dict(metadata))

    def _read_metadata_file(self, path: str) -> TableMetadata:
        doc = self.storage.read_json(path)
        if is_legacy_document(doc):
            self._raise_legacy(f"snake_case document {path}")
        metadata = self._dict_to_metadata(doc)
        if metadata.location != self.location_uri:
            self.recorded_location = metadata.location
            if not self._warned_moved:
                self._warned_moved = True
                logger.warning(
                    f"Table {self.table_path} records location {metadata.location} but is opened at "
                    f"{self.location_uri}. datashard resolves both; foreign readers need "
                    f"allow_moved_paths (DuckDB) until 'datashard relocate' rewrites the metadata."
                )
        else:
            self.recorded_location = metadata.location
        return metadata

    def _metadata_to_dict(self, metadata: TableMetadata) -> Dict[str, Any]:
        return metadata_to_dict(metadata)

    def _dict_to_metadata(self, metadata_dict: Dict[str, Any]) -> TableMetadata:
        return dict_to_metadata(metadata_dict, actual_location=self.location_uri)

    def known_locations(self) -> List[Optional[str]]:
        """URIs paths in manifests may be relative to (recorded and actual root)."""
        return [self.recorded_location, self.location_uri]
