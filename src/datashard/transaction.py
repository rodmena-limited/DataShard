"""
ACID transaction implementation for the Python Iceberg implementation
"""

import copy
import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

from .data_structures import (
    DataFile,
    Schema,
    TableMetadata,
)
from .file_manager import FileManager
from .logging_config import get_logger
from .metadata_manager import (
    AmbiguousCommitError,
    ConcurrentModificationException,
    MetadataManager,
)
from .snapshot_manager import SnapshotManager
from .transaction_append import _AppendMixin
from .transaction_commit import _CommitOpsMixin

logger = get_logger(__name__)

# Directory (relative to table root) for in-flight GC-protection markers.
# Kept in sync with garbage_collector.INFLIGHT_PATH.
_INFLIGHT_PATH = "metadata/inflight"

@dataclass
class _Plan:
    """Queued operations of one transaction, partitioned for commit."""

    append_files: List[DataFile] = field(default_factory=list)
    deleted_paths: Set[str] = field(default_factory=set)
    expire_cutoff: Optional[int] = None
    retain_last: Optional[int] = None
    properties: Dict[str, Optional[str]] = field(default_factory=dict)
    compact: bool = False


class Transaction(_AppendMixin, _CommitOpsMixin):
    """Represents a database transaction with ACID properties"""

    def __init__(
        self,
        metadata_manager: MetadataManager,
        snapshot_manager: SnapshotManager,
        file_manager: FileManager,
    ):
        self.metadata_manager = metadata_manager
        self.snapshot_manager = snapshot_manager
        self.file_manager = file_manager
        self.table_path = metadata_manager.table_path

        # Transaction state
        self._is_active = False
        self._is_committed = False
        self._is_rolled_back = False

        # Operations queue
        self._operations: List[Dict[str, Any]] = []
        # True after commit() when a NEW snapshot was created (metadata-only
        # commits and no-op compactions leave it False).
        self.did_commit_snapshot = False

        # Track files written during transaction for cleanup on rollback
        self._written_files: List[str] = []
        # GC-protection markers written for those files
        self._inflight_markers: List[str] = []
        # Table-relative paths already protected by a marker (one marker per path)
        self._marked_paths: Set[str] = set()
        self._marker_names: Set[str] = set()
        # Manifests / manifest lists written by the CURRENT commit attempt: a lost
        # OCC race means they were never referenced, so the retry removes them
        # instead of leaving one orphan pair per attempt for GC (#74).
        self._attempt_files: List[str] = []
        # The table schema is read once per transaction (no schema evolution): every
        # append_* used to re-read the metadata - 2-4 S3 round trips each (#67).
        self._schema_cache: Optional[Schema] = None
        self._schema_cache_set = False
        # Metadata read while resolving the schema doubles as the first commit
        # attempt's OCC base; a stale base just retries with a fresh read (#67).
        self._base_metadata_cache: Optional[TableMetadata] = None

        self._lock = threading.RLock()

    def begin(self) -> "Transaction":
        """Start a new transaction"""
        with self._lock:
            if self._is_active:
                raise RuntimeError("Transaction already active")

            self._is_active = True
            self._is_committed = False
            self._is_rolled_back = False
            # Reset ALL per-transaction state. Leaving _operations populated
            # would silently re-apply a previous transaction's operations when
            # a Transaction object is reused.
            self._operations = []
            self._written_files = []
            self._inflight_markers = []
            self._marked_paths = set()
            self._marker_names = set()
            self._attempt_files = []
            self._schema_cache = None
            self._schema_cache_set = False
            self._base_metadata_cache = None

            return self

    def is_active(self) -> bool:
        """Check if transaction is active"""
        return self._is_active and not self._is_committed and not self._is_rolled_back

    def _register_inflight(self, file_path: str) -> None:
        """Write a GC-protection marker for a file this transaction is about to
        write but that no snapshot references yet.

        Used for data files AND for the manifests / manifest lists of a commit
        in progress: without a marker, a concurrent garbage collection running
        with a short grace period can delete a file between its write and the
        metadata commit that makes it reachable. Marker write failures
        propagate - a file is never written unprotected (fail closed).
        """
        rel_path = file_path.replace("\\", "/").lstrip("/")
        if rel_path in self._marked_paths:
            return  # already protected (append_data marks before writing, then queues)
        marker_name = rel_path.rsplit("/", 1)[-1]
        if marker_name in self._marker_names:
            # Two caller-provided files with the same basename in different
            # directories must not share (and overwrite) one marker.
            marker_name = f"{marker_name}.{uuid.uuid4().hex[:8]}"
        marker_path = f"{_INFLIGHT_PATH}/{marker_name}.inflight"
        marker_payload = json.dumps({"file_path": rel_path}).encode("utf-8")
        self.file_manager.storage.write_file(marker_path, marker_payload)
        self._inflight_markers.append(marker_path)
        self._marked_paths.add(rel_path)
        self._marker_names.add(marker_name)
        if rel_path.startswith(self.file_manager.manifests_path + "/"):
            self._attempt_files.append(rel_path)

    def delete_files(self, file_paths: List[str]) -> "Transaction":
        """Queue files to delete from the table"""
        if not self.is_active():
            raise RuntimeError("Transaction is not active")

        self._operations.append({"type": "delete_files", "file_paths": file_paths})

        return self

    def overwrite_by_filter(self, filter_func: Callable[[Any], bool]) -> "Transaction":
        """NOT IMPLEMENTED - raises instead of silently doing nothing.

        Earlier versions queued this operation, committed "successfully", and
        changed nothing. An overwrite API that reports success without
        overwriting is a data-integrity hazard, so until row-level overwrite is
        actually implemented this raises loudly.
        """
        raise NotImplementedError(
            "overwrite_by_filter is not implemented. Use delete_files() + append_data() "
            "to replace whole files. (Previous versions accepted this call and silently "
            "did nothing.)"
        )

    def expire_snapshots(
        self, older_than_ms: Optional[int] = None, retain_last: Optional[int] = None
    ) -> "Transaction":
        """Queue snapshot expiration, applied at commit.

        Snapshots with timestamp_ms below `older_than_ms` are removed, except the
        `retain_last` most recent ones and the current snapshot, which are always
        kept. At least one criterion is required. Physical file cleanup is done by
        garbage_collect() once the snapshots are unreachable."""
        if not self.is_active():
            raise RuntimeError("Transaction is not active")
        if older_than_ms is None and retain_last is None:
            raise ValueError("expire_snapshots needs older_than_ms and/or retain_last")
        if retain_last is not None and retain_last < 1:
            raise ValueError("retain_last must be >= 1")

        self._operations.append({
            "type": "expire_snapshots", "older_than_ms": older_than_ms, "retain_last": retain_last,
        })

        return self

    def set_properties(self, properties: Dict[str, Optional[str]]) -> "Transaction":
        """Queue table-property changes (value None removes a property). Properties
        drive retention and compaction policies - e.g. write.metadata.previous-versions-max,
        datashard.manifest.compaction-threshold, datashard.snapshot.retention-count (#68)."""
        if not self.is_active():
            raise RuntimeError("Transaction is not active")
        for k, v in properties.items():
            if not isinstance(k, str) or not k:
                raise ValueError(f"property names must be non-empty strings, got {k!r}")
            if v is not None and not isinstance(v, str):
                raise ValueError(f"property values must be strings or None, got {v!r} for {k}")
        self._operations.append({"type": "set_properties", "properties": dict(properties)})
        return self

    def compact_manifests(self) -> "Transaction":
        """Queue a rewrite of the active manifests into one (no-op below 2 manifests)."""
        if not self.is_active():
            raise RuntimeError("Transaction is not active")
        self._operations.append({"type": "compact_manifests"})
        return self

    def _plan_operations(self) -> _Plan:
        plan = _Plan()
        for operation in self._operations:
            kind = operation["type"]
            if kind == "append_files":
                plan.append_files.extend(operation["files"])
            elif kind == "delete_files":
                plan.deleted_paths.update(operation["file_paths"])
            elif kind == "expire_snapshots":
                cutoff = operation.get("older_than_ms")
                if cutoff is not None:
                    cutoff = int(cutoff)
                    plan.expire_cutoff = cutoff if plan.expire_cutoff is None else max(plan.expire_cutoff, cutoff)
                keep = operation.get("retain_last")
                if keep is not None:
                    plan.retain_last = int(keep) if plan.retain_last is None else min(plan.retain_last, int(keep))
            elif kind == "set_properties":
                plan.properties.update(operation["properties"])
            elif kind == "compact_manifests":
                plan.compact = True
            else:
                raise RuntimeError(f"Unknown queued operation type {kind!r}")
        return plan

    def commit(self) -> bool:
        """Commit the transaction with ACID properties using Optimistic Concurrency Control.

        Failure semantics (bank-grade, fail closed):
        - ConcurrentModificationException: clean conflict, retried with backoff
          against a freshly-read base.
        - AmbiguousCommitError: the commit-point write may have succeeded;
          written data files are KEPT (a durable snapshot may reference them)
          and the error is re-raised. True orphans are GC'd later.
        - After the commit point, no fallible operation runs before commit()
          returns - a post-commit failure can never trigger a rollback that
          deletes committed data.
        """
        import random

        if not self.is_active():
            raise RuntimeError("Transaction is not active")

        with self._lock:
            if not self._operations:
                # Empty transaction: nothing to persist - do NOT create a snapshot.
                self._finish_committed()
                return True

        max_retries = 50  # High-contention production environments
        retry_count = 0
        base_delay = 0.010  # 10ms base delay

        while retry_count < max_retries:
            try:
                with self._lock:
                    # Base for OCC. The first attempt reuses the metadata this
                    # transaction read while resolving the schema; a stale base
                    # just fails the OCC check and retries with a fresh read (#67).
                    if retry_count == 0 and self._base_metadata_cache is not None:
                        base_metadata: Optional[TableMetadata] = self._base_metadata_cache
                    else:
                        base_metadata = self.metadata_manager.refresh()
                    self._base_metadata_cache = None
                    if base_metadata is None:
                        raise RuntimeError("No current metadata - table is not initialized")

                    plan = self._plan_operations()
                    mutators: List[Callable[[TableMetadata], None]] = []
                    if plan.expire_cutoff is not None or plan.retain_last is not None:
                        mutators.append(self._make_expire_mutator(plan.expire_cutoff, plan.retain_last))
                    if plan.properties:
                        mutators.append(self._make_properties_mutator(plan.properties))
                    mutator = self._chain_mutators(mutators)

                    committed = False
                    if plan.append_files or plan.deleted_paths or plan.compact:
                        committed = self._commit_file_ops(
                            base_metadata, plan.append_files, plan.deleted_paths, mutator,
                            compact=plan.compact,
                        )
                    if not committed and mutator is not None:
                        # Metadata-only transaction (expire / properties): commit the
                        # metadata change directly without fabricating a snapshot.
                        new_metadata = self._deep_copy_metadata(base_metadata)
                        mutator(new_metadata)
                        self.metadata_manager.commit(base_metadata, new_metadata)

                    # ---- COMMIT POINT PASSED ----
                    # Only infallible bookkeeping below (no storage reads, no
                    # refresh): nothing here may throw us into the rollback path.
                    self.did_commit_snapshot = committed
                    self._finish_committed()
                    return True

            except ConcurrentModificationException as e:
                # Clean loss: nothing we wrote this attempt is referenced by any
                # snapshot. Drop the attempt's manifests now rather than leaving a
                # pair of orphans per retry for GC to find (#74).
                self._discard_attempt_files()
                retry_count += 1
                if retry_count >= max_retries:
                    # Final failure - cannot commit even after retries
                    self._rollback()
                    raise e
                else:
                    # Exponential backoff with jitter and cap to reduce contention
                    max_delay = 2.0  # Cap at 2 seconds
                    delay = min(base_delay * (2 ** retry_count), max_delay)
                    delay += random.uniform(0, delay * 0.5)  # Add up to 50% jitter
                    time.sleep(delay)
                    continue  # Retry the transaction
            except AmbiguousCommitError:
                # The version-hint write failed in a way that may still have
                # become durable. The committed snapshot (if any) references our
                # written files - deleting them would corrupt the table.
                self._rollback(delete_files=False)
                raise
            except Exception as e:
                # Known-pre-commit-point failure - safe to clean up written files
                self._rollback()
                raise e

        # This line should not be reached if max_retries > 0, but added for completeness
        self._rollback()
        raise ConcurrentModificationException(f"Failed to commit after {max_retries} retries")

    def _finish_committed(self) -> None:
        """Mark the transaction committed. Infallible by design (only local
        state changes and best-effort marker cleanup) - runs after the commit
        point, where an exception must never cascade into a rollback."""
        self._is_active = False
        self._is_committed = True

        # Best-effort removal of GC-protection markers; a leftover marker only
        # extends protection and is swept by GC after the abandonment window.
        self._delete_markers()
        self._written_files = []
        self._marked_paths = set()
        self._marker_names = set()

    def rollback(self) -> bool:
        """Rollback the transaction"""
        if not self.is_active():
            return False

        with self._lock:
            return self._rollback()

    def _rollback(self, delete_files: bool = True) -> bool:
        """Internal method to perform rollback.

        Args:
            delete_files: When True (known-pre-commit failure), files written by
                this transaction are deleted. When False (AMBIGUOUS commit-point
                failure), files AND their protection markers are kept - a
                durable snapshot may reference them; GC handles true orphans.
        """
        self._is_active = False
        self._is_rolled_back = True

        if not delete_files:
            logger.warning(
                "Transaction outcome ambiguous: keeping %d written file(s) - the commit "
                "may be durable. Orphans (if any) will be garbage-collected.",
                len(self._written_files),
            )
            return True

        # Clean up files written during this transaction (best-effort)
        for file_path in self._written_files:
            try:
                if self.file_manager.storage.exists(file_path):
                    self.file_manager.storage.delete_file(file_path)
            except Exception as e:
                logger.warning(f"Failed to clean up file {file_path} during rollback: {e}")

        self._delete_markers()

        self._written_files = []
        self._marked_paths = set()
        self._marker_names = set()

        return True

    def _discard_attempt_files(self) -> None:
        """Best-effort removal of manifests written by a commit attempt that lost its race."""
        if self._attempt_files:
            try:
                self.file_manager.storage.delete_files(list(self._attempt_files))
            except Exception as e:
                logger.debug(f"Could not remove attempt manifests (GC will): {e}")
        self._attempt_files = []

    def _delete_markers(self) -> None:
        """Best-effort bulk removal of this transaction's GC-protection markers (#67)."""
        if self._inflight_markers:
            try:
                self.file_manager.storage.delete_files(list(self._inflight_markers))
            except Exception as e:
                logger.debug(f"Marker cleanup failed (GC sweeps leftovers): {e}")
        self._inflight_markers = []

    def _deep_copy_metadata(self, metadata: TableMetadata) -> TableMetadata:
        """Create a deep copy of metadata for transaction isolation"""
        return copy.deepcopy(metadata)

    def __enter__(self) -> "Transaction":
        """Context manager entry"""
        return self.begin()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit"""
        if exc_type is not None:
            self.rollback()
        elif self.is_active():
            self.commit()


class TransactionManager:
    """Manages multiple transactions and ensures ACID compliance"""

    def __init__(
        self,
        metadata_manager: MetadataManager,
        snapshot_manager: SnapshotManager,
        file_manager: FileManager,
    ):
        self.metadata_manager = metadata_manager
        self.snapshot_manager = snapshot_manager
        self.file_manager = file_manager
        self._active_transactions: Dict[int, "Transaction"] = {}
        self._lock = threading.RLock()

    def begin_transaction(self) -> Transaction:
        """Begin a new transaction"""
        with self._lock:
            # Evict finished transactions so long-running processes don't leak
            # one Transaction object per commit.
            self._cleanup_locked()

            transaction = Transaction(
                self.metadata_manager, self.snapshot_manager, self.file_manager
            )
            transaction_id = id(transaction)
            self._active_transactions[transaction_id] = transaction
            return transaction

    def get_active_transactions(self) -> List[Transaction]:
        """Get all active transactions"""
        with self._lock:
            return [tx for tx in self._active_transactions.values() if tx.is_active()]

    def cleanup_completed_transactions(self) -> None:
        """Remove completed/failed transactions from tracking"""
        with self._lock:
            self._cleanup_locked()

    def _cleanup_locked(self) -> None:
        completed_ids = [
            tx_id for tx_id, tx in self._active_transactions.items() if not tx.is_active()
        ]
        for tx_id in completed_ids:
            del self._active_transactions[tx_id]




def __getattr__(name: str) -> Any:
    """`Table` moved to datashard.table (#71); keep `datashard.transaction.Table` importable."""
    if name == "Table":
        from .table import Table

        return Table
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
