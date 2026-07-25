"""
ACID transaction implementation for the Python Iceberg implementation
"""

import copy
import json
import os
import threading
import uuid
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Union

from .data_structures import (
    DataFile,
    FileFormat,
    ManifestContent,
    ManifestFile,
    Schema,
    Snapshot,
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

logger = get_logger(__name__)

# Directory (relative to table root) for in-flight GC-protection markers.
# Kept in sync with garbage_collector.INFLIGHT_PATH.
_INFLIGHT_PATH = "metadata/inflight"


class Transaction:
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

        # Track files written during transaction for cleanup on rollback
        self._written_files: List[str] = []
        # GC-protection markers written for those files
        self._inflight_markers: List[str] = []

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

            return self

    def is_active(self) -> bool:
        """Check if transaction is active"""
        return self._is_active and not self._is_committed and not self._is_rolled_back

    def append_files(self, files: List[DataFile]) -> "Transaction":
        """Queue pre-built data files to append to the table.

        Each file must exist AND (for parquet, on a table that has a persisted
        schema) carry a schema identical to the table's - see
        _validate_file_schema for why divergence is rejected here rather than
        discovered at scan time.
        """
        if not self.is_active():
            raise RuntimeError("Transaction is not active")

        # Validate that the files exist in the file system
        # This is a critical check in production systems
        table_schema = self._resolve_table_schema()
        for data_file in files:
            if not self.file_manager.validate_file_exists(data_file.file_path):
                raise FileNotFoundError(f"Data file does not exist: {data_file.file_path}")
            if table_schema is not None:
                self._validate_file_schema(data_file, table_schema)

        self._operations.append({"type": "append_files", "files": files})

        return self

    def _validate_file_schema(self, data_file: DataFile, table_schema: Schema) -> None:
        """Reject a pre-built data file whose stored schema diverges from the table's.

        pa.concat_tables requires identical schemas (field names, order, types
        AND nullability), so a divergent file commits happily and then makes
        every subsequent full scan fail - the file is accepted, the table is
        bricked. append_data(records) already enforces this via
        _validate_schema_against_table; this closes the same hole on the
        file-level API (#49).

        Only parquet files can be checked (the schema lives in the footer);
        other formats are queued unchecked, as before.
        """
        fmt = data_file.file_format
        fmt_name = fmt.value if isinstance(fmt, FileFormat) else str(fmt)
        if fmt_name.lower() != FileFormat.PARQUET.value:
            return

        import pyarrow.parquet as pq

        dfm = self.file_manager.data_file_manager
        expected = dfm.create_arrow_schema(table_schema)
        arrow_path = dfm._get_arrow_path(data_file.file_path)

        try:
            actual = pq.ParquetFile(arrow_path, filesystem=dfm._pyarrow_fs).schema_arrow
        except Exception as e:
            raise ValueError(
                f"Cannot read the parquet schema of '{data_file.file_path}': {e}. "
                f"Refusing to append a file whose schema cannot be verified against the "
                f"table schema - an unverifiable file can make every later scan fail."
            ) from e

        if not actual.equals(expected, check_metadata=False):
            raise ValueError(
                f"Data file '{data_file.file_path}' has a schema that does not match the "
                f"table's persisted schema. Appending it would make table scans fail. "
                f"Table schema: {expected}; file schema: {actual}"
            )

    def append_pandas(
        self,
        df: Any,
        schema: Optional["Schema"] = None,
    ) -> "Transaction":
        """Append a pandas DataFrame to the table.

        Args:
            df: pandas DataFrame to append
            schema: Optional Schema. If None, uses the table's current schema.

        Returns:
            Self for chaining
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for append_pandas") from None

        if not isinstance(df, pd.DataFrame):
            raise ValueError("Expected a pandas DataFrame")

        records = df.to_dict("records")
        return self.append_data(records, schema)

    def _resolve_table_schema(self) -> Optional[Schema]:
        """Resolve the table's persisted current schema, or None if the table
        has no usable (non-empty) schema."""
        metadata = self.metadata_manager.refresh()
        if metadata and metadata.schemas:
            for s in metadata.schemas:
                if s.schema_id == metadata.current_schema_id and s.fields:
                    return s
            # Fallback: any non-empty schema
            for s in metadata.schemas:
                if s.fields:
                    return s
        return None

    @staticmethod
    def _schema_signature(schema: Schema) -> Set[Any]:
        """Comparable signature of a schema's fields (name, type, required)."""
        sig = set()
        for f in schema.fields:
            f_type = f.get("type")
            type_key = json.dumps(f_type, sort_keys=True) if isinstance(f_type, (dict, list)) else f_type
            sig.add((f.get("name"), type_key, bool(f.get("required", False))))
        return sig

    def _validate_schema_against_table(self, schema: Schema) -> None:
        """Reject appends whose schema diverges from the table's persisted schema.

        A divergent append would write parquet files whose schema differs from
        the rest of the table, making every subsequent full scan fail on
        concat - effectively bricking reads for the whole table.
        """
        table_schema = self._resolve_table_schema()
        if table_schema is None:
            return  # No persisted schema (legacy table): nothing to enforce
        if self._schema_signature(schema) != self._schema_signature(table_schema):
            raise ValueError(
                "Provided schema does not match the table's persisted schema. "
                "Appending with a divergent schema would make table scans fail. "
                f"Table fields: {table_schema.fields}; provided fields: {schema.fields}"
            )

    def append_data(
        self,
        records: List[Dict[str, Any]],
        schema: Optional["Schema"] = None,
        partition_values: Optional[Dict[str, Any]] = None,
    ) -> "Transaction":
        """Append actual data records to the table by creating new data files.

        The schema is resolved from the table metadata when not provided; a
        table without a usable schema raises instead of silently writing
        zero-column files.
        """
        if not self.is_active():
            raise RuntimeError("Transaction is not active")

        if schema is None:
            schema = self._resolve_table_schema()
            if schema is None:
                raise ValueError(
                    "No schema available: the table has no persisted schema and none was "
                    "provided. Create the table with create_table(path, schema=...) or pass "
                    "schema= explicitly - appending without a schema would silently discard "
                    "all record fields."
                )
        else:
            self._validate_schema_against_table(schema)

        # Create a data file with the records using UUID for uniqueness
        file_id = uuid.uuid4().hex[:16]  # Use 16 chars of UUID hex
        file_name = f"auto_{file_id}.parquet"
        # Use relative path for storage backend (works for both local and S3)
        file_path = f"data/{file_name}"

        # Register a GC-protection marker BEFORE writing the data file: the file
        # is unreachable until commit, and without the marker a long-running
        # transaction's files could exceed the GC grace period and be deleted
        # out from under the commit. Marker write failure aborts the append
        # (fail closed - never write unprotected files).
        self._register_inflight(file_path)

        # Use the data file manager to write the data
        data_file = self.file_manager.data_file_manager.write_data_file(
            file_path=file_path,
            records=records,
            iceberg_schema=schema,
            file_format=FileFormat.PARQUET,
            partition_values=partition_values or {},
        )

        # Track written file for cleanup on rollback
        self._written_files.append(file_path)

        # When using append_files, the file path in the DataFile object should be
        # relative to the table for Iceberg-style path resolution
        # Modify the file_path to be relative to the table (in Iceberg format)
        relative_path = f"/data/{file_name}"
        updated_data_file = DataFile(
            file_path=relative_path,
            file_format=data_file.file_format,
            partition_values=data_file.partition_values,
            record_count=data_file.record_count,
            file_size_in_bytes=data_file.file_size_in_bytes,
            # Copy any other important fields
            column_sizes=data_file.column_sizes,
            value_counts=data_file.value_counts,
            null_value_counts=data_file.null_value_counts,
            lower_bounds=data_file.lower_bounds,
            upper_bounds=data_file.upper_bounds,
            checksum=data_file.checksum,
        )

        # Queue the newly created file for appending
        self.append_files([updated_data_file])

        return self

    def _register_inflight(self, file_path: str) -> None:
        """Write a GC-protection marker for a file this transaction is about to
        write but that no snapshot references yet.

        Used for data files AND for the manifests / manifest lists of a commit
        in progress: without a marker, a concurrent garbage collection running
        with a short grace period can delete a file between its write and the
        metadata commit that makes it reachable. Marker write failures
        propagate - a file is never written unprotected (fail closed).
        """
        marker_name = file_path.rsplit("/", 1)[-1]
        marker_path = f"{_INFLIGHT_PATH}/{marker_name}.inflight"
        marker_payload = json.dumps({"file_path": file_path.lstrip("/")}).encode("utf-8")
        self.file_manager.storage.write_file(marker_path, marker_payload)
        self._inflight_markers.append(marker_path)

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

    def expire_snapshots(self, older_than_ms: int) -> "Transaction":
        """Queue snapshot expiration: snapshots with timestamp_ms older than the
        given cutoff are removed from table metadata at commit (the current
        snapshot is never expired). Physical file cleanup is done by
        garbage_collect() once the snapshots are unreachable."""
        if not self.is_active():
            raise RuntimeError("Transaction is not active")

        self._operations.append({"type": "expire_snapshots", "older_than_ms": older_than_ms})

        return self

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
        import time

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
                    # Get current metadata as the "base" for our operations
                    base_metadata = self.metadata_manager.refresh()
                    if base_metadata is None:
                        raise RuntimeError("No current metadata - table is not initialized")

                    # Partition queued operations
                    append_files: List[DataFile] = []
                    deleted_paths: Set[str] = set()
                    expire_cutoff: Optional[int] = None
                    for operation in self._operations:
                        if operation["type"] == "append_files":
                            append_files.extend(operation["files"])
                        elif operation["type"] == "delete_files":
                            deleted_paths.update(operation["file_paths"])
                        elif operation["type"] == "expire_snapshots":
                            cutoff = int(operation["older_than_ms"])
                            expire_cutoff = (
                                cutoff if expire_cutoff is None else max(expire_cutoff, cutoff)
                            )

                    mutator = (
                        self._make_expire_mutator(expire_cutoff)
                        if expire_cutoff is not None
                        else None
                    )

                    if append_files or deleted_paths:
                        self._commit_file_ops(
                            base_metadata, append_files, deleted_paths, mutator
                        )
                    else:
                        # Metadata-only transaction (expire_snapshots): commit the
                        # metadata change directly without fabricating a snapshot.
                        new_metadata = self._deep_copy_metadata(base_metadata)
                        if mutator is not None:
                            mutator(new_metadata)
                        self.metadata_manager.commit(base_metadata, new_metadata)

                    # ---- COMMIT POINT PASSED ----
                    # Only infallible bookkeeping below (no storage reads, no
                    # refresh): nothing here may throw us into the rollback path.
                    self._finish_committed()
                    return True

            except ConcurrentModificationException as e:
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

    def _commit_file_ops(
        self,
        base_metadata: TableMetadata,
        append_files: List[DataFile],
        deleted_paths: Set[str],
        mutator: Optional[Callable[[TableMetadata], None]],
    ) -> None:
        """Build manifests for file-level operations and commit the snapshot."""
        # One snapshot id for EVERYTHING this commit writes: manifest entries,
        # manifest-list filename, and the Snapshot itself must agree, or
        # lineage joins dangle.
        snapshot_id = (uuid.uuid4().int & ((1 << 63) - 1))
        # Same for the Iceberg v2 sequence number: derived from the SAME base
        # metadata the OCC commit will be validated against, so a lost race
        # re-derives it from the fresh base on retry.
        sequence_number = base_metadata.last_sequence_number + 1

        # 1. Read existing manifests from the base snapshot. Failure ABORTS the
        # commit: falling back to an empty manifest set would silently drop
        # every pre-existing file from the new snapshot (and GC would then
        # delete them permanently).
        existing_manifests: List[ManifestFile] = []
        if (
            base_metadata.current_snapshot_id is not None
            and base_metadata.current_snapshot_id != -1
        ):
            base_snapshot = None
            for s in base_metadata.snapshots:
                if s.snapshot_id == base_metadata.current_snapshot_id:
                    base_snapshot = s
                    break

            if base_snapshot is None:
                # Dangling current_snapshot_id: the metadata is inconsistent, so
                # the base file set is UNKNOWN. Continuing with an empty base
                # would write a snapshot that silently omits every pre-existing
                # file (and GC would then delete them). Fail closed.
                raise RuntimeError(
                    f"Table metadata is inconsistent: current_snapshot_id "
                    f"{base_metadata.current_snapshot_id} does not match any snapshot in "
                    f"metadata.snapshots ({[s.snapshot_id for s in base_metadata.snapshots]}). "
                    f"Aborting commit: proceeding would silently drop all prior table data "
                    f"from the new snapshot."
                )

            path = base_snapshot.manifest_list
            if path.startswith("/"):
                path = path.lstrip("/")
            try:
                existing_manifests = self.file_manager.read_manifest_list_file(path)
            except Exception as e:
                raise RuntimeError(
                    f"Cannot read base snapshot manifest list "
                    f"'{base_snapshot.manifest_list}'. Aborting commit: proceeding "
                    f"would silently drop all prior table data from the new snapshot."
                ) from e

        # 2. Process deletes (rewrite affected manifests)
        final_manifests: List[ManifestFile] = []
        if deleted_paths:
            for manifest in existing_manifests:
                manifest_path = manifest.manifest_path
                if manifest_path.startswith("/"):
                    manifest_path = manifest_path.lstrip("/")

                try:
                    data_files = self.file_manager.read_manifest_file(manifest_path)
                except Exception as e:
                    # If we can't read a manifest, we can't safely filter it.
                    raise RuntimeError(
                        f"Failed to read manifest {manifest.manifest_path} during delete operation"
                    ) from e

                surviving_files = [
                    f for f in data_files
                    if f.file_path not in deleted_paths
                    and f.file_path.lstrip("/") not in deleted_paths
                ]

                if len(surviving_files) == len(data_files):
                    # No changes, keep manifest
                    final_manifests.append(manifest)
                elif len(surviving_files) > 0:
                    # Partial delete: rewrite. Survivors keep status EXISTING and
                    # their original added_snapshot_id (no falsified history).
                    new_manifest = self.file_manager.create_manifest_file(
                        [],
                        ManifestContent.DATA,
                        snapshot_id,
                        existing_files=surviving_files,
                        sequence_number=sequence_number,
                        pre_write_hook=self._register_inflight,
                    )
                    new_manifest.partition_spec_id = manifest.partition_spec_id
                    final_manifests.append(new_manifest)
                # else: all files deleted -> drop this manifest
        else:
            final_manifests = list(existing_manifests)

        # 3. Process appends (create new manifest)
        if append_files:
            self.file_manager.validate_data_files(append_files)
            new_append_manifest = self.file_manager.create_manifest_file(
                append_files,
                ManifestContent.DATA,
                snapshot_id,
                sequence_number=sequence_number,
                pre_write_hook=self._register_inflight,
            )
            final_manifests.append(new_append_manifest)

        # 4. Create the manifest list (ALL active manifests for the table)
        manifest_list_path = self.file_manager.create_manifest_list_file(
            final_manifests, snapshot_id, pre_write_hook=self._register_inflight
        )

        # 5. Commit the snapshot - with the SAME id stamped into the manifests.
        self.snapshot_manager.create_snapshot(
            manifest_list_path=manifest_list_path,
            operation="append" if append_files else "delete",
            parent_snapshot_id=(
                base_metadata.current_snapshot_id
                if base_metadata.current_snapshot_id is not None
                else -1
            ),
            base_metadata=base_metadata,  # Fresh base for OCC
            snapshot_id=snapshot_id,
            metadata_mutator=mutator,
            sequence_number=sequence_number,
        )

    @staticmethod
    def _make_expire_mutator(cutoff_ms: int) -> Callable[[TableMetadata], None]:
        """Mutator removing snapshots older than cutoff (never the current one)."""

        def mutator(metadata: TableMetadata) -> None:
            from .snapshot_manager import repoint_parents_to_surviving_ancestors

            kept = [
                s for s in metadata.snapshots
                if s.timestamp_ms >= cutoff_ms
                or s.snapshot_id == metadata.current_snapshot_id
            ]
            kept_ids = {s.snapshot_id for s in kept}
            # Survivors must not keep parent links to expired snapshots.
            repoint_parents_to_surviving_ancestors(metadata.snapshots, kept)
            metadata.snapshots = kept
            metadata.snapshot_log = [
                e for e in metadata.snapshot_log if e.snapshot_id in kept_ids
            ]

        return mutator

    def _finish_committed(self) -> None:
        """Mark the transaction committed. Infallible by design (only local
        state changes and best-effort marker cleanup) - runs after the commit
        point, where an exception must never cascade into a rollback."""
        self._is_active = False
        self._is_committed = True

        # Best-effort removal of GC-protection markers; a leftover marker only
        # extends protection and is swept by GC after the abandonment window.
        for marker in self._inflight_markers:
            try:
                self.file_manager.storage.delete_file(marker)
            except Exception:
                pass
        self._inflight_markers = []
        self._written_files = []

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

        for marker in self._inflight_markers:
            try:
                self.file_manager.storage.delete_file(marker)
            except Exception:
                pass

        self._written_files = []
        self._inflight_markers = []

        return True

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


class Table:
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

        NOTE: This returns snapshot METADATA (id, timestamp, manifest list
        reference); it does not switch the table's state, and scan() always
        reads the CURRENT snapshot. Reading data as-of an old snapshot is not
        implemented yet.
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

    def garbage_collect(self, grace_period_ms: int = 3600000) -> Dict[str, int]:
        """Delete orphaned files not referenced by any snapshot.

        Fail closed: if any reachable manifest cannot be read, GC aborts
        (GarbageCollectionAborted) without deleting anything. Files belonging
        to in-flight transactions are protected via markers regardless of age.

        Args:
            grace_period_ms: Only delete orphaned files older than this age (default 1 hour).

        Returns:
            Dict with counts of deleted files by type.
        """
        from .garbage_collector import GarbageCollector
        gc = GarbageCollector(self.table_path, self.metadata_manager, self.file_manager)
        return gc.collect(grace_period_ms)

    def row_count(self) -> int:
        """Get total row count from parquet metadata without scanning data.

        This is a fast O(manifest_files) operation that reads only metadata,
        not the actual parquet data files. Use this for count-only queries
        instead of len(table.scan()).

        Returns:
            Total number of rows across all data files in current snapshot.
        """
        data_files = self._get_all_data_files()
        return sum(df.record_count for df in data_files)

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_verify_checksums(param: Optional[bool]) -> bool:
        """Resolve checksum-verification setting: explicit param wins, then the
        DATASHARD_VERIFY_CHECKSUMS env var, default ON (bank-grade)."""
        if param is not None:
            return param
        return os.getenv("DATASHARD_VERIFY_CHECKSUMS", "true").strip().lower() in (
            "1", "true", "yes", "on",
        )

    def _read_datafile_table(
        self,
        data_file: DataFile,
        columns: Optional[List[str]],
        compute_expr: Any,
        verify: bool,
        pa: Any,
        pq: Any,
    ) -> Any:
        """Read one data file as a pyarrow Table, optionally verifying its checksum.

        Errors propagate: a data file that is referenced by the current snapshot
        but unreadable/corrupt is a table integrity failure, not something to
        silently skip (silent skips make partial results indistinguishable from
        complete ones).
        """
        from io import BytesIO

        from .integrity import CorruptDataError, IntegrityChecker

        data_file_manager = self.file_manager.data_file_manager

        if verify and data_file.checksum:
            rel_path = data_file.file_path.lstrip("/")
            raw = self.storage.read_file(rel_path)
            if not IntegrityChecker.verify_checksum(raw, data_file.checksum):
                raise CorruptDataError(
                    f"Checksum mismatch for data file {data_file.file_path}: "
                    f"stored data does not match the checksum recorded at write time"
                )
            # Read all columns, filter, THEN project: the filter may reference a
            # column not in `columns` (predicate pushdown would read it; a manual
            # post-projection filter would fail with "no match for FieldRef").
            table = pq.read_table(BytesIO(raw))
            if compute_expr is not None:
                table = table.filter(compute_expr)
            if columns is not None:
                table = table.select(columns)
            return table

        arrow_path = data_file_manager._get_arrow_path(data_file.file_path)
        pyarrow_fs = data_file_manager._pyarrow_fs
        if compute_expr is not None:
            # pyarrow applies `filters` against all needed columns during the
            # scan and returns only `columns`, so pushdown is correct here.
            return pq.read_table(
                arrow_path, columns=columns, filters=compute_expr, filesystem=pyarrow_fs
            )
        return pq.read_table(arrow_path, columns=columns, filesystem=pyarrow_fs)

    def _scan_table(
        self,
        columns: Optional[List[str]],
        filter_dict: Optional[Dict[str, Any]],
        parallel: Union[bool, int],
        verify_checksums: Optional[bool],
    ) -> Any:
        """Shared scan core: returns a pyarrow Table, or None for an empty table.

        All filters (including is_null / is_not_null) are applied through a
        single compute-expression engine, so every scan API returns identical
        results for identical filters.
        """
        from concurrent.futures import ThreadPoolExecutor

        import pyarrow as pa
        import pyarrow.parquet as pq

        from .filters import (
            parse_filter_dict,
            prune_files_by_bounds,
            to_pyarrow_compute_expression,
        )

        data_files = self._get_all_data_files()
        if not data_files:
            return None

        expressions = parse_filter_dict(filter_dict) if filter_dict else []
        compute_expr = to_pyarrow_compute_expression(expressions) if expressions else None

        # File-level pruning via column bounds
        if expressions:
            schema = self._get_current_schema()
            if schema:
                data_files = prune_files_by_bounds(data_files, expressions, schema)
        if not data_files:
            return None

        verify = self._resolve_verify_checksums(verify_checksums)

        def read_one(df: DataFile) -> Any:
            return self._read_datafile_table(df, columns, compute_expr, verify, pa, pq)

        if parallel:
            n_workers = parallel if isinstance(parallel, int) else (os.cpu_count() or 4)
            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                tables = list(executor.map(read_one, data_files))
        else:
            tables = [read_one(df) for df in data_files]

        return pa.concat_tables(tables)

    def scan(
        self,
        columns: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        parallel: Union[bool, int] = False,
        verify_checksums: Optional[bool] = None,
    ) -> List[Dict[str, Any]]:
        """Scan the table's CURRENT snapshot and return records.

        Args:
            columns: Optional list of column names to read. If None, reads all columns.
            filter: Optional filter dict for predicate pushdown.
                Examples:
                    {"status": "failed"}              # status == "failed"
                    {"age": (">", 18)}                # age > 18
                    {"id": ("in", [1, 2, 3])}         # id in [1, 2, 3]
                    {"ts": ("between", (t1, t2))}     # t1 <= ts <= t2
                    {"name": ("is_null", True)}       # name IS NULL
                Null handling follows SQL semantics: comparison operators and
                in/not_in never match NULL values; use is_null / is_not_null.
            parallel: Enable parallel reading.
                - False: Sequential reading (default)
                - True: Use all CPU cores
                - int: Use specified number of threads
            verify_checksums: Verify each data file against its stored checksum
                (raises CorruptDataError on mismatch). Defaults to the
                DATASHARD_VERIFY_CHECKSUMS env var, which defaults to true.

        Returns:
            List of dictionaries, each representing a record.

        Raises:
            RuntimeError / OSError: If any referenced manifest or data file
                cannot be read - errors are never swallowed into partial results.
            CorruptDataError: If checksum verification fails.
        """
        combined = self._scan_table(columns, filter, parallel, verify_checksums)
        if combined is None:
            return []
        result: List[Dict[str, Any]] = combined.to_pylist()
        return result

    def to_pandas(
        self,
        columns: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        parallel: Union[bool, int] = False,
        verify_checksums: Optional[bool] = None,
    ) -> Any:
        """Read the table's CURRENT snapshot as a pandas DataFrame.

        Same filtering, error, and checksum semantics as scan().

        Raises:
            ImportError: If pandas is not installed.
        """
        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError(
                "pandas is required for to_pandas(). Install with: pip install pandas"
            ) from e

        combined = self._scan_table(columns, filter, parallel, verify_checksums)
        if combined is None:
            return pd.DataFrame()
        return combined.to_pandas()

    def scan_batches(
        self,
        batch_size: int = 10000,
        columns: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        verify_checksums: Optional[bool] = None,
    ) -> Iterator[List[Dict[str, Any]]]:
        """Scan data in batches for memory-efficient processing.

        Yields batches of records, processing one parquet file at a time
        using PyArrow's iter_batches for memory efficiency. Uses the same
        filter engine (and error semantics) as scan().

        Args:
            batch_size: Approximate number of records per batch
            columns: Optional column projection
            filter: Optional predicate pushdown filter
            verify_checksums: As in scan().

        Yields:
            List of records (dicts) per batch
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        from .filters import (
            parse_filter_dict,
            prune_files_by_bounds,
            to_pyarrow_compute_expression,
        )

        data_files = self._get_all_data_files()

        expressions = parse_filter_dict(filter) if filter else []
        if expressions and data_files:
            schema = self._get_current_schema()
            if schema:
                data_files = prune_files_by_bounds(data_files, expressions, schema)

        if not data_files:
            return

        compute_expr = to_pyarrow_compute_expression(expressions) if expressions else None
        verify = self._resolve_verify_checksums(verify_checksums)

        yield from self._iter_file_batches(
            data_files, batch_size, columns, compute_expr, verify, pa, pq
        )

    def _iter_file_batches(
        self,
        data_files: List[DataFile],
        batch_size: int,
        columns: Optional[List[str]],
        compute_expr: Any,
        verify: bool,
        pa: Any,
        pq: Any,
    ) -> Iterator[List[Dict[str, Any]]]:
        """Iterate over batches from data files. Read errors propagate."""
        from io import BytesIO

        from .integrity import CorruptDataError, IntegrityChecker

        data_file_manager = self.file_manager.data_file_manager
        pyarrow_fs = data_file_manager._pyarrow_fs

        # When filtering, read every column (the predicate may reference one not
        # in `columns`); project down to `columns` only after filtering.
        read_columns = None if compute_expr is not None else columns

        for data_file in data_files:
            if verify and data_file.checksum:
                rel_path = data_file.file_path.lstrip("/")
                raw = self.storage.read_file(rel_path)
                if not IntegrityChecker.verify_checksum(raw, data_file.checksum):
                    raise CorruptDataError(
                        f"Checksum mismatch for data file {data_file.file_path}"
                    )
                pf = pq.ParquetFile(BytesIO(raw))
            else:
                arrow_path = data_file_manager._get_arrow_path(data_file.file_path)
                pf = pq.ParquetFile(arrow_path, filesystem=pyarrow_fs)

            for batch in pf.iter_batches(batch_size=batch_size, columns=read_columns):
                # Convert batch to table for filtering
                table = pa.Table.from_batches([batch])

                # Apply filter if needed
                if compute_expr is not None:
                    table = table.filter(compute_expr)
                    if columns is not None:
                        table = table.select(columns)

                if table.num_rows > 0:
                    yield table.to_pylist()

    def iter_records(
        self,
        columns: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        verify_checksums: Optional[bool] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Iterate over records one at a time.

        Memory efficient - only one batch in memory at a time.
        Ideal for row-by-row processing of large tables.

        Args:
            columns: Optional column projection
            filter: Optional predicate pushdown filter
            verify_checksums: As in scan().

        Yields:
            Individual records as dicts
        """
        for batch in self.scan_batches(
            batch_size=1000, columns=columns, filter=filter, verify_checksums=verify_checksums
        ):
            for record in batch:
                yield record

    def iter_pandas(
        self,
        chunksize: int = 50000,
        columns: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        verify_checksums: Optional[bool] = None,
    ) -> Iterator[Any]:
        """Iterate over data as pandas DataFrame chunks.

        Memory efficient - only one chunk in memory at a time.
        Ideal for processing large tables with pandas operations.

        Args:
            chunksize: Approximate rows per chunk
            columns: Optional column projection
            filter: Optional predicate pushdown filter
            verify_checksums: As in scan().

        Yields:
            pandas DataFrame chunks

        Raises:
            ImportError: If pandas is not installed.
        """
        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError(
                "pandas is required for iter_pandas(). Install with: pip install pandas"
            ) from e

        for batch in self.scan_batches(
            batch_size=chunksize, columns=columns, filter=filter, verify_checksums=verify_checksums
        ):
            yield pd.DataFrame(batch)

    def _get_all_data_files(self) -> List[DataFile]:
        """Get ALL data files referenced by the CURRENT snapshot.

        Fail closed: a snapshot that references a missing or unreadable
        manifest (list) raises instead of returning partial/empty results -
        readers must be able to distinguish "empty table" from "broken table".
        """
        snapshot = self.current_snapshot()
        if not snapshot:
            # An unset current_snapshot_id means "empty table". A SET id that
            # resolves to nothing means the metadata is inconsistent - returning
            # [] there would report a broken table as an empty one (#48).
            metadata = self.metadata_manager.refresh()
            current_id = metadata.current_snapshot_id if metadata else None
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

        if not self.storage.exists(manifest_list_path):
            raise RuntimeError(
                f"Current snapshot {snapshot.snapshot_id} references missing manifest "
                f"list '{snapshot.manifest_list}' - table metadata is inconsistent"
            )

        manifest_files = self.file_manager.read_manifest_list_file(manifest_list_path)

        all_data_files = []
        seen_paths = set()

        for manifest_ref in manifest_files:
            manifest_path = manifest_ref.manifest_path
            if not manifest_path:
                continue
            if manifest_path.startswith("/"):
                manifest_path = manifest_path.lstrip("/")

            if not self.storage.exists(manifest_path):
                raise RuntimeError(
                    f"Manifest list references missing manifest '{manifest_ref.manifest_path}' "
                    f"- table metadata is inconsistent"
                )

            manifest_data_files = self.file_manager.read_manifest_file(manifest_path)

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

    def _get_current_schema(self) -> Optional[Schema]:
        """Get the current schema from metadata.

        Returns:
            Current Schema object, or None if the table has no schema.
        """
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
