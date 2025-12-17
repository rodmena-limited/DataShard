"""
ACID transaction implementation for the Python Iceberg implementation
"""

import json
import os
import threading
import uuid
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

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
from .metadata_manager import ConcurrentModificationException, MetadataManager
from .snapshot_manager import SnapshotManager


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

        self._lock = threading.RLock()

    def begin(self) -> "Transaction":
        """Start a new transaction"""
        with self._lock:
            if self._is_active:
                raise RuntimeError("Transaction already active")

            self._is_active = True
            self._is_committed = False
            self._is_rolled_back = False
            self._written_files = []  # Reset written files tracker

            return self

    def is_active(self) -> bool:
        """Check if transaction is active"""
        return self._is_active and not self._is_committed and not self._is_rolled_back

    def append_files(self, files: List[DataFile]) -> "Transaction":
        """Queue files to append to the table"""
        if not self.is_active():
            raise RuntimeError("Transaction is not active")

        # Validate that the files exist in the file system
        # This is a critical check in production systems
        for data_file in files:
            if not self.file_manager.validate_file_exists(data_file.file_path):
                raise FileNotFoundError(f"Data file does not exist: {data_file.file_path}")

        self._operations.append({"type": "append_files", "files": files})

        return self

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

        if schema is None:
            # Try to get current schema from metadata
            metadata = self.metadata_manager.refresh()
            if metadata and metadata.schemas:
                # Find current schema
                current_schema_id = metadata.current_schema_id
                for s in metadata.schemas:
                    if s.schema_id == current_schema_id:
                        schema = s
                        break

        if schema is None:
            raise ValueError("Schema must be provided or table must have an existing schema")

        records = df.to_dict("records")
        return self.append_data(records, schema)

    def append_data(
        self,
        records: List[Dict[str, Any]],
        schema: Optional["Schema"] = None,
        partition_values: Optional[Dict[str, Any]] = None,
    ) -> "Transaction":
        """Append actual data records to the table by creating new data files.

        CRITICAL FIX: Track written files for cleanup on rollback.
        """
        if not self.is_active():
            raise RuntimeError("Transaction is not active")

        if schema is None:
            # Try to get current schema from metadata
            metadata = self.metadata_manager.refresh()
            if metadata and metadata.schemas:
                # Find current schema
                current_schema_id = metadata.current_schema_id
                for s in metadata.schemas:
                    if s.schema_id == current_schema_id:
                        schema = s
                        break

        if schema is None:
            raise ValueError("Schema must be provided or table must have an existing schema")

        # Create a data file with the records using UUID for uniqueness
        file_id = uuid.uuid4().hex[:16]  # Use 16 chars of UUID hex
        file_name = f"auto_{file_id}.parquet"
        # Use relative path for storage backend (works for both local and S3)
        file_path = f"data/{file_name}"

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

    def delete_files(self, file_paths: List[str]) -> "Transaction":
        """Queue files to delete from the table"""
        if not self.is_active():
            raise RuntimeError("Transaction is not active")

        self._operations.append({"type": "delete_files", "file_paths": file_paths})

        return self

    def overwrite_by_filter(self, filter_func: Callable[[Any], bool]) -> "Transaction":
        """Queue an overwrite operation by filter"""
        if not self.is_active():
            raise RuntimeError("Transaction is not active")

        self._operations.append({"type": "overwrite_by_filter", "filter": filter_func})

        return self

    def expire_snapshots(self, older_than_ms: int) -> "Transaction":
        """Queue snapshot expiration operation"""
        if not self.is_active():
            raise RuntimeError("Transaction is not active")

        self._operations.append({"type": "expire_snapshots", "older_than_ms": older_than_ms})

        return self

    def commit(self) -> bool:  # noqa: C901
        """Commit the transaction with ACID properties using Optimistic Concurrency Control"""
        import random
        import time

        if not self.is_active():
            raise RuntimeError("Transaction is not active")

        # Implement retry logic for OCC - this is essential for handling concurrent modifications
        # CRITICAL FIX: Increased retries and improved backoff for high-contention scenarios
        # With 8+ concurrent workers, we need more aggressive retry parameters
        max_retries = 50  # Increased from 10 for high-contention production environments
        retry_count = 0
        base_delay = 0.010  # 10ms base delay (doubled from 5ms)

        while retry_count < max_retries:
            try:
                with self._lock:
                    # Get current metadata as the "base" for our operations
                    base_metadata = self.metadata_manager.refresh()
                    if base_metadata is None:
                        raise RuntimeError("No current metadata")

                    # Create a copy of the metadata to work with (our working copy)
                    new_metadata = self._deep_copy_metadata(base_metadata)

                    # Apply all operations to the metadata
                    for operation in self._operations:
                        new_metadata = self._apply_operation(new_metadata, operation)

                    # Generate a unique snapshot ID using UUID to prevent collisions
                    # CRITICAL FIX: Timestamp-based IDs could collide with concurrent transactions
                    # Ensure it fits in signed 64-bit integer (Java/Avro long compatibility)
                    snapshot_id = (uuid.uuid4().int & ((1 << 63) - 1))

                    # --- START OF MANIFEST PROCESSING ---

                    # 1. Collect files to append and delete
                    append_files = []
                    deleted_paths = set()

                    for operation in self._operations:
                        if operation["type"] == "append_files":
                            append_files.extend(operation["files"])
                        elif operation["type"] == "delete_files":
                            deleted_paths.update(operation["file_paths"])

                    # 2. Get existing manifests from base snapshot (if any)
                    existing_manifests: List[ManifestFile] = []
                    if base_metadata.current_snapshot_id is not None:
                        # Find the snapshot object
                        base_snapshot = None
                        for s in base_metadata.snapshots:
                            if s.snapshot_id == base_metadata.current_snapshot_id:
                                base_snapshot = s
                                break

                        if base_snapshot:
                             # Read the manifest list
                             try:
                                 # Remove leading slash if needed
                                 path = base_snapshot.manifest_list
                                 if path.startswith("/"):
                                     path = path.lstrip("/")
                                 existing_manifests = self.file_manager.read_manifest_list_file(path)
                             except Exception:
                                 # If we can't read it, start fresh (safety fallback)
                                 existing_manifests = []

                    # 3. Process Deletes (Rewrite manifests if needed)
                    final_manifests: List[ManifestFile] = []

                    if deleted_paths:
                        for manifest in existing_manifests:
                            # Read data files in this manifest
                            try:
                                manifest_path = manifest.manifest_path
                                if manifest_path.startswith("/"):
                                    manifest_path = manifest_path.lstrip("/")

                                data_files = self.file_manager.read_manifest_file(manifest_path)

                                # Filter out deleted files
                                surviving_files = [
                                    f for f in data_files
                                    if f.file_path not in deleted_paths and f.file_path.lstrip("/") not in deleted_paths
                                ]

                                if len(surviving_files) == len(data_files):
                                    # No changes, keep manifest
                                    final_manifests.append(manifest)
                                elif len(surviving_files) > 0:
                                    # Partial delete: create new manifest
                                    new_manifest = self.file_manager.create_manifest_file(
                                        surviving_files,
                                        ManifestContent.DATA,
                                        snapshot_id
                                    )
                                    # Preserve partition spec ID if possible, else default 0
                                    new_manifest.partition_spec_id = manifest.partition_spec_id
                                    final_manifests.append(new_manifest)
                                else:
                                    # All files deleted: drop this manifest
                                    pass
                            except Exception as e:
                                # If we can't read a manifest, we can't safely filter it.
                                # For safety, keep it (fail safe) or fail transaction?
                                # Failing is safer for consistency.
                                raise RuntimeError(f"Failed to read manifest {manifest.manifest_path} during delete operation") from e
                    else:
                        final_manifests = list(existing_manifests)

                    # 4. Process Appends (Create new manifest)
                    if append_files:
                        # Validate existence
                        self.file_manager.validate_data_files(append_files)

                        new_append_manifest = self.file_manager.create_manifest_file(
                            append_files,
                            ManifestContent.DATA,
                            snapshot_id
                        )
                        final_manifests.append(new_append_manifest)

                    # 5. Create the Master Manifest List
                    # This list now contains ALL active manifests for the table
                    manifest_list_path = self.file_manager.create_manifest_list_file(
                        final_manifests, snapshot_id
                    )

                    # --- END OF MANIFEST PROCESSING ---

                    # Create a new snapshot for this transaction
                    # CRITICAL FIX: Pass base_metadata to avoid race condition where
                    # create_snapshot() would do its own refresh() and potentially
                    # get stale metadata compared to what we just read above.
                    self.snapshot_manager.create_snapshot(
                        manifest_list_path=manifest_list_path,
                        operation=(
                            "append"
                            if any(op["type"] == "append_files" for op in self._operations)
                            else "delete" if any(op["type"] == "delete_files" for op in self._operations)
                            else "update"
                        ),
                        parent_snapshot_id=(
                            base_metadata.current_snapshot_id
                            if base_metadata.current_snapshot_id is not None
                            else -1
                        ),
                        base_metadata=base_metadata,  # Pass our fresh base for OCC
                    )

                    # The snapshot manager already updated the metadata, so get the latest
                    refreshed_metadata = self.metadata_manager.refresh()
                    if refreshed_metadata is not None:
                        new_metadata = refreshed_metadata

                    self._is_active = False
                    self._is_committed = True

                    # Clear written files on successful commit (don't clean them up)
                    self._written_files = []

                    return True

            except ConcurrentModificationException as e:
                retry_count += 1
                if retry_count >= max_retries:
                    # Final failure - cannot commit even after retries
                    self._rollback()
                    raise e
                else:
                    # Exponential backoff with jitter and cap to reduce contention
                    # Formula: min(base_delay * 2^retry + random jitter, max_delay)
                    max_delay = 2.0  # Cap at 2 seconds
                    delay = min(base_delay * (2 ** retry_count), max_delay)
                    delay += random.uniform(0, delay * 0.5)  # Add up to 50% jitter
                    time.sleep(delay)
                    continue  # Retry the transaction
            except Exception as e:
                # Any other exception - rollback
                self._rollback()
                raise e

        # This line should not be reached if max_retries > 0, but added for completeness
        self._rollback()
        raise ConcurrentModificationException(f"Failed to commit after {max_retries} retries")

    def rollback(self) -> bool:
        """Rollback the transaction"""
        if not self.is_active():
            return False

        with self._lock:
            return self._rollback()

    def _rollback(self) -> bool:
        """Internal method to perform rollback.

        CRITICAL FIX: Clean up files written during the transaction.
        This prevents orphaned data files from accumulating on disk.
        """
        self._is_active = False
        self._is_rolled_back = True

        # Clean up files written during this transaction
        # Use best-effort cleanup - log errors but don't fail rollback
        for file_path in self._written_files:
            try:
                if self.file_manager.storage.exists(file_path):
                    self.file_manager.storage.delete_file(file_path)
            except Exception as e:
                # Log the error but continue with rollback
                # In production, this should use proper logging
                import sys
                print(f"Warning: Failed to clean up file {file_path} during rollback: {e}",
                      file=sys.stderr)

        # Clear the written files list
        self._written_files = []

        return True

    def _apply_operation(
        self, metadata: TableMetadata, operation: Dict[str, Any]
    ) -> TableMetadata:
        """Apply a single operation to the metadata"""
        op_type = operation["type"]

        if op_type == "append_files":
            # In a real implementation, this would add files to the manifest
            # For now, we'll just update the metadata
            operation["files"]
            # Update the schema if needed (add new columns)
            # Update last_sequence_number if needed
            metadata.last_updated_ms = int(
                threading.current_thread().ident or 0
            )  # Just for illustration

        elif op_type == "delete_files":
            operation["file_paths"]
            # In a real implementation, this would remove files from the manifest
            pass

        elif op_type == "overwrite_by_filter":
            operation["filter"]
            # In a real implementation, this would apply the filter
            pass

        elif op_type == "expire_snapshots":
            older_than_ms = operation["older_than_ms"]
            # Remove old snapshots
            metadata.snapshots = [s for s in metadata.snapshots if s.timestamp_ms >= older_than_ms]

        return metadata

    def _create_manifest_list(self, metadata: TableMetadata) -> str:
        """Create a manifest list file for the current state"""
        # Create a unique name for the manifest list
        timestamp = metadata.last_updated_ms
        # Use current_snapshot_id if available, otherwise use a temporary ID
        snap_id = (
            metadata.current_snapshot_id if metadata.current_snapshot_id is not None else "temp"
        )
        manifest_list_file = f"manifests/snap-{timestamp}-{snap_id}.avro"
        manifest_list_path = os.path.join(self.table_path, manifest_list_file)

        # Ensure the manifests directory exists
        os.makedirs(os.path.join(self.table_path, "manifests"), exist_ok=True)

        # In a real implementation, this would create an Avro manifest list file
        # For now, we'll create a simple JSON placeholder
        manifest_list_data: Dict[str, Any] = {
            "manifests": [],
            "snapshot_id": metadata.current_snapshot_id,
            "timestamp": metadata.last_updated_ms,
        }

        with open(manifest_list_path, "w") as f:
            json.dump(manifest_list_data, f)

        return manifest_list_path

    def _create_manifest_list_with_id(
        self,
        metadata: TableMetadata,
        snapshot_id: int,
        data_files: Optional[List[DataFile]] = None,
    ) -> str:
        """Create a manifest list file for a specific snapshot ID"""
        if data_files is None:
            data_files = []

        # Validate that all data files exist before creating manifests
        if data_files:
            self.file_manager.validate_data_files(data_files)

        # Create manifest files for the data files
        if data_files:
            # Create one manifest file containing all these data files
            manifest_file = self.file_manager.create_manifest_file(
                data_files, ManifestContent.DATA, snapshot_id
            )

            # Create the manifest list containing this manifest
            manifest_list_path = self.file_manager.create_manifest_list_file(
                [manifest_file], snapshot_id
            )
        else:
            # Create empty manifest list
            manifest_list_path = self.file_manager.create_manifest_list_file([], snapshot_id)

        return manifest_list_path

    def _deep_copy_metadata(self, metadata: TableMetadata) -> TableMetadata:
        """Create a deep copy of metadata for transaction isolation"""
        import copy

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
            completed_ids = []
            for tx_id, tx in self._active_transactions.items():
                if not tx.is_active():
                    completed_ids.append(tx_id)

            for tx_id in completed_ids:
                del self._active_transactions[tx_id]


class Table:
    """Main table interface with transaction support"""

    def __init__(self, table_path: str, create_if_not_exists: bool = True):
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

        # Initialize if needed
        if create_if_not_exists and not self.storage.exists("metadata"):
            self._initialize_table()

    def _initialize_table(self) -> None:
        """Initialize a new table with default metadata"""
        from .data_structures import TableMetadata

        initial_metadata = TableMetadata(location=self.table_path)
        self.metadata_manager.initialize_table(initial_metadata)

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
        """Time travel to a specific snapshot or timestamp"""
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

        Args:
            grace_period_ms: Only delete orphaned files older than this age (default 1 hour).
                             This protects against deleting files currently being written.

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

    def scan(
        self,
        columns: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        parallel: Union[bool, int] = False,
    ) -> List[Dict[str, Any]]:
        """Scan all data in the table and return as list of records.

        This method reads all parquet files in the data directory, providing
        a complete view of all data across all snapshots.

        Args:
            columns: Optional list of column names to read. If None, reads all columns.
            filter: Optional filter dict for predicate pushdown.
                Examples:
                    {"status": "failed"}              # status == "failed"
                    {"age": (">", 18)}                # age > 18
                    {"id": ("in", [1, 2, 3])}         # id in [1, 2, 3]
                    {"ts": ("between", (t1, t2))}     # t1 <= ts <= t2
            parallel: Enable parallel reading.
                - False: Sequential reading (default)
                - True: Use all CPU cores
                - int: Use specified number of threads

        Returns:
            List of dictionaries, each representing a record.
        """
        from concurrent.futures import ThreadPoolExecutor

        import pyarrow as pa
        import pyarrow.parquet as pq

        from .filters import parse_filter_dict, prune_files_by_bounds, to_pyarrow_filter

        return self._scan_impl(
            columns=columns,
            filter_dict=filter,
            parallel=parallel,
            parse_filter_dict=parse_filter_dict,
            prune_files_by_bounds=prune_files_by_bounds,
            to_pyarrow_filter=to_pyarrow_filter,
            pa=pa,
            pq=pq,
            ThreadPoolExecutor=ThreadPoolExecutor,
        )

    def _scan_impl(
        self,
        columns: Optional[List[str]],
        filter_dict: Optional[Dict[str, Any]],
        parallel: Union[bool, int],
        parse_filter_dict: Any,
        prune_files_by_bounds: Any,
        to_pyarrow_filter: Any,
        pa: Any,
        pq: Any,
        ThreadPoolExecutor: Any,
    ) -> List[Dict[str, Any]]:
        """Internal implementation of scan logic."""
        # Get ALL data files from ALL snapshots for full table scan
        # Note: current_snapshot() only returns latest snapshot's manifest,
        # but we need files from ALL snapshots to get complete data.
        data_files = self._get_all_data_files()

        # Parse filter if provided
        expressions = []
        pa_filters = None
        if filter_dict:
            expressions = parse_filter_dict(filter_dict)
            pa_filters = to_pyarrow_filter(expressions)

        # Apply partition pruning if we have manifest data with bounds
        if expressions and data_files:
            schema = self._get_current_schema()
            if schema:
                data_files = prune_files_by_bounds(data_files, expressions, schema)

        # Get file paths - use PyArrow-compatible paths for S3
        data_file_manager = self.file_manager.data_file_manager
        pyarrow_fs = data_file_manager._pyarrow_fs
        parquet_files = self._get_parquet_files_arrow(data_files, data_file_manager)

        if not parquet_files:
            return []

        # Define file reader function with filesystem support for S3
        def read_file(file_path: str) -> Optional[pa.Table]:
            try:
                if pa_filters:
                    return pq.read_table(file_path, columns=columns, filters=pa_filters, filesystem=pyarrow_fs)
                else:
                    return pq.read_table(file_path, columns=columns, filesystem=pyarrow_fs)
            except Exception:
                return None

        # Read files (parallel or sequential)
        if parallel:
            n_workers = parallel if isinstance(parallel, int) else (os.cpu_count() or 4)
            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                tables = list(executor.map(read_file, parquet_files))
        else:
            tables = [read_file(f) for f in parquet_files]

        # Filter out None results
        tables = [t for t in tables if t is not None]

        if not tables:
            return []

        # Concatenate all tables
        combined = pa.concat_tables(tables)
        result: List[Dict[str, Any]] = combined.to_pylist()
        return result

    def _get_parquet_files(self, data_files: List[DataFile]) -> List[str]:
        """Get list of parquet file paths from data files."""
        if data_files:
            return [self._resolve_file_path(df.file_path) for df in data_files]
        return []

    def _get_parquet_files_arrow(self, data_files: List[DataFile], data_file_manager: Any) -> List[str]:
        """Get list of parquet file paths converted for PyArrow (S3-compatible).

        For S3 storage, paths are converted to bucket/key format.
        For local storage, paths are resolved to absolute paths.
        """
        if not data_files:
            return []
        result = []
        for df in data_files:
            # Use DataFileManager's path conversion for PyArrow compatibility
            arrow_path = data_file_manager._get_arrow_path(df.file_path)
            result.append(arrow_path)
        return result

    def to_pandas(
        self,
        columns: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        parallel: Union[bool, int] = False,
    ) -> Any:
        """Read all data from the table as a pandas DataFrame.

        This method reads all parquet files in the data directory, providing
        a complete view of all data across all snapshots.

        Args:
            columns: Optional list of column names to read. If None, reads all columns.
            filter: Optional filter dict for predicate pushdown.
                Examples:
                    {"status": "failed"}              # status == "failed"
                    {"age": (">", 18)}                # age > 18
                    {"id": ("in", [1, 2, 3])}         # id in [1, 2, 3]
                    {"ts": ("between", (t1, t2))}     # t1 <= ts <= t2
            parallel: Enable parallel reading.
                - False: Sequential reading (default)
                - True: Use all CPU cores
                - int: Use specified number of threads

        Returns:
            pandas DataFrame with all records.

        Raises:
            ImportError: If pandas is not installed.
        """
        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError(
                "pandas is required for to_pandas(). Install with: pip install pandas"
            ) from e

        from concurrent.futures import ThreadPoolExecutor

        import pyarrow as pa
        import pyarrow.parquet as pq

        from .filters import parse_filter_dict, prune_files_by_bounds, to_pyarrow_filter

        # Get ALL data files from ALL snapshots for full table scan
        data_files = self._get_all_data_files()

        # Parse filter if provided
        expressions = []
        pa_filters = None
        if filter:
            expressions = parse_filter_dict(filter)
            pa_filters = to_pyarrow_filter(expressions)

        # Apply partition pruning if we have manifest data with bounds
        if expressions and data_files:
            schema = self._get_current_schema()
            if schema:
                data_files = prune_files_by_bounds(data_files, expressions, schema)

        # Get file paths - use PyArrow-compatible paths for S3
        data_file_manager = self.file_manager.data_file_manager
        pyarrow_fs = data_file_manager._pyarrow_fs
        parquet_files = self._get_parquet_files_arrow(data_files, data_file_manager)

        if not parquet_files:
            return pd.DataFrame()

        # Define file reader function with filesystem support for S3
        def read_file(file_path: str) -> Optional[pa.Table]:
            try:
                if pa_filters:
                    return pq.read_table(file_path, columns=columns, filters=pa_filters, filesystem=pyarrow_fs)
                else:
                    return pq.read_table(file_path, columns=columns, filesystem=pyarrow_fs)
            except Exception:
                return None

        # Read files (parallel or sequential)
        if parallel:
            n_workers = parallel if isinstance(parallel, int) else (os.cpu_count() or 4)
            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                tables = list(executor.map(read_file, parquet_files))
        else:
            tables = [read_file(f) for f in parquet_files]

        # Filter out None results
        tables = [t for t in tables if t is not None]

        if not tables:
            return pd.DataFrame()

        # Concatenate and convert to pandas
        combined = pa.concat_tables(tables)
        return combined.to_pandas()

    def scan_batches(
        self,
        batch_size: int = 10000,
        columns: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
    ) -> Iterator[List[Dict[str, Any]]]:
        """Scan data in batches for memory-efficient processing.

        Yields batches of records, processing one parquet file at a time
        using PyArrow's iter_batches for memory efficiency.

        Args:
            batch_size: Approximate number of records per batch
            columns: Optional column projection
            filter: Optional predicate pushdown filter

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

        # Get ALL data files from ALL snapshots for full table scan
        data_files = self._get_all_data_files()

        # Parse filter
        expressions = []
        if filter:
            expressions = parse_filter_dict(filter)

            # Apply partition pruning
            if data_files:
                schema = self._get_current_schema()
                if schema:
                    data_files = prune_files_by_bounds(data_files, expressions, schema)

        # Get file paths - use PyArrow-compatible paths for S3
        data_file_manager = self.file_manager.data_file_manager
        pyarrow_fs = data_file_manager._pyarrow_fs
        parquet_files = self._get_parquet_files_arrow(data_files, data_file_manager)

        if not parquet_files:
            return

        # Build compute expression for filtering
        compute_expr = to_pyarrow_compute_expression(expressions) if expressions else None

        # Process files one at a time
        yield from self._iter_file_batches(
            parquet_files, batch_size, columns, compute_expr, pa, pq, pyarrow_fs
        )

    def _iter_file_batches(
        self,
        parquet_files: List[str],
        batch_size: int,
        columns: Optional[List[str]],
        compute_expr: Any,
        pa: Any,
        pq: Any,
        filesystem: Any = None,
    ) -> Iterator[List[Dict[str, Any]]]:
        """Iterate over batches from parquet files."""
        for file_path in parquet_files:
            try:
                pf = pq.ParquetFile(file_path, filesystem=filesystem)

                for batch in pf.iter_batches(batch_size=batch_size, columns=columns):
                    # Convert batch to table for filtering
                    table = pa.Table.from_batches([batch])

                    # Apply filter if needed
                    if compute_expr is not None:
                        table = table.filter(compute_expr)

                    if table.num_rows > 0:
                        yield table.to_pylist()
            except Exception:
                continue

    def iter_records(
        self,
        columns: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Iterate over records one at a time.

        Memory efficient - only one record in memory at a time.
        Ideal for row-by-row processing of large tables.

        Args:
            columns: Optional column projection
            filter: Optional predicate pushdown filter

        Yields:
            Individual records as dicts
        """
        for batch in self.scan_batches(batch_size=1000, columns=columns, filter=filter):
            for record in batch:
                yield record

    def iter_pandas(
        self,
        chunksize: int = 50000,
        columns: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Any]:
        """Iterate over data as pandas DataFrame chunks.

        Memory efficient - only one chunk in memory at a time.
        Ideal for processing large tables with pandas operations.

        Args:
            chunksize: Approximate rows per chunk
            columns: Optional column projection
            filter: Optional predicate pushdown filter

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

        for batch in self.scan_batches(batch_size=chunksize, columns=columns, filter=filter):
            yield pd.DataFrame(batch)

    def _get_all_data_files(self) -> List[DataFile]:
        """Get ALL data files from current snapshot.

        Correctly uses the Iceberg model where the current snapshot's manifest list
        contains references to ALL valid data in the table.
        """
        snapshot = self.current_snapshot()
        if not snapshot:
            return []

        try:
            manifest_list_path = snapshot.manifest_list
            if manifest_list_path.startswith("/"):
                manifest_list_path = manifest_list_path.lstrip("/")

            if not self.storage.exists(manifest_list_path):
                return []

            # Use FileManager to read manifest list
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
                    continue

                # Read manifest file
                manifest_data_files = self.file_manager.read_manifest_file(manifest_path)

                for data_file in manifest_data_files:
                    file_path = data_file.file_path
                    if file_path in seen_paths:
                        continue
                    seen_paths.add(file_path)
                    all_data_files.append(data_file)

            return all_data_files

        except Exception:
            return []

    def _get_data_files_from_manifest(self) -> List[DataFile]:
        """Get data files from current snapshot's manifest only.

        Use this for operations that only need the latest snapshot's data.
        For full table scan, use _get_all_data_files() instead.

        Returns:
            List of DataFile objects from the current snapshot's manifest,
            or empty list if no snapshot exists.
        """
        snapshot = self.current_snapshot()
        if not snapshot:
            return []

        try:
            # Read manifest list
            manifest_list_path = snapshot.manifest_list
            if manifest_list_path.startswith("/"):
                manifest_list_path = manifest_list_path.lstrip("/")

            if not self.storage.exists(manifest_list_path):
                return []

            # Use FileManager to read manifest list (supports Avro)
            manifest_files = self.file_manager.read_manifest_list_file(manifest_list_path)

            data_files = []
            for manifest_ref in manifest_files:
                manifest_path = manifest_ref.manifest_path
                if not manifest_path:
                    continue

                if not self.storage.exists(manifest_path):
                    continue

                # Use FileManager to read manifest file (supports Avro)
                manifest_data_files = self.file_manager.read_manifest_file(manifest_path)
                data_files.extend(manifest_data_files)

            return data_files
        except Exception:
            return []

    def _get_current_schema(self) -> Optional[Schema]:
        """Get the current schema from metadata.

        Returns:
            Current Schema object, or None if not available.
        """
        try:
            metadata = self.metadata_manager.refresh()
            if metadata and metadata.schemas:
                # Find current schema by ID
                for schema in metadata.schemas:
                    if schema.schema_id == metadata.current_schema_id:
                        return schema
                # Fallback to first schema
                return metadata.schemas[0]
        except Exception:
            pass
        return None

    def _resolve_file_path(self, file_path: str) -> str:
        """Resolve a file path to absolute path.

        Handles Iceberg-style paths that start with '/' (relative to table).

        Args:
            file_path: File path (possibly Iceberg-style starting with '/')

        Returns:
            Absolute file path
        """
        if file_path.startswith("/"):
            # Iceberg-style path relative to table location
            return os.path.join(self.table_path, file_path.lstrip("/"))
        else:
            # Already relative or absolute
            if os.path.isabs(file_path):
                return file_path
            return os.path.join(self.table_path, file_path)
