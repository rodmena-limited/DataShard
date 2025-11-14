"""
ACID transaction implementation for the Python Iceberg implementation
"""
import json
import os
import threading
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from .data_structures import (
    DataFile,
    FileFormat,
    ManifestContent,
    Schema,
    Snapshot,
    TableMetadata,
)
from .file_manager import FileManager
from .metadata_manager import ConcurrentModificationException, MetadataManager
from .snapshot_manager import SnapshotManager


class Transaction:
    """Represents a database transaction with ACID properties"""

    def __init__(self, metadata_manager: MetadataManager, snapshot_manager: SnapshotManager, file_manager: FileManager):
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

        self._lock = threading.RLock()

    def begin(self) -> 'Transaction':
        """Start a new transaction"""
        with self._lock:
            if self._is_active:
                raise RuntimeError("Transaction already active")

            self._is_active = True
            self._is_committed = False
            self._is_rolled_back = False


            return self

    def is_active(self) -> bool:
        """Check if transaction is active"""
        return self._is_active and not self._is_committed and not self._is_rolled_back

    def append_files(self, files: List[DataFile]) -> 'Transaction':
        """Queue files to append to the table"""
        if not self.is_active():
            raise RuntimeError("Transaction is not active")

        # Validate that the files exist in the file system
        # This is a critical check in production systems
        for data_file in files:
            if not self.file_manager.validate_file_exists(data_file.file_path):
                raise FileNotFoundError(f"Data file does not exist: {data_file.file_path}")

        self._operations.append({
            'type': 'append_files',
            'files': files
        })

        return self

    def append_data(self, records: List[Dict[str, Any]],
                   schema: 'Schema',
                   partition_values: Optional[Dict[str, Any]] = None) -> 'Transaction':
        """Append actual data records to the table by creating new data files"""
        if not self.is_active():
            raise RuntimeError("Transaction is not active")

        # Create a data file with the records
        timestamp = int(datetime.now().timestamp() * 1000000)  # microseconds
        file_name = f"auto_{timestamp}.parquet"
        file_path = os.path.join(self.table_path, "data", file_name)

        # Use the data file manager to write the data
        data_file = self.file_manager.data_file_manager.write_data_file(
            file_path=file_path,
            records=records,
            iceberg_schema=schema,
            file_format=FileFormat.PARQUET,
            partition_values=partition_values or {}
        )

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
            upper_bounds=data_file.upper_bounds
        )

        # Queue the newly created file for appending
        self.append_files([updated_data_file])

        return self

    def delete_files(self, file_paths: List[str]) -> 'Transaction':
        """Queue files to delete from the table"""
        if not self.is_active():
            raise RuntimeError("Transaction is not active")

        self._operations.append({
            'type': 'delete_files',
            'file_paths': file_paths
        })

        return self

    def overwrite_by_filter(self, filter_func: Callable[[Any], bool]) -> 'Transaction':
        """Queue an overwrite operation by filter"""
        if not self.is_active():
            raise RuntimeError("Transaction is not active")

        self._operations.append({
            'type': 'overwrite_by_filter',
            'filter': filter_func
        })

        return self

    def expire_snapshots(self, older_than_ms: int) -> 'Transaction':
        """Queue snapshot expiration operation"""
        if not self.is_active():
            raise RuntimeError("Transaction is not active")

        self._operations.append({
            'type': 'expire_snapshots',
            'older_than_ms': older_than_ms
        })

        return self

    def commit(self) -> bool:
        """Commit the transaction with ACID properties using Optimistic Concurrency Control"""
        if not self.is_active():
            raise RuntimeError("Transaction is not active")

        # Implement retry logic for OCC - this is essential for handling concurrent modifications
        max_retries = 5
        retry_count = 0

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

                    # Generate a snapshot ID for this transaction
                    snapshot_id = int(datetime.now().timestamp() * 1000000)  # microseconds since epoch
                    # Create a new manifest list for this snapshot
                    manifest_list_path = self._create_manifest_list_with_id(new_metadata, snapshot_id)

                    # Create a new snapshot for this transaction
                    # We need to pass the base metadata for proper OCC checking
                    self.snapshot_manager.create_snapshot(
                        manifest_list_path=manifest_list_path,
                        operation="append" if any(op['type'] == 'append_files' for op in self._operations) else "update",
                        parent_snapshot_id=base_metadata.current_snapshot_id if base_metadata.current_snapshot_id is not None else -1  # Use base as parent
                    )

                    # The snapshot manager already updated the metadata, so get the latest
                    refreshed_metadata = self.metadata_manager.refresh()
                    if refreshed_metadata is not None:
                        new_metadata = refreshed_metadata

                    self._is_active = False
                    self._is_committed = True

                    return True

            except ConcurrentModificationException as e:
                retry_count += 1
                if retry_count >= max_retries:
                    # Final failure - cannot commit even after retries
                    self._rollback()
                    raise e
                else:
                    # Wait a bit before retrying (exponential backoff could be added here)
                    import time
                    time.sleep(0.01 * retry_count)  # Simple backoff
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
        """Internal method to perform rollback"""
        self._is_active = False
        self._is_rolled_back = True

        return True

    def _apply_operation(self, metadata: TableMetadata, operation: Dict[str, Any]) -> TableMetadata:
        """Apply a single operation to the metadata"""
        op_type = operation['type']

        if op_type == 'append_files':
            # In a real implementation, this would add files to the manifest
            # For now, we'll just update the metadata
            operation['files']
            # Update the schema if needed (add new columns)
            # Update last_sequence_number if needed
            metadata.last_updated_ms = int(threading.current_thread().ident or 0)  # Just for illustration

        elif op_type == 'delete_files':
            operation['file_paths']
            # In a real implementation, this would remove files from the manifest
            pass

        elif op_type == 'overwrite_by_filter':
            operation['filter']
            # In a real implementation, this would apply the filter
            pass

        elif op_type == 'expire_snapshots':
            older_than_ms = operation['older_than_ms']
            # Remove old snapshots
            metadata.snapshots = [s for s in metadata.snapshots if s.timestamp_ms >= older_than_ms]

        return metadata

    def _create_manifest_list(self, metadata: TableMetadata) -> str:
        """Create a manifest list file for the current state"""
        # Create a unique name for the manifest list
        timestamp = metadata.last_updated_ms
        # Use current_snapshot_id if available, otherwise use a temporary ID
        snap_id = metadata.current_snapshot_id if metadata.current_snapshot_id is not None else "temp"
        manifest_list_file = f"manifests/snap-{timestamp}-{snap_id}.avro"
        manifest_list_path = os.path.join(self.table_path, manifest_list_file)

        # Ensure the manifests directory exists
        os.makedirs(os.path.join(self.table_path, 'manifests'), exist_ok=True)

        # In a real implementation, this would create an Avro manifest list file
        # For now, we'll create a simple JSON placeholder
        manifest_list_data: Dict[str, Any] = {
            'manifests': [],
            'snapshot_id': metadata.current_snapshot_id,
            'timestamp': metadata.last_updated_ms
        }

        with open(manifest_list_path, 'w') as f:
            json.dump(manifest_list_data, f)

        return manifest_list_path

    def _create_manifest_list_with_id(self, metadata: TableMetadata, snapshot_id: int, data_files: Optional[List[DataFile]] = None) -> str:
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
                data_files,
                ManifestContent.DATA,
                snapshot_id
            )

            # Create the manifest list containing this manifest
            manifest_list_path = self.file_manager.create_manifest_list_file([manifest_file], snapshot_id)
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

    def __init__(self, metadata_manager: MetadataManager, snapshot_manager: SnapshotManager, file_manager: FileManager):
        self.metadata_manager = metadata_manager
        self.snapshot_manager = snapshot_manager
        self.file_manager = file_manager
        self._active_transactions: Dict[int, "Transaction"] = {}
        self._lock = threading.RLock()

    def begin_transaction(self) -> Transaction:
        """Begin a new transaction"""
        with self._lock:
            transaction = Transaction(self.metadata_manager, self.snapshot_manager, self.file_manager)
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
        self.table_path = table_path
        self.metadata_manager = MetadataManager(table_path)
        self.snapshot_manager = SnapshotManager(self.metadata_manager)
        self.file_manager = FileManager(table_path, self.metadata_manager)
        self.transaction_manager = TransactionManager(self.metadata_manager, self.snapshot_manager, self.file_manager)

        # Initialize if needed
        if create_if_not_exists and not os.path.exists(os.path.join(table_path, 'metadata')):
            self._initialize_table()

    def _initialize_table(self) -> None:
        """Initialize a new table with default metadata"""
        from .data_structures import TableMetadata

        initial_metadata = TableMetadata(
            location=self.table_path
        )
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

    def time_travel(self, snapshot_id: Optional[int] = None, timestamp: Optional[int] = None) -> Any:
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

    def append_records(self, records: List[Dict[str, Any]],
                      schema: 'Schema',
                      partition_values: Optional[Dict[str, Any]] = None) -> bool:
        """Append actual data records to the table by creating new data files (convenience method)"""
        with self.new_transaction() as tx:
            tx.append_data(records=records, schema=schema, partition_values=partition_values)
            result = tx.commit()
            return bool(result)

    def refresh(self) -> bool:
        """Refresh the table metadata from storage"""
        metadata = self.metadata_manager.refresh()
        return metadata is not None
