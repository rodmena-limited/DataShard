# Python Iceberg Implementation

This is a simplified but powerful Python implementation of Apache Iceberg that provides core functionality including ACID transactions, time travel, metadata management, and proper file system operations.

## Features

- **ACID Transactions**: Supports atomic, consistent, isolated, and durable transactions
- **Time Travel**: Ability to query table state at any point in time
- **Metadata Management**: Comprehensive metadata tracking and persistence
- **Snapshotting**: Point-in-time snapshots of table state
- **File Management**: Proper file system operations, validation, and manifest management
- **Data Operations**: Reading and writing actual data files (Parquet) with schema management

## Architecture

The implementation consists of several key components:

### Core Data Structures
- `TableMetadata`: Contains table schema, partition spec, sort order, properties, and snapshot information
- `Snapshot`: Represents a point-in-time view of the table
- `DataFile`: Represents individual data files in the table
- `ManifestFile`: Lists data files and their metadata
- `Schema`: Defines the table schema
- `PartitionSpec`: Defines how the table is partitioned

### Core Components
- `MetadataManager`: Handles metadata persistence and updates
- `SnapshotManager`: Manages snapshots and time travel functionality
- `Transaction`: Provides ACID transaction support
- `Table`: Main table interface that ties everything together

## Usage

### Creating a Table

```python
from rewrite.iceberg import create_table, DataFile, FileFormat

# Create a new table
table = create_table("/path/to/your/table")

# Add data files to the table using a transaction
from rewrite.data_structures import DataFile, FileFormat

data_files = [
    DataFile(
        file_path="/data/file1.parquet",
        file_format=FileFormat.PARQUET,
        partition_values={"year": 2023, "month": 1},
        record_count=1000,
        file_size_in_bytes=102400
    )
]

# Use a transaction to add files
with table.new_transaction() as tx:
    tx.append_files(data_files)
    tx.commit()
```

### Time Travel

```python
# Get the current snapshot
current = table.current_snapshot()

# Get a snapshot by ID
snapshot = table.snapshot_by_id(123456789)

# Get all snapshots
snapshots = table.snapshots()

# Time travel to a specific snapshot
traveled_snapshot = table.time_travel(snapshot_id=123456789)

# Time travel to a snapshot as of a specific time
from datetime import datetime
timestamp_ms = int(datetime.now().timestamp() * 1000)
traveled_snapshot = table.time_travel(timestamp=timestamp_ms)
```

### Transactions

```python
# Create a transaction
with table.new_transaction() as tx:
    # Add files
    tx.append_files([data_file1, data_file2])
    
    # Perform other operations
    tx.expire_snapshots(older_than_ms=1609459200000)  # Jan 1, 2021
    
    # Transaction commits automatically when exiting the 'with' block
    # or explicitly call tx.commit()
```

## Data Operations

The implementation includes full data file operations:

- **Parquet Support**: Reading and writing Parquet files using PyArrow
- **Schema Conversion**: Automatic conversion between Iceberg and PyArrow schemas
- **Data Validation**: Verification of data compatibility with schema
- **File Readers/Writers**: Efficient reading and writing of data files
- **Transaction Integration**: Data operations integrated with ACID transactions

## Storage Format

The implementation stores metadata in JSON format following Iceberg's metadata evolution principles:

- `metadata/`: Contains versioned metadata files (v0.json, v1.json, etc.)
- `metadata.version-hint.text`: Points to the current metadata version
- `snapshots/`: Contains snapshot-specific information
- `manifests/`: Contains manifest list files for each snapshot

## ACID Properties

1. **Atomicity**: All transactions are atomic - they either complete entirely or are rolled back completely
2. **Consistency**: Metadata is validated on each update to maintain consistency
3. **Isolation**: Achieved through Optimistic Concurrency Control with base state comparison and retry logic
4. **Durability**: All metadata changes are immediately persisted to disk

## Limitations

This is a simplified implementation focusing on the core Iceberg concepts. Some advanced features of Apache Iceberg are not implemented:

- Advanced partition transforms beyond basic ones
- Row-level deletes beyond file-level deletes
- Advanced optimization operations like file compaction
- Encryption support

## Optimistic Concurrency Control (OCC)

This implementation properly follows Apache Iceberg's atomic commit pattern:

- Uses base metadata comparison to detect concurrent modifications
- Implements automatic retry logic with backoff for conflicts
- Provides proper exception handling when operations cannot be resolved
- Maintains consistency in concurrent environments without distributed locks
- Follows the same atomic commit patterns as the Apache Iceberg Java implementation

## Testing

Run the comprehensive test suite:

```bash
cd rewrite
python test_iceberg.py
```