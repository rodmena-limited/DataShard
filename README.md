# DataShard

```
                  //
                 //
                //
               //
              //
             //
            //
           //
          //
         //
        //
       //
      //___________________
     //                   /
    //___________________/

       D A T A S H A R D
```

**Iceberg-Inspired Safe Concurrent Data Operations for Python**

---

## What is DataShard?

DataShard is a Python implementation of Apache Iceberg's core concepts, providing ACID-compliant data operations for ML/AI workloads. It ensures your data stays safe even when multiple processes read and write simultaneously.

### Key Features

- **ACID Transactions:** Operations fully complete or fully rollback
- **Time Travel:** Query data as it existed at any point in time
- **Safe Concurrency:** Multiple processes can write without corruption
- **Optimistic Concurrency Control (OCC):** Automatic conflict resolution
- **S3-Compatible Storage:** AWS S3, MinIO, DigitalOcean Spaces support
- **Distributed Workflows:** Run workers across different machines with shared S3 storage
- **Predicate Pushdown:** Filter at parquet level for 90%+ I/O reduction (NEW in v0.3.3)
- **Partition Pruning:** Skip files based on column statistics (NEW in v0.3.3)
- **Parallel Reading:** Multi-threaded scan for 2-4x speedup (NEW in v0.3.3)
- **Streaming API:** Memory-efficient iteration over large tables (NEW in v0.3.3)
- **Pure Python:** No Java dependencies, easy setup
- **pandas Integration:** Native DataFrame support

---

## Why DataShard Matters: The Data Corruption Problem

**The Problem:** Regular file operations in Python are NOT safe for concurrent access.

When multiple processes write to the same file, you get:
- Data loss from race conditions
- Partial writes that corrupt your data
- Silent failures that go unnoticed
- Unpredictable results

### Our Stress Test Results

**Test Setup:** 12 processes, each performing 10,000 operations
**Expected Result:** 120,000 total operations

**Regular Files:**
- Actual Result: Only 8,566 operations completed
- Data Loss: 111,434 operations LOST (93% failure rate!)
- Cause: Race conditions from simultaneous file access

**DataShard (Iceberg):**
- Actual Result: 120,000 operations completed
- Data Loss: ZERO (0% failure rate)
- Cause: ACID transactions with OCC prevent all race conditions

---
Already used in Highway Workflow Engine:

![DataShard Screenshot](docs/Screenshot%202025-12-03%20at%2004.24.10.png)

---

## Installation

Basic installation:
```bash
pip install datashard
```

With pandas support (recommended):
```bash
pip install datashard[pandas]
```

With S3 support (AWS S3, MinIO, etc.):
```bash
pip install datashard[s3]
```

With all features:
```bash
pip install datashard[pandas,s3]
```

With development tools:
```bash
pip install datashard[dev]
```

---

## Quick Start

```python
from datashard import create_table, Schema

# 1. Define your schema
schema = Schema(
    schema_id=1,
    fields=[
        {"id": 1, "name": "user_id", "type": "long", "required": True},
        {"id": 2, "name": "event", "type": "string", "required": True},
        {"id": 3, "name": "timestamp", "type": "long", "required": True}
    ]
)

# 2. Create a table
table = create_table("/path/to/my_table", schema)

# 3. Append records safely (even from multiple processes!)
records = [
    {"user_id": 100, "event": "login", "timestamp": 1700000000},
    {"user_id": 101, "event": "logout", "timestamp": 1700000100}
]
success = table.append_records(records, schema)

# 4. Query current data
snapshot = table.current_snapshot()
print(f"Snapshot ID: {snapshot.snapshot_id}")

# 5. Time travel to previous state
old_snapshot = table.snapshot_by_id(previous_snapshot_id)
```

---

## Real-World Use Case: Workflow Execution Logging

### Problem

A distributed workflow engine runs 100s of tasks across multiple workers. Each task needs to log its execution details (status, duration, errors) to a central location. Traditional approaches fail:

- **Database:** Too slow, adds latency to task execution
- **Log Files:** Corrupted by concurrent writes, not queryable
- **JSON Files:** Race conditions cause data loss

### Solution: DataShard as a Queryable Audit Log

DataShard provides ACID-compliant logging with pandas-queryable tables:

```python
from datashard import create_table, Schema
import os
import pandas as pd

# Define task log schema
task_log_schema = Schema(
    schema_id=1,
    fields=[
        {"id": 1, "name": "task_id", "type": "string", "required": True},
        {"id": 2, "name": "workflow_id", "type": "string", "required": True},
        {"id": 3, "name": "status", "type": "string", "required": True},
        {"id": 4, "name": "started_at", "type": "long", "required": True},
        {"id": 5, "name": "completed_at", "type": "long", "required": True},
        {"id": 6, "name": "duration_ms", "type": "long", "required": True},
        {"id": 7, "name": "error_message", "type": "string", "required": False},
        {"id": 8, "name": "worker_id", "type": "string", "required": True}
    ]
)

# Create task logs table (shared across all workers)
task_logs = create_table("/shared/storage/task_logs", task_log_schema)

# Worker 1: Log successful task execution
task_logs.append_records([{
    "task_id": "task-001",
    "workflow_id": "wf-123",
    "status": "success",
    "started_at": 1700000000000,
    "completed_at": 1700000150000,
    "duration_ms": 150000,
    "error_message": "",
    "worker_id": "worker-1"
}], task_log_schema)

# Worker 2: Log failed task (concurrent with Worker 1)
task_logs.append_records([{
    "task_id": "task-002",
    "workflow_id": "wf-123",
    "status": "failed",
    "started_at": 1700000000000,
    "completed_at": 1700000200000,
    "duration_ms": 200000,
    "error_message": "Connection timeout",
    "worker_id": "worker-2"
}], task_log_schema)

# Query logs with pandas (from any machine)
from datashard import load_table
from datashard.file_manager import FileManager

table = load_table("/shared/storage/task_logs")
snapshot = table.current_snapshot()
file_manager = FileManager(table.table_path, table.metadata_manager)

# Read manifest list
manifest_list_path = os.path.join(table.table_path, snapshot.manifest_list)
manifests = file_manager.read_manifest_list_file(manifest_list_path)

# Read all data files
data_frames = []
for manifest_file in manifests:
    manifest_path = os.path.join(table.table_path, manifest_file.manifest_path)
    data_files = file_manager.read_manifest_file(manifest_path)

    for data_file in data_files:
        parquet_path = os.path.join(table.table_path, data_file.file_path.lstrip('/'))
        df = pd.read_parquet(parquet_path)
        data_frames.append(df)

logs_df = pd.concat(data_frames, ignore_index=True)

# Analyze logs
failed_tasks = logs_df[logs_df['status'] == 'failed']
avg_duration = logs_df['duration_ms'].mean()
print(f"Failed tasks: {len(failed_tasks)}")
print(f"Average duration: {avg_duration}ms")
```

### Why DataShard Wins for Logging

- **Safe Concurrent Writes:** 100 workers can log simultaneously without corruption
- **Queryable with pandas:** Analyze logs with familiar DataFrame operations
- **Time Travel:** Debug by querying logs as they existed at incident time
- **ACID Guarantees:** Never lose log entries, even during crashes
- **Scalable:** Handles millions of log entries efficiently
- **No Database Needed:** File-based storage, no DB maintenance overhead

---

## Other Real-World Use Cases

### 1. ML Training Metrics
- Multiple training runs logging metrics simultaneously
- Query metrics history for model comparison
- Time travel to analyze model performance at specific epochs

### 2. A/B Test Results
- Concurrent experiments writing results safely
- Aggregate results across all test variants
- Historical analysis of test performance

### 3. Data Pipeline Checkpointing
- Multiple pipeline stages writing progress markers
- Safe recovery from failures using checkpoints
- Query pipeline state for monitoring

### 4. Feature Store
- Multiple processes computing and storing features
- Time travel for point-in-time feature retrieval
- Consistent feature snapshots for reproducibility

---

## S3-Compatible Storage (NEW in v0.2.2)

DataShard now supports S3-compatible storage backends, enabling truly distributed workflows across multiple machines without shared filesystems.

### Supported Storage Backends

- **AWS S3** - Amazon's Simple Storage Service
- **MinIO** - Self-hosted S3-compatible storage
- **DigitalOcean Spaces** - Cloud object storage
- **Wasabi** - S3-compatible cloud storage
- **Any S3-compatible API** - Including on-premise solutions

### Configuration

DataShard automatically selects the storage backend based on environment variables:

```bash
# Local filesystem (default - no configuration needed)
# Just use create_table("/path/to/table", schema)

# S3-compatible storage
export DATASHARD_STORAGE_TYPE=s3
export DATASHARD_S3_ENDPOINT=https://s3.amazonaws.com  # AWS S3
export DATASHARD_S3_ACCESS_KEY=your-access-key
export DATASHARD_S3_SECRET_KEY=your-secret-key
export DATASHARD_S3_BUCKET=my-datashard-bucket
export DATASHARD_S3_REGION=us-east-1
export DATASHARD_S3_PREFIX=optional/prefix/  # Optional: namespace within bucket
```

### Usage Example: Distributed Workflow Logging

**Scenario:** Workers running on different EC2 instances need to log to the same table.

```python
from datashard import create_table, load_table, Schema

# Define schema (same across all workers)
schema = Schema(
    schema_id=1,
    fields=[
        {"id": 1, "name": "task_id", "type": "string", "required": True},
        {"id": 2, "name": "worker_id", "type": "string", "required": True},
        {"id": 3, "name": "status", "type": "string", "required": True},
        {"id": 4, "name": "timestamp", "type": "long", "required": True},
    ]
)

# Worker 1 on EC2 instance us-east-1a
table = create_table("task-logs", schema)  # Creates in S3
table.append_records([
    {"task_id": "t1", "worker_id": "worker-1", "status": "success", "timestamp": 1700000000}
], schema)

# Worker 2 on EC2 instance us-east-1b (simultaneously)
table = load_table("task-logs")  # Loads from S3
table.append_records([
    {"task_id": "t2", "worker_id": "worker-2", "status": "success", "timestamp": 1700000001}
], schema)

# Both writes succeed with ACID guarantees!
# No shared filesystem required - just S3 access
```

### MinIO Example (Self-Hosted)

```bash
# Configure MinIO endpoint
export DATASHARD_STORAGE_TYPE=s3
export DATASHARD_S3_ENDPOINT=https://minio.mycompany.com
export DATASHARD_S3_ACCESS_KEY=minioadmin
export DATASHARD_S3_SECRET_KEY=minioadmin
export DATASHARD_S3_BUCKET=datashard
export DATASHARD_S3_REGION=us-east-1
```

```python
# Same code works with MinIO
from datashard import create_table, Schema

schema = Schema(schema_id=1, fields=[
    {"id": 1, "name": "metric", "type": "string", "required": True},
    {"id": 2, "name": "value", "type": "double", "required": True},
])

table = create_table("metrics", schema)
table.append_records([
    {"metric": "cpu_usage", "value": 45.2},
    {"metric": "memory_usage", "value": 67.8}
], schema)
```

### AWS S3 with IAM Roles

On EC2/ECS/Lambda, boto3 automatically uses IAM roles - no credentials needed:

```bash
# Minimal configuration (credentials from IAM role)
export DATASHARD_STORAGE_TYPE=s3
export DATASHARD_S3_BUCKET=my-bucket
export DATASHARD_S3_REGION=us-east-1
```

### Benefits of S3 Storage

**Distributed Workflows:**
- Workers on different machines/regions can share tables
- No NFS or shared filesystem required
- Auto-scaling friendly architecture

**Durability:**
- 99.999999999% (11 9's) durability with S3
- Automatic replication and redundancy
- No risk of local disk failures

**Cost-Effective:**
- Pay only for storage used
- ~$0.023/GB/month for S3 Standard
- Cheaper than EBS for cold data

**Performance:**
- Concurrent reads/writes from multiple workers
- Same ACID guarantees as local storage
- Optimistic concurrency control prevents conflicts

### Local vs S3 Performance

| Operation | Local Filesystem | S3 (same region) |
|-----------|-----------------|------------------|
| Write (1000 records) | ~1ms | ~50ms |
| Read (50k records) | ~20ms | ~100ms |
| Concurrent writes | ✅ Safe | ✅ Safe |
| Cross-machine access | ❌ Needs NFS | ✅ Native |
| Durability | Single disk | 11 9's |

**Recommendation:** Use local storage for single-machine workloads, S3 for distributed workflows.

### Switching Between Storage Backends

The same Python code works for both backends - just change environment variables:

```python
# This code works unchanged with local OR S3 storage
from datashard import create_table, Schema

schema = Schema(schema_id=1, fields=[...])
table = create_table("my-table", schema)
table.append_records(records, schema)
```

**Local mode:** No env vars (default)
**S3 mode:** Set `DATASHARD_STORAGE_TYPE=s3` + S3 credentials

### Security Best Practices

**Never hardcode credentials:**
```python
# ❌ WRONG - hardcoded
table = create_table("s3://bucket/table", schema)

# ✅ CORRECT - use environment variables
export DATASHARD_S3_ACCESS_KEY=...
table = create_table("table", schema)
```

**Use IAM roles on AWS:**
- EC2/ECS/Lambda auto-discover IAM credentials
- No access keys needed in code or env vars
- Automatic credential rotation

**Enable S3 encryption:**
- Server-side encryption (SSE-S3 or SSE-KMS)
- Configured at bucket level in AWS Console
- Transparent to DataShard - works automatically

---

## API Reference

### Creating and Loading Tables

```python
from datashard import create_table, load_table, Schema

# Create new table
schema = Schema(schema_id=1, fields=[...])
table = create_table("/path/to/table", schema)

# Load existing table
table = load_table("/path/to/table")
```

### Writing Data

```python
# Append records
records = [{"id": 1, "value": "data"}]
success = table.append_records(records, schema)

# Append with transactions (for complex operations)
with table.new_transaction() as tx:
    tx.append_data(records, schema)
    tx.commit()
```

### Reading Data

```python
# Get current snapshot
snapshot = table.current_snapshot()

# Get specific snapshot
snapshot = table.snapshot_by_id(snapshot_id)

# List all snapshots
snapshots = table.snapshots()

# Time travel
snapshot = table.time_travel(snapshot_id=12345)
snapshot = table.time_travel(timestamp=1700000000000)

# Scan all data
records = table.scan()
df = table.to_pandas()

# Scan with predicate pushdown (filters at parquet level)
records = table.scan(filter={"status": "failed"})
records = table.scan(filter={"age": (">", 30)})
records = table.scan(filter={"id": ("in", [1, 2, 3])})
records = table.scan(filter={"ts": ("between", (t1, t2))})

# Column projection (only read specified columns)
records = table.scan(columns=["id", "name"])

# Parallel reading (2-4x speedup with multiple files)
records = table.scan(parallel=True)      # Use all CPU cores
records = table.scan(parallel=4)         # Use 4 threads
df = table.to_pandas(parallel=True, filter={"status": "active"})

# Streaming API (memory-efficient for large tables)
for batch in table.scan_batches(batch_size=10000):
    process(batch)

for record in table.iter_records(filter={"status": "failed"}):
    handle_failure(record)

for chunk_df in table.iter_pandas(chunksize=50000):
    results.append(chunk_df.groupby('status').count())
```

### Schema Definition

```python
from datashard import Schema

schema = Schema(
    schema_id=1,  # Required: unique schema version ID
    fields=[      # Required: list of field definitions
        {
            "id": 1,              # Field ID (unique within schema)
            "name": "user_id",    # Field name
            "type": "long",       # Data type: long, int, string, double, boolean
            "required": True      # Whether field is required
        },
        # ... more fields
    ]
)
```

---

## How DataShard Compares to Apache Iceberg

### Apache Iceberg
- Java-based, requires JVM
- Built for big data platforms (Spark, Flink, Hive)
- Excellent performance for petabyte-scale data
- Complex setup, requires distributed infrastructure
- Production-grade for enterprise data lakes

### DataShard
- Pure Python, no JVM required
- Built for individual data scientists and ML engineers
- Optimized for personal projects and small teams
- Simple pip install, file-based storage
- Production-ready for Python-centric workflows

**Use Apache Iceberg if:** You're working with petabytes of data in a big data platform with Spark/Flink/Hive infrastructure.

**Use DataShard if:** You're a Python developer who needs safe concurrent data operations without Java dependencies or complex setup.

---

## Technical Details

DataShard implements:
- Optimistic Concurrency Control (OCC) with automatic retry
- Snapshot isolation for consistent reads
- Manifest-based metadata tracking (Iceberg's approach) using Avro
- Parquet format for efficient columnar storage
- ACID transaction semantics
- S3-native distributed locking for cloud storage

---

## Performance Characteristics

- **Writes:** O(1) for append operations (no data rewriting)
- **Reads:** O(n) where n = number of data files in snapshot
- **Concurrency:** Scales linearly with number of processes
- **Storage:** Efficient columnar compression with Parquet
- **Metadata:** Efficient Avro-based manifest tracking

---

## Limitations

### DataShard is optimized for:
- Append-heavy workloads (logging, metrics, events)
- Hundreds of concurrent writers
- Datasets up to hundreds of millions of records
- Local or network file systems

### DataShard is NOT optimized for:
- High-frequency updates/deletes (use a database)
- Complex queries (use a query engine like DuckDB)
- Petabyte-scale data (use Apache Iceberg with Spark)
- Distributed query processing (use Presto/Trino)

---

## Bug Fix: v0.1.3

**Fixed:** Manifest list creation bug where empty manifest arrays were written during append operations, causing queries to return no data despite parquet files being written correctly.

**Impact:** Queries using the manifest API now return correct data. If you're upgrading from v0.1.2, existing tables will continue to work but won't benefit from the fix until new snapshots are created.

**Details:** Transaction.commit() now correctly extracts data files from queued operations and passes them to _create_manifest_list_with_id().

---

## Development and Testing

Run tests:
```bash
pytest tests/ -v
```

Run specific test:
```bash
pytest tests/test_manifest_creation.py -v
```

With coverage:
```bash
pytest tests/ --cov=datashard --cov-report=html
```

---

## Why I Built DataShard

DataShard was born from my work on the Highway Workflow Engine, a strictly atomic workflow system. I needed to record massive amounts of execution metadata from hundreds of concurrent workers without risking data corruption.

I've had painful experiences with Java-based solutions in the past. Apache Iceberg conceptually fit my needs perfectly, but I wanted a pure Python solution that was simple to deploy and maintain.

Building a production-grade concurrent storage system seemed daunting, but we're in 2025 now, and modern AI tools (specifically Gemini) made it possible to implement complex Optimistic Concurrency Control logic in pure Python.

The result is DataShard: a battle-tested, production-ready solution for safe concurrent data operations that's helped Highway log millions of workflow executions without a single data corruption issue.

If you're building distributed systems in Python and need safe concurrent data operations, DataShard can help.

Enjoy using DataShard!
**Farshid**

---

## License

Copyright (c) RODMENA LIMITED
Licensed under Apache License 2.0

For full license text, see LICENSE file in the repository.

---

## Links

- **Homepage:** https://github.com/rodmena-limited/datashard
- **Documentation:** https://datashard.readthedocs.io/
- **Issues:** https://github.com/rodmena-limited/datashard/issues
