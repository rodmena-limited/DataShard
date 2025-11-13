datashard Documentation
=======================

Getting Started
===============

### Installation

Install datashard using pip:

```
pip install datashard
```

### Basic Tutorial

Here's a quick tutorial to get you started with datashard:

```python
from datashard import create_table

# Create a new table
table = create_table("/path/to/your/data/table")

# Add data to your table
data = [
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob", "age": 25},
    {"id": 3, "name": "Charlie", "age": 35},
]

# Safely append data with ACID properties
success = table.append_records(data)
print(f"Data appended successfully: {success}")

# Time travel - access data as it existed at a specific point
snapshot = table.current_snapshot()
print(f"Current snapshot ID: {snapshot.snapshot_id}")

# You can travel back to any snapshot
historical_data = table.time_travel(snapshot_id=snapshot.snapshot_id)
```

Avro Support
============

datashard supports Apache Avro files alongside Parquet and ORC. You can work with Avro files just like other formats:

```python
from datashard import DataFile, FileFormat

# Create Avro data file reference
avro_file = DataFile(
    file_path="/data/sample.avro",
    file_format=FileFormat.AVRO,
    partition_values={"year": 2023},
    record_count=1000,
    file_size_in_bytes=512000
)
```

datashard automatically handles reading and writing Avro files with proper schema management, just like with other supported formats.

Features
========

- **ACID Transactions**: Atomic, Consistent, Isolated, Durable operations
- **Time Travel**: Query data as it existed at any point in time  
- **Safe Concurrency**: Multiple processes can read/write without corruption
- **Schema Evolution**: Add/remove columns while maintaining history
- **Multiple File Formats**: Parquet, Avro, ORC support
- **Data Integrity**: Built-in validation and error handling

Why datashard?
==============

Traditional approaches to concurrent data access often lead to corruption and inconsistencies. datashard solves this by implementing Apache Iceberg's proven concepts in pure Python, ensuring:

- **No Data Loss**: Operations are atomic - they succeed completely or fail entirely
- **Consistent Views**: Readers always see a consistent snapshot of data
- **Safe Concurrency**: Multiple processes can operate simultaneously without conflicts
- **Production Ready**: Built on battle-tested principles from Apache Iceberg

Comparison to Apache Iceberg
=============================

Apache Iceberg is a mature Java-based system for big data platforms. datashard offers:

- Pure Python implementation without Java complexity
- Designed for ML and AI workflows
- Easy installation and configuration
- Same safety guarantees as Apache Iceberg
- Ideal for smaller datasets and individual developers

While Apache Iceberg runs on Spark/Hive/Flink, datashard runs natively in Python environments.