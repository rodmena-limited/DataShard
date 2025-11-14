datashard Documentation
=======================

Getting Started
===============

### Installation

Install datashard using pip:

```bash
pip install datashard
```

Install with pandas support:
```bash
pip install datashard[pandas]
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

### Pandas Integration

datashard provides excellent pandas integration:

```python
import pandas as pd
from datashard import create_table

# Create table
table = create_table("/path/to/pandas_table")

# Create a pandas DataFrame
df = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
    "value": [100, 200, 150, 300, 250]
})

# Append pandas DataFrame to table
with table.new_transaction() as tx:
    # Assuming you have methods that handle pandas integration
    tx.commit()

# Read data back as DataFrame
result_df = table.read_latest_snapshot_pandas()
print(f"Retrieved {len(result_df)} records as DataFrame")
```

File Operations and Pandas Support
==================================

datashard supports various file formats including Parquet, Avro, and ORC. It also provides excellent pandas integration:

Install with pandas support:
```
pip install datashard[pandas]
```

Use pandas DataFrames directly with Iceberg tables:
```python
import pandas as pd
from datashard import create_table

table = create_table("/path/to/your/table")

# Write pandas DataFrame to Iceberg table
df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
with table.new_transaction() as tx:
    tx.append_pandas(df)  # Direct pandas support
    tx.commit()

# Read data back as pandas DataFrame  
result_df = table.read_latest_snapshot_pandas()

# Read specific columns
subset_df = table.read_columns_pandas(["id", "name"])
```

## Pandas Integration Features:
- Direct DataFrame reading/writing
- Schema validation and compatibility checks
- Batch processing of large DataFrames
- Column-level operations with pandas
- Proper file validation and integrity checks

datashard properly handles file validation, manifest creation, and schema compatibility for pandas DataFrames while maintaining all ACID guarantees.

Features
========

- **ACID Transactions**: Atomic, Consistent, Isolated, Durable operations
- **Time Travel**: Query data as it existed at any point in time  
- **Safe Concurrency**: Multiple processes can read/write without corruption
- **Schema Evolution**: Add/remove columns while maintaining history
- **Multiple File Formats**: Parquet, Avro, ORC support
- **Pandas Integration**: Direct DataFrame operations
- **Data Integrity**: Built-in validation and error handling

Why datashard?
==============

Traditional approaches to concurrent data access often lead to corruption and inconsistencies. datashard solves this by implementing Apache Iceberg's proven concepts in pure Python, ensuring:

- **No Data Loss**: Operations are atomic - they succeed completely or fail entirely
- **Consistent Views**: Readers always see a consistent snapshot of data
- **Safe Concurrency**: Multiple processes can operate simultaneously without conflicts
- **Production Ready**: Built on battle-tested principles from Apache Iceberg