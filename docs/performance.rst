Performance Optimization
========================

This section covers best practices and techniques for optimizing datashard performance.

File Format Selection
---------------------

Choosing the Right Format
^^^^^^^^^^^^^^^^^^^^^^^^^

datashard supports multiple file formats, each with different performance characteristics:

Parquet
"""""""

- Best for analytical workloads with complex queries
- Excellent compression ratios
- Columnar storage enables efficient scans of specific columns
- Supports predicate pushdown for faster filtering

.. code-block:: python

   from datashard import FileFormat, DataFile

   # Parquet is typically the best choice for most use cases
   parquet_file = DataFile(
       file_path="data.parquet",
       file_format=FileFormat.PARQUET,
       partition_values={},
       record_count=1000,
       file_size_in_bytes=50000
   )

Avro
""""

- Good for schema evolution scenarios
- Efficient for append-heavy workloads
- Good compression with complex nested types
- Schema stored with data, self-describing

.. code-block:: python

   avro_file = DataFile(
       file_path="data.avro",
       file_format=FileFormat.AVRO,
       partition_values={},
       record_count=1000,
       file_size_in_bytes=45000
   )

ORC
"""

- Optimized for read-heavy workloads
- Good compression and predicate pushdown
- Built-in ACID transaction support in Hadoop ecosystems

Partitioning Strategies
-----------------------

Vertical Partitioning
^^^^^^^^^^^^^^^^^^^^^

Organize data by columns frequently accessed together:

.. code-block:: python

   from datashard import create_table, Schema

   # Define schema for hot data
   hot_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "record_id", "type": "long", "required": True},
           {"id": 2, "name": "name", "type": "string", "required": True},
           {"id": 3, "name": "status", "type": "string", "required": True}
       ]
   )

   table = create_table("/path/to/partitioned_table", hot_schema)

   # Good: Separate frequently accessed columns from rarely accessed ones
   with table.new_transaction() as tx:
       # Hot data partition
       tx.append_data(
           records=[{"record_id": 1, "name": "Alice", "status": "active"}],
           schema=hot_schema,
           partition_values={"type": "hot"}
       )
       tx.commit()

Horizontal Partitioning
^^^^^^^^^^^^^^^^^^^^^^^

Organize data by frequently queried dimensions:

.. code-block:: python

   from datashard import create_table, Schema
   import datetime

   # Define schema for time-series data
   event_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "event", "type": "string", "required": True},
           {"id": 2, "name": "user_id", "type": "long", "required": True}
       ]
   )

   table = create_table("/path/to/events", event_schema)

   # Partition by time for time-series data
   today = datetime.date.today()
   with table.new_transaction() as tx:
       tx.append_data(
           records=[{"event": "login", "user_id": 123}],
           schema=event_schema,
           partition_values={
               "year": today.year,
               "month": today.month,
               "day": today.day
           }
       )
       tx.commit()

Transaction Optimization
------------------------

Batch Operations
^^^^^^^^^^^^^^^^

Minimize transaction overhead by batching operations:

.. code-block:: python

   from datashard import create_table, Schema

   # Define schema
   batch_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "record_id", "type": "long", "required": True},
           {"id": 2, "name": "value", "type": "string", "required": True}
       ]
   )

   table = create_table("/path/to/batch_table", batch_schema)

   # Efficient: Single transaction for multiple records
   batch_records = []
   for i in range(1000):
       batch_records.append({"record_id": i, "value": f"value_{i}"})

   with table.new_transaction() as tx:
       tx.append_data(records=batch_records, schema=batch_schema)
       tx.commit()

Transaction Size Guidelines
^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Keep individual transactions under 100MB when possible
- For large operations, split into multiple smaller transactions
- Monitor for timeout errors which may indicate transactions are too large

.. code-block:: python

   from datashard import create_table, Schema

   # Define schema
   large_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "record_id", "type": "long", "required": True},
           {"id": 2, "name": "data", "type": "string", "required": True}
       ]
   )

   table = create_table("/path/to/large_table", large_schema)

   def safe_large_write(table, large_dataset, schema, batch_size=500):
       for i in range(0, len(large_dataset), batch_size):
           batch = large_dataset[i:i + batch_size]
           with table.new_transaction() as tx:
               tx.append_data(records=batch, schema=schema)
               success = tx.commit()
               if not success:
                   # Handle failure and possibly retry
                   break

   # Example usage
   large_data = [{"record_id": i, "data": f"data_{i}"} for i in range(10000)]
   safe_large_write(table, large_data, large_schema, batch_size=500)

Memory Management
-----------------

Data File Management
^^^^^^^^^^^^^^^^^^^^

Control memory usage when working with large datasets:

.. code-block:: python

   from datashard import create_table, Schema
   import gc

   # Define schema
   memory_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "record_id", "type": "long", "required": True},
           {"id": 2, "name": "data", "type": "string", "required": True}
       ]
   )

   table = create_table("/path/to/memory_table", memory_schema)

   def memory_efficient_write(table, large_records, schema):
       # Process in chunks to control memory usage
       chunk_size = 100

       for i in range(0, len(large_records), chunk_size):
           chunk = large_records[i:i + chunk_size]

           with table.new_transaction() as tx:
               tx.append_data(records=chunk, schema=schema)
               tx.commit()

           # Explicitly clean up if needed
           if i % (chunk_size * 10) == 0:  # Every 10 chunks
               gc.collect()

   # Example usage
   large_dataset = [{"record_id": i, "data": f"data_{i}"} for i in range(5000)]
   memory_efficient_write(table, large_dataset, memory_schema)

Metadata Cache Optimization
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The metadata manager caches information to improve performance:

.. code-block:: python

   # Efficiently refresh metadata only when needed
   current_metadata = table.metadata_manager.refresh()

   # Reuse metadata for multiple operations in a short period
   snapshot = table.current_snapshot()  # Uses cached metadata
   history = table.metadata_manager.get_snapshot_history()  # Also uses cache

Concurrency Optimization
------------------------

Reducing Contention
^^^^^^^^^^^^^^^^^^^

Minimize conflicts in high-concurrency scenarios:

.. code-block:: python

   from datashard import create_table, Schema
   import time
   import random

   # Define schema
   concurrency_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "record_id", "type": "long", "required": True},
           {"id": 2, "name": "data", "type": "string", "required": True}
       ]
   )

   table = create_table("/path/to/concurrent_table", concurrency_schema)

   def low_contention_write(table, record, schema, base_delay=0.01):
       # Add jitter to reduce thundering herd problem
       jitter = random.uniform(0, 0.01)
       time.sleep(base_delay + jitter)

       with table.new_transaction() as tx:
           tx.append_data(records=[record], schema=schema)
           return tx.commit()

   # Example usage
   sample_record = {"record_id": 1, "data": "example"}
   low_contention_write(table, sample_record, concurrency_schema)

Parallel Processing
^^^^^^^^^^^^^^^^^^^

Structure operations to allow parallel execution:

.. code-block:: python

   from datashard import create_table, Schema
   import concurrent.futures

   # Define schema
   parallel_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "record_id", "type": "long", "required": True},
           {"id": 2, "name": "value", "type": "double", "required": True}
       ]
   )

   table = create_table("/path/to/parallel_table", parallel_schema)

   def parallel_writes(table, datasets, schema, max_workers=4):
       def write_dataset(data_subset):
           with table.new_transaction() as tx:
               tx.append_data(records=data_subset, schema=schema)
               return tx.commit()

       with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
           futures = [executor.submit(write_dataset, subset) for subset in datasets]
           results = [future.result() for future in futures]

       return all(results)

   # Example usage
   datasets = [
       [{"record_id": i, "value": float(i)} for i in range(0, 250)],
       [{"record_id": i, "value": float(i)} for i in range(250, 500)],
       [{"record_id": i, "value": float(i)} for i in range(500, 750)],
       [{"record_id": i, "value": float(i)} for i in range(750, 1000)]
   ]
   parallel_writes(table, datasets, parallel_schema, max_workers=4)

Monitoring and Profiling
------------------------

Performance Metrics
^^^^^^^^^^^^^^^^^^^

Track key performance indicators:

.. code-block:: python

   from datashard import create_table, Schema
   import time
   from collections import defaultdict

   # Define schema
   metrics_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "metric_id", "type": "long", "required": True},
           {"id": 2, "name": "value", "type": "double", "required": True}
       ]
   )

   table = create_table("/path/to/metrics_table", metrics_schema)

   class PerformanceTracker:
       def __init__(self):
           self.metrics = defaultdict(list)

       def time_operation(self, name, operation, *args, **kwargs):
           start = time.time()
           result = operation(*args, **kwargs)
           duration = time.time() - start
           self.metrics[name].append(duration)
           return result

       def get_avg_time(self, name):
           times = self.metrics[name]
           return sum(times) / len(times) if times else 0

   # Usage
   tracker = PerformanceTracker()
   sample_records = [{"metric_id": 1, "value": 42.0}]
   success = tracker.time_operation("append_records", table.append_records,
                                    records=sample_records, schema=metrics_schema)
   avg_time = tracker.get_avg_time("append_records")
   print(f"Average append_records time: {avg_time:.4f}s")

Common Performance Issues
^^^^^^^^^^^^^^^^^^^^^^^^^

1. **Slow Transaction Commits**: Often caused by high contention; implement retry logic with backoff
2. **High Memory Usage**: Process large datasets in smaller chunks
3. **Slow Metadata Refresh**: Cache metadata when performing multiple operations
4. **File I/O Bottlenecks**: Ensure sufficient disk I/O capacity for your workload

Query Optimization (NEW in v0.3.3)
----------------------------------

Predicate Pushdown
^^^^^^^^^^^^^^^^^^

Filter at the parquet level to reduce I/O by 90%+:

.. code-block:: python

   # Without predicate pushdown (reads all data, filters in Python)
   all_records = table.scan()
   filtered = [r for r in all_records if r['status'] == 'failed']  # Slow!

   # With predicate pushdown (filters at parquet level)
   filtered = table.scan(filter={"status": "failed"})  # 90%+ faster!

   # Multiple filters (AND)
   results = table.scan(filter={
       "status": "active",
       "age": (">=", 30)
   })

   # Comparison operators
   table.scan(filter={"value": (">", 100)})
   table.scan(filter={"value": ("<=", 50)})

   # Membership filters
   table.scan(filter={"category": ("in", ["A", "B", "C"])})

   # Range filters
   table.scan(filter={"timestamp": ("between", (start_ts, end_ts))})

Partition Pruning
^^^^^^^^^^^^^^^^^

DataShard automatically skips files that cannot contain matching records:

.. code-block:: python

   # Column bounds (min/max) are computed during write
   # and stored in manifest files

   # When you filter, files are pruned based on bounds:
   # - File with timestamp range [1000, 2000] is skipped
   #   when filtering for timestamp > 3000

   # This happens automatically - no configuration needed!
   results = table.scan(filter={"timestamp": (">", 3000)})

Parallel Reading
^^^^^^^^^^^^^^^^

Use multi-threaded I/O for 2-4x speedup:

.. code-block:: python

   # Sequential (default)
   records = table.scan()

   # Parallel with all CPU cores
   records = table.scan(parallel=True)

   # Parallel with specific thread count
   records = table.scan(parallel=4)

   # Combine with filter for maximum performance
   records = table.scan(
       filter={"status": "active"},
       parallel=True
   )

   # Works with to_pandas too
   df = table.to_pandas(parallel=True)

Column Projection
^^^^^^^^^^^^^^^^^

Only read the columns you need:

.. code-block:: python

   # Read all columns (slower)
   records = table.scan()

   # Read only needed columns (faster)
   records = table.scan(columns=["id", "name"])

   # Combine with filter and parallel
   records = table.scan(
       columns=["id", "status"],
       filter={"status": "active"},
       parallel=True
   )

Streaming for Large Tables
^^^^^^^^^^^^^^^^^^^^^^^^^^

Process large tables with constant memory:

.. code-block:: python

   # Instead of loading entire table into memory:
   all_records = table.scan()  # May OOM for large tables!

   # Stream in batches:
   for batch in table.scan_batches(batch_size=10000):
       process(batch)  # Only batch_size records in memory

   # Stream record by record:
   for record in table.iter_records():
       handle(record)  # Only 1 record in memory

   # Stream as pandas chunks:
   for chunk_df in table.iter_pandas(chunksize=50000):
       results.append(chunk_df.groupby('x').sum())

Performance Comparison
^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: Query Optimization Impact
   :header-rows: 1
   :widths: 30 35 35

   * - Feature
     - Without Optimization
     - With Optimization
   * - Predicate Pushdown
     - Read 100% of data
     - Read only matching rows (90%+ reduction)
   * - Partition Pruning
     - Scan all files
     - Skip non-matching files (99% reduction for time-range)
   * - Parallel Reading
     - 1 file at a time
     - N files concurrent (2-4x speedup)
   * - Column Projection
     - Read all columns
     - Read only needed columns (proportional reduction)
   * - Streaming
     - Entire table in memory
     - Constant memory (~100MB for any size)

Hardware Considerations
-----------------------

For optimal performance, consider:

- **SSD Storage**: Faster I/O for metadata and manifest files
- **Sufficient RAM**: For caching frequently accessed metadata
- **Multiple CPU Cores**: For handling concurrent transactions and parallel reads
- **Network Bandwidth**: If using network-attached storage or S3
