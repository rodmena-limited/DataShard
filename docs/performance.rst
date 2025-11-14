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
       # ... other parameters
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
       # ... other parameters
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

   # Good: Separate frequently accessed columns from rarely accessed ones
   with table.new_transaction() as tx:
       # Hot data partition
       tx.append_data(
           records=[{"id": 1, "name": "Alice", "status": "active"}],
           schema=None,
           partition_values={"type": "hot"}
       )
       # Cold data partition  
       tx.append_data(
           records=[{"id": 1, "detailed_history": "...", "audit_log": "..."}],
           schema=None,
           partition_values={"type": "cold"}
       )
       tx.commit()

Horizontal Partitioning
^^^^^^^^^^^^^^^^^^^^^^^

Organize data by frequently queried dimensions:

.. code-block:: python

   # Partition by time for time-series data
   import datetime
   
   today = datetime.date.today()
   with table.new_transaction() as tx:
       tx.append_data(
           records=[{"event": "login", "user_id": 123}],
           schema=None,
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

   # Efficient: Single transaction for multiple records
   batch_records = []
   for i in range(1000):
       batch_records.append({"id": i, "value": f"value_{i}"})
   
   with table.new_transaction() as tx:
       tx.append_data(records=batch_records, schema=None)
       tx.commit()

Transaction Size Guidelines
^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Keep individual transactions under 100MB when possible
- For large operations, split into multiple smaller transactions
- Monitor for timeout errors which may indicate transactions are too large

.. code-block:: python

   def safe_large_write(table, large_dataset, batch_size=500):
       for i in range(0, len(large_dataset), batch_size):
           batch = large_dataset[i:i + batch_size]
           with table.new_transaction() as tx:
               tx.append_data(records=batch, schema=None)
               success = tx.commit()
               if not success:
                   # Handle failure and possibly retry
                   break

Memory Management
-----------------

Data File Management
^^^^^^^^^^^^^^^^^^^^

Control memory usage when working with large datasets:

.. code-block:: python

   import gc

   def memory_efficient_write(table, large_records):
       # Process in chunks to control memory usage
       chunk_size = 100
       
       for i in range(0, len(large_records), chunk_size):
           chunk = large_records[i:i + chunk_size]
           
           with table.new_transaction() as tx:
               tx.append_data(records=chunk, schema=None)
               tx.commit()
           
           # Explicitly clean up if needed
           if i % (chunk_size * 10) == 0:  # Every 10 chunks
               gc.collect()

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

   import time
   import random

   def low_contention_write(table, record, base_delay=0.01):
       # Add jitter to reduce thundering herd problem
       jitter = random.uniform(0, 0.01)
       time.sleep(base_delay + jitter)
       
       with table.new_transaction() as tx:
           tx.append_data(records=[record], schema=None)
           return tx.commit()

Parallel Processing
^^^^^^^^^^^^^^^^^^^

Structure operations to allow parallel execution:

.. code-block:: python

   import concurrent.futures
   from threading import Lock

   def parallel_writes(table, datasets, max_workers=4):
       def write_dataset(data_subset):
           with table.new_transaction() as tx:
               tx.append_data(records=data_subset, schema=None)
               return tx.commit()
       
       with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
           futures = [executor.submit(write_dataset, subset) for subset in datasets]
           results = [future.result() for future in futures]
       
       return all(results)

Monitoring and Profiling
------------------------

Performance Metrics
^^^^^^^^^^^^^^^^^^^

Track key performance indicators:

.. code-block:: python

   import time
   from collections import defaultdict

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
   success = tracker.time_operation("append_data", table.append_data, records=[{"id": 1}], schema=None)
   avg_time = tracker.get_avg_time("append_data")
   print(f"Average append_data time: {avg_time:.4f}s")

Common Performance Issues
^^^^^^^^^^^^^^^^^^^^^^^^^

1. **Slow Transaction Commits**: Often caused by high contention; implement retry logic with backoff
2. **High Memory Usage**: Process large datasets in smaller chunks
3. **Slow Metadata Refresh**: Cache metadata when performing multiple operations
4. **File I/O Bottlenecks**: Ensure sufficient disk I/O capacity for your workload

Hardware Considerations
-----------------------

For optimal performance, consider:

- **SSD Storage**: Faster I/O for metadata and manifest files
- **Sufficient RAM**: For caching frequently accessed metadata
- **Multiple CPU Cores**: For handling concurrent transactions
- **Network Bandwidth**: If using network-attached storage