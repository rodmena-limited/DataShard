Concurrency and Safe Multi-Process Access
=========================================

This section explains how datashard enables safe concurrent access to data from multiple processes.

Concurrency Challenges
----------------------

In traditional file-based data storage, concurrent access can lead to:

- Data corruption when multiple processes write simultaneously
- Inconsistent reads when processes read during write operations
- Lost updates when processes overwrite each other's changes
- Race conditions that can cause unpredictable behavior

datashard solves these challenges through ACID transactions and Optimistic Concurrency Control (OCC).

Optimistic Concurrency Control (OCC)
------------------------------------

How OCC Works
^^^^^^^^^^^^^

datashard uses Optimistic Concurrency Control to handle concurrent access:

1. **Read Phase**: Each transaction reads the current state of the table
2. **Validation Phase**: Before committing, the system checks if the table has changed
3. **Commit Phase**: If no conflicts, the transaction commits; otherwise, it fails

.. code-block:: python

   from datashard import create_table, Schema

   # Define schema
   data_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "record_id", "type": "long", "required": True},
           {"id": 2, "name": "value", "type": "string", "required": True}
       ]
   )

   table = create_table("/path/to/table", data_schema)

   # OCC in action - automatic conflict detection
   with table.new_transaction() as tx:
       # Transaction reads current state
       tx.append_data(records=[{"record_id": 1, "value": "A"}], schema=data_schema)
       # When commit() is called, system checks for conflicts
       success = tx.commit()
       # If another process modified the table since reading, commit fails

Handling Conflicts
^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from datashard import ConcurrentModificationException, create_table, Schema
   import time

   # Define schema
   conflict_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "record_id", "type": "long", "required": True},
           {"id": 2, "name": "value", "type": "string", "required": True}
       ]
   )

   table = create_table("/path/to/table", conflict_schema)

   max_retries = 5

   for attempt in range(max_retries):
       try:
           with table.new_transaction() as tx:
               # Perform operations
               tx.append_data(records=[{"record_id": 2, "value": "B"}], schema=conflict_schema)
               result = tx.commit()

               if result:
                   print("Transaction succeeded")
                   break
       except ConcurrentModificationException:
           print(f"Conflict detected, attempt {attempt + 1}")
           if attempt < max_retries - 1:
               # Wait before retrying (exponential backoff)
               time.sleep(0.01 * (2 ** attempt))
           else:
               print("Max retries exceeded, transaction failed")
               raise

Multiple Process Scenarios
--------------------------

Reading While Writing
^^^^^^^^^^^^^^^^^^^^^

datashard safely allows concurrent reads and writes:

.. code-block:: python

   from datashard import create_table, Schema
   import time

   # Define schema
   concurrent_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "record_id", "type": "long", "required": True},
           {"id": 2, "name": "value", "type": "string", "required": True}
       ]
   )

   table = create_table("/path/to/concurrent_table", concurrent_schema)

   # Process A: Reading data
   def read_process():
       while True:
           # Readers always see a consistent snapshot of the data
           current_snapshot = table.current_snapshot()
           # Even if writers are modifying the table, readers see
           # the state as of the snapshot creation time
           time.sleep(1)

   # Process B: Writing data
   def write_process():
       while True:
           write_data = [{"record_id": 3, "value": "C"}]
           table.append_records(records=write_data, schema=concurrent_schema)
           time.sleep(2)

Multiple Writers
^^^^^^^^^^^^^^^^

Multiple processes can safely write to the same table:

.. code-block:: python

   from datashard import create_table, Schema
   import time

   # Define schema
   writer_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "record_id", "type": "long", "required": True},
           {"id": 2, "name": "source", "type": "string", "required": True}
       ]
   )

   table = create_table("/path/to/multiwriter_table", writer_schema)

   # Writer Process 1
   def writer_1():
       for i in range(100):
           record_data = [{"record_id": i, "source": "writer1"}]
           success = table.append_records(records=record_data, schema=writer_schema)
           if not success:
               print(f"Writer 1 failed at iteration {i}")
           time.sleep(0.1)

   # Writer Process 2
   def writer_2():
       for i in range(100):
           record_data = [{"record_id": i, "source": "writer2"}]
           success = table.append_records(records=record_data, schema=writer_schema)
           if not success:
               print(f"Writer 2 failed at iteration {i}")
           time.sleep(0.1)

Best Practices for Concurrency
------------------------------

Retry Logic Implementation
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   import random
   import time
   from datashard import ConcurrentModificationException, create_table, Schema

   # Define schema
   retry_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "record_id", "type": "long", "required": True},
           {"id": 2, "name": "data", "type": "string", "required": True}
       ]
   )

   table = create_table("/path/to/retry_table", retry_schema)

   def safe_write_with_retry(table, records, schema, max_retries=5):
       for attempt in range(max_retries):
           try:
               result = table.append_records(records=records, schema=schema)
               return result
           except ConcurrentModificationException:
               if attempt == max_retries - 1:
                   raise
               # Jitter to avoid thundering herd problem
               sleep_time = 0.01 * (2 ** attempt) + random.uniform(0, 0.01)
               time.sleep(sleep_time)

       return False  # Should not reach here

   # Usage
   sample_records = [{"record_id": 1, "data": "example"}]
   safe_write_with_retry(table, sample_records, retry_schema)

Batch Operations
^^^^^^^^^^^^^^^^

For high-throughput scenarios, batch operations reduce conflicts:

.. code-block:: python

   from datashard import create_table, Schema
   import time

   # Define schema
   batch_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "record_id", "type": "long", "required": True},
           {"id": 2, "name": "batch_data", "type": "string", "required": True}
       ]
   )

   table = create_table("/path/to/batch_table", batch_schema)

   def batch_write_process(table, all_records, schema, batch_size=100):
       for i in range(0, len(all_records), batch_size):
           batch = all_records[i:i + batch_size]

           success = table.append_records(records=batch, schema=schema)

           if not success:
               print(f"Batch {i//batch_size} failed, retrying...")
               # Implement retry logic here

           time.sleep(0.01)  # Brief pause between batches

   # Example usage
   all_records = [{"record_id": i, "batch_data": f"data_{i}"} for i in range(1000)]
   batch_write_process(table, all_records, batch_schema, batch_size=100)

Transaction Scope
^^^^^^^^^^^^^^^^^

Keep transactions as short as possible:

.. code-block:: python

   from datashard import create_table, Schema

   # Define schema
   scope_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "record_id", "type": "long", "required": True},
           {"id": 2, "name": "value", "type": "string", "required": True}
       ]
   )

   table = create_table("/path/to/scope_table", scope_schema)

   # Good: Short transaction
   def good_example(table, records, schema):
       # Process data BEFORE transaction
       processed_records = [process_record(r) for r in records]
       # Short transaction to just write
       return table.append_records(records=processed_records, schema=schema)

   # Avoid: Long-running transaction
   def bad_example(table, records, schema):
       with table.new_transaction() as tx:
           # Processing large amounts of data inside transaction
           processed_records = []
           for record in records:
               # Expensive processing that takes time
               processed_record = expensive_processing(record)
               processed_records.append(processed_record)

           tx.append_data(records=processed_records, schema=schema)
           return tx.commit()

Monitoring Concurrency
----------------------

Detecting High Contention
^^^^^^^^^^^^^^^^^^^^^^^^^

Monitor for frequent conflicts that may indicate high contention:

.. code-block:: python

   import logging
   import time
   from datashard import ConcurrentModificationException, create_table, Schema

   # Define schema
   monitor_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "record_id", "type": "long", "required": True},
           {"id": 2, "name": "data", "type": "string", "required": True}
       ]
   )

   table = create_table("/path/to/monitor_table", monitor_schema)

   def monitored_write(table, records, schema, max_retries=5):
       start_time = time.time()
       retry_count = 0

       for attempt in range(max_retries):
           try:
               result = table.append_records(records=records, schema=schema)

               end_time = time.time()
               logging.info(f"Transaction completed in {end_time - start_time:.3f}s, "
                          f"with {retry_count} retries")
               return result
           except ConcurrentModificationException:
               retry_count += 1
               logging.warning(f"Transaction conflict, retry {retry_count}/{max_retries}")

               if attempt == max_retries - 1:
                   logging.error("Transaction failed after max retries")
                   raise

               time.sleep(0.01 * (2 ** attempt))

   # Example usage
   sample_records = [{"record_id": 1, "data": "monitored"}]
   monitored_write(table, sample_records, monitor_schema)

Performance Considerations
^^^^^^^^^^^^^^^^^^^^^^^^^^

- OCC works best with low-to-moderate contention
- High contention scenarios may require application-level coordination
- Consider partitioning strategies to reduce contention on hot data
- Monitor conflict rates to optimize write patterns

Advanced Concurrency Patterns
-----------------------------

Leader/Follower Pattern
^^^^^^^^^^^^^^^^^^^^^^^

For scenarios requiring strict ordering:

.. code-block:: python

   import os
   import tempfile
   import time
   from datashard import create_table, Schema

   # Define schema
   leader_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "record_id", "type": "long", "required": True},
           {"id": 2, "name": "data", "type": "string", "required": True}
       ]
   )

   table = create_table("/path/to/leader_table", leader_schema)

   def coordinated_write(table, records, schema, process_id):
       # Use file-based coordination
       lock_file = os.path.join(tempfile.gettempdir(), f"datashard_lock_{table.table_path}")

       while True:
           if try_acquire_lock(lock_file, process_id):
               try:
                   return table.append_records(records=records, schema=schema)
               finally:
                   release_lock(lock_file, process_id)
           else:
               time.sleep(0.01)  # Wait and retry

Partition-Aware Writing
^^^^^^^^^^^^^^^^^^^^^^^

Reduce conflicts by organizing writes around partitions:

.. code-block:: python

   from datashard import create_table, Schema

   # Define schema with partition support
   partition_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "record_id", "type": "long", "required": True},
           {"id": 2, "name": "data", "type": "string", "required": True},
           {"id": 3, "name": "partition", "type": "string", "required": True}
       ]
   )

   table = create_table("/path/to/partitioned_table", partition_schema)

   def partitioned_write(table, record_groups_by_partition, schema):
       """Write to different partitions to reduce conflicts"""
       results = []

       for partition_value, records in record_groups_by_partition.items():
           with table.new_transaction() as tx:
               tx.append_data(
                   records=records,
                   schema=schema,
                   partition_values={"partition": partition_value}
               )
               results.append(tx.commit())

       return all(results)

   # Example usage
   record_groups = {
       "2024-01": [{"record_id": 1, "data": "jan_data", "partition": "2024-01"}],
       "2024-02": [{"record_id": 2, "data": "feb_data", "partition": "2024-02"}]
   }
   partitioned_write(table, record_groups, partition_schema)

These patterns help optimize concurrent access based on your specific workload requirements.
