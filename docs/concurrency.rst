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

   # OCC in action - automatic conflict detection
   with table.new_transaction() as tx:
       # Transaction reads current state
       tx.append_data(records=[{"id": 1, "value": "A"}], schema=None)
       # When commit() is called, system checks for conflicts
       success = tx.commit()
       # If another process modified the table since reading, commit fails

Handling Conflicts
^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from datashard import ConcurrentModificationException
   import time

   max_retries = 5
   
   for attempt in range(max_retries):
       try:
           with table.new_transaction() as tx:
               # Perform operations
               tx.append_data(records=[{"id": 2, "value": "B"}], schema=None)
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
           with table.new_transaction() as tx:
               tx.append_data(records=[{"id": 3, "value": "C"}], schema=None)
               tx.commit()
           time.sleep(2)

Multiple Writers
^^^^^^^^^^^^^^^^

Multiple processes can safely write to the same table:

.. code-block:: python

   # Writer Process 1
   def writer_1():
       for i in range(100):
           with table.new_transaction() as tx:
               tx.append_data(records=[{"id": i, "source": "writer1"}], schema=None)
               success = tx.commit()
               if not success:
                   print(f"Writer 1 failed at iteration {i}")
               time.sleep(0.1)

   # Writer Process 2  
   def writer_2():
       for i in range(100):
           with table.new_transaction() as tx:
               tx.append_data(records=[{"id": i, "source": "writer2"}], schema=None)
               success = tx.commit()
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
   from datashard import ConcurrentModificationException

   def safe_write_with_retry(table, records, max_retries=5):
       for attempt in range(max_retries):
           try:
               with table.new_transaction() as tx:
                   tx.append_data(records=records, schema=None)
                   result = tx.commit()
                   return result
           except ConcurrentModificationException:
               if attempt == max_retries - 1:
                   raise
               # Jitter to avoid thundering herd problem
               sleep_time = 0.01 * (2 ** attempt) + random.uniform(0, 0.01)
               time.sleep(sleep_time)
       
       return False  # Should not reach here

Batch Operations
^^^^^^^^^^^^^^^^

For high-throughput scenarios, batch operations reduce conflicts:

.. code-block:: python

   def batch_write_process(table, all_records, batch_size=100):
       for i in range(0, len(all_records), batch_size):
           batch = all_records[i:i + batch_size]
           
           with table.new_transaction() as tx:
               tx.append_data(records=batch, schema=None)
               success = tx.commit()
               
           if not success:
               print(f"Batch {i//batch_size} failed, retrying...")
               # Implement retry logic here
               
           time.sleep(0.01)  # Brief pause between batches

Transaction Scope
^^^^^^^^^^^^^^^^^

Keep transactions as short as possible:

.. code-block:: python

   # Good: Short transaction
   def good_example(table, records):
       with table.new_transaction() as tx:
           tx.append_data(records=records, schema=None)
           return tx.commit()

   # Avoid: Long-running transaction
   def bad_example(table, records):
       with table.new_transaction() as tx:
           # Processing large amounts of data inside transaction
           processed_records = []
           for record in records:
               # Expensive processing that takes time
               processed_record = expensive_processing(record)
               processed_records.append(processed_record)
           
           tx.append_data(records=processed_records, schema=None)
           return tx.commit()

Monitoring Concurrency
----------------------

Detecting High Contention
^^^^^^^^^^^^^^^^^^^^^^^^^

Monitor for frequent conflicts that may indicate high contention:

.. code-block:: python

   import logging

   def monitored_write(table, records, max_retries=5):
       start_time = time.time()
       retry_count = 0
       
       for attempt in range(max_retries):
           try:
               with table.new_transaction() as tx:
                   tx.append_data(records=records, schema=None)
                   result = tx.commit()
                   
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

   def coordinated_write(table, records, process_id):
       # Use file-based coordination
       lock_file = os.path.join(tempfile.gettempdir(), f"datashard_lock_{table.table_path}")
       
       while True:
           if try_acquire_lock(lock_file, process_id):
               try:
                   with table.new_transaction() as tx:
                       tx.append_data(records=records, schema=None)
                       return tx.commit()
               finally:
                   release_lock(lock_file, process_id)
           else:
               time.sleep(0.01)  # Wait and retry

Partition-Aware Writing
^^^^^^^^^^^^^^^^^^^^^^^

Reduce conflicts by organizing writes around partitions:

.. code-block:: python

   def partitioned_write(table, record_groups_by_partition):
       """Write to different partitions to reduce conflicts"""
       results = []
       
       for partition_value, records in record_groups_by_partition.items():
           with table.new_transaction() as tx:
               tx.append_data(
                   records=records, 
                   schema=None,
                   partition_values={"partition": partition_value}
               )
               results.append(tx.commit())
       
       return all(results)

These patterns help optimize concurrent access based on your specific workload requirements.