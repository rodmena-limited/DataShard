Transactions and ACID Properties
================================

This section explains how datashard implements ACID transactions for safe concurrent data operations.

Understanding Transactions
--------------------------

A transaction in datashard is a series of operations that are performed as a single, indivisible unit. Transactions ensure that data remains consistent even when multiple processes are accessing it simultaneously.

The ACID Properties
-------------------

datashard implements the four ACID properties:

Atomicity
^^^^^^^^^
All operations within a transaction are completed successfully or none of them are applied. This prevents partial updates that could leave the table in an inconsistent state.

Consistency
^^^^^^^^^^^
Transactions maintain all data integrity constraints, ensuring the table remains in a valid state after each operation.

Isolation
^^^^^^^^^
Concurrent transactions are isolated from each other. An operation in one transaction cannot interfere with operations in another transaction until the first transaction is committed.

Durability
^^^^^^^^^^
Once a transaction is committed, the changes are permanent and will survive system failures.

Using Transactions
------------------

There are two main patterns for working with transactions in datashard:

Automatic Transaction Management (Recommended)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``append_records()`` method handles transactions automatically, making it the simplest approach for most use cases:

.. code-block:: python

   from datashard import create_table, Schema

   # Define schema
   user_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "user_id", "type": "long", "required": True},
           {"id": 2, "name": "name", "type": "string", "required": True},
           {"id": 3, "name": "age", "type": "long", "required": False}
       ]
   )

   # Create table
   table = create_table("/path/to/table", user_schema)

   # Append records - transaction is handled automatically
   sample_data = [
       {"user_id": 1, "name": "Alice", "age": 30},
       {"user_id": 2, "name": "Bob", "age": 25}
   ]

   success = table.append_records(records=sample_data, schema=user_schema)

   if success:
       print("Data added successfully")
   else:
       print("Transaction failed")

Manual Transaction Control
^^^^^^^^^^^^^^^^^^^^^^^^^^^

For more complex operations or when you need explicit control over commit timing, use manual transaction management:

.. code-block:: python

   from datashard import create_table, Schema

   # Define schema
   employee_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "emp_id", "type": "long", "required": True},
           {"id": 2, "name": "name", "type": "string", "required": True},
           {"id": 3, "name": "department", "type": "string", "required": True}
       ]
   )

   # Create table
   table = create_table("/path/to/employees", employee_schema)

   # Manual transaction using context manager
   with table.new_transaction() as tx:
       # Add records using the transaction object
       tx.append_data(
           records=[
               {"emp_id": 1, "name": "Charlie", "department": "Engineering"},
               {"emp_id": 2, "name": "David", "department": "Sales"}
           ],
           schema=employee_schema
       )
       # Transaction commits automatically when context exits successfully
       # If exception occurs, transaction rolls back

   print("Transaction completed")

Transaction Operations
----------------------

datashard supports several types of operations within transactions:

Appending Records
^^^^^^^^^^^^^^^^^

The most common operation is appending new records:

.. code-block:: python

   from datashard import create_table, Schema

   # Define schema for metrics data
   metrics_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "timestamp", "type": "long", "required": True},
           {"id": 2, "name": "metric_name", "type": "string", "required": True},
           {"id": 3, "name": "value", "type": "double", "required": True}
       ]
   )

   metrics_table = create_table("/path/to/metrics", metrics_schema)

   # Using automatic transaction management
   metrics_data = [
       {"timestamp": 1700000000000, "metric_name": "cpu_usage", "value": 45.2},
       {"timestamp": 1700000001000, "metric_name": "memory_usage", "value": 62.8}
   ]

   success = metrics_table.append_records(records=metrics_data, schema=metrics_schema)

Appending Files
^^^^^^^^^^^^^^^

For advanced use cases where you have pre-existing Parquet files:

.. code-block:: python

   from datashard import DataFile, FileFormat

   with table.new_transaction() as tx:
       # Create a data file reference
       data_file = DataFile(
           file_path="/path/to/data.parquet",
           file_format=FileFormat.PARQUET,
           partition_values={},
           record_count=100,
           file_size_in_bytes=10240
       )

       # Add the file to the transaction
       tx.append_files([data_file])

Explicit Commit and Rollback
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For cases where you need fine-grained control:

.. code-block:: python

   # Create transaction
   tx = table.new_transaction().begin()

   try:
       # Add operations to the transaction
       tx.append_data(
           records=[{"emp_id": 3, "name": "Eve", "department": "Marketing"}],
           schema=employee_schema
       )

       # Explicitly commit the transaction
       if tx.commit():
           print("Transaction committed successfully")
       else:
           print("Transaction failed to commit")
   except Exception as e:
       # Explicitly roll back on error
       tx.rollback()
       print(f"Transaction rolled back due to error: {e}")

Concurrency Control
-------------------

datashard uses Optimistic Concurrency Control (OCC) to handle concurrent access:

- Each transaction reads the current state of the table
- Operations are queued locally during the transaction
- When committing, datashard checks if the table has changed since the transaction began
- If changes occurred, the transaction is rejected and may be retried

Handling Concurrent Modifications
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When multiple processes write to the same table, transactions may fail due to conflicts. Implement retry logic to handle this:

.. code-block:: python

   from datashard import ConcurrentModificationException, create_table, Schema

   # Define schema
   order_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "order_id", "type": "long", "required": True},
           {"id": 2, "name": "customer", "type": "string", "required": True},
           {"id": 3, "name": "amount", "type": "double", "required": True}
       ]
   )

   table = create_table("/path/to/orders", order_schema)

   # Retry logic with exponential backoff
   max_retries = 5
   for attempt in range(max_retries):
       try:
           order_data = [{"order_id": 100, "customer": "Alice", "amount": 250.50}]
           success = table.append_records(records=order_data, schema=order_schema)

           if success:
               print("Order added successfully")
               break
       except ConcurrentModificationException:
           if attempt == max_retries - 1:
               raise
           # Wait and retry with exponential backoff
           import time
           time.sleep(0.1 * (2 ** attempt))

Transaction Isolation Levels
-----------------------------

datashard provides snapshot isolation, which means:

- Each transaction sees a consistent snapshot of the table as it existed when the transaction began
- Concurrent transactions do not see each other's uncommitted changes
- This prevents dirty reads, non-repeatable reads, and phantom reads

Example: Snapshot Isolation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Transaction 1 starts
   tx1 = table.new_transaction().begin()

   # Transaction 2 starts and commits
   tx2_data = [{"emp_id": 4, "name": "Frank", "department": "HR"}]
   table.append_records(records=tx2_data, schema=employee_schema)

   # Transaction 1 continues - does NOT see tx2's changes
   # tx1 sees the snapshot from when it started
   tx1.append_data(
       records=[{"emp_id": 5, "name": "Grace", "department": "Finance"}],
       schema=employee_schema
   )
   tx1.commit()

Error Handling
--------------

When using transactions, it's important to handle potential errors:

.. code-block:: python

   from datashard import create_table, Schema

   # Define schema
   log_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "log_id", "type": "long", "required": True},
           {"id": 2, "name": "message", "type": "string", "required": True},
           {"id": 3, "name": "level", "type": "string", "required": True}
       ]
   )

   table = create_table("/path/to/logs", log_schema)

   try:
       log_data = [
           {"log_id": 1, "message": "Application started", "level": "INFO"},
           {"log_id": 2, "message": "User logged in", "level": "INFO"}
       ]

       success = table.append_records(records=log_data, schema=log_schema)

       if not success:
           print("Transaction did not commit successfully")
       else:
           print("Logs added successfully")

   except FileNotFoundError as e:
       print(f"File referenced in transaction does not exist: {e}")
   except ConcurrentModificationException as e:
       print(f"Another process modified the table: {e}")
   except Exception as e:
       print(f"Unexpected error during transaction: {e}")

Best Practices
--------------

Transaction Design
^^^^^^^^^^^^^^^^^^

1. **Keep Transactions Short**: Shorter transactions reduce the chance of conflicts
2. **Use Automatic Transactions**: Let ``append_records()`` handle transactions when possible
3. **Implement Retry Logic**: Handle ``ConcurrentModificationException`` with exponential backoff
4. **Batch Operations**: Group related operations into a single transaction for atomicity

Performance Considerations
^^^^^^^^^^^^^^^^^^^^^^^^^^

- Larger transactions (more records) are more efficient than many small transactions
- Snapshot isolation adds minimal overhead
- OCC works best when conflicts are rare
- Consider partitioning data to reduce transaction conflicts

Example: Complete Transaction Workflow
---------------------------------------

.. code-block:: python

   from datashard import create_table, Schema, ConcurrentModificationException
   import time

   # 1. Define schema
   event_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "event_id", "type": "long", "required": True},
           {"id": 2, "name": "event_type", "type": "string", "required": True},
           {"id": 3, "name": "timestamp", "type": "long", "required": True},
           {"id": 4, "name": "data", "type": "string", "required": False}
       ]
   )

   # 2. Create table
   table = create_table("/tmp/events", event_schema)

   # 3. Add data with retry logic
   events = [
       {"event_id": 1, "event_type": "click", "timestamp": 1700000000000, "data": "button1"},
       {"event_id": 2, "event_type": "pageview", "timestamp": 1700000001000, "data": "home"}
   ]

   max_retries = 3
   for attempt in range(max_retries):
       try:
           success = table.append_records(records=events, schema=event_schema)
           if success:
               print(f"Successfully added {len(events)} events")
               break
       except ConcurrentModificationException:
           if attempt == max_retries - 1:
               print("Failed after maximum retries")
               raise
           time.sleep(0.1 * (2 ** attempt))

   # 4. Verify the transaction
   current_snapshot = table.current_snapshot()
   print(f"Current snapshot: {current_snapshot.snapshot_id if current_snapshot else 'None'}")

   # 5. Clean up
   import shutil
   shutil.rmtree("/tmp/events")

   print("Transaction workflow completed successfully")
