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

There are two ways to use transactions in datashard:

Context Manager Approach (Recommended)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from datashard import create_table

   table = create_table("/path/to/table")

   # Using the context manager automatically handles commit/rollback
   with table.new_transaction() as tx:
       # Add operations to the transaction
       table.append_records(
           records=[{"id": 1, "name": "Alice"}],
           schema=None
       )
       # Transaction automatically commits upon successful exit
       # If an exception occurs, transaction is rolled back

Explicit Transaction Management
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Create and manage the transaction manually
   tx = table.new_transaction().begin()

   try:
       # Add operations to the transaction
       table.append_records(
           records=[{"id": 2, "name": "Bob"}],
           schema=None
       )
       
       # Commit the transaction
       if tx.commit():
           print("Transaction committed successfully")
       else:
           print("Transaction failed to commit")
   except Exception as e:
       # Roll back on error
       tx.rollback()
       print(f"Transaction rolled back due to error: {e}")

Transaction Operations
----------------------

datashard supports several types of operations within transactions:

Append Files
^^^^^^^^^^^^

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

Append Records
^^^^^^^^^^^^^^

.. code-block:: python

   with table.new_transaction() as tx:
       # Add records directly (datashard will create appropriate data files)
       tx.append_data(
           records=[{"id": 3, "name": "Charlie", "age": 28}],
           schema=None,  # Use default schema
           partition_values={"year": 2023}
       )

Delete Files
^^^^^^^^^^^^

.. code-block:: python

   with table.new_transaction() as tx:
       # Delete specific files from the table
       tx.delete_files(["/path/to/old_data.parquet"])

Overwrite by Filter
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   with table.new_transaction() as tx:
       # Replace data matching a filter condition
       tx.overwrite_by_filter(lambda record: record.get("age", 0) > 30)

Concurrency Control
-------------------

datashard uses Optimistic Concurrency Control (OCC) to handle concurrent access:

- Each transaction reads the current state of the table
- Operations are queued locally during the transaction
- When committing, datashard checks if the table has changed since the transaction began
- If changes occurred, the transaction is rejected and may be retried

Retry Logic
^^^^^^^^^^^

.. code-block:: python

   from datashard import ConcurrentModificationException

   max_retries = 5
   for attempt in range(max_retries):
       try:
           with table.new_transaction() as tx:
               # Perform operations
               tx.append_data(records=[{"id": 4, "name": "David"}], schema=None)
               success = tx.commit()
               if success:
                   break
       except ConcurrentModificationException:
           if attempt == max_retries - 1:
               raise
           # Wait and retry
           import time
           time.sleep(0.1 * (attempt + 1))  # Exponential backoff

Transaction Isolation Levels
----------------------------

datashard provides snapshot isolation, which means:

- Each transaction sees a consistent snapshot of the table as it existed when the transaction began
- Concurrent transactions do not see each other's uncommitted changes
- This prevents dirty reads, non-repeatable reads, and phantom reads

Error Handling
--------------

When using transactions, it's important to handle potential errors:

.. code-block:: python

   try:
       with table.new_transaction() as tx:
           # Perform operations
           tx.append_data(records=[{"id": 5, "name": "Eve"}], schema=None)
           result = tx.commit()
           
           if not result:
               print("Transaction did not commit successfully")
           else:
               print("Transaction completed successfully")
   except FileNotFoundError as e:
       print(f"File referenced in transaction does not exist: {e}")
   except Exception as e:
       print(f"Unexpected error during transaction: {e}")