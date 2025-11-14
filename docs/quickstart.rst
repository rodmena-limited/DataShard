Quickstart Guide
================

This guide will get you up and running with datashard in just a few minutes. We'll cover the basics of creating tables, adding data, and using time travel.

Creating Your First Table
-------------------------

Let's create a simple table to store user data:

.. code-block:: python

   from datashard import create_table, Schema, FileFormat
   import os

   # Create a new table
   table_path = "/tmp/my_first_table"
   table = create_table(table_path)

   print(f"Table created at: {table_path}")

Adding Data with Transactions
-----------------------------

Now let's add some data using ACID transactions:

.. code-block:: python

   from datashard import DataFile
   import json

   # Sample data to add
   sample_data = [
       {"user_id": 1, "name": "Alice", "age": 30, "email": "alice@example.com"},
       {"user_id": 2, "name": "Bob", "age": 25, "email": "bob@example.com"},
       {"user_id": 3, "name": "Charlie", "age": 35, "email": "charlie@example.com"}
   ]

   # Add data using a transaction
   with table.new_transaction() as tx:
       # Append the records to the table
       success = table.append_records(
           records=sample_data,
           schema=None  # Using default schema for this example
       )
       result = tx.commit()
       
   print(f"Data added successfully: {result}")

Reading Data and Snapshots
--------------------------

Let's check what snapshots are available and access the current state:

.. code-block:: python

   # Get current snapshot
   current_snapshot = table.current_snapshot()
   print(f"Current snapshot ID: {current_snapshot.snapshot_id if current_snapshot else 'None'}")

   # List all snapshots
   all_snapshots = table.snapshots()
   print(f"Number of snapshots: {len(all_snapshots)}")

   for snapshot in all_snapshots:
       print(f"  - Snapshot {snapshot['snapshot_id']} at {snapshot['timestamp']}")

Time Travel Example
-------------------

One of the key features of datashard is time travel. Let's see how to travel back to a previous snapshot:

.. code-block:: python

   # If you have multiple snapshots, you can time travel to a specific one
   if len(all_snapshots) > 0:
       # Get first snapshot (oldest)
       first_snapshot = all_snapshots[0]
       historical_data = table.time_travel(snapshot_id=first_snapshot['snapshot_id'])
       print(f"Traveled to snapshot: {first_snapshot['snapshot_id']}")

Advanced Example: Complex Data Operations
-----------------------------------------

Here's a more comprehensive example that demonstrates multiple features:

.. code-block:: python

   from datashard import DataFile, FileFormat
   import os

   # Create a new table with more complex data
   complex_table_path = "/tmp/complex_table"
   complex_table = create_table(complex_table_path)

   # Add more complex data using multiple transactions
   complex_data = [
       {"id": 1, "category": "A", "value": 100.0, "timestamp": "2023-01-01"},
       {"id": 2, "category": "B", "value": 200.5, "timestamp": "2023-01-02"},
       {"id": 3, "category": "A", "value": 150.0, "timestamp": "2023-01-03"}
   ]

   # Add data in a transaction
   with complex_table.new_transaction() as tx:
       success = complex_table.append_records(
           records=complex_data,
           schema=None
       )
       tx.commit()

   # Verify the data was added
   print(f"Table has {len(complex_table.snapshots())} snapshots after adding complex data")

   # Clean up (optional)
   import shutil
   shutil.rmtree("/tmp/my_first_table")
   shutil.rmtree("/tmp/complex_table")

What's Next?
------------

Now that you've completed the quickstart, you can:

- Read about :doc:`concepts` to understand the core ideas behind datashard
- Learn more about :doc:`transactions` for safe concurrent operations
- Explore :doc:`time_travel` for historical data access
- Dive into the :doc:`api/iceberg` for detailed API documentation