Quickstart Guide
================

This guide will get you up and running with datashard in just a few minutes. We'll cover the basics of creating tables, adding data, and using time travel.

Creating Your First Table
-------------------------

Let's create a simple table to store user data:

.. code-block:: python

   from datashard import create_table, Schema
   import os

   # Define schema for user data
   user_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "user_id", "type": "long", "required": True},
           {"id": 2, "name": "name", "type": "string", "required": True},
           {"id": 3, "name": "age", "type": "long", "required": True},
           {"id": 4, "name": "email", "type": "string", "required": True}
       ]
   )

   # Create a new table
   table_path = "/tmp/my_first_table"
   table = create_table(table_path, user_schema)

   print(f"Table created at: {table_path}")

Adding Data with Transactions
-----------------------------

Now let's add some data using ACID transactions:

.. code-block:: python

   # Sample data to add
   sample_data = [
       {"user_id": 1, "name": "Alice", "age": 30, "email": "alice@example.com"},
       {"user_id": 2, "name": "Bob", "age": 25, "email": "bob@example.com"},
       {"user_id": 3, "name": "Charlie", "age": 35, "email": "charlie@example.com"}
   ]

   # Add data - append_records handles transactions automatically
   success = table.append_records(records=sample_data, schema=user_schema)

   print(f"Data added successfully: {success}")

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

   from datashard import create_table, Schema
   import os

   # Define schema for complex data
   complex_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "id", "type": "long", "required": True},
           {"id": 2, "name": "category", "type": "string", "required": True},
           {"id": 3, "name": "value", "type": "double", "required": True},
           {"id": 4, "name": "timestamp", "type": "string", "required": True}
       ]
   )

   # Create a new table with more complex data
   complex_table_path = "/tmp/complex_table"
   complex_table = create_table(complex_table_path, complex_schema)

   # Add more complex data
   complex_data = [
       {"id": 1, "category": "A", "value": 100.0, "timestamp": "2023-01-01"},
       {"id": 2, "category": "B", "value": 200.5, "timestamp": "2023-01-02"},
       {"id": 3, "category": "A", "value": 150.0, "timestamp": "2023-01-03"}
   ]

   # Add data
   success = complex_table.append_records(records=complex_data, schema=complex_schema)

   # Verify the data was added
   print(f"Table has {len(complex_table.snapshots())} snapshots after adding complex data")

Reading Data with Filters
-------------------------

DataShard supports efficient data reading with predicate pushdown and parallel processing:

.. code-block:: python

   # Basic scan - read all data
   all_records = table.scan()
   print(f"Total records: {len(all_records)}")

   # Scan with filter (predicate pushdown - filters at parquet level)
   filtered = table.scan(filter={"name": "Alice"})
   print(f"Records for Alice: {len(filtered)}")

   # Comparison filters
   young_users = table.scan(filter={"age": ("<", 30)})

   # Range filter
   age_range = table.scan(filter={"age": ("between", (25, 35))})

   # IN filter
   specific_users = table.scan(filter={"user_id": ("in", [1, 2])})

   # Column projection (only read specific columns)
   names_only = table.scan(columns=["name", "email"])

   # Parallel reading (2-4x speedup with multiple files)
   results = table.scan(parallel=True)  # Use all CPU cores
   results = table.scan(parallel=4)     # Use 4 threads

   # Combine filter, columns, and parallel
   df = table.to_pandas(
       columns=["name", "age"],
       filter={"age": (">=", 25)},
       parallel=True
   )

Streaming Large Tables
----------------------

For memory-efficient processing of large tables:

.. code-block:: python

   # Process in batches (memory-efficient)
   for batch in table.scan_batches(batch_size=1000):
       for record in batch:
           print(record)

   # Iterate record by record
   for record in table.iter_records(filter={"age": (">", 30)}):
       print(record["name"])

   # Iterate as pandas DataFrame chunks
   for chunk_df in table.iter_pandas(chunksize=10000):
       # Process each chunk with pandas operations
       summary = chunk_df.groupby("name").count()
       print(summary)

Cleaning Up
-----------

After you're done, you can remove the tables:

.. code-block:: python

   import shutil
   # Clean up (optional)
   if os.path.exists("/tmp/my_first_table"):
       shutil.rmtree("/tmp/my_first_table")
   if os.path.exists("/tmp/complex_table"):
       shutil.rmtree("/tmp/complex_table")

What's Next?
------------

Now that you've completed the quickstart, you can:

- Read about :doc:`concepts` to understand the core ideas behind datashard
- Learn more about :doc:`transactions` for safe concurrent operations
- Explore :doc:`time_travel` for historical data access
- Dive into the :doc:`api/iceberg` for detailed API documentation
