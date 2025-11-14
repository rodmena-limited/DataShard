Time Travel and Historical Data Access
======================================

One of datashard's most powerful features is time travel, which allows you to query data as it existed at any point in time.

Understanding Time Travel
-------------------------

Time travel in datashard allows you to access historical states of your data by navigating between snapshots. Each snapshot represents the table's state at a specific point in time.

Snapshot Basics
---------------

What is a Snapshot?
^^^^^^^^^^^^^^^^^^^

A snapshot in datashard captures the complete state of a table at a specific moment:

- **Snapshot ID**: A unique identifier for the snapshot
- **Timestamp**: When the snapshot was created
- **Manifest List**: References to all data files at that point in time
- **Operation**: What operation created the snapshot (append, replace, delete)
- **Parent ID**: Link to the previous snapshot for lineage tracking

Creating Snapshots
^^^^^^^^^^^^^^^^^^

Snapshots are automatically created when you commit a transaction:

.. code-block:: python

   from datashard import create_table

   table = create_table("/path/to/historical_table")

   # Each transaction commit creates a new snapshot
   with table.new_transaction() as tx:
       table.append_records(
           records=[{"id": 1, "name": "Alice"}],
           schema=None
       )
       tx.commit()  # Creates snapshot 1

   with table.new_transaction() as tx:
       table.append_records(
           records=[{"id": 2, "name": "Bob"}],
           schema=None
       )
       tx.commit()  # Creates snapshot 2

Accessing Snapshots
-------------------

Listing Snapshots
^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Get all snapshots for the table
   snapshots = table.snapshots()
   
   print(f"Table has {len(snapshots)} snapshots")
   
   for snapshot in snapshots:
       print(f"Snapshot {snapshot['snapshot_id']} at {snapshot['timestamp']}")
       print(f"  Operation: {snapshot['operation']}")
       print(f"  Parent ID: {snapshot['parent_id']}")

Getting Current Snapshot
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Get the current (most recent) snapshot
   current_snapshot = table.current_snapshot()
   
   if current_snapshot:
       print(f"Current snapshot ID: {current_snapshot.snapshot_id}")
       print(f"Created at: {current_snapshot.timestamp_ms}")

Accessing Specific Snapshots
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Get a specific snapshot by ID
   specific_snapshot = table.snapshot_by_id(1234567890)
   
   if specific_snapshot:
       print(f"Found snapshot: {specific_snapshot.snapshot_id}")

Time Travel Operations
----------------------

Travel to Specific Snapshot
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Time travel to a specific snapshot
   historical_data = table.time_travel(snapshot_id=1234567890)
   
   # The returned data represents the table state at that snapshot
   if historical_data:
       print(f"Traveled to snapshot: {historical_data.snapshot_id}")

Travel to Specific Time
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   import time
   
   # Record a timestamp before making changes
   before_time_ms = int(time.time() * 1000)
   
   # Make some changes
   with table.new_transaction() as tx:
       table.append_records(
           records=[{"id": 3, "name": "Charlie"}],
           schema=None
       )
       tx.commit()
   
   # Later, travel back to the state before the changes
   historical_data = table.time_travel(timestamp=before_time_ms)
   if historical_data:
       print(f"Traveled to state at timestamp: {before_time_ms}")

Practical Time Travel Example
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from datetime import datetime, timedelta
   import time

   # Create a table and add data over time
   historical_table = create_table("/tmp/time_travel_demo")

   # Add initial data
   with historical_table.new_transaction() as tx:
       tx.append_data(
           records=[{"event": "start", "value": 100}],
           schema=None
       )
       tx.commit()

   # Record the state after initial data
   initial_time = int(time.time() * 1000)
   time.sleep(1)  # Wait a moment

   # Add more data
   with historical_table.new_transaction() as tx:
       tx.append_data(
           records=[{"event": "update", "value": 200}],
           schema=None
       )
       tx.commit()

   # Travel back to the initial state
   initial_state = historical_table.time_travel(timestamp=initial_time)
   print(f"State at initial_time: {initial_state.snapshot_id if initial_state else 'None'}")

   # Travel to the current state
   current_state = historical_table.time_travel()
   print(f"Current state: {current_state.snapshot_id if current_state else 'None'}")

Managing Snapshots
------------------

Snapshot History
^^^^^^^^^^^^^^^^

.. code-block:: python

   # Get the complete history of snapshots
   history = table.metadata_manager.get_snapshot_history()
   
   for entry in history:
       print(f"Snapshot {entry.snapshot_id} at {entry.timestamp_ms}")

Snapshot Retention
^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # You can expire old snapshots to save space
   with table.new_transaction() as tx:
       # Expire snapshots older than 1 week (in milliseconds)
       one_week_ms = 7 * 24 * 60 * 60 * 1000
       tx.expire_snapshots(older_than_ms=one_week_ms)
       tx.commit()

Use Cases for Time Travel
-------------------------

Data Recovery
^^^^^^^^^^^^^

Time travel is invaluable for data recovery scenarios:

.. code-block:: python

   # If bad data was accidentally written, recover from a previous snapshot
   good_snapshot_id = 1678886400000000  # A known good state
   good_data = table.time_travel(snapshot_id=good_snapshot_id)
   print(f"Recovered to known good state: {good_data.snapshot_id}")

Reproducible Analysis
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # For ML/AI workflows, ensure analysis is reproducible
   analysis_timestamp = int(time.time() * 1000)
   
   # Perform analysis on historical data
   historical_analysis_data = table.time_travel(timestamp=analysis_timestamp)
   
   # Now your analysis is tied to a specific point in time
   # and can be reproduced later

Auditing and Compliance
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Track how data has changed over time for compliance
   snapshots = table.snapshots()
   for snapshot in snapshots:
       print(f"Audit log - Snapshot {snapshot['snapshot_id']} created by {snapshot['operation']} operation")

Advanced Time Travel
--------------------

Working with Multiple Tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Synchronize time travel across multiple related tables
   # This requires careful coordination of snapshot IDs across tables

   table1 = create_table("/tmp/table1")
   table2 = create_table("/tmp/table2")

   # Get a snapshot ID from one table
   ref_snapshot = table1.current_snapshot()

   # Travel both tables to consistent points in time
   if ref_snapshot:
       table1_data = table1.time_travel(snapshot_id=ref_snapshot.snapshot_id)
       table2_data = table2.time_travel(snapshot_id=ref_snapshot.snapshot_id)

Performance Considerations
^^^^^^^^^^^^^^^^^^^^^^^^^^

- Time travel queries may be slower than current data access
- Keeping many snapshots requires additional storage
- Consider snapshot retention policies for large datasets
- Accessing very old snapshots may require reading more historical metadata files