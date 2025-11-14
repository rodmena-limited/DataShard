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

   from datashard import create_table, Schema

   # Define schema
   history_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "event_id", "type": "long", "required": True},
           {"id": 2, "name": "event_name", "type": "string", "required": True},
           {"id": 3, "name": "timestamp", "type": "long", "required": True}
       ]
   )

   # Create table
   table = create_table("/path/to/historical_table", history_schema)

   # Each append creates a new snapshot
   event1 = [{"event_id": 1, "event_name": "user_login", "timestamp": 1700000000000}]
   table.append_records(records=event1, schema=history_schema)  # Creates snapshot 1

   event2 = [{"event_id": 2, "event_name": "user_logout", "timestamp": 1700000100000}]
   table.append_records(records=event2, schema=history_schema)  # Creates snapshot 2

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

   from datashard import create_table, Schema
   import time

   # Define schema
   event_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "event_id", "type": "long", "required": True},
           {"id": 2, "name": "event_type", "type": "string", "required": True},
           {"id": 3, "name": "value", "type": "long", "required": True}
       ]
   )

   table = create_table("/path/to/table", event_schema)

   # Record a timestamp before making changes
   before_time_ms = int(time.time() * 1000)

   # Make some changes
   changes = [{"event_id": 3, "event_type": "update", "value": 100}]
   table.append_records(records=changes, schema=event_schema)

   # Later, travel back to the state before the changes
   historical_data = table.time_travel(timestamp=before_time_ms)
   if historical_data:
       print(f"Traveled to state at timestamp: {before_time_ms}")

Practical Time Travel Example
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from datashard import create_table, Schema
   from datetime import datetime, timedelta
   import time

   # Define schema for events
   demo_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "event", "type": "string", "required": True},
           {"id": 2, "name": "value", "type": "long", "required": True}
       ]
   )

   # Create a table and add data over time
   historical_table = create_table("/tmp/time_travel_demo", demo_schema)

   # Add initial data
   initial_data = [{"event": "start", "value": 100}]
   historical_table.append_records(records=initial_data, schema=demo_schema)

   # Record the state after initial data
   initial_time = int(time.time() * 1000)
   time.sleep(1)  # Wait a moment

   # Add more data
   update_data = [{"event": "update", "value": 200}]
   historical_table.append_records(records=update_data, schema=demo_schema)

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

   from datashard import create_table, Schema

   # Define schema for user data
   user_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "user_id", "type": "long", "required": True},
           {"id": 2, "name": "username", "type": "string", "required": True},
           {"id": 3, "name": "status", "type": "string", "required": True}
       ]
   )

   table = create_table("/path/to/users", user_schema)

   # Add good data
   good_data = [{"user_id": 1, "username": "alice", "status": "active"}]
   table.append_records(records=good_data, schema=user_schema)

   # Get the good snapshot ID
   good_snapshot = table.current_snapshot()
   good_snapshot_id = good_snapshot.snapshot_id if good_snapshot else None

   # If bad data was accidentally written, recover from a previous snapshot
   if good_snapshot_id:
       recovered_data = table.time_travel(snapshot_id=good_snapshot_id)
       print(f"Recovered to known good state: {recovered_data.snapshot_id}")

Reproducible Analysis
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from datashard import create_table, Schema
   import time

   # Define schema for ML training data
   training_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "sample_id", "type": "long", "required": True},
           {"id": 2, "name": "feature_1", "type": "double", "required": True},
           {"id": 3, "name": "feature_2", "type": "double", "required": True},
           {"id": 4, "name": "label", "type": "long", "required": True}
       ]
   )

   table = create_table("/path/to/ml_data", training_schema)

   # Add training data
   training_data = [
       {"sample_id": 1, "feature_1": 0.5, "feature_2": 0.8, "label": 1},
       {"sample_id": 2, "feature_1": 0.3, "feature_2": 0.6, "label": 0}
   ]
   table.append_records(records=training_data, schema=training_schema)

   # For ML/AI workflows, ensure analysis is reproducible
   analysis_timestamp = int(time.time() * 1000)

   # Perform analysis on historical data
   historical_analysis_data = table.time_travel(timestamp=analysis_timestamp)

   # Now your analysis is tied to a specific point in time
   # and can be reproduced later

Auditing and Compliance
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from datashard import create_table, Schema

   # Define schema for audit logs
   audit_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "audit_id", "type": "long", "required": True},
           {"id": 2, "name": "operation", "type": "string", "required": True},
           {"id": 3, "name": "user", "type": "string", "required": True},
           {"id": 4, "name": "timestamp", "type": "long", "required": True}
       ]
   )

   table = create_table("/path/to/audit_log", audit_schema)

   # Add audit records
   audit_records = [
       {"audit_id": 1, "operation": "INSERT", "user": "admin", "timestamp": 1700000000000},
       {"audit_id": 2, "operation": "UPDATE", "user": "alice", "timestamp": 1700000100000}
   ]
   table.append_records(records=audit_records, schema=audit_schema)

   # Track how data has changed over time for compliance
   snapshots = table.snapshots()
   for snapshot in snapshots:
       print(f"Audit log - Snapshot {snapshot['snapshot_id']} created by {snapshot['operation']} operation")

Advanced Time Travel
--------------------

Working with Multiple Tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from datashard import create_table, Schema

   # Define schemas for related tables
   orders_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "order_id", "type": "long", "required": True},
           {"id": 2, "name": "customer_id", "type": "long", "required": True},
           {"id": 3, "name": "amount", "type": "double", "required": True}
       ]
   )

   customers_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "customer_id", "type": "long", "required": True},
           {"id": 2, "name": "name", "type": "string", "required": True}
       ]
   )

   # Synchronize time travel across multiple related tables
   # This requires careful coordination of snapshot IDs across tables

   table1 = create_table("/tmp/orders", orders_schema)
   table2 = create_table("/tmp/customers", customers_schema)

   # Add data to both tables
   table1.append_records(
       records=[{"order_id": 1, "customer_id": 100, "amount": 99.99}],
       schema=orders_schema
   )
   table2.append_records(
       records=[{"customer_id": 100, "name": "Alice"}],
       schema=customers_schema
   )

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

Complete Time Travel Workflow
------------------------------

.. code-block:: python

   from datashard import create_table, Schema
   import time
   import shutil

   # 1. Define schema
   workflow_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "step_id", "type": "long", "required": True},
           {"id": 2, "name": "step_name", "type": "string", "required": True},
           {"id": 3, "name": "status", "type": "string", "required": True},
           {"id": 4, "name": "timestamp", "type": "long", "required": True}
       ]
   )

   # 2. Create table
   table = create_table("/tmp/workflow_history", workflow_schema)

   # 3. Add initial data
   step1 = [{"step_id": 1, "step_name": "init", "status": "completed", "timestamp": int(time.time() * 1000)}]
   table.append_records(records=step1, schema=workflow_schema)

   snapshot1 = table.current_snapshot()
   snapshot1_id = snapshot1.snapshot_id if snapshot1 else None
   print(f"Created snapshot 1: {snapshot1_id}")

   time.sleep(0.1)

   # 4. Add more data
   step2 = [{"step_id": 2, "step_name": "process", "status": "completed", "timestamp": int(time.time() * 1000)}]
   table.append_records(records=step2, schema=workflow_schema)

   snapshot2 = table.current_snapshot()
   snapshot2_id = snapshot2.snapshot_id if snapshot2 else None
   print(f"Created snapshot 2: {snapshot2_id}")

   # 5. Time travel to snapshot 1
   if snapshot1_id:
       historical = table.time_travel(snapshot_id=snapshot1_id)
       print(f"Time traveled to snapshot: {historical.snapshot_id if historical else 'None'}")

   # 6. List all snapshots
   all_snapshots = table.snapshots()
   print(f"Total snapshots: {len(all_snapshots)}")

   # 7. Clean up
   shutil.rmtree("/tmp/workflow_history")

   print("Time travel workflow completed successfully")
