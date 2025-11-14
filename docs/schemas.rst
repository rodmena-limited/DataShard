Schemas and Data Types
======================

This section covers how datashard handles schemas and the various data types supported.

Schema Overview
---------------

A schema in datashard defines the structure of data in a table. It specifies what columns are available, their data types, and other properties. Schemas are immutable - each schema change creates a new schema version.

Creating Schemas
----------------

Schema Definition
^^^^^^^^^^^^^^^^^

.. code-block:: python

   from datashard import Schema

   # Define schema fields with required field IDs
   user_schema = Schema(
       schema_id=1,  # Required: unique schema version ID
       fields=[
           {"id": 1, "name": "id", "type": "long", "required": True},
           {"id": 2, "name": "name", "type": "string", "required": True},
           {"id": 3, "name": "age", "type": "long", "required": False},
           {"id": 4, "name": "salary", "type": "double", "required": False}
       ]
   )

Data Types
----------

datashard supports various data types through the underlying Parquet file format:

Supported Types
^^^^^^^^^^^^^^^

- **boolean**: True/False values
- **int**: 32-bit signed integers
- **long**: 64-bit signed integers
- **float**: 32-bit IEEE 754 floating point
- **double**: 64-bit IEEE 754 floating point
- **string**: UTF-8 encoded character sequences

Type Selection Guidelines
^^^^^^^^^^^^^^^^^^^^^^^^^^

- Use **long** for integer IDs and counters
- Use **int** for smaller integer values where range is limited
- Use **double** for floating-point numbers (prices, measurements)
- Use **string** for text data (names, descriptions, emails)
- Use **boolean** for true/false flags

Working with Schemas
--------------------

Creating a Table with Schema
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from datashard import create_table, Schema

   # Define schema
   employee_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "employee_id", "type": "long", "required": True},
           {"id": 2, "name": "name", "type": "string", "required": True},
           {"id": 3, "name": "department", "type": "string", "required": True},
           {"id": 4, "name": "salary", "type": "double", "required": True}
       ]
   )

   # Create table with schema
   table = create_table("/path/to/employee_table", employee_schema)

   print(f"Table created with schema version {employee_schema.schema_id}")

Appending Data with Schema
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Sample employee records
   employee_records = [
       {"employee_id": 1, "name": "Alice", "department": "Engineering", "salary": 95000.0},
       {"employee_id": 2, "name": "Bob", "department": "Sales", "salary": 75000.0},
       {"employee_id": 3, "name": "Charlie", "department": "Marketing", "salary": 80000.0}
   ]

   # Append records - schema ensures data consistency
   success = table.append_records(records=employee_records, schema=employee_schema)

   if success:
       print(f"Successfully added {len(employee_records)} employees")

Schema Validation
^^^^^^^^^^^^^^^^^

datashard automatically validates that records match the expected schema during append operations, preventing invalid data from being added:

.. code-block:: python

   # This will fail validation - missing required field
   invalid_record = [
       {"employee_id": 4, "name": "David"}  # Missing department and salary
   ]

   try:
       table.append_records(records=invalid_record, schema=employee_schema)
   except Exception as e:
       print(f"Validation failed: {e}")

Schema Evolution
----------------

Schemas are immutable in datashard. To evolve a schema, create a new schema version:

Creating a New Schema Version
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Original schema (version 1)
   original_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "user_id", "type": "long", "required": True},
           {"id": 2, "name": "name", "type": "string", "required": True}
       ]
   )

   # Evolved schema (version 2) - adds email field
   evolved_schema = Schema(
       schema_id=2,
       fields=[
           {"id": 1, "name": "user_id", "type": "long", "required": True},
           {"id": 2, "name": "name", "type": "string", "required": True},
           {"id": 3, "name": "email", "type": "string", "required": False}
       ]
   )

   # New data uses the evolved schema
   new_records = [
       {"user_id": 5, "name": "Eve", "email": "eve@example.com"}
   ]

   table.append_records(records=new_records, schema=evolved_schema)

Schema Evolution Rules
^^^^^^^^^^^^^^^^^^^^^^

When evolving schemas:

1. **Keep field IDs stable**: Existing field IDs should not change
2. **Add new fields as optional**: New fields should have ``required: False``
3. **Don't remove fields**: Historical data may reference removed fields
4. **Don't change field types**: Type changes can break historical data access
5. **Increment schema_id**: Each schema version needs a unique ID

Schema Management
^^^^^^^^^^^^^^^^^

Accessing Current Schema
""""""""""""""""""""""""

.. code-block:: python

   # Get the current schema of a table
   current_metadata = table.metadata_manager.refresh()

   if current_metadata and current_metadata.schemas:
       current_schema_id = current_metadata.current_schema_id
       current_schema = next(
           (s for s in current_metadata.schemas if s.schema_id == current_schema_id),
           None
       )

       if current_schema:
           print(f"Current schema version: {current_schema.schema_id}")
           print(f"Number of fields: {len(current_schema.fields)}")

           for field in current_schema.fields:
               print(f"  - {field['name']}: {field['type']}")

Schema History
""""""""""""""

.. code-block:: python

   # Access all schema versions
   if current_metadata:
       print("Schema history:")
       for schema in current_metadata.schemas:
           print(f"  Schema {schema.schema_id}: {len(schema.fields)} fields")

Best Practices
--------------

Schema Design
^^^^^^^^^^^^^

1. **Plan Ahead**: Consider future data evolution needs when designing schemas
2. **Use Appropriate Types**: Choose the most specific type that fits your data

   - Use ``long`` for IDs and large integers
   - Use ``double`` for decimal numbers
   - Use ``string`` for text
   - Use ``boolean`` for flags

3. **Required vs Optional**: Mark fields as required only if they will always have values
4. **Field IDs**: Assign field IDs sequentially starting from 1
5. **Naming**: Use clear, descriptive field names (snake_case recommended)

Performance Considerations
^^^^^^^^^^^^^^^^^^^^^^^^^^

- Fewer fields = faster reads
- Simple types (long, string) are faster than complex computations
- Schema validation adds minimal overhead
- Field order doesn't affect performance

Example: Complete Schema Workflow
----------------------------------

.. code-block:: python

   from datashard import create_table, Schema
   import os

   # 1. Define initial schema
   metrics_schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "timestamp", "type": "long", "required": True},
           {"id": 2, "name": "metric_name", "type": "string", "required": True},
           {"id": 3, "name": "value", "type": "double", "required": True}
       ]
   )

   # 2. Create table
   metrics_table = create_table("/tmp/metrics", metrics_schema)

   # 3. Add initial data
   initial_metrics = [
       {"timestamp": 1700000000000, "metric_name": "cpu_usage", "value": 45.2},
       {"timestamp": 1700000000000, "metric_name": "memory_usage", "value": 62.8}
   ]

   metrics_table.append_records(records=initial_metrics, schema=metrics_schema)

   # 4. Evolve schema to add tags
   enhanced_schema = Schema(
       schema_id=2,
       fields=[
           {"id": 1, "name": "timestamp", "type": "long", "required": True},
           {"id": 2, "name": "metric_name", "type": "string", "required": True},
           {"id": 3, "name": "value", "type": "double", "required": True},
           {"id": 4, "name": "tags", "type": "string", "required": False}  # New field
       ]
   )

   # 5. Add new data with enhanced schema
   tagged_metrics = [
       {"timestamp": 1700000100000, "metric_name": "cpu_usage", "value": 52.1, "tags": "production"}
   ]

   metrics_table.append_records(records=tagged_metrics, schema=enhanced_schema)

   # 6. Clean up
   import shutil
   shutil.rmtree("/tmp/metrics")

   print("Schema workflow completed successfully")
