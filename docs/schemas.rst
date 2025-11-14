Schemas and Data Types
======================

This section covers how datashard handles schemas and the various data types supported.

Schema Overview
---------------

A schema in datashard defines the structure of data in a table. It specifies what columns are available, their data types, and other properties. Schemas can evolve over time while maintaining historical consistency.

Creating Schemas
----------------

Schema Definition
^^^^^^^^^^^^^^^^^

.. code-block:: python

   from datashard import Schema

   # Define schema fields
   fields = [
       {"name": "id", "type": "int", "required": True},
       {"name": "name", "type": "string", "required": True},
       {"name": "age", "type": "int", "required": False},
       {"name": "salary", "type": "float", "required": False}
   ]

   # Create schema
   schema = Schema(
       schema_id=1,
       fields=fields
   )

Schema Evolution
----------------

datashard supports schema evolution, allowing you to modify table schemas over time while preserving historical data access.

Adding Columns
^^^^^^^^^^^^^^

.. code-block:: python

   # Add a new column to existing data
   new_field = {"name": "department", "type": "string", "required": False, "default": "unassigned"}

   # This would typically be handled through metadata updates
   # The old data files remain unchanged, but new files can include the new column

Data Types
----------

datashard supports various data types through the underlying file formats (Parquet, Avro, ORC):

Primitive Types
^^^^^^^^^^^^^^^

- **Boolean**: True/False values
- **Integer**: 32-bit signed integers
- **Long**: 64-bit signed integers  
- **Float**: 32-bit IEEE 754 floating point
- **Double**: 64-bit IEEE 754 floating point
- **String**: UTF-8 encoded character sequences
- **UUID**: Universally unique identifiers
- **Fixed**: Fixed-length byte arrays
- **Binary**: Arbitrary-length byte arrays

Nested Types
^^^^^^^^^^^^

- **Struct**: Nested records with named fields (like a table within a table)
- **List**: Ordered sequence of values of the same type
- **Map**: Key-value pairs where keys are primitives and values can be complex

Example with Complex Types
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   complex_schema_fields = [
       {
           "name": "user",
           "type": "struct",
           "fields": [
               {"name": "id", "type": "long"},
               {"name": "name", "type": "string"},
               {"name": "email", "type": "string"}
           ]
       },
       {
           "name": "tags",
           "type": "list",
           "element_type": "string"
       },
       {
           "name": "properties",
           "type": "map",
           "key_type": "string",
           "value_type": "string"
       }
   ]

   complex_schema = Schema(
       schema_id=2,
       fields=complex_schema_fields
   )

Working with Schemas
--------------------

Schema in Transactions
^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from datashard import create_table

   table = create_table("/path/to/structured_table")

   # Use schema when appending records
   sample_records = [
       {"id": 1, "name": "Alice", "age": 30, "salary": 75000.0},
       {"id": 2, "name": "Bob", "age": 25, "salary": 65000.0}
   ]

   with table.new_transaction() as tx:
       tx.append_data(
           records=sample_records,
           schema=complex_schema,  # Use the defined schema
           partition_values={"year": 2023, "month": 1}
       )
       tx.commit()

Schema Validation
^^^^^^^^^^^^^^^^^

datashard performs schema validation to ensure data consistency:

.. code-block:: python

   # The system automatically validates that records match the expected schema
   # during append operations, preventing invalid data from being added

Schema Management
^^^^^^^^^^^^^^^^^

Accessing Current Schema
""""""""""""""""""""""""

.. code-block:: python

   # Get the current schema of a table
   current_metadata = table.metadata_manager.refresh()
   if current_metadata:
       current_schema_id = current_metadata.current_schema_id
       current_schema = next(
           (s for s in current_metadata.schemas if s.schema_id == current_schema_id),
           None
       )
       if current_schema:
           print(f"Current schema has {len(current_schema.fields)} fields")

Schema History
""""""""""""""

.. code-block:: python

   # Access historical schemas
   if current_metadata:
       for schema in current_metadata.schemas:
           print(f"Schema ID {schema.schema_id}: {len(schema.fields)} fields")

Best Practices
--------------

Schema Design
^^^^^^^^^^^^^

1. **Plan Ahead**: Consider future data evolution needs when designing schemas
2. **Use Appropriate Types**: Choose the most specific type that fits your data
3. **Optional vs Required**: Carefully consider which fields should be required
4. **Partition Columns**: Include partition columns in your schema design

Performance Considerations
^^^^^^^^^^^^^^^^^^^^^^^^^^

- Complex nested types can impact read performance
- Wide schemas (many columns) may require more memory
- Schema evolution should be planned to minimize performance impacts

Versioning
^^^^^^^^^^

- Each schema change creates a new schema version
- Historical data remains accessible with its original schema
- New data uses the current schema version
- This enables time travel with schema consistency