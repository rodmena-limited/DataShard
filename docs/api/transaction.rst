transaction Module API
======================

This section documents the transaction and table management functionality.

Classes
-------

Transaction
^^^^^^^^^^^

.. autoclass:: datashard.Transaction
   :members:
   :undoc-members:
   :show-inheritance:

TransactionManager
^^^^^^^^^^^^^^^^^^

.. autoclass:: datashard.TransactionManager
   :members:
   :undoc-members:
   :show-inheritance:

Table
^^^^^

.. autoclass:: datashard.Table
   :members:
   :undoc-members:
   :show-inheritance:

Table.scan() Method
"""""""""""""""""""

Read data from the table with optional filtering and parallel processing.

**Parameters:**

- ``columns`` (List[str], optional): Column names to read. If None, reads all columns.
- ``filter`` (Dict[str, Any], optional): Filter dict for predicate pushdown.
- ``parallel`` (bool or int, optional): Enable parallel reading. True uses all cores, int specifies thread count.

**Filter Syntax:**

.. code-block:: python

   # Equality
   {"column": value}
   {"column": ("==", value)}

   # Comparison
   {"column": (">", value)}
   {"column": (">=", value)}
   {"column": ("<", value)}
   {"column": ("<=", value)}

   # Membership
   {"column": ("in", [v1, v2, v3])}
   {"column": ("not_in", [v1, v2])}

   # Range
   {"column": ("between", (low, high))}

**Example:**

.. code-block:: python

   # Basic scan
   records = table.scan()

   # With filter
   records = table.scan(filter={"status": "active"})

   # With columns and parallel
   records = table.scan(columns=["id", "name"], parallel=True)

Table.to_pandas() Method
""""""""""""""""""""""""

Read data from the table as a pandas DataFrame.

**Parameters:**

Same as ``scan()`` plus returns a pandas DataFrame instead of list of dicts.

**Example:**

.. code-block:: python

   df = table.to_pandas(filter={"age": (">", 30)}, parallel=True)

Table.scan_batches() Method
"""""""""""""""""""""""""""

Iterate over data in batches for memory-efficient processing.

**Parameters:**

- ``batch_size`` (int): Approximate number of records per batch. Default: 10000.
- ``columns`` (List[str], optional): Column names to read.
- ``filter`` (Dict[str, Any], optional): Filter dict for predicate pushdown.

**Example:**

.. code-block:: python

   for batch in table.scan_batches(batch_size=5000):
       process(batch)  # batch is List[Dict]

Table.iter_records() Method
"""""""""""""""""""""""""""

Iterate over records one at a time.

**Parameters:**

- ``columns`` (List[str], optional): Column names to read.
- ``filter`` (Dict[str, Any], optional): Filter dict for predicate pushdown.

**Example:**

.. code-block:: python

   for record in table.iter_records(filter={"status": "failed"}):
       handle_failure(record)

Table.iter_pandas() Method
""""""""""""""""""""""""""

Iterate over data as pandas DataFrame chunks.

**Parameters:**

- ``chunksize`` (int): Approximate rows per chunk. Default: 50000.
- ``columns`` (List[str], optional): Column names to read.
- ``filter`` (Dict[str, Any], optional): Filter dict for predicate pushdown.

**Example:**

.. code-block:: python

   for chunk_df in table.iter_pandas(chunksize=10000):
       results.append(chunk_df.groupby('x').sum())