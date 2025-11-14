Troubleshooting
===============

This section helps you diagnose and resolve common issues with datashard.

Common Issues and Solutions
---------------------------

Installation Issues
^^^^^^^^^^^^^^^^^^^

**Problem**: Module not found after installation
**Solution**: Ensure that the installation completed successfully and that you're using the correct Python environment:

.. code-block:: bash

   pip install datashard
   python -c "import datashard; print(datashard.__version__)"

**Problem**: Permission errors during installation
**Solution**: Install in user mode or use a virtual environment:

.. code-block:: bash

   pip install --user datashard
   # OR
   python -m venv venv && source venv/bin/activate && pip install datashard

Transaction Issues
^^^^^^^^^^^^^^^^^^

**Problem**: ConcurrentModificationException occurring frequently
**Solution**: Implement proper retry logic with exponential backoff:

.. code-block:: python

   from datashard import ConcurrentModificationException
   import time
   
   def safe_transaction_with_retry(table, operations, max_retries=5):
       for attempt in range(max_retries):
           try:
               with table.new_transaction() as tx:
                   operations(tx)  # Perform your operations
                   return tx.commit()
           except ConcurrentModificationException:
               if attempt == max_retries - 1:
                   raise
               # Exponential backoff with jitter
               sleep_time = 0.01 * (2 ** attempt) + random.uniform(0, 0.01)
               time.sleep(sleep_time)

**Problem**: Transactions taking too long to complete
**Solution**: Keep transactions small and fast; consider batching operations differently

.. code-block:: python

   # Instead of large transactions
   # with table.new_transaction() as tx:
   #     for record in huge_dataset:  # Millions of records
   #         tx.append_data(records=[record], schema=None)
   #     tx.commit()

   # Use smaller transactions
   batch_size = 1000
   for i in range(0, len(huge_dataset), batch_size):
       batch = huge_dataset[i:i + batch_size]
       with table.new_transaction() as tx:
           tx.append_data(records=batch, schema=None)
           success = tx.commit()

File and Path Issues
^^^^^^^^^^^^^^^^^^^^

**Problem**: FileNotFoundError when accessing data files
**Solution**: Ensure file paths are correct and accessible:

.. code-block:: python

   import os
   from datashard import DataFile, FileFormat
   
   file_path = "/absolute/path/to/data.parquet"
   if os.path.exists(file_path):
       data_file = DataFile(
           file_path=file_path,
           file_format=FileFormat.PARQUET,
           # ... other parameters
       )
   else:
       raise FileNotFoundError(f"Data file does not exist: {file_path}")

**Problem**: Permission errors accessing table directories
**Solution**: Ensure the process has read/write permissions to the table location:

.. code-block:: bash

   # Check permissions
   ls -la /path/to/table
   
   # Set appropriate permissions (example)
   chmod -R 755 /path/to/table

Memory Issues
^^^^^^^^^^^^^

**Problem**: OutOfMemoryError with large datasets
**Solution**: Process data in smaller chunks:

.. code-block:: python

   def process_large_dataset_in_chunks(table, large_dataset, chunk_size=1000):
       results = []
       for i in range(0, len(large_dataset), chunk_size):
           chunk = large_dataset[i:i + chunk_size]
           with table.new_transaction() as tx:
               tx.append_data(records=chunk, schema=None)
               results.append(tx.commit())
           # Allow garbage collection between chunks
           import gc
           gc.collect()
       return results

Debugging Techniques
--------------------

Enable Logging
^^^^^^^^^^^^^^

Add logging to understand what's happening:

.. code-block:: python

   import logging
   
   # Enable debug logging
   logging.basicConfig(level=logging.DEBUG)
   
   # Or set specific loggers
   metadata_logger = logging.getLogger('datashard.metadata_manager')
   metadata_logger.setLevel(logging.DEBUG)

Diagnostic Information
^^^^^^^^^^^^^^^^^^^^^^

Collect diagnostic information for troubleshooting:

.. code-block:: python

   def diagnose_table(table):
       print(f"Table location: {table.table_path}")
       
       # Check metadata
       metadata = table.metadata_manager.refresh()
       if metadata:
           print(f"Metadata format version: {metadata.format_version}")
           print(f"Current snapshot ID: {metadata.current_snapshot_id}")
           print(f"Number of snapshots: {len(metadata.snapshots)}")
           print(f"Number of schemas: {len(metadata.schemas)}")
       else:
           print("No metadata found")
       
       # Check snapshots
       snapshots = table.snapshots()
       print(f"Available snapshots: {len(snapshots)}")
       
       # Check active transactions
       active_txs = table.transaction_manager.get_active_transactions()
       print(f"Active transactions: {len(active_txs)}")

Common Configuration Issues
---------------------------

Wrong Table Path
^^^^^^^^^^^^^^^^

**Problem**: Unable to create or access table at specified path
**Solution**: Verify path exists and is accessible:

.. code-block:: python

   import os
   from datashard import create_table
   
   table_path = "/valid/path/for/table"
   
   # Ensure parent directory exists
   parent_dir = os.path.dirname(table_path)
   os.makedirs(parent_dir, exist_ok=True)
   
   # Create table
   table = create_table(table_path)

Incompatible File Formats
^^^^^^^^^^^^^^^^^^^^^^^^^

**Problem**: Issues reading/writing files in certain formats
**Solution**: Ensure dependencies are properly installed:

.. code-block:: python

   # datashard relies on pyarrow for file format support
   # Make sure it's properly installed
   try:
       import pyarrow as pa
       print(f"PyArrow version: {pa.__version__}")
   except ImportError:
       print("PyArrow not installed - install with: pip install pyarrow")

Environment-Specific Issues
---------------------------

Virtual Environment Issues
^^^^^^^^^^^^^^^^^^^^^^^^^^

If using virtual environments, ensure datashard is installed in the active environment:

.. code-block:: bash

   # Activate your virtual environment first
   source /path/to/venv/bin/activate
   
   # Verify which Python you're using
   which python
   
   # Install datashard in the virtual environment
   pip install datashard

Path Issues
^^^^^^^^^^^

Make sure the table path is accessible and has appropriate permissions:

.. code-block:: python

   import os
   
   def validate_table_path(path):
       # Check if path exists, if not create it
       if not os.path.exists(path):
           os.makedirs(path, exist_ok=True)
       
       # Check if path is writable
       if not os.access(path, os.W_OK):
           raise PermissionError(f"Path not writable: {path}")
       
       # Check if it's actually a directory
       if not os.path.isdir(path):
           raise NotADirectoryError(f"Path is not a directory: {path}")

Support and Community
---------------------

When to Seek Help
^^^^^^^^^^^^^^^^^

- If you've reviewed this troubleshooting guide and still have issues
- For feature requests or enhancement suggestions
- To report bugs or unexpected behavior

Where to Get Help
^^^^^^^^^^^^^^^^^

1. Check the GitHub repository for similar issues
2. File an issue with detailed information about your problem
3. Include Python version, datashard version, and a reproducible example

.. code-block:: python

   import sys
   import datashard
   
   print(f"Python version: {sys.version}")
   print(f"datashard version: {datashard.__version__}")
   print(f"System platform: {sys.platform}")

Common Error Messages
^^^^^^^^^^^^^^^^^^^^^

- **ConcurrentModificationException**: Indicates another process modified data during your transaction
- **FileNotFoundError**: The specified file path doesn't exist
- **PermissionError**: Insufficient permissions to access files/directories
- **ValueError**: Invalid parameter values provided to functions
- **RuntimeError**: General runtime issues with transaction state