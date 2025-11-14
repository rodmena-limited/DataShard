datashard Documentation
=======================

Welcome to the official documentation for **datashard**, a Python implementation of Apache Iceberg concepts for safe concurrent data operations in ML/AI workloads.

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   installation
   quickstart

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   concepts
   transactions
   schemas
   time_travel
   concurrency
   s3_storage

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/iceberg
   api/transaction
   api/data_structures
   api/metadata_manager
   api/snapshot_manager

.. toctree::
   :maxdepth: 2
   :caption: Advanced Topics

   performance
   troubleshooting

.. toctree::
   :maxdepth: 1
   :caption: Community

   contributing
   changelog


About datashard
---------------

datashard provides ACID transactions, time travel, and safe concurrent access to data in a pure Python implementation of Apache Iceberg concepts. It's designed specifically for ML/AI workloads where data integrity and concurrent access are critical.

Key Features:

- **ACID Transactions**: Atomic, Consistent, Isolated, Durable operations
- **Time Travel**: Query data as it existed at any point in time
- **Safe Concurrency**: Multiple processes can read/write without corruption
- **S3-Compatible Storage**: AWS S3, MinIO, DigitalOcean Spaces support (v0.2.2+)
- **Distributed Workflows**: Run workers across different machines with shared S3 storage
- **Schema Evolution**: Add/remove columns while maintaining history
- **Multiple File Formats**: Parquet, Avro, ORC support
- **Data Integrity**: Built-in validation and error handling


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`