Core Concepts
=============

This section explains the fundamental concepts behind datashard and how they relate to Apache Iceberg.

What is datashard?
------------------

datashard is a Python implementation of Apache Iceberg concepts that provides ACID transactions, time travel, and safe concurrent access to data. It allows multiple processes to read and write data safely without the risk of corruption or inconsistency.

Key Concepts
------------

Table
^^^^^
A table in datashard represents a collection of data files with associated metadata. Each table has:

- A location on the filesystem
- Metadata describing schema, partitioning, and other properties
- Snapshots that capture the state of the table at different points in time
- Manifest files that catalog the data files in the table

Snapshot
^^^^^^^^
A snapshot is a point-in-time view of the table. Each snapshot includes:

- A unique ID and timestamp
- A manifest list pointing to the data files at that point in time
- Information about the operation that created the snapshot
- References to parent snapshots for lineage tracking

Metadata
^^^^^^^^
Metadata contains all the information about the table structure, including:

- Schema definitions
- Partition specifications
- Current and historical snapshots
- Table properties and configuration

Manifest
^^^^^^^^
A manifest is a file that contains metadata about data files. It includes:

- File paths and sizes
- Partition values for each file
- Record counts
- Column statistics

Transactions
^^^^^^^^^^^^

datashard uses ACID transactions to ensure data consistency:

- **Atomic**: Operations either complete entirely or not at all
- **Consistent**: Transactions maintain data integrity constraints
- **Isolated**: Concurrent transactions don't interfere with each other
- **Durable**: Once committed, changes are permanent

How it Relates to Apache Iceberg
--------------------------------

While Apache Iceberg is a specification implemented in Java for use with Spark, Hive, and other big data tools, datashard implements the same concepts in pure Python:

1. **Same Metadata Format**: Uses the same metadata structure as Apache Iceberg
2. **Same Concurrency Model**: Implements Optimistic Concurrency Control (OCC)
3. **Same Time Travel**: Provides the ability to examine historical data states
4. **Same Schema Evolution**: Supports adding, removing, and changing columns
5. **Same File Organization**: Uses similar manifest and metadata file structures

Benefits of datashard
---------------------

- **Pure Python**: No Java dependencies or JVM required
- **ML/AI Focus**: Designed for machine learning and data science workflows
- **Easy Integration**: Can be used in any Python environment
- **Scalable**: Can handle large datasets with appropriate file formats
- **Safe Concurrency**: Multiple processes can read/write safely
- **Historical Access**: Time travel capabilities for reproducible analysis