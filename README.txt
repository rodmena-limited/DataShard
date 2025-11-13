				  //
                 //
                //
               //
              //
             //
            //
           //
          //
         //
        //
       //
      //___________________
     //                   /
    //___________________/

       D A T A S H A R D




datashard
========
Iceberg-inspired safe concurrent data operations for ML/AI workloads in Python.


Stress Test Results
===================

  FUCKING AWESOME RESULTS: 

  Normal Files: X CORRUPTED
   - Expected: 120,000 (12 processes × 10,000 operations each)
   - Actual: Only 8,566 operations and massive data loss         <----- That's normal files :(
   - Data Loss: 111,434 operations lost due to race conditions
   - Result: Massive data corruption from simultaneous file access

  Iceberg Implementation: SAƒE and SOUND!
   - Expected: 120,000 operations                                <----- That's datashard :)
   - Actual: 120,000 operations completed successfully
   - Data Integrity: 100% preserved despite concurrency
   - Result: All 120,000 operations safely processed

  Key Proof Points:
   1. 12 concurrent processes operating on shared data
   2. Heavy workload: 10,000 operations per process = 120,000 total
   3. Normal files: Lost ~93% of operations due to race conditions
   4. Iceberg: Maintained 100% of operations with no corruption
   5. Definitive proof: Same test, radically different outcomes


What is datashard?
==================

datashard is a Python package that provides safe concurrent data operations for machine learning and AI workloads. It implements the core concepts of Apache Iceberg to give you:


- ACID transactions: Operations either fully complete or fully fail
- Time travel: Ability to look at your data as it existed at any point in time  
- Safe concurrent access: Multiple processes can read and write safely without corrupting data
- Data integrity: No matter how many processes access your data simultaneously, it stays intact


Demo: 12 Processes Lost 111,434 Operations Due to Data Corruption
===================================================================

In our test, 12 processes each tried to increment a counter 10,000 times:
- Expected result: 120,000 (12 × 10,000)
- Normal files result: Only 8,566 operations completed (111,434 LOST!)
- datashard result: 120,000 (all operations completed safely)

This is the difference between using regular files and using datashard.

Why Do You Need This for ML/AI?
===============================

Machine learning and AI projects often involve multiple processes:
- Different models training on the same data
- Data pipelines running concurrently  
- Multiple experiments accessing shared datasets
- Real-time inference and batch processing happening together

Without proper data management, these operations can corrupt your data. datashard ensures that even when 10 or 100 processes access your data simultaneously, everything works safely.

Real-World Analogy
==================

Think of regular file access like a shared notebook where 10 people write simultaneously. You get smudged, overlapping text. 

datashard is like a smart filing system with multiple copies of the notebook. When someone updates it, everyone sees a consistent version at the time they looked, and no one's work gets overwritten.

How It Compares to Apache Iceberg
=================================

Apache Iceberg is a Java-based system built for big data platforms like Spark, Flink, and Hive. It's complex and requires significant infrastructure.

datashard is a pure Python implementation designed for:
- Individual data scientists and ML engineers
- Smaller datasets and personal projects
- Direct Python integration without Java dependencies
- Quick setup without complex infrastructure

Apache Iceberg offers more features and better performance for huge datasets, but datashard provides the same safety guarantees in a simple Python package.

Installation
============

pip install datashard

Avro Support
============

datashard supports Apache Avro files alongside Parquet and ORC formats. You can work with Avro files just like other formats:

from datashard import DataFile, FileFormat

# Create Avro data file reference
avro_file = DataFile(
    file_path="/data/sample.avro",
    file_format=FileFormat.AVRO,  # Avro support
    partition_values={"year": 2023},
    record_count=1000,
    file_size_in_bytes=512000
)

datashard automatically handles reading and writing Avro files with proper schema management.

Quick Start
===========

from datashard import create_table

# Create a table to store your data safely
table = create_table("/path/to/your/data")

# Multiple processes can safely add data
table.append_records([
    {"id": 1, "name": "data_point", "value": 42}
])

# Access data from any point in time
historical_snapshot = table.time_travel(snapshot_id=12345)

# Complete example:
from datashard import create_table

# Create a new table
my_table = create_table("./my_data_table")

# Add sample data
sample_data = [
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob", "age": 25},
    {"id": 3, "name": "Charlie", "age": 35}
]

# Safely append data
success = my_table.append_records(sample_data)
print(f"Data successfully added: {success}")

# Check current snapshot
current = my_table.current_snapshot()
print(f"Current snapshot: {current.snapshot_id}")

Safety Guarantee
================

No matter how many Python processes access your data simultaneously, datashard ensures:
- No data loss or corruption
- All operations complete successfully  
- Consistent views for readers
- Atomic commits (never partial updates)
- Recovery from failures without data damage


Why I Built datashard
=======================
Datashard was born as part of my effort to build a strictly Atomic workflow system. 
I needed to record huge amounts of metadata from multiple concurrent processes without risking data corruption.
In the meantime, I do not have good memories of almost all java-based solutions I tried. Iceberg conceptually fit my needs perfectly,
but I wanted a pure Python solution. There were complexities in the implementation that I couldn't purely do it myslef, by we're in 2025 now and
Gemiini has made it possible for me to build complex OCC parts in pure Python. 
This makes it safe for production ML/AI systems where data corruption could be expensive or dangerous.

Enjoy using datashard!
Farshid.


Copyright (c) RODMENA LIMITED. Licensed under Apache 2.0.
