====================
S3-Compatible Storage
====================

.. note::
   S3 storage support is available since DataShard v0.2.2

Overview
========

DataShard supports S3-compatible storage backends, enabling distributed workflows across multiple machines without shared filesystems. This feature allows you to store your DataShard tables in:

* AWS S3
* MinIO
* DigitalOcean Spaces
* Wasabi
* Any S3-compatible object storage

Benefits
========

✅ **Distributed Workflows**: Workers on different machines can write concurrently

✅ **Cloud-Native**: No shared filesystem required

✅ **ACID Guarantees**: Same transactional safety as local storage

✅ **High Durability**: 99.999999999% with AWS S3

✅ **Cost-Effective**: ~1,500x cheaper than RDS for workflow logs

✅ **Scalable**: Auto-scaling workers can write without coordination

Configuration
=============

Environment Variables
---------------------

Configure S3 storage using environment variables:

.. code-block:: bash

   # Enable S3 storage
   export DATASHARD_STORAGE_TYPE=s3

   # S3 endpoint (use AWS S3 or custom endpoint for MinIO)
   export DATASHARD_S3_ENDPOINT=https://s3.amazonaws.com

   # S3 credentials
   export DATASHARD_S3_ACCESS_KEY=your-access-key-id
   export DATASHARD_S3_SECRET_KEY=your-secret-access-key

   # S3 bucket and region
   export DATASHARD_S3_BUCKET=my-datashard-bucket
   export DATASHARD_S3_REGION=us-east-1

   # Optional: Prefix within bucket
   export DATASHARD_S3_PREFIX=optional/prefix/

AWS S3 Setup
------------

Create an S3 bucket:

.. code-block:: bash

   aws s3 mb s3://my-datashard-bucket --region us-east-1

IAM Policy (Minimum Permissions):

.. code-block:: json

   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "s3:GetObject",
           "s3:PutObject",
           "s3:DeleteObject",
           "s3:ListBucket"
         ],
         "Resource": [
           "arn:aws:s3:::my-datashard-bucket",
           "arn:aws:s3:::my-datashard-bucket/*"
         ]
       }
     ]
   }

MinIO Setup
-----------

Start MinIO with Docker:

.. code-block:: bash

   docker run -d \
     -p 9000:9000 \
     -p 9001:9001 \
     -e MINIO_ROOT_USER=minioadmin \
     -e MINIO_ROOT_PASSWORD=minioadmin \
     --name minio \
     minio/minio server /data --console-address ":9001"

Configure DataShard for MinIO:

.. code-block:: bash

   export DATASHARD_STORAGE_TYPE=s3
   export DATASHARD_S3_ENDPOINT=http://localhost:9000
   export DATASHARD_S3_ACCESS_KEY=minioadmin
   export DATASHARD_S3_SECRET_KEY=minioadmin
   export DATASHARD_S3_BUCKET=datashard
   export DATASHARD_S3_REGION=us-east-1

Usage Examples
==============

Basic Table Creation
--------------------

.. code-block:: python

   from datashard import create_table, Schema

   # Configure S3 via environment variables
   # Then use DataShard normally - API is identical!

   schema = Schema(
       schema_id=1,
       fields=[
           {"id": 1, "name": "user_id", "type": "long", "required": True},
           {"id": 2, "name": "event", "type": "string", "required": True},
           {"id": 3, "name": "timestamp", "type": "long", "required": True}
       ]
   )

   # Creates table in S3 bucket
   table = create_table("user-events", schema)

   # Append records (stored in S3)
   table.append_records([
       {"user_id": 1, "event": "login", "timestamp": 1700000000},
       {"user_id": 2, "event": "purchase", "timestamp": 1700000100}
   ], schema)

Distributed Workflow
--------------------

**Worker 1** (us-east-1a):

.. code-block:: python

   from datashard import create_table, Schema

   schema = Schema(schema_id=1, fields=[
       {"id": 1, "name": "task_id", "type": "string", "required": True},
       {"id": 2, "name": "status", "type": "string", "required": True},
   ])

   table = create_table("task-results", schema)
   table.append_records([
       {"task_id": "t1", "status": "success"}
   ], schema)

**Worker 2** (us-east-1b, simultaneously):

.. code-block:: python

   from datashard import load_table

   # Loads from S3, sees Worker 1's table
   table = load_table("task-results")

   # Concurrent append with ACID guarantees
   table.append_records([
       {"task_id": "t2", "status": "success"}
   ], schema)

**Analysis Server** (us-west-2):

.. code-block:: python

   from datashard import load_table

   # Query from different region
   table = load_table("task-results")

   # Get current snapshot
   snapshot = table.current_snapshot()
   print(f"Snapshot ID: {snapshot.snapshot_id}")

Storage Structure
=================

DataShard creates the following structure in S3:

.. code-block:: text

   s3://my-bucket/optional-prefix/
   └── table-name/
       ├── data/
       │   ├── auto_1763162900677093.parquet
       │   └── auto_1763162915234567.parquet
       ├── metadata/
       │   ├── v0.metadata.json
       │   ├── v1.metadata.json
       │   └── manifests/
       │       ├── manifest_1763162900796257.avro
       │       └── manifest_list_*.avro
       └── metadata.version-hint.text

Architecture
============

DataShard uses a dual-API approach for S3:

**Metadata Layer** (boto3):
  - JSON metadata files
  - Manifest files
  - Version hints
  - Fast small-file operations

**Data Layer** (PyArrow S3FileSystem):
  - Parquet data files
  - Native compression
  - Efficient columnar I/O
  - Optimized for large files

Both APIs are coordinated through the ``StorageBackend`` abstraction.

Performance
===========

Latency Comparison
------------------

.. list-table::
   :header-rows: 1
   :widths: 40 20 20 20

   * - Operation
     - Local Disk
     - S3 (same region)
     - S3 (cross-region)
   * - Create table
     - <1ms
     - ~50ms
     - ~150ms
   * - Append 100 records
     - ~2ms
     - ~100ms
     - ~300ms
   * - Append 10,000 records
     - ~50ms
     - ~500ms
     - ~1500ms
   * - Read 50,000 records
     - ~20ms
     - ~150ms
     - ~500ms

Cost Analysis
-------------

**Example**: 100,000 workflow executions/month (~1GB logs)

**S3 Standard (us-east-1)**:

* Storage: $0.023/GB/month = $0.023
* Writes: 100,000 × $0.005/1000 = $0.50
* Reads: 10,000 × $0.0004/1000 = $0.004
* **Total: ~$0.53/month**

**Compare to**:

* RDS PostgreSQL (db.t3.micro): $12.41/month
* EBS volume (100GB gp3): $8/month

**S3 is 23x cheaper than RDS!**

Optimization Tips
-----------------

1. **Batch writes** - Group multiple records into single transactions
2. **Regional colocation** - Deploy workers in same region as S3 bucket
3. **VPC endpoints** - Use VPC endpoints to avoid internet gateway charges
4. **Connection pooling** - DataShard automatically reuses connections

Troubleshooting
===============

NoSuchBucket Error
------------------

**Problem**: Bucket doesn't exist.

**Solution**:

.. code-block:: bash

   aws s3 mb s3://my-bucket

Access Denied
-------------

**Problem**: Insufficient IAM permissions.

**Solution**: Verify IAM policy includes ``s3:PutObject``, ``s3:GetObject``, ``s3:ListBucket``.

Slow Performance
----------------

**Problem**: High latency to S3.

**Solutions**:

1. Use same AWS region as workers
2. Enable VPC endpoints
3. Batch writes (100+ records per transaction)
4. Consider S3 Transfer Acceleration

ImportError: boto3
------------------

**Problem**: boto3 not installed.

**Solution**:

.. code-block:: bash

   pip install datashard[s3]
   # or
   pip install boto3

Best Practices
==============

1. **Use IAM Roles** on AWS compute instead of hardcoded credentials
2. **Enable S3 Encryption** at rest (SSE-S3 or SSE-KMS)
3. **Set up Lifecycle Policies** to archive old snapshots to Glacier
4. **Monitor S3 Costs** with AWS Cost Explorer
5. **Use VPC Endpoints** for S3 to reduce costs
6. **Enable S3 Versioning** for critical tables
7. **Set up CloudWatch Alarms** for S3 access errors

Security
========

Encryption at Rest
------------------

Enable server-side encryption:

.. code-block:: bash

   aws s3api put-bucket-encryption \
     --bucket my-bucket \
     --server-side-encryption-configuration '{
       "Rules": [{
         "ApplyServerSideEncryptionByDefault": {
           "SSEAlgorithm": "AES256"
         }
       }]
     }'

IAM Roles (Recommended)
-----------------------

For EC2/ECS/Lambda, use IAM roles instead of access keys:

.. code-block:: bash

   # No credentials needed - IAM role provides them
   export DATASHARD_STORAGE_TYPE=s3
   export DATASHARD_S3_BUCKET=my-bucket
   export DATASHARD_S3_REGION=us-east-1

See Also
========

* :doc:`installation` - Installing DataShard with S3 support
* :doc:`quickstart` - Getting started with DataShard
* :doc:`transactions` - Understanding ACID transactions
* :doc:`troubleshooting` - Common issues and solutions
* `Full S3 Storage Guide <https://github.com/rodmena-limited/datashard/blob/main/docs/S3_STORAGE.md>`_
