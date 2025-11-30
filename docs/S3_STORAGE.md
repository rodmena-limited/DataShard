# S3-Compatible Storage Guide

DataShard v0.2.2 introduces S3-compatible storage support, enabling distributed workflows across multiple machines without shared filesystems.

## Table of Contents

- [Overview](#overview)
- [Supported Backends](#supported-backends)
- [Configuration](#configuration)
- [Usage Examples](#usage-examples)
- [Performance](#performance)
- [Security](#security)
- [Troubleshooting](#troubleshooting)
- [Migration Guide](#migration-guide)

---

## Overview

### What is S3-Compatible Storage?

S3-compatible storage backends implement the Amazon S3 API, allowing DataShard to store tables in cloud object storage instead of local filesystems. This enables:

- **Distributed workflows** across multiple machines/regions
- **Cloud-native architecture** without shared filesystems
- **High durability** (99.999999999% with AWS S3)
- **Auto-scaling friendly** - workers can start/stop independently
- **Cost-effective** cold storage for historical data

### Architecture

DataShard uses a dual-API approach for S3 storage:

1. **Metadata Layer** (boto3 S3 client)
   - JSON metadata files (table metadata)
   - Avro manifest files
   
   - Version hints
   - Fast small-file operations

2. **Data Layer** (PyArrow S3FileSystem)
   - Parquet data files
   - Native compression
   - Efficient columnar I/O
   - Optimized for large files

Both APIs are coordinated through the `StorageBackend` abstraction, providing a unified interface to DataShard's core.

---

## Supported Backends

### AWS S3

Amazon's Simple Storage Service - the gold standard for cloud object storage.

**Endpoint:** `https://s3.amazonaws.com` (or region-specific)
**Authentication:** Access keys or IAM roles
**Regions:** All AWS regions supported

### MinIO

Open-source, self-hosted S3-compatible storage server.

**Endpoint:** Your MinIO server URL
**Authentication:** MinIO access/secret keys
**Deployment:** On-premise, Kubernetes, Docker

### DigitalOcean Spaces

DigitalOcean's S3-compatible object storage.

**Endpoint:** `https://{region}.digitaloceanspaces.com`
**Authentication:** Spaces access keys
**Regions:** NYC, SFO, AMS, SGP, FRA

### Wasabi

S3-compatible cloud storage with predictable pricing.

**Endpoint:** `https://s3.wasabisys.com`
**Authentication:** Wasabi access keys
**Regions:** US, EU, AP

### Other S3-Compatible Services

Any service implementing the S3 API should work:
- Backblaze B2
- Cloudflare R2
- IBM Cloud Object Storage
- Oracle Cloud Infrastructure Object Storage

---

## Configuration

### Environment Variables

DataShard selects the storage backend based on environment variables:

```bash
# Storage type (local or s3)
export DATASHARD_STORAGE_TYPE=s3

# S3 endpoint URL
export DATASHARD_S3_ENDPOINT=https://s3.amazonaws.com

# Authentication
export DATASHARD_S3_ACCESS_KEY=your-access-key-id
export DATASHARD_S3_SECRET_KEY=your-secret-access-key

# Bucket and region
export DATASHARD_S3_BUCKET=my-datashard-bucket
export DATASHARD_S3_REGION=us-east-1

# Optional: Prefix within bucket
export DATASHARD_S3_PREFIX=optional/prefix/
```

### AWS S3 Configuration

```bash
export DATASHARD_STORAGE_TYPE=s3
export DATASHARD_S3_ENDPOINT=https://s3.amazonaws.com
export DATASHARD_S3_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE
export DATASHARD_S3_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export DATASHARD_S3_BUCKET=my-datashard-tables
export DATASHARD_S3_REGION=us-east-1
```

### AWS S3 with IAM Roles (Recommended)

On EC2, ECS, or Lambda, credentials are auto-discovered:

```bash
export DATASHARD_STORAGE_TYPE=s3
export DATASHARD_S3_BUCKET=my-datashard-tables
export DATASHARD_S3_REGION=us-east-1
# No credentials needed - IAM role provides them
```

### MinIO Configuration

```bash
export DATASHARD_STORAGE_TYPE=s3
export DATASHARD_S3_ENDPOINT=https://minio.mycompany.com
export DATASHARD_S3_ACCESS_KEY=minioadmin
export DATASHARD_S3_SECRET_KEY=minioadmin
export DATASHARD_S3_BUCKET=datashard
export DATASHARD_S3_REGION=us-east-1
```

### Local Development with MinIO

Start MinIO with Docker:

```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

Configure DataShard:

```bash
export DATASHARD_STORAGE_TYPE=s3
export DATASHARD_S3_ENDPOINT=http://localhost:9000
export DATASHARD_S3_ACCESS_KEY=minioadmin
export DATASHARD_S3_SECRET_KEY=minioadmin
export DATASHARD_S3_BUCKET=datashard
export DATASHARD_S3_REGION=us-east-1
```

Create bucket (one-time):

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)
s3.create_bucket(Bucket="datashard")
```

---

## Usage Examples

### Basic S3 Table Creation

```python
from datashard import create_table, Schema

# Configure S3 via environment variables (see Configuration section)
# Then use DataShard normally - storage backend is transparent

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
```

### Distributed Workflow Example

**Worker 1** (us-east-1a):
```python
from datashard import create_table, Schema

schema = Schema(schema_id=1, fields=[
    {"id": 1, "name": "task_id", "type": "string", "required": True},
    {"id": 2, "name": "status", "type": "string", "required": True},
])

table = create_table("task-results", schema)
table.append_records([
    {"task_id": "t1", "status": "success"}
], schema)
```

**Worker 2** (us-east-1b, simultaneously):
```python
from datashard import load_table

# Loads from S3, sees Worker 1's table
table = load_table("task-results")

# Concurrent append with ACID guarantees
table.append_records([
    {"task_id": "t2", "status": "success"}
], schema)
```

**Analysis Server** (us-west-2):
```python
from datashard import load_table

# Query from different region
table = load_table("task-results")
df = table.to_pandas()
print(f"Total tasks: {len(df)}")
```

### Multi-Region Deployment

```python
# All regions share the same S3 bucket
# Just configure DATASHARD_S3_REGION appropriately

# us-east-1 workers
export DATASHARD_S3_REGION=us-east-1

# eu-west-1 workers
export DATASHARD_S3_REGION=eu-west-1

# Both write to the same logical table in S3
# Cross-region latency applies, but ACID guarantees maintained
```

---

## Performance

### Latency Comparison

| Operation | Local Disk | S3 (same region) | S3 (cross-region) |
|-----------|-----------|------------------|-------------------|
| Create table | <1ms | ~50ms | ~150ms |
| Append 100 records | ~2ms | ~100ms | ~300ms |
| Append 10,000 records | ~50ms | ~500ms | ~1500ms |
| Read 50,000 records | ~20ms | ~150ms | ~500ms |
| Concurrent writes (10 workers) | Safe ✅ | Safe ✅ | Safe ✅ |

### Throughput Optimization

**Batch writes:**
```python
# ❌ Slow - 1000 individual transactions
for record in records:
    table.append_records([record], schema)

# ✅ Fast - 1 transaction with 1000 records
table.append_records(records, schema)
```

**Regional colocation:**
- Deploy workers in the same AWS region as S3 bucket
- Reduces latency from ~150ms to ~50ms
- Use VPC endpoints to avoid internet gateway

**Connection pooling:**
- DataShard automatically reuses boto3 connections
- No manual configuration needed

### Cost Analysis

**S3 Standard Pricing (us-east-1):**
- Storage: $0.023/GB/month
- PUT requests: $0.005 per 1,000 requests
- GET requests: $0.0004 per 1,000 requests

**Example: 1 million workflow task logs (~100MB):**
- Storage: $0.0023/month
- Writes: $0.005 (1,000 append operations)
- Reads: $0.0004 (1,000 queries)
- **Total: ~$0.008/month** 🎯

Compare to:
- RDS PostgreSQL (db.t3.micro): $12.41/month
- EBS volume (100GB gp3): $8/month

**S3 is 1,500x cheaper for this workload!**

---

## Security

### IAM Policy (Minimum Permissions)

```json
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
```

### Encryption at Rest

**Server-Side Encryption (SSE-S3):**
```bash
aws s3api put-bucket-encryption \
  --bucket my-datashard-bucket \
  --server-side-encryption-configuration '{
    "Rules": [{
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "AES256"
      }
    }]
  }'
```

**SSE-KMS (for compliance requirements):**
```bash
aws s3api put-bucket-encryption \
  --bucket my-datashard-bucket \
  --server-side-encryption-configuration '{
    "Rules": [{
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "aws:kms",
        "KMSMasterKeyID": "arn:aws:kms:us-east-1:123456789012:key/..."
      }
    }]
  }'
```

### Bucket Policies

**Prevent public access:**
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Deny",
    "Principal": "*",
    "Action": "s3:*",
    "Resource": [
      "arn:aws:s3:::my-datashard-bucket",
      "arn:aws:s3:::my-datashard-bucket/*"
    ],
    "Condition": {
      "StringNotEquals": {
        "aws:PrincipalAccount": "123456789012"
      }
    }
  }]
}
```

### Credential Management

**✅ Best Practices:**
- Use IAM roles on AWS compute (EC2/ECS/Lambda)
- Rotate access keys every 90 days
- Use AWS Secrets Manager for non-AWS environments
- Enable MFA for sensitive buckets

**❌ Never:**
- Hardcode credentials in code
- Commit credentials to git
- Share credentials across teams
- Use root account credentials

---

## Troubleshooting

### "NoSuchBucket" Error

**Problem:** Bucket doesn't exist.

**Solution:**
```python
import boto3

s3 = boto3.client("s3", endpoint_url=os.getenv("DATASHARD_S3_ENDPOINT"))
s3.create_bucket(Bucket="datashard")
```

### "Access Denied" Error

**Problem:** Insufficient IAM permissions.

**Solution:** Check IAM policy includes `s3:PutObject`, `s3:GetObject`, `s3:ListBucket`.

### Slow Performance

**Problem:** High latency to S3.

**Solutions:**
1. Use same AWS region as workers
2. Enable VPC endpoints
3. Batch writes (100+ records per transaction)
4. Consider S3 Transfer Acceleration

### "ImportError: boto3 is required"

**Problem:** boto3 not installed.

**Solution:**
```bash
pip install datashard[s3]
# or
pip install boto3
```

### Connection Timeouts

**Problem:** Network connectivity issues.

**Solutions:**
1. Check security groups allow HTTPS (port 443)
2. Verify VPC route tables
3. Test connectivity: `aws s3 ls s3://bucket-name`

---

## Migration Guide

### Local to S3 Migration

**Step 1: Export local data**
```python
from datashard import load_table

# Load local table
local_table = load_table("/local/path/my-table")

# Export to pandas
df = local_table.to_pandas()
```

**Step 2: Configure S3**
```bash
export DATASHARD_STORAGE_TYPE=s3
export DATASHARD_S3_BUCKET=my-bucket
# ... other S3 config
```

**Step 3: Create S3 table and import**
```python
from datashard import create_table, Schema

# Create table in S3
schema = Schema(...)  # Same schema as local table
s3_table = create_table("my-table", schema)

# Import data
records = df.to_dict('records')
s3_table.append_records(records, schema)
```

### S3 to Local Migration

Reverse the process:

```python
# Load from S3
export DATASHARD_STORAGE_TYPE=s3
s3_table = load_table("my-table")
df = s3_table.to_pandas()

# Create local table
unset DATASHARD_STORAGE_TYPE
local_table = create_table("/local/path/my-table", schema)
local_table.append_records(df.to_dict('records'), schema)
```

---

## Advanced Topics

### Custom S3 Endpoint (MinIO, etc.)

```python
# Environment variables handle this automatically
export DATASHARD_S3_ENDPOINT=https://custom-endpoint.com
```

### Multi-Bucket Strategy

Use different buckets for different purposes:

```bash
# Production data
export DATASHARD_S3_BUCKET=prod-datashard

# Staging data
export DATASHARD_S3_BUCKET=staging-datashard

# Development
export DATASHARD_S3_BUCKET=dev-datashard
```

### S3 Lifecycle Policies

Archive old snapshots to S3 Glacier:

```json
{
  "Rules": [{
    "Id": "archive-old-snapshots",
    "Status": "Enabled",
    "Prefix": "metadata/",
    "Transitions": [{
      "Days": 90,
      "StorageClass": "GLACIER"
    }]
  }]
}
```

---

## Summary

DataShard's S3 support enables:
- ✅ Distributed workflows without shared filesystems
- ✅ Cloud-native architecture
- ✅ Same ACID guarantees as local storage
- ✅ Transparent API - same code for local or S3
- ✅ Cost-effective storage ($0.023/GB/month)
- ✅ High durability (11 9's with AWS S3)

**Start using S3 storage today** - just configure environment variables and DataShard handles the rest!
