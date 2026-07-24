"""
Create S3 bucket on MinIO for testing.

Credentials are read from the environment (DATASHARD_S3_*) - never hardcode
credentials in this file; this repository is public.
"""

import os
import sys

import boto3

# S3 configuration from environment
endpoint_url = os.environ.get("DATASHARD_S3_ENDPOINT")
access_key = os.environ.get("DATASHARD_S3_ACCESS_KEY")
secret_key = os.environ.get("DATASHARD_S3_SECRET_KEY")
bucket_name = os.environ.get("DATASHARD_S3_BUCKET", "datashard")
region = os.environ.get("DATASHARD_S3_REGION", "us-east-1")

if not (endpoint_url and access_key and secret_key):
    print(
        "Missing S3 configuration. Set DATASHARD_S3_ENDPOINT, "
        "DATASHARD_S3_ACCESS_KEY and DATASHARD_S3_SECRET_KEY."
    )
    sys.exit(1)

# Create S3 client
s3 = boto3.client(
    "s3",
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region,
)

try:
    # Check if bucket exists
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' already exists")
    except Exception:
        # Create bucket
        s3.create_bucket(Bucket=bucket_name)
        print(f"✅ Created bucket: {bucket_name}")

except Exception as e:
    print(f"❌ Error: {e}")
