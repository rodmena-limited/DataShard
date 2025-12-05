"""
Create S3 bucket on MinIO for testing
"""


import boto3

# S3 configuration
endpoint_url = "https://s3.london.cloudsci.uk"
access_key = "rodmena-limited"
secret_key = "pX1r4t3dS3cr3tK3y!"
bucket_name = "datashard"
region = "us-east-1"

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
