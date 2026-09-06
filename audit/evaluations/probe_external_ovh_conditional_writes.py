"""EXTERNAL probe - talks to the S3 endpoint configured in the environment / .env.
Refuses to run unless AUDIT_ALLOW_EXTERNAL=1. Blast radius: writes ONE small object
under a unique 'audit-probe-<uuid>/' prefix in the configured bucket and deletes it.

Question it answers: does this provider honour conditional PUT (If-None-Match /
If-Match)? If yes, DATASHARD_S3_USE_CONDITIONAL_WRITES=false in production is a
misconfiguration and the CAS lock + CAS commit point can be enabled. If the
provider silently IGNORES the precondition (200 instead of 412), enabling CAS would
be unsafe - that is worse than unsupported, and the probe says so.
"""
import os
import sys
import uuid

import _harness as H

if os.environ.get("AUDIT_ALLOW_EXTERNAL") != "1":
    print("SKIP external-ovh-conditional-writes: set AUDIT_ALLOW_EXTERNAL=1 to run against the configured endpoint")
    sys.exit(0)
env = dict(os.environ)
if not env.get("DATASHARD_S3_BUCKET"):
    envfile = os.path.join(H.ROOT, ".env")
    if os.path.exists(envfile):
        for line in open(envfile):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env.setdefault(k, v)
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

bucket = env["DATASHARD_S3_BUCKET"]
s3 = boto3.client(
    "s3",
    endpoint_url=env.get("DATASHARD_S3_ENDPOINT"),
    aws_access_key_id=env.get("DATASHARD_S3_ACCESS_KEY"),
    aws_secret_access_key=env.get("DATASHARD_S3_SECRET_KEY"),
    region_name=env.get("DATASHARD_S3_REGION", "us-east-1"),
    config=Config(connect_timeout=10, read_timeout=30, retries={"max_attempts": 2}),
)
host = (env.get("DATASHARD_S3_ENDPOINT") or "").split("//")[-1]
key = f"audit-probe-{uuid.uuid4().hex}/cas.txt"
print(f"  info: endpoint={host} bucket={bucket} key={key}")


def code(e):
    return f"{e.response.get('ResponseMetadata', {}).get('HTTPStatusCode')} {e.response.get('Error', {}).get('Code')}"


def body():
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


try:
    etag = s3.put_object(Bucket=bucket, Key=key, Body=b"v1")["ETag"]
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=b"v2", IfNoneMatch="*")
        r1 = "200 (precondition IGNORED - overwrote)"
    except ClientError as e:
        r1 = code(e)
    b1 = body()
    H.report("provider-honours-if-none-match-on-put", b1 == b"v1" and "412" in r1, f"PUT If-None-Match:* on existing key -> {r1}; body now {b1!r}")
    try:
        r2 = s3.put_object(Bucket=bucket, Key=key, Body=b"v3", IfMatch=etag)
        ok2, m2 = True, "200"
    except ClientError as e:
        ok2, m2 = False, code(e)
    H.report("provider-accepts-if-match-with-current-etag", ok2 and body() == b"v3", f"PUT If-Match:{etag} -> {m2}")
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=b"v4", IfMatch='"0123456789abcdef0123456789abcdef"')
        r3 = "200 (precondition IGNORED - overwrote)"
    except ClientError as e:
        r3 = code(e)
    b3 = body()
    H.report("provider-rejects-if-match-with-stale-etag", "412" in r3 and b3 == b"v3", f"PUT If-Match:<stale> -> {r3}; body now {b3!r}")
finally:
    try:
        s3.delete_object(Bucket=bucket, Key=key)
        print("  info: probe object deleted")
    except Exception as e:  # noqa: BLE001
        print(f"  info: cleanup failed: {e}")
H.finish()
