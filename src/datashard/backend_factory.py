"""
Environment-driven storage backend selection (split out of storage_backend.py for
the 500-line file cap, #71).
"""

import os
from typing import Optional

from .logging_config import get_logger
from .storage_backend import LocalStorageBackend, StorageBackend

logger = get_logger(__name__)


def create_storage_backend(table_path: str) -> StorageBackend:
    """
    Create storage backend based on environment configuration.

    Environment variables:
        DATASHARD_STORAGE_TYPE: "local" (default) or "s3"

        For S3:
            DATASHARD_S3_ENDPOINT: S3 endpoint URL (optional, for MinIO/custom endpoints)
            DATASHARD_S3_ACCESS_KEY: AWS access key
            DATASHARD_S3_SECRET_KEY: AWS secret key
            DATASHARD_S3_BUCKET: S3 bucket name
            DATASHARD_S3_REGION: AWS region (default: us-east-1)
            DATASHARD_S3_PREFIX: Optional prefix for all objects (default: "")
            DATASHARD_S3_USE_CONDITIONAL_WRITES: unset (default) = probe the
                provider once and use CAS locking when it honours conditional PUTs
                (AWS S3, OVH Object Storage, MinIO and most others do); "true" forces
                CAS; "false" selects the best-effort polling lock, which REQUIRES
                DATASHARD_S3_ALLOW_UNSAFE_LOCK=1 because it can lose commits (#59).

    Args:
        table_path: Table location (local path or S3-style identifier)

    Returns:
        StorageBackend instance
    """
    storage_type = os.getenv("DATASHARD_STORAGE_TYPE", "local").lower()

    if storage_type == "s3":
        # S3 configuration
        bucket = os.getenv("DATASHARD_S3_BUCKET")
        if not bucket:
            raise ValueError("DATASHARD_S3_BUCKET environment variable is required for S3 storage")

        endpoint_url = os.getenv("DATASHARD_S3_ENDPOINT")
        access_key = os.getenv("DATASHARD_S3_ACCESS_KEY")
        secret_key = os.getenv("DATASHARD_S3_SECRET_KEY")
        region = os.getenv("DATASHARD_S3_REGION", "us-east-1")
        env_prefix = os.getenv("DATASHARD_S3_PREFIX", "")
        raw_cas = os.getenv("DATASHARD_S3_USE_CONDITIONAL_WRITES", "").strip().lower()
        use_conditional_writes: Optional[bool] = (
            None if raw_cas == "" else raw_cas in ("true", "1", "yes")
        )
        allow_unsafe_lock = os.getenv("DATASHARD_S3_ALLOW_UNSAFE_LOCK", "").strip().lower() in (
            "true", "1", "yes",
        )

        # Combine environment prefix with table path for full S3 prefix
        # table_path is the logical location of the table (e.g., "logs/workflow_logs")
        table_prefix = table_path.strip("/")
        if env_prefix and table_prefix:
            full_prefix = f"{env_prefix.rstrip('/')}/{table_prefix}"
        elif env_prefix:
            full_prefix = env_prefix.rstrip("/")
        elif table_prefix:
            full_prefix = table_prefix
        else:
            full_prefix = ""

        # Validate credentials - Optional, falls back to IAM/Env if missing
        if not (access_key and secret_key):
            logger.info("No explicit S3 credentials provided. Using default AWS credential chain (IAM Role, Env Vars, etc.)")

        from .s3_backend import S3StorageBackend

        return S3StorageBackend(
            bucket=bucket,
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            region=region,
            prefix=full_prefix,
            use_conditional_writes=use_conditional_writes,
            allow_unsafe_lock=allow_unsafe_lock,
        )
    else:
        # Local filesystem (default)
        return LocalStorageBackend(table_path)

