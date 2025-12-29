"""
Storage backend abstraction for DataShard.

Supports both local filesystem and S3-compatible storage (AWS S3, MinIO, etc.)
Configuration via environment variables:

Local filesystem (default):
    No configuration needed

S3-compatible storage:
    DATASHARD_STORAGE_TYPE=s3
    DATASHARD_S3_ENDPOINT=https://s3.amazonaws.com (or MinIO endpoint)
    DATASHARD_S3_ACCESS_KEY=your-access-key
    DATASHARD_S3_SECRET_KEY=your-secret-key
    DATASHARD_S3_BUCKET=your-bucket-name
    DATASHARD_S3_REGION=us-east-1
    DATASHARD_S3_PREFIX=optional/prefix/ (optional, default: "")
"""

import json
import os
import tempfile
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from .disk_utils import check_disk_space, estimate_write_size
from .integrity import IntegrityChecker
from .logging_config import get_logger

if TYPE_CHECKING:
    from .lock_provider import LockProvider

logger = get_logger(__name__)

try:
    import boto3
    from botocore.exceptions import ClientError

    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False


class StorageBackend(ABC):
    """Abstract base class for storage backends"""

    @abstractmethod
    def read_file(self, path: str) -> bytes:
        """Read file contents as bytes"""
        pass

    @abstractmethod
    def open_file(self, path: str) -> Any:
        """Open file as a binary stream context manager"""
        pass

    @abstractmethod
    def write_file(self, path: str, content: bytes) -> None:
        """Write bytes to file"""
        pass

    @abstractmethod
    def read_json(self, path: str) -> Dict[str, Any]:
        """Read JSON file"""
        pass

    @abstractmethod
    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        """Write JSON to file"""
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if file exists"""
        pass

    @abstractmethod
    def list_files(self, prefix: str) -> List[str]:
        """List files with given prefix"""
        pass

    @abstractmethod
    def delete_file(self, path: str) -> None:
        """Delete file"""
        pass

    @abstractmethod
    def makedirs(self, path: str, exist_ok: bool = True) -> None:
        """Create directory (no-op for S3)"""
        pass

    @abstractmethod
    def get_size(self, path: str) -> int:
        """Get file size in bytes"""
        pass

    @abstractmethod
    def get_modified_time(self, path: str) -> float:
        """Get file modification time as unix timestamp"""
        pass

    @abstractmethod
    def create_lock(self, path: str, timeout: float = 30.0) -> "LockProvider":
        """Create a distributed lock for the given path"""
        pass


class LocalStorageBackend(StorageBackend):
    """Local filesystem storage backend"""

    def __init__(self, base_path: str):
        self.base_path = base_path

    def _resolve_path(self, path: str) -> str:
        """Resolve path relative to base_path"""
        if path.startswith("/"):
            # Iceberg-style absolute path relative to table
            joined_path = os.path.join(self.base_path, path.lstrip("/"))
        elif os.path.isabs(path):
            # True system absolute path - treat as relative to base for security
            # This handles cases where a user might provide "/etc/passwd"
            joined_path = os.path.join(self.base_path, path.lstrip("/"))
        else:
            # Relative path
            joined_path = os.path.join(self.base_path, path)

        # Canonicalize paths to resolve '..'
        full_path = os.path.abspath(joined_path)
        base_path = os.path.abspath(self.base_path)

        # Ensure the resolved path is within the base directory
        if not full_path.startswith(base_path):
            raise ValueError(f"Security Error: Path traversal attempt detected. Resolved path '{full_path}' is outside base directory '{base_path}'")

        return full_path

    def read_file(self, path: str) -> bytes:
        full_path = self._resolve_path(path)
        with open(full_path, "rb") as f:
            return f.read()

    def open_file(self, path: str) -> Any:
        """Open local file for reading as a stream."""
        full_path = self._resolve_path(path)
        return open(full_path, "rb")

    def write_file(self, path: str, content: bytes) -> None:
        """Atomically write file with fsync for durability.

        Uses temp file + fsync + atomic rename pattern to ensure:
        1. No partial writes visible to readers
        2. Crash during write doesn't corrupt existing file
        3. Data is persisted to disk before success is reported

        PHASE 2 improvements:
        - Disk space checking before write
        - Comprehensive logging
        - Checksum computation for integrity
        """
        logger.info(f"Writing file: {path} ({len(content)} bytes)")

        full_path = self._resolve_path(path)
        dir_path = os.path.dirname(full_path)
        os.makedirs(dir_path, exist_ok=True)

        # PHASE 2: Check disk space before writing
        required_space = estimate_write_size(content)
        try:
            check_disk_space(dir_path, required_space)
        except IOError as e:
            logger.error(f"Disk space check failed for {path}: {e}")
            raise

        # PHASE 2: Compute checksum for integrity verification
        checksum = IntegrityChecker.compute_checksum(content)
        logger.debug(f"Computed checksum for {path}: {checksum[:16]}...")

        # Create temp file in same directory for atomic rename
        # Using same filesystem ensures os.replace() is atomic
        fd, temp_path = tempfile.mkstemp(
            dir=dir_path,
            prefix=".tmp.",
            suffix=f".{os.path.basename(full_path)}"
        )

        try:
            # Write content to temp file
            os.write(fd, content)

            # Ensure data is written to disk (durability guarantee)
            os.fsync(fd)

            # Close file descriptor
            os.close(fd)

            # Atomic rename - makes new content visible atomically
            # os.replace() is atomic on both POSIX and Windows
            os.replace(temp_path, full_path)

            # Sync directory to ensure rename is persisted
            # This is critical for crash recovery
            try:
                dir_fd = os.open(dir_path, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except (OSError, AttributeError):
                # Some filesystems/OSes don't support directory fsync
                # This is acceptable - the file fsync is the critical part
                pass

        except Exception:
            # Clean up temp file on any error
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                    # Ignore cleanup errors - we're already handling an exception
                pass
            raise

        logger.info(f"Successfully wrote file: {path}")

    def read_json(self, path: str) -> Dict[str, Any]:
        content = self.read_file(path)
        return json.loads(content.decode("utf-8"))

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        """Atomically write JSON file with fsync.

        JSON serialization happens before any file I/O to minimize
        time spent with file handles open.
        """
        content = json.dumps(data, indent=2).encode("utf-8")
        self.write_file(path, content)  # Uses atomic write

    def exists(self, path: str) -> bool:
        full_path = self._resolve_path(path)
        return os.path.exists(full_path)

    def list_files(self, prefix: str) -> List[str]:
        full_prefix = self._resolve_path(prefix)
        if not os.path.exists(full_prefix):
            return []

        result = []
        for root, _dirs, files in os.walk(full_prefix):
            for file in files:
                full_path = os.path.join(root, file)
                # Return path relative to base_path
                rel_path = os.path.relpath(full_path, self.base_path)
                result.append(rel_path)
        return result

    def delete_file(self, path: str) -> None:
        full_path = self._resolve_path(path)
        if os.path.exists(full_path):
            os.remove(full_path)

    def makedirs(self, path: str, exist_ok: bool = True) -> None:
        full_path = self._resolve_path(path)
        os.makedirs(full_path, exist_ok=exist_ok)

    def get_size(self, path: str) -> int:
        full_path = self._resolve_path(path)
        return os.path.getsize(full_path)

    def get_modified_time(self, path: str) -> float:
        full_path = self._resolve_path(path)
        return os.path.getmtime(full_path)

    def create_lock(self, path: str, timeout: float = 30.0) -> "LockProvider":
        from .lock_provider import LocalLockProvider
        full_path = self._resolve_path(path)
        return LocalLockProvider(full_path, timeout)


class S3FileStream:
    """Wrapper for S3 StreamingBody to support context manager protocol."""

    def __init__(self, body: Any):
        self.body = body

    def read(self, n: Optional[int] = None) -> bytes:
        return self.body.read(n)

    def close(self) -> None:
        self.body.close()

    def __enter__(self) -> "S3FileStream":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

class S3StorageBackend(StorageBackend):
    """S3-compatible storage backend (AWS S3, MinIO, etc.)"""

    def __init__(
        self,
        bucket: str,
        endpoint_url: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: str = "us-east-1",
        prefix: str = "",
        use_conditional_writes: bool = True,
    ):
        if not BOTO3_AVAILABLE:
            raise ImportError(
                "boto3 is required for S3 storage backend. "
                "Install with: pip install datashard[s3]"
            )

        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.use_conditional_writes = use_conditional_writes

        # Create S3 client
        session = boto3.session.Session()

        s3_config = {
            "region_name": region,
        }

        if endpoint_url:
            s3_config["endpoint_url"] = endpoint_url

        if access_key and secret_key:
            s3_config["aws_access_key_id"] = access_key
            s3_config["aws_secret_access_key"] = secret_key

        self.s3 = session.client("s3", **s3_config)

        if not use_conditional_writes:
            logger.info("S3 conditional writes disabled. Using polling-based locking (less robust).")

    def _get_s3_key(self, path: str) -> str:
        """Convert path to S3 key"""
        # Remove leading slash if present
        path = path.lstrip("/")

        # Add prefix if configured
        if self.prefix:
            return f"{self.prefix}/{path}"
        return path

    def read_file(self, path: str) -> bytes:
        """Read file from S3 with retry logic for eventual consistency.

        PHASE 2: Added retry logic to handle S3 eventual consistency.
        """
        from .s3_consistency import with_s3_retry

        key = self._get_s3_key(path)
        logger.debug(f"Reading S3 file: s3://{self.bucket}/{key}")

        def read_op() -> bytes:
            try:
                response = self.s3.get_object(Bucket=self.bucket, Key=key)
                return response["Body"].read()
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    raise FileNotFoundError(
                        f"S3 object not found: s3://{self.bucket}/{key}"
                    ) from e
                raise

        data = with_s3_retry(read_op, f"S3 read: {key}")
        logger.debug(f"Read {len(data)} bytes from s3://{self.bucket}/{key}")
        return data

    def open_file(self, path: str) -> Any:
        """Open S3 object as a read-only binary stream."""

        from .s3_consistency import with_s3_retry

        key = self._get_s3_key(path)

        def open_op() -> Any:
            try:
                response = self.s3.get_object(Bucket=self.bucket, Key=key)
                # Cast to BinaryIO because S3FileStream implements the necessary protocol
                # but is not explicitly inheriting from io.BytesIO/BinaryIO
                return S3FileStream(response["Body"])
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    raise FileNotFoundError(
                        f"S3 object not found: s3://{self.bucket}/{key}"
                    ) from e
                raise

        return with_s3_retry(open_op, f"S3 open: {key}")

    def write_file(self, path: str, content: bytes) -> None:
        """Write file to S3 (inherently atomic).

        S3 PutObject is atomic - either the entire object is written or nothing.
        No partial writes are visible to readers.

        PHASE 2 improvements:
        - S3 consistency handling with retries
        - Comprehensive logging
        - Checksum computation
        """
        from .s3_consistency import with_s3_retry

        logger.info(f"Writing S3 file: {path} ({len(content)} bytes)")

        key = self._get_s3_key(path)

        # PHASE 2: Compute checksum
        checksum = IntegrityChecker.compute_checksum(content)
        logger.debug(f"Computed checksum for s3://{self.bucket}/{key}: {checksum[:16]}...")

        # PHASE 2: Write with retry logic for S3 eventual consistency
        def write_op() -> None:
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=content)

        with_s3_retry(write_op, f"S3 write: {key}")
        logger.info(f"Successfully wrote S3 file: {path}")

    def read_json(self, path: str) -> Dict[str, Any]:
        content = self.read_file(path)
        return json.loads(content.decode("utf-8"))

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        content = json.dumps(data, indent=2).encode("utf-8")
        self.write_file(path, content)

    def exists(self, path: str) -> bool:
        key = self._get_s3_key(path)

        # First try exact object match
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] != "404":
                raise

        # If exact match fails, check if it's a prefix (directory-like)
        # by checking if any objects exist with this prefix
        prefix = key if key.endswith("/") else key + "/"
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix, MaxKeys=1)
            return "Contents" in response and len(response["Contents"]) > 0
        except ClientError:
            return False

    def list_files(self, prefix: str) -> List[str]:
        s3_prefix = self._get_s3_key(prefix)

        result = []
        paginator = self.s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self.bucket, Prefix=s3_prefix):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]
                # Remove prefix to get relative path
                if self.prefix and key.startswith(self.prefix + "/"):
                    rel_path = key[len(self.prefix) + 1 :]
                else:
                    rel_path = key
                result.append(rel_path)

        return result

    def delete_file(self, path: str) -> None:
        key = self._get_s3_key(path)
        self.s3.delete_object(Bucket=self.bucket, Key=key)

    def makedirs(self, path: str, exist_ok: bool = True) -> None:
        """No-op for S3 - directories don't need to be created"""
        pass

    def get_size(self, path: str) -> int:
        key = self._get_s3_key(path)
        try:
            response = self.s3.head_object(Bucket=self.bucket, Key=key)
            return response["ContentLength"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise FileNotFoundError(f"S3 object not found: s3://{self.bucket}/{key}") from e
            raise

    def get_modified_time(self, path: str) -> float:
        key = self._get_s3_key(path)
        try:
            response = self.s3.head_object(Bucket=self.bucket, Key=key)
            return response["LastModified"].timestamp()
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                 raise FileNotFoundError(f"S3 object not found: s3://{self.bucket}/{key}") from e
            raise

    def create_lock(self, path: str, timeout: float = 30.0) -> "LockProvider":
        key = self._get_s3_key(path)
        if self.use_conditional_writes:
            from .lock_provider import S3LockProvider
            return S3LockProvider(self.s3, self.bucket, key, timeout)
        else:
            from .lock_provider import S3PollingLockProvider
            return S3PollingLockProvider(self.s3, self.bucket, key, timeout)


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
            DATASHARD_S3_USE_CONDITIONAL_WRITES: "true" (default) or "false"
                Set to "false" for S3 providers that don't support If-None-Match
                headers (e.g., OVH Object Storage). Uses polling-based locking instead.

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
        use_conditional_writes = os.getenv("DATASHARD_S3_USE_CONDITIONAL_WRITES", "true").lower() in ("true", "1", "yes")

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

        return S3StorageBackend(
            bucket=bucket,
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            region=region,
            prefix=full_prefix,
            use_conditional_writes=use_conditional_writes,
        )
    else:
        # Local filesystem (default)
        return LocalStorageBackend(table_path)
