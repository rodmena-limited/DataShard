"""
S3-compatible storage backend (AWS S3, MinIO, OVH, ...). Split out of
storage_backend.py for the 500-line file cap (#71).
"""

import io
import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from .integrity import IntegrityChecker
from .logging_config import get_logger
from .s3_range_file import S3FileStream, S3RangeFile
from .storage_backend import CASConflictError, StorageBackend

if TYPE_CHECKING:
    from .lock_provider import LockProvider

logger = get_logger(__name__)

try:
    import boto3
    from botocore.exceptions import ClientError

    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False


UNSAFE_LOCK_MESSAGE = (
    "This S3 endpoint does not honour conditional writes (If-None-Match / If-Match), "
    "or DATASHARD_S3_USE_CONDITIONAL_WRITES=false was set. Without them datashard "
    "can only offer the polling lock, under which two concurrent writers can both "
    "commit and one snapshot is silently LOST (issue #59). Refusing to start. Either "
    "use a provider with conditional-write support (AWS S3, OVH, MinIO, ...) and "
    "leave DATASHARD_S3_USE_CONDITIONAL_WRITES unset, or - only if this table has a "
    "single writer - set DATASHARD_S3_ALLOW_UNSAFE_LOCK=1 to accept the risk."
)


class S3StorageBackend(StorageBackend):
    """S3-compatible storage backend (AWS S3, MinIO, OVH, etc.)"""

    def __init__(
        self,
        bucket: str,
        endpoint_url: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: str = "us-east-1",
        prefix: str = "",
        use_conditional_writes: Optional[bool] = None,
        allow_unsafe_lock: bool = False,
    ):
        """
        Args:
            use_conditional_writes: True = CAS lock + CAS commit point; False = the
                best-effort polling lock; None (default) = PROBE the provider once
                with a conditional PUT and use CAS if it is honoured (#59).
            allow_unsafe_lock: Required to construct without conditional writes.
                The polling lock can lose commits under concurrency, so refusing is
                the default (fail closed).
        """
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

        # Create S3 client
        session = boto3.session.Session()

        from botocore.config import Config

        s3_config: Dict[str, Any] = {
            "region_name": region,
            # botocore's own retries multiply datashard's with_s3_retry layer; one
            # low-level retry for connection blips is enough (#74). The pool size
            # serves parallel scans without 'connection pool is full' churn.
            "config": Config(retries={"max_attempts": 2, "mode": "standard"}, max_pool_connections=32),
        }

        if endpoint_url:
            s3_config["endpoint_url"] = endpoint_url

        if access_key and secret_key:
            s3_config["aws_access_key_id"] = access_key
            s3_config["aws_secret_access_key"] = secret_key

        self.s3 = session.client("s3", **s3_config)

        if endpoint_url and endpoint_url.lower().startswith("http://"):
            logger.warning(
                f"S3 endpoint {endpoint_url} uses plain HTTP - credentials and data "
                f"travel unencrypted. Use https:// except for isolated test setups."
            )

        if use_conditional_writes is None:
            use_conditional_writes = self._detect_conditional_writes()
        if not use_conditional_writes and not allow_unsafe_lock:
            raise RuntimeError(UNSAFE_LOCK_MESSAGE)
        self.use_conditional_writes = use_conditional_writes

        if not use_conditional_writes:
            logger.warning(
                "S3 conditional writes disabled (DATASHARD_S3_ALLOW_UNSAFE_LOCK=1). Using "
                "polling-based locking, which is BEST-EFFORT ONLY: under contention two "
                "writers can both acquire the lock and a commit can be silently lost (#59)."
            )

    def _detect_conditional_writes(self) -> bool:
        """Ask the provider whether it honours conditional PUTs (see s3_cas_probe, #59)."""
        from .s3_cas_probe import detect_conditional_writes

        return detect_conditional_writes(self.s3, self.bucket, self._get_s3_key, self.endpoint_url)

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
                body: bytes = response["Body"].read()
                return body
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

    def open_seekable(self, path: str) -> Any:
        """Seekable, range-reading view of an S3 object.

        Buffered: pyarrow issues many small reads while walking a parquet
        footer, and each unbuffered read would be its own HTTP range request.
        """
        key = self._get_s3_key(path)
        # Size is learnt from the first (suffix-range) read - no HEAD (#67).
        return io.BufferedReader(
            S3RangeFile(self.s3, self.bucket, key), buffer_size=1 << 20
        )

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

        logger.debug(f"Writing S3 file: {path} ({len(content)} bytes)")

        key = self._get_s3_key(path)

        # PHASE 2: Compute checksum
        checksum = IntegrityChecker.compute_checksum(content)
        logger.debug(f"Computed checksum for s3://{self.bucket}/{key}: {checksum[:16]}...")

        # PHASE 2: Write with retry logic for S3 eventual consistency
        def write_op() -> None:
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=content)

        with_s3_retry(write_op, f"S3 write: {key}")
        logger.debug(f"Successfully wrote S3 file: {path}")

    def write_files(self, items: List[Tuple[str, bytes]]) -> None:
        """Independent PUTs issued concurrently; one commit's marker writes used to be
        serial round trips of ~200 ms each on OVH (#80). Raises the first failure."""
        if len(items) <= 1:
            for path, content in items:
                self.write_file(path, content)
            return
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=min(8, len(items))) as pool:
            for _ in pool.map(lambda item: self.write_file(item[0], item[1]), items):
                pass

    @property
    def supports_cas(self) -> bool:
        """CAS via conditional PUT (If-Match / If-None-Match) when the provider supports it."""
        return self.use_conditional_writes

    def read_file_with_etag(self, path: str) -> Tuple[bytes, Optional[str]]:
        """Read file contents plus the object's ETag (for CAS writes)."""
        from .s3_consistency import with_s3_retry

        key = self._get_s3_key(path)

        def read_op() -> Tuple[bytes, Optional[str]]:
            try:
                response = self.s3.get_object(Bucket=self.bucket, Key=key)
                return response["Body"].read(), response.get("ETag")
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    raise FileNotFoundError(
                        f"S3 object not found: s3://{self.bucket}/{key}"
                    ) from e
                raise

        return with_s3_retry(read_op, f"S3 read+etag: {key}")

    def write_file_cas(self, path: str, content: bytes, etag: Optional[str]) -> None:
        """Conditional PUT: create-if-absent (etag=None) or replace-if-unchanged.

        Raises CASConflictError when the precondition fails (concurrent writer won).
        NOT retried: a retried conditional PUT after an ambiguous failure could
        conflict with our own first attempt; callers decide how to handle it.
        """
        key = self._get_s3_key(path)
        kwargs: Dict[str, Any] = {"Bucket": self.bucket, "Key": key, "Body": content}
        if etag is None:
            kwargs["IfNoneMatch"] = "*"
        else:
            kwargs["IfMatch"] = etag
        try:
            self.s3.put_object(**kwargs)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("PreconditionFailed", "412", "ConditionalRequestConflict"):
                raise CASConflictError(
                    f"CAS write lost race for s3://{self.bucket}/{key}"
                ) from e
            raise

    def read_json(self, path: str) -> Dict[str, Any]:
        content = self.read_file(path)
        parsed: Dict[str, Any] = json.loads(content.decode("utf-8"))
        return parsed

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        content = json.dumps(data, indent=2).encode("utf-8")
        self.write_file(path, content)

    def exists(self, path: str) -> bool:
        """Existence check with transient-error retry.

        404s resolve to False without retrying; transient (non-404) errors are
        retried like every other S3 operation - exists() sits on hot paths
        (version-hint reads, manifest reachability) where a first-try failure
        must not surface as a hard error.

        Only paths written as directories (trailing '/') fall back to a prefix
        listing. Answering True for 'data/x.parquet' merely because objects
        exist UNDER that name would let a missing data file pass validation.
        """
        from .s3_consistency import with_s3_retry

        key = self._get_s3_key(path)

        def exists_op() -> bool:
            # First try exact object match
            try:
                self.s3.head_object(Bucket=self.bucket, Key=key)
                return True
            except ClientError as e:
                if e.response["Error"]["Code"] != "404":
                    raise

            if not key.endswith("/"):
                return False

            # Directory-like path: any object under the prefix counts.
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=key, MaxKeys=1)
            return "Contents" in response and len(response["Contents"]) > 0

        return with_s3_retry(exists_op, f"S3 exists: {key}")

    def list_files(self, prefix: str) -> List[str]:
        """List objects under a prefix, retried like every other S3 read.

        Garbage collection decides what to delete from this listing, so a
        transient failure here must surface as an error (the caller aborts),
        never as a short list.
        """
        from .s3_consistency import with_s3_retry

        # Directory semantics: '<table>/data' must list '<table>/data/...' only,
        # never the sibling '<table>/data_export/...' (#63: GC deleted those).
        s3_prefix = self._get_s3_key(prefix).rstrip("/")
        if s3_prefix:
            s3_prefix += "/"

        def list_op() -> List[str]:
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

        return with_s3_retry(list_op, f"S3 list: {s3_prefix}")

    def delete_file(self, path: str) -> None:
        """Delete an object, retrying transient errors.

        Without a retry a blip leaves an orphan behind on every GC pass.
        """
        from .s3_consistency import with_s3_retry

        key = self._get_s3_key(path)

        def delete_op() -> None:
            self.s3.delete_object(Bucket=self.bucket, Key=key)

        with_s3_retry(delete_op, f"S3 delete: {key}")

    def delete_files(self, paths: List[str]) -> None:
        """Bulk delete (DeleteObjects, 1000 keys per request), retried (#67)."""
        from .s3_consistency import with_s3_retry

        keys = [self._get_s3_key(p) for p in paths]
        for i in range(0, len(keys), 1000):
            chunk = keys[i : i + 1000]

            def delete_op(chunk: List[str] = chunk) -> None:
                resp = self.s3.delete_objects(
                    Bucket=self.bucket,
                    Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
                )
                errors = resp.get("Errors") or []
                if errors:
                    raise IOError(f"S3 DeleteObjects reported {len(errors)} error(s): {errors[:3]}")

            with_s3_retry(delete_op, f"S3 bulk delete: {len(chunk)} keys")

    def makedirs(self, path: str, exist_ok: bool = True) -> None:
        """No-op for S3 - directories don't need to be created"""
        pass

    def get_size(self, path: str) -> int:
        from .s3_consistency import with_s3_retry

        key = self._get_s3_key(path)

        def size_op() -> int:
            try:
                response = self.s3.head_object(Bucket=self.bucket, Key=key)
                return int(response["ContentLength"])
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    raise FileNotFoundError(
                        f"S3 object not found: s3://{self.bucket}/{key}"
                    ) from e
                raise

        return with_s3_retry(size_op, f"S3 size: {key}")

    def list_files_with_mtime(self, prefix: str) -> List[Tuple[str, float]]:
        """Listing with LastModified per key: no HEAD per object (#77)."""
        from .s3_consistency import with_s3_retry

        s3_prefix = self._get_s3_key(prefix).rstrip("/")
        if s3_prefix:
            s3_prefix += "/"

        def list_op() -> List[Tuple[str, float]]:
            result: List[Tuple[str, float]] = []
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=s3_prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if self.prefix and key.startswith(self.prefix + "/"):
                        rel_path = key[len(self.prefix) + 1 :]
                    else:
                        rel_path = key
                    result.append((rel_path, float(obj["LastModified"].timestamp())))
            return result

        return with_s3_retry(list_op, f"S3 list+mtime: {s3_prefix}")

    def clock_ms(self) -> float:
        """The S3 server's clock, from the HTTP Date header of a HEAD request (#77).

        Falls back to the local clock (with a warning) only if the provider sends
        no Date header - HTTP/1.1 requires one, so that should not happen.
        """
        from email.utils import parsedate_to_datetime

        from .s3_consistency import with_s3_retry

        key = self._get_s3_key("metadata/version-hint.text")

        def head_op() -> Dict[str, Any]:
            try:
                return dict(self.s3.head_object(Bucket=self.bucket, Key=key))
            except ClientError as e:
                if e.response.get("Error", {}).get("Code", "") in ("404", "NoSuchKey", "NotFound"):
                    return dict(e.response)  # the headers travel with the error response
                raise

        resp = with_s3_retry(head_op, "S3 clock (HEAD)")
        headers = resp.get("ResponseMetadata", {}).get("HTTPHeaders", {})
        date_header = headers.get("date") or headers.get("Date")
        if date_header:
            try:
                return parsedate_to_datetime(date_header).timestamp() * 1000
            except (TypeError, ValueError):
                pass
        logger.warning("S3 response carried no usable Date header; using the local clock for GC age decisions")
        return super().clock_ms()

    def get_modified_time(self, path: str) -> float:
        """Object mtime, retried: GC compares it against the grace period, and
        a failure there is treated as 'cannot classify'."""
        from .s3_consistency import with_s3_retry

        key = self._get_s3_key(path)

        def mtime_op() -> float:
            try:
                response = self.s3.head_object(Bucket=self.bucket, Key=key)
                return float(response["LastModified"].timestamp())
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    raise FileNotFoundError(
                        f"S3 object not found: s3://{self.bucket}/{key}"
                    ) from e
                raise

        return with_s3_retry(mtime_op, f"S3 mtime: {key}")

    def create_lock(self, path: str, timeout: float = 30.0) -> "LockProvider":
        key = self._get_s3_key(path)
        if self.use_conditional_writes:
            from .lock_provider import S3LockProvider
            return S3LockProvider(self.s3, self.bucket, key, timeout)
        else:
            from .lock_provider_polling import S3PollingLockProvider
            return S3PollingLockProvider(self.s3, self.bucket, key, timeout)


