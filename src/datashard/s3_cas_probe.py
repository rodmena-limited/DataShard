"""
Does this S3 endpoint honour conditional PUTs? Ask it, never assume (#59). Split out of
s3_backend.py for the 500-line file cap (#71).
"""

from typing import Any, Callable, Dict, Optional, Tuple

from .logging_config import get_logger

try:
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover - boto3 optional
    ClientError = Exception

logger = get_logger(__name__)

# Providers found to honour / ignore conditional PUTs, keyed by (endpoint, bucket).
# One probe per process per bucket, not one per Table.
_CAS_SUPPORT_CACHE: Dict[Tuple[Optional[str], str], bool] = {}


def detect_conditional_writes(
s3: Any, bucket: str, key_for: Callable[[str], str], endpoint_url: Optional[str]
) -> bool:
    """Ask the provider, don't believe a comment (#59).

    Writes a small probe object, then re-PUTs it with If-None-Match:* and with a
    stale If-Match. A provider that honours preconditions answers 412 to both;
    one that silently overwrites (or rejects the header) is treated as having
    NO conditional-write support - enabling CAS there would be unsafe. The
    result is cached per (endpoint, bucket) for the process lifetime.
    """
    import uuid

    cache_key = (endpoint_url, bucket)
    if cache_key in _CAS_SUPPORT_CACHE:
        return _CAS_SUPPORT_CACHE[cache_key]

    key = key_for(f".locks/.cas-probe-{uuid.uuid4().hex}")
    supported = False
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=b"probe")
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=b"overwrite", IfNoneMatch="*")
            none_match_honoured = False  # overwrote: precondition ignored
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            none_match_honoured = code in ("PreconditionFailed", "412", "ConditionalRequestConflict")
        try:
            s3.put_object(
                Bucket=bucket, Key=key, Body=b"overwrite",
                IfMatch='"00000000000000000000000000000000"',
            )
            if_match_honoured = False
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if_match_honoured = code in ("PreconditionFailed", "412", "ConditionalRequestConflict")
        supported = none_match_honoured and if_match_honoured
    finally:
        try:
            s3.delete_object(Bucket=bucket, Key=key)
        except Exception:  # noqa: BLE001 - best-effort cleanup of the probe object
            pass

    if supported:
        logger.info(f"S3 endpoint {endpoint_url or 'aws'} honours conditional writes: using CAS locking")
    else:
        logger.error(
            f"S3 endpoint {endpoint_url or 'aws'} does NOT honour conditional writes "
            f"(If-None-Match / If-Match); only the unsafe polling lock is available"
        )
    _CAS_SUPPORT_CACHE[cache_key] = supported
    return supported

