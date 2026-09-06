"""
Table locations as URIs and the conversion between the absolute paths Iceberg
readers require and the table-relative paths datashard uses internally (#87).

Internally EVERYTHING stays table-relative ('data/x.parquet', or Iceberg-style
'/data/x.parquet' for data files). URIs appear only at the storage boundary:
metadata_serde / manifest_writer join them on the way out, and the decoders strip
them on the way in through to_relative().
"""

import os
from typing import TYPE_CHECKING, Iterable, Optional

from .logging_config import get_logger

if TYPE_CHECKING:
    from .storage_backend import StorageBackend

logger = get_logger(__name__)

_SCHEMES = ("file://", "s3://", "s3a://", "gs://", "abfs://", "abfss://", "hdfs://")


def location_uri(storage: "StorageBackend", table_path: str) -> str:
    """The URI foreign readers should see as the table location."""
    from .s3_backend import S3StorageBackend

    if isinstance(storage, S3StorageBackend):
        return f"s3://{storage.bucket}/{storage.prefix}" if storage.prefix else f"s3://{storage.bucket}"
    base = getattr(storage, "_real_base_path", None)
    root = base() if callable(base) else os.path.realpath(table_path)
    root = root.replace(os.sep, "/")
    if not root.startswith("/"):
        root = "/" + root  # Windows drive letters
    return "file://" + root


def join_uri(location: str, relative: str) -> str:
    return f"{location.rstrip('/')}/{relative.replace(os.sep, '/').lstrip('/')}"


def is_uri(path: str) -> bool:
    return path.startswith(_SCHEMES)


def to_relative(path: str, locations: Iterable[Optional[str]]) -> str:
    """Table-relative form (no leading slash) of a path found in metadata.

    Accepts the table URI form (any of `locations`: the actual root and the one
    recorded in the metadata, so a moved table still reads - the reader's own
    'allow_moved_paths'), the legacy Iceberg-style '/data/x' and plain 'data/x'.
    Any other absolute path or foreign URI is refused: a manifest entry must
    never open a file outside the table root (#47).
    """
    norm = path.replace("\\", "/")
    if is_uri(norm):
        for loc in locations:
            if not loc:
                continue
            prefix = loc.rstrip("/") + "/"
            if norm.startswith(prefix):
                return norm[len(prefix):]
        raise ValueError(
            f"Path {path!r} is outside this table (known locations: "
            f"{[loc for loc in locations if loc]}). If the table was moved, run "
            f"'datashard relocate <table>' to rewrite its metadata."
        )
    if norm.startswith("/"):
        first = norm.split("/", 2)[1] if "/" in norm[1:] else norm[1:]
        if first not in ("data", "metadata"):
            raise ValueError(
                f"Path {path!r} is neither table-relative nor under this table's location"
            )
        return norm.lstrip("/")
    return norm
