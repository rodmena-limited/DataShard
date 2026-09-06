"""
Disk space utilities for datashard.

Provides disk space checking and management.
"""

import os
import shutil
from typing import NamedTuple

from .logging_config import get_logger

logger = get_logger(__name__)


class DiskSpace(NamedTuple):
    """Disk space information."""

    total: int  # Total bytes
    used: int  # Used bytes
    free: int  # Free bytes
    percent_used: float  # Percentage used (0-100)


def get_disk_space(path: str) -> DiskSpace:
    """Get disk space information for a path.

    Args:
        path: Path to check (file or directory)

    Returns:
        DiskSpace information
    """
    # Get directory path
    if os.path.isfile(path):
        path = os.path.dirname(path)
    elif not os.path.exists(path):
        # Use parent directory
        path = os.path.dirname(path) or "."

    # Get disk usage
    stat = shutil.disk_usage(path)

    percent_used = (stat.used / stat.total * 100) if stat.total > 0 else 0

    return DiskSpace(
        total=stat.total,
        used=stat.used,
        free=stat.free,
        percent_used=percent_used,
    )


DEFAULT_MIN_FREE_BYTES = 1 << 30  # 1 GiB


def check_disk_space(
    path: str,
    required_bytes: int,
    warn_threshold: float = 90.0,
    error_threshold: float = 95.0,
    min_free_bytes: int | None = None,
) -> None:
    """Refuse a write only when the volume is genuinely short of space.

    A write is refused when free bytes < max(2 x required_bytes, min_free_bytes);
    min_free_bytes defaults to DATASHARD_MIN_FREE_BYTES or 1 GiB. The percentage
    thresholds only WARN: a 95 %-full 10 TB volume still has 500 GB free, and
    refusing every write there - including the commit-point hint - turned the
    whole lake read-only (#70).

    Raises:
        IOError: If free space is below the absolute floor.
    """
    if min_free_bytes is None:
        raw = os.getenv("DATASHARD_MIN_FREE_BYTES", "").strip()
        try:
            min_free_bytes = int(raw) if raw else DEFAULT_MIN_FREE_BYTES
        except ValueError:
            logger.warning(f"Ignoring invalid DATASHARD_MIN_FREE_BYTES={raw!r}")
            min_free_bytes = DEFAULT_MIN_FREE_BYTES

    space = get_disk_space(path)
    needed = max(2 * required_bytes, min_free_bytes)

    logger.debug(
        f"Disk space check for {path}: "
        f"{space.free / (1024**3):.2f} GB free "
        f"({100 - space.percent_used:.1f}% available)"
    )

    if space.free < needed:
        msg = (
            f"Insufficient disk space: {space.free / (1024**3):.2f} GB free, "
            f"need at least {needed / (1024**3):.2f} GB (2 x write size or the "
            f"{min_free_bytes / (1024**3):.2f} GB floor)"
        )
        logger.error(msg)
        raise IOError(msg)

    if space.percent_used >= error_threshold:
        logger.warning(
            f"Disk nearly full: {space.percent_used:.1f}% used "
            f"({space.free / (1024**3):.2f} GB free); writes continue above the free-space floor"
        )
    elif space.percent_used >= warn_threshold:
        logger.warning(
            f"Disk space low: {space.percent_used:.1f}% used (threshold: {warn_threshold}%)"
        )


def estimate_write_size(data: bytes, overhead_factor: float = 1.2) -> int:
    """Estimate disk space required for writing data.

    Args:
        data: Data to write
        overhead_factor: Overhead multiplier (for filesystem overhead, temp files, etc.)

    Returns:
        Estimated bytes required
    """
    return int(len(data) * overhead_factor)
