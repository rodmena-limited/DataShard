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


def check_disk_space(
    path: str,
    required_bytes: int,
    warn_threshold: float = 90.0,
    error_threshold: float = 95.0,
) -> None:
    """Check if sufficient disk space is available.

    Args:
        path: Path to check
        required_bytes: Required free bytes
        warn_threshold: Warning threshold (percent used)
        error_threshold: Error threshold (percent used)

    Raises:
        IOError: If insufficient space or disk is too full
    """
    space = get_disk_space(path)

    logger.debug(
        f"Disk space check for {path}: "
        f"{space.free / (1024**3):.2f} GB free "
        f"({100 - space.percent_used:.1f}% available)"
    )

    # Check if we have required space
    if space.free < required_bytes:
        msg = (
            f"Insufficient disk space: {space.free / (1024**3):.2f} GB free, "
            f"need {required_bytes / (1024**3):.2f} GB"
        )
        logger.error(msg)
        raise IOError(msg)

    # Check if disk is getting too full
    if space.percent_used >= error_threshold:
        msg = f"Disk critically full: {space.percent_used:.1f}% used (threshold: {error_threshold}%)"
        logger.error(msg)
        raise IOError(msg)
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
