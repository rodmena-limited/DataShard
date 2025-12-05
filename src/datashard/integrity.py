"""
Data integrity and corruption detection for datashard.

Provides checksum computation and validation to detect corrupted data.
"""

import hashlib
from typing import BinaryIO

from .logging_config import get_logger

logger = get_logger(__name__)


class IntegrityChecker:
    """Handles data integrity checking with checksums."""

    @staticmethod
    def compute_checksum(data: bytes, algorithm: str = "sha256") -> str:
        """Compute checksum for data.

        Args:
            data: Data to checksum
            algorithm: Hash algorithm (md5, sha1, sha256, sha512)

        Returns:
            Hexadecimal checksum string

        Raises:
            ValueError: If algorithm not supported
        """
        try:
            hasher = hashlib.new(algorithm)
            hasher.update(data)
            checksum = hasher.hexdigest()
            logger.debug(f"Computed {algorithm} checksum: {checksum[:16]}...")
            return checksum
        except ValueError as e:
            logger.error(f"Unsupported hash algorithm: {algorithm}")
            raise ValueError(f"Unsupported hash algorithm: {algorithm}") from e

    @staticmethod
    def compute_checksum_from_stream(stream: BinaryIO, algorithm: str = "sha256") -> str:
        """Compute checksum for a stream.

        Args:
            stream: Binary stream to checksum
            algorithm: Hash algorithm

        Returns:
            Hexadecimal checksum string
        """
        try:
            hasher = hashlib.new(algorithm)
            while True:
                chunk = stream.read(8192)
                if not chunk:
                    break
                hasher.update(chunk)
            checksum = hasher.hexdigest()
            logger.debug(f"Computed {algorithm} checksum from stream: {checksum[:16]}...")
            return checksum
        except ValueError as e:
            logger.error(f"Unsupported hash algorithm: {algorithm}")
            raise ValueError(f"Unsupported hash algorithm: {algorithm}") from e

    @staticmethod
    def verify_checksum(
        data: bytes, expected_checksum: str, algorithm: str = "sha256"
    ) -> bool:
        """Verify data against expected checksum.

        Args:
            data: Data to verify
            expected_checksum: Expected checksum
            algorithm: Hash algorithm

        Returns:
            True if checksum matches, False otherwise
        """
        actual_checksum = IntegrityChecker.compute_checksum(data, algorithm)
        matches = actual_checksum == expected_checksum

        if not matches:
            logger.error(
                f"Checksum mismatch! Expected: {expected_checksum[:16]}..., "
                f"Got: {actual_checksum[:16]}..."
            )
        else:
            logger.debug("Checksum verification passed")

        return matches

    @staticmethod
    def verify_stream_checksum(
        stream: BinaryIO, expected_checksum: str, algorithm: str = "sha256"
    ) -> bool:
        """Verify stream data against expected checksum.

        Args:
            stream: Data stream to verify
            expected_checksum: Expected checksum
            algorithm: Hash algorithm

        Returns:
            True if checksum matches, False otherwise
        """
        actual_checksum = IntegrityChecker.compute_checksum_from_stream(stream, algorithm)
        matches = actual_checksum == expected_checksum

        if not matches:
            logger.error(
                f"Checksum mismatch! Expected: {expected_checksum[:16]}..., "
                f"Got: {actual_checksum[:16]}..."
            )
        else:
            logger.debug("Checksum verification passed")

        return matches

    @staticmethod
    def compute_file_checksum(file_path: str, algorithm: str = "sha256") -> str:
        """Compute checksum for a file.

        Args:
            file_path: Path to file
            algorithm: Hash algorithm

        Returns:
            Hexadecimal checksum string
        """
        hasher = hashlib.new(algorithm)
        with open(file_path, "rb") as f:
            # Read in chunks to handle large files
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                hasher.update(chunk)

        checksum = hasher.hexdigest()
        logger.debug(f"Computed file checksum for {file_path}: {checksum[:16]}...")
        return checksum


def add_checksum_to_metadata(metadata: dict, data: bytes) -> dict:
    """Add checksum to metadata dictionary.

    Args:
        metadata: Metadata dictionary
        data: Data that was written

    Returns:
        Updated metadata with checksum
    """
    metadata = metadata.copy()
    metadata["checksum"] = IntegrityChecker.compute_checksum(data)
    metadata["checksum_algorithm"] = "sha256"
    return metadata


def verify_data_integrity(data: bytes, metadata: dict) -> bool:
    """Verify data integrity using metadata checksum.

    Args:
        data: Data to verify
        metadata: Metadata with checksum

    Returns:
        True if valid, False otherwise
    """
    if "checksum" not in metadata:
        logger.warning("No checksum in metadata - cannot verify integrity")
        return True  # Assume valid if no checksum

    algorithm = metadata.get("checksum_algorithm", "sha256")
    expected = metadata["checksum"]

    return IntegrityChecker.verify_checksum(data, expected, algorithm)
