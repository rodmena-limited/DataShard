"""
S3 eventual consistency handling for datashard.

Provides retry logic and consistency checks for S3 operations.
"""

import time
from typing import Any, Callable, Optional, TypeVar

from .logging_config import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


try:
    from botocore.exceptions import BotoCoreError, ClientError
    # Catch specific S3 errors + generic IO errors
    RETRYABLE_EXCEPTIONS: Any = (ClientError, BotoCoreError, IOError, OSError)
except ImportError:
    # Fallback if boto3 not installed (though unlikely if using S3)
    RETRYABLE_EXCEPTIONS = (IOError, OSError)


class S3ConsistencyHandler:
    """Handles S3 eventual consistency with retry logic."""

    def __init__(
        self,
        max_retries: int = 5,
        initial_delay: float = 0.1,
        max_delay: float = 5.0,
        backoff_factor: float = 2.0,
        retryable_exceptions: tuple = RETRYABLE_EXCEPTIONS,
    ):
        """Initialize S3 consistency handler.

        Args:
            max_retries: Maximum number of retries
            initial_delay: Initial delay in seconds
            max_delay: Maximum delay in seconds
            backoff_factor: Exponential backoff multiplier
            retryable_exceptions: Tuple of exceptions to retry
        """
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.retryable_exceptions = retryable_exceptions

    def retry_with_backoff(
        self,
        operation: Callable[[], T],
        operation_name: str = "S3 operation",
        expected_value: Optional[Any] = None,
    ) -> T:
        """Retry operation with exponential backoff.

        Args:
            operation: Operation to retry
            operation_name: Description for logging
            expected_value: If provided, retry until result equals this

        Returns:
            Result of operation

        Raises:
            Exception: If all retries exhausted or non-retryable exception occurs
        """
        delay = self.initial_delay
        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                logger.debug(
                    f"{operation_name} - attempt {attempt + 1}/{self.max_retries + 1}"
                )
                result = operation()

                # If expected value specified, check if we got it
                if expected_value is not None and result != expected_value:
                    if attempt < self.max_retries:
                        logger.warning(
                            f"{operation_name} returned unexpected value, "
                            f"retrying in {delay:.2f}s..."
                        )
                        time.sleep(delay)
                        delay = min(delay * self.backoff_factor, self.max_delay)
                        continue
                    else:
                        logger.error(
                            f"{operation_name} - max retries exhausted, "
                            f"got unexpected value"
                        )
                        return result

                logger.debug(f"{operation_name} succeeded")
                return result

            except self.retryable_exceptions as e:
                last_exception = e
                if attempt < self.max_retries:
                    # Check for ClientError 404/NoSuchKey - if we are reading, we might want to fail fast?
                    # But due to eventual consistency, a 404 might be temporary.
                    # So we stick to retrying.
                    logger.warning(
                        f"{operation_name} failed: {e}, retrying in {delay:.2f}s..."
                    )
                    time.sleep(delay)
                    delay = min(delay * self.backoff_factor, self.max_delay)
                else:
                    logger.error(f"{operation_name} - max retries exhausted: {e}")
                    raise
            except Exception as e:
                # Non-retryable exception (e.g. ValueError, TypeError)
                logger.error(f"{operation_name} failed with non-retryable error: {e}")
                raise

        # Should not reach here, but just in case
        if last_exception:
            raise last_exception
        raise RuntimeError(f"{operation_name} failed after all retries")

    def read_after_write(
        self,
        write_op: Callable[[], None],
        read_op: Callable[[], T],
        operation_name: str = "S3 write-then-read",
    ) -> T:
        """Perform write then ensure read sees the write (handles eventual consistency).

        Args:
            write_op: Write operation
            read_op: Read operation
            operation_name: Description for logging

        Returns:
            Result of read operation
        """
        logger.debug(f"{operation_name} - performing write")
        write_op()

        # Wait a bit for S3 eventual consistency
        logger.debug(f"{operation_name} - waiting for consistency")
        time.sleep(self.initial_delay)

        # Retry read with backoff to handle consistency delay
        logger.debug(f"{operation_name} - performing read with retry")
        return self.retry_with_backoff(read_op, f"{operation_name} (read)")


# Global instance for easy use
default_handler = S3ConsistencyHandler()


def with_s3_retry(operation: Callable[[], T], operation_name: str = "S3 operation") -> T:
    """Convenience function to retry S3 operations.

    Args:
        operation: Operation to retry
        operation_name: Description for logging

    Returns:
        Result of operation
    """
    return default_handler.retry_with_backoff(operation, operation_name)
