"""
Comprehensive logging configuration for datashard.

Provides structured logging for debugging, monitoring, and auditing.
"""

import logging
import sys
from typing import Optional


class DataShardLogger:
    """Centralized logger for datashard operations."""

    _instance: Optional[logging.Logger] = None
    _initialized = False

    @classmethod
    def get_logger(cls, name: str = "datashard") -> logging.Logger:
        """Get or create the datashard logger.

        Args:
            name: Logger name

        Returns:
            Configured logger instance
        """
        if not cls._initialized:
            cls._setup_logging()

        return logging.getLogger(name)

    @classmethod
    def _setup_logging(cls) -> None:
        """Setup logging configuration."""
        if cls._initialized:
            return

        # Create logger
        logger = logging.getLogger("datashard")
        logger.setLevel(logging.INFO)

        # Prevent duplicate handlers
        if logger.handlers:
            return

        # Console handler
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setLevel(logging.INFO)

        # Format: timestamp - level - module - message
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        console_handler.setFormatter(formatter)

        logger.addHandler(console_handler)

        cls._initialized = True

    @classmethod
    def set_level(cls, level: int) -> None:
        """Set logging level.

        Args:
            level: Logging level (logging.DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        logger = cls.get_logger()
        logger.setLevel(level)
        for handler in logger.handlers:
            handler.setLevel(level)


# Convenience function
def get_logger(name: str = "datashard") -> logging.Logger:
    """Get datashard logger.

    Args:
        name: Logger name

    Returns:
        Configured logger instance
    """
    return DataShardLogger.get_logger(name)
