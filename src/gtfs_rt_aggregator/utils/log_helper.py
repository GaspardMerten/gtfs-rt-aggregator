"""
Logging utilities for the GTFS-RT pipeline.
"""

import logging
from typing import Optional


def setup_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """
    Set up and return a logger with the given name.

    Args:
        name: The name of the logger
        level: Optional logging level to set for this logger

    Returns:
        A configured logger instance
    """
    logger = logging.getLogger(name)
    if level is not None:
        logger.setLevel(level)
    return logger
