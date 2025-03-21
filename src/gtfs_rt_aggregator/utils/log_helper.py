"""
Logging utilities for the GTFS-RT pipeline.
"""

import logging
import sys
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


def configure_root_logger(
    level: int = logging.INFO, log_file: Optional[str] = None, console: bool = True
) -> None:
    """
    Configure the root logger with handlers and formatting.

    Args:
        level: The logging level to set
        log_file: Optional path to a log file
        console: Whether to log to console
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Clear any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Add console handler if requested
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    # Add file handler if log_file is provided
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
