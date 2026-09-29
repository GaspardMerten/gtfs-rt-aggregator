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


def configure_root_logger(
    level: int = logging.INFO, log_file: Optional[str] = None, console: bool = True
) -> None:
    """Deprecated since 0.7.4: configure logging with the logging module."""
    import warnings

    warnings.warn(
        "configure_root_logger is deprecated: use logging.basicConfig",
        DeprecationWarning,
        stacklevel=2,
    )
    handlers = [logging.StreamHandler()] if console else []
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=handlers or None,
        force=True,
    )
    from ..utils.redact import install_redaction

    install_redaction(logging.getLogger())
