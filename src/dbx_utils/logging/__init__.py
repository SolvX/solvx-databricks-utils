"""
Databricks-friendly logger built on the standard :mod:`logging` package.
"""

from __future__ import annotations

import logging
import sys
from typing import Optional

DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def getLogger(name: Optional[str] = None, level: int = logging.INFO) -> logging.Logger:
    """Return a pre-configured logger."""
    logger_name = name or __name__
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    if not any(isinstance(handler, logging.StreamHandler) for handler in logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))
        logger.addHandler(handler)

    return logger


__all__ = ["getLogger", "DEFAULT_LOG_FORMAT"]