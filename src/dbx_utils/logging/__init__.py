"""
Databricks-friendly logger built on the standard :mod:`logging` package.
"""

import logging
import sys
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo
from typing import List, Dict, Any
from pyspark.sql import SparkSession

DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_TIMEZONE = "Europe/Amsterdam"


class TimezoneFormatter(logging.Formatter):
    """Formatter that applies a timezone to log timestamps."""

    def __init__(self, fmt=DEFAULT_LOG_FORMAT, timezone=DEFAULT_TIMEZONE):
        super().__init__(fmt)
        self.timezone = ZoneInfo(timezone)

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, self.timezone)
        return dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S")


def getLogger(
    name: Optional[str] = None,
    level: int | str = logging.INFO,
    timezone: str = DEFAULT_TIMEZONE,
) -> logging.Logger:
    """
    Return a timezone-aware logger with normal logging behavior.
    """

    logger_name = name or __name__
    logger = logging.getLogger(logger_name)

    # Convert "INFO" → 20, "WARNING" → 30, etc.
    level_value = logging._checkLevel(level)
    logger.setLevel(level_value)

    # Add a handler only once
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level_value)
        handler.setFormatter(TimezoneFormatter(timezone=timezone))
        logger.addHandler(handler)

    return logger


class DatabricksTableHandler(logging.Handler):
    """
    Logging handler that buffers records and can flush them
    to a Databricks table (Delta) using Spark.
    """

    def __init__(
        self,
        spark: SparkSession,
        table_name: str,
        timezone: str = DEFAULT_TIMEZONE,
    ) -> None:
        super().__init__()
        self.spark = spark
        self.table_name = table_name
        self.timezone = ZoneInfo(timezone)
        self._buffer: List[Dict[str, Any]] = []

    def emit(self, record: logging.LogRecord) -> None:
        """
        Called by logging for each LogRecord.
        We just convert it to a dict and store in memory.
        """
        try:
            ts = datetime.fromtimestamp(record.created, self.timezone)
            entry = {
                "timestamp": ts,                  # timestamp
                "level": record.levelname,        # "INFO", "WARNING", ...
                "logger": record.name,            # logger name
                "message": record.getMessage(),   # plain message
                "module": record.module,
                "func_name": record.funcName,
                "line_no": record.lineno,
            }
            self._buffer.append(entry)
        except Exception:
            # Don't let logging failures crash the app
            self.handleError(record)

    def flush_to_table(self) -> None:
        """
        Write all buffered log entries to the Databricks table.
        - If table does not exist: create it.
        - If table exists: check schema and append.
        """
        if not self._buffer:
            return  # nothing to do

        df = self.spark.createDataFrame(self._buffer)

        # Create table if needed
        if not self.spark.catalog.tableExists(self.table_name):
            (
                df.write
                .mode("overwrite")
                .saveAsTable(self.table_name)
            )
        else:
            # Check schema compatibility (simple check)
            existing_schema = self.spark.table(self.table_name).schema
            if existing_schema != df.schema:
                raise ValueError(
                    f"Existing table {self.table_name!r} has different schema.\n"
                    f"Existing: {existing_schema}\n"
                    f"New: {df.schema}"
                )

            (
                df.write
                .mode("append")
                .saveAsTable(self.table_name)
            )

        # Clear buffer after successful write
        self._buffer.clear()


def enable_table_logging(
    logger: logging.Logger,
    spark: SparkSession,
    table_name: str,
    timezone: str = DEFAULT_TIMEZONE,
) -> DatabricksTableHandler:
    """
    Attach a DatabricksTableHandler to an existing logger.

    Usage:
        logger = getLogger("my_app")
        handler = enable_table_logging(logger, spark, "dev.tools.logging")
    """
    handler = DatabricksTableHandler(
        spark=spark,
        table_name=table_name,
        timezone=timezone,
    )
    logger.addHandler(handler)
    return handler



__all__ = [
    "getLogger",
    "DEFAULT_LOG_FORMAT",
    "DEFAULT_TIMEZONE",
    "DatabricksTableHandler",
    "enable_table_logging",
]

