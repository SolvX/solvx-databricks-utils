"""
Databricks-friendly logger built on the standard :mod:`logging` package.
"""

import logging
import sys
from datetime import datetime
from typing import Optional, List, Dict, Any
from zoneinfo import ZoneInfo
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    StringType,
    IntegerType,
)


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
        compute_id: Optional[str] = None,
    ) -> None:
        """
        Parameters
        ----------
        spark : SparkSession
            Active SparkSession.
        table_name : str
            Fully qualified table name, e.g. "dev.tools.logging".
        timezone : str
            IANA timezone name.
        compute_id : Optional[str]
            Identifier for the job/compute/cluster/run.
        """
        super().__init__()
        self.spark = spark
        self.table_name = table_name
        self.timezone = ZoneInfo(timezone)
        self.compute_id = compute_id
        self._buffer: List[Dict[str, Any]] = []

        # Explicit schema so Spark doesn't have to infer types
        self._schema = StructType(
            [
                StructField("timestamp", TimestampType(), nullable=False),
                StructField("level", StringType(), nullable=False),
                StructField("logger", StringType(), nullable=False),
                StructField("message", StringType(), nullable=True),
                StructField("module", StringType(), nullable=True),
                StructField("func_name", StringType(), nullable=True),
                StructField("line_no", IntegerType(), nullable=True),
                StructField("compute_id", StringType(), nullable=True),
            ]
        )

    def emit(self, record: logging.LogRecord) -> None:
        """
        Called by logging for each LogRecord.
        We just convert it to a dict and store in memory.
        """
        try:
            ts = datetime.fromtimestamp(record.created, self.timezone)
            entry = {
                "timestamp": ts,
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                "module": record.module,
                "func_name": record.funcName,
                "line_no": record.lineno,
                "compute_id": self.compute_id,
            }
            self._buffer.append(entry)
        except Exception:
            # Don't let logging failures crash the app
            self.handleError(record)

    def _check_schema_compatible(self, existing_schema, new_schema) -> None:
        """
        Ensure the existing table has at least all required columns
        with the same types as the new DataFrame.

        Extra columns in the existing table are allowed.
        """
        existing_fields = {f.name: f for f in existing_schema}

        missing_or_mismatched: List[str] = []
        for f in new_schema:
            existing_field = existing_fields.get(f.name)
            if existing_field is None:
                missing_or_mismatched.append(
                    f"- missing required column '{f.name}' (type {f.dataType})"
                )
            elif existing_field.dataType != f.dataType:
                missing_or_mismatched.append(
                    f"- column '{f.name}' has type {existing_field.dataType}, "
                    f"expected {f.dataType}"
                )

        if missing_or_mismatched:
            details = "\n".join(missing_or_mismatched)
            raise ValueError(
                f"Cannot use table '{self.table_name}' for logging because its schema "
                f"is not compatible with the expected logging schema.\n\n"
                f"The table will NOT be changed or overwritten to protect existing data.\n\n"
                f"This most likely means you already have a table with different "
                f"column types or missing columns.\n\n"
                f"To fix this, either:\n"
                f"  • DROP TABLE {self.table_name}\n"
                f"  • or configure a different table name in enable_table_logging(...).\n\n"
                f"Issues found:\n{details}\n\n"
                f"Existing schema: {existing_schema}\n"
                f"Required schema: {new_schema}"
            )


    def flush_to_table(self) -> None:
        """
        Write all buffered log entries to the Databricks table.

        - If table does not exist: create it (no overwrite).
        - If table exists: ensure it has all required columns with correct types.
          Extra columns are allowed.
        """
        if not self._buffer:
            return  # nothing to do

        # Use explicit schema to avoid CANNOT_DETERMINE_TYPE inference errors
        df = self.spark.createDataFrame(self._buffer, schema=self._schema)

        if not self.spark.catalog.tableExists(self.table_name):
            # Table does not exist: create it (no overwrite).
            df.write.saveAsTable(self.table_name)
        else:
            # Table exists: check schema compatibility before appending.
            existing_schema = self.spark.table(self.table_name).schema
            self._check_schema_compatible(existing_schema, df.schema)

            df.write.mode("append").saveAsTable(self.table_name)

        # Clear buffer after successful write
        self._buffer.clear()


def enable_table_logging(
    logger: logging.Logger,
    spark: SparkSession,
    table_name: str,
    timezone: str = DEFAULT_TIMEZONE,
    compute_id: Optional[str] = None,
) -> DatabricksTableHandler:
    """
    Attach a DatabricksTableHandler to an existing logger.

    Usage:
        logger = getLogger("my_app")
        handler = enable_table_logging(
            logger,
            spark,
            "dev.tools.logging",
            compute_id=spark.conf.get("spark.databricks.job.runId", None),
        )
    """
    handler = DatabricksTableHandler(
        spark=spark,
        table_name=table_name,
        timezone=timezone,
        compute_id=compute_id,
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
