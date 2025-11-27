from pyspark.sql import SparkSession
from pyspark.errors import AnalysisException
from dbx_utils.logging import getLogger


# -------------------------------------------------------------
# Helper: resolve the SparkSession just like Databricks does
# -------------------------------------------------------------
def _resolve_spark(spark: SparkSession | None) -> SparkSession:
    if spark is not None:
        return spark

    active = SparkSession.getActiveSession()
    if active is None:
        raise RuntimeError(
            "No active SparkSession found. "
            "On Databricks this should always exist. "
            "Outside Databricks, pass `spark=` explicitly."
        )
    return active


# -------------------------------------------------------------
# 1. CREATE ENDPOINT CONFIG TABLE
# -------------------------------------------------------------
def create_endpoint_table(
    catalog: str,
    schema: str,
    table: str,
    spark: SparkSession | None = None,
    managed_location: str | None = None,
) -> str:
    """
    Create (if not exists) the API configuration table that stores:
      - endpoint STRING
      - params MAP<STRING, STRING>
      - job_settings MAP<STRING, STRING>

    The setup only needs to run once.
    """
    logger = getLogger(__name__)
    spark = _resolve_spark(spark)
    full_table_name = f"{catalog}.{schema}.{table}"

    try:
        logger.info(f"Preparing endpoint table '{full_table_name}'.")

        # ------------------
        # Create catalog
        # ------------------
        if managed_location:
            logger.info(
                f"Ensuring catalog '{catalog}' exists with managed location "
                f"'{managed_location}'."
            )
            spark.sql(
                f"CREATE CATALOG IF NOT EXISTS {catalog} "
                f"MANAGED LOCATION '{managed_location}'"
            )
        else:
            try:
                logger.info(f"Ensuring catalog '{catalog}' exists.")
                spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
            except AnalysisException as ae:
                message = (
                    f"Cannot create catalog '{catalog}' without a managed location.\n"
                    f"Your Unity Catalog metastore has no default storage root.\n\n"
                    f"Fix: Provide managed_location=..., for example:\n\n"
                    f"create_endpoint_table(\n"
                    f"  catalog='{catalog}',\n"
                    f"  schema='{schema}',\n"
                    f"  table='{table}',\n"
                    f"  managed_location='abfss://container@storage.dfs.core.windows.net/'\n"
                    f")\n\n"
                    f"Original error: {ae}"
                )
                logger.error(message)
                raise RuntimeError(message) from ae

        # ------------------
        # Create schema
        # ------------------
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

        # ------------------
        # Table exists? -> do nothing
        # ------------------
        if spark.catalog.tableExists(full_table_name):
            logger.info(
                f"Endpoint table '{full_table_name}' already exists. "
                f"No changes made."
            )
            return full_table_name

        # ------------------
        # Create table
        # ------------------
        logger.info(f"Creating new endpoint table '{full_table_name}'.")
        spark.sql(f"""
        CREATE TABLE {full_table_name} (
          id BIGINT GENERATED ALWAYS AS IDENTITY,
          endpoint STRING,
          params MAP<STRING, STRING>,
          job_settings MAP<STRING, STRING>
        )
        USING DELTA
        """)

        logger.info(f"Successfully created endpoint table '{full_table_name}'.")
        return full_table_name

    except Exception as e:
        logger.error(
            f"Failed to create endpoint table '{full_table_name}': {e}",
            exc_info=True,
        )
        raise


# -------------------------------------------------------------
# 2. CREATE VOLUME
# -------------------------------------------------------------
def create_volume(
    catalog: str,
    schema: str,
    volume: str,
    location: str,
    spark: SparkSession | None = None,
) -> str:
    """
    Create a Unity Catalog Volume.

    Example volume name: dev.tools.myvolume
    Example location: 'abfss://delta@storage.dfs.core.windows.net/myvolume/'
    """
    logger = getLogger(__name__)
    spark = _resolve_spark(spark)

    full_volume_name = f"{catalog}.{schema}.{volume}"

    try:
        logger.info(f"Preparing volume '{full_volume_name}'.")

        # Ensure catalog & schema exist (volume cannot create them)
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

        # Check if volume exists
        volume_exists = False
        try:
            spark.sql(f"DESCRIBE VOLUME {full_volume_name}")
            volume_exists = True
        except Exception:
            volume_exists = False

        if volume_exists:
            logger.info(
                f"Volume '{full_volume_name}' already exists. No changes made."
            )
            return full_volume_name

        # Create the volume
        logger.info(
            f"Creating volume '{full_volume_name}' "
            f"at location '{location}'."
        )

        spark.sql(f"""
        CREATE VOLUME {full_volume_name}
        LOCATION '{location}'
        """)

        logger.info(f"Successfully created volume '{full_volume_name}'.")
        return full_volume_name

    except Exception as e:
        logger.error(
            f"Failed to create volume '{full_volume_name}': {e}",
            exc_info=True,
        )
        raise
