"""
Move JSON files from a UC volume into a Delta table.

Usage example (in a Databricks notebook/job):

from step_4_blob_to_delta import volume_json_to_delta

volume_json_to_delta(
    volume_path="/Volumes/dev/tools/testing/test/",
    # target_table="dev.tools.test",  # optional, will be derived if omitted
)
"""

from __future__ import annotations

from typing import Optional

from dbx_utils.logging import getLogger

logger = getLogger(__name__)


def _get_spark():
    """
    Resolve the active SparkSession from the Databricks environment.

    Assumes a global `spark` variable exists in __main__.
    """
    try:
        from __main__ import spark as _spark  # type: ignore
        return _spark
    except ImportError as exc:
        raise RuntimeError(
            "Spark session 'spark' not found in __main__. "
            "This code assumes it is running on Databricks."
        ) from exc


def _derive_table_name_from_volume(volume_path: str) -> tuple[str, str, str]:
    """
    Derive (catalog, schema, table) from a UC Volume path.

    Expected pattern:
        /Volumes/<catalog>/<schema>/<volume>/<.../<table-folder>>

    For example:
        /Volumes/dev/tools/testing/test/
        → catalog=dev, schema=tools, table=test
    """
    # Normalize path
    path = volume_path.rstrip("/")
    if path.startswith("dbfs:"):
        path = path.replace("dbfs:", "", 1)

    if not path.startswith("/Volumes/"):
        raise ValueError(
            f"volume_path must start with '/Volumes/', got: {volume_path!r}"
        )

    parts = path.split("/")
    # ['', 'Volumes', catalog, schema, volume, *rest]
    if len(parts) < 5:
        raise ValueError(
            "volume_path must look like "
            "'/Volumes/<catalog>/<schema>/<volume>/...'. "
            f"Got: {volume_path!r}"
        )

    catalog = parts[2]
    schema = parts[3]

    # Last non-empty segment as table name
    last_non_empty = ""
    for p in parts[::-1]:
        if p:
            last_non_empty = p
            break

    if not last_non_empty:
        raise ValueError(f"Could not derive table name from path: {volume_path!r}")

    table = last_non_empty
    return catalog, schema, table


def _resolve_full_table_name(
    volume_path: str,
    target_table: Optional[str],
) -> str:
    """
    Resolve the fully qualified table name.

    - If `target_table` is provided:
        - If it contains dots (.), assume user gave full name and return as-is.
        - If not, attach to catalog/schema derived from volume_path.
    - If `target_table` is None:
        - Derive catalog/schema/table from volume_path.
    """
    catalog, schema, derived_table = _derive_table_name_from_volume(volume_path)

    if target_table:
        if "." in target_table:
            # Assume fully qualified already (e.g. dev.tools.mytable)
            full_name = target_table
        else:
            # User only gave table name → attach catalog/schema from volume
            full_name = f"{catalog}.{schema}.{target_table}"
    else:
        # Use derived table name
        full_name = f"{catalog}.{schema}.{derived_table}"

    return full_name


def import_json(
    *,
    volume_path: str,
    target_table: Optional[str] = None,
    multiline_json: bool = True,
) -> None:
    """
    Read all JSON files under a UC Volume folder and write them into a Delta table.

    - If a top-level 'results' field exists and is an array, only its elements
      are written to the table.
    - Every column is cast to STRING for safety.
    - The table name is either:
        - `target_table` (if provided), or
        - `<catalog>.<schema>.<last_segment_of_path>` derived from `volume_path`.
    """
    spark = _get_spark()
    from pyspark.sql import functions as F  # imported here to keep module lightweight

    full_table_name = _resolve_full_table_name(volume_path, target_table)
    logger.info(
        "Starting JSON → Delta load from %s into table %s",
        volume_path,
        full_table_name,
    )

    read_options = {}
    if multiline_json:
        read_options["multiLine"] = "true"

    # Read all JSON files in the folder (and subfolders) in a scalable way.
    df = spark.read.options(**read_options).json(volume_path)

    if not df.columns:
        logger.warning(
            "No columns were inferred from JSON files at %s. "
            "Is the folder empty or the JSON invalid?",
            volume_path,
        )

    # If there is a top-level 'results' field, assume the useful data lives there
    # and that it's a flat JSON object (or array of objects).
    if "results" in df.columns:
        logger.info(
            "Detected 'results' column; extracting only its entries "
            "and ignoring other top-level fields (next/previous/etc.)."
        )
        # Treat results as an array of objects and explode to one row per object.
        df = df.select(F.explode("results").alias("row"))
        # Flatten the struct to top-level columns.
        df = df.select("row.*")

    # Cast EVERY column to STRING as requested.
    df_str = df.select(
        [F.col(c).cast("string").alias(c) for c in df.columns]
    )

    logger.info(
        "Read %s rows with %s columns from %s",
        df_str.count(),
        len(df_str.columns),
        volume_path,
    )

    # Decide whether to create or append to the Delta table
    table_exists = spark.catalog.tableExists(full_table_name)
    write_mode = "append" if table_exists else "overwrite"

    logger.info(
        "Writing DataFrame to Delta table %s (mode=%s)",
        full_table_name,
        write_mode,
    )

    (
        df_str.write
        .format("delta")
        .mode(write_mode)
        .saveAsTable(full_table_name)
    )

    logger.info(
        "Completed JSON → Delta load. Table %s is ready.",
        full_table_name,
    )
