"""
Reusable download logic for paginated APIs on Databricks.

This module uses the Databricks-friendly logger from:
    dbx_utils.logging.getLogger

Assumptions:
  - Code always runs on Azure Databricks.
  - A global `dbutils` object exists in the __main__ module.
"""

from __future__ import annotations

import socket
import time
from typing import Mapping, Optional, Callable
from urllib.parse import urlparse

import requests

from dbx_utils.logging import getLogger

logger = getLogger(__name__)


# --------------------------------------------------------------------
# DNS retry helper
# --------------------------------------------------------------------
def wait_for_dns(hostname: str, attempts: int, base_delay: float) -> None:
    """
    Retry DNS lookups so temporary resolver hiccups do not fail the job.
    """
    delay = base_delay
    attempt = 1

    while attempt <= attempts:
        try:
            socket.gethostbyname(hostname)
            return
        except socket.gaierror as exc:
            if attempt >= attempts:
                raise requests.exceptions.ConnectionError(
                    f"DNS lookup failed for {hostname} after {attempts} attempts."
                ) from exc

            logger.warning(
                "DNS lookup failed for %s (attempt %s/%s): %s. Retrying in %.1f sec",
                hostname, attempt, attempts, exc, delay
            )
            time.sleep(delay)
            attempt += 1
            delay *= 2


# --------------------------------------------------------------------
# HTTP GET with exponential backoff
# --------------------------------------------------------------------
def request_with_backoff(
    session: requests.Session,
    url: str,
    params: Optional[Mapping[str, str]] = None,
    timeout: tuple[float, float] = (30.0, 300.0),
    max_attempts: int = 5,
    base_delay: float = 1.0,
    backoff_factor: float = 2.0,
) -> requests.Response:
    """
    GET request with retries, DNS backoff and logging.
    """
    attempt = 1
    delay = base_delay
    hostname = urlparse(url).hostname

    while True:
        try:
            if hostname:
                wait_for_dns(hostname, attempts=max_attempts, base_delay=base_delay)

            response = session.get(url=url, params=params, timeout=timeout)
            response.raise_for_status()
            return response

        except requests.exceptions.RequestException as exc:
            if attempt >= max_attempts:
                logger.error("Request failed after %s attempts: %s", attempt, exc)
                raise

            logger.warning(
                "Request failed (attempt %s/%s): %s. Retrying in %.1f sec",
                attempt, max_attempts, exc, delay
            )
            time.sleep(delay)
            attempt += 1
            delay *= backoff_factor


# --------------------------------------------------------------------
# Default saver → Databricks Volume (synchronous)
# --------------------------------------------------------------------
def _get_dbutils():
    """
    Resolve dbutils from the Databricks environment.

    Assumes code is running on Databricks where `dbutils` is defined
    in the __main__ module (notebook / job).
    """
    try:
        from __main__ import dbutils as _dbutils  # type: ignore
        return _dbutils
    except ImportError as exc:
        raise RuntimeError(
            "dbutils is not available in __main__. "
            "This code assumes it is running on Databricks."
        ) from exc


def default_save_data(
    response: requests.Response,
    download_folder: str,
    count: int,
) -> None:
    """
    Save JSON page as a file in a UC Volume.
    """
    dbutils = _get_dbutils()

    file_path = f"{download_folder}/{str(count).zfill(5)}.json"
    dbutils.fs.put(file_path, response.text, overwrite=True)
    logger.info("Saved page %s → %s", count, file_path)


# --------------------------------------------------------------------
# Main: Download ONE endpoint into a UC Volume
# --------------------------------------------------------------------
def download_endpoint_to_volume(
    *,
    session: requests.Session,
    endpoint: str,
    params: Optional[Mapping[str, str]],
    options: Optional[Mapping[str, str]],
    download_folder: str,
    save_fn: Optional[Callable[[requests.Response, str, int], None]] = None,
    timeout: tuple[float, float] = (30.0, 300.0),
    max_attempts: int = 5,
    base_delay: float = 1.0,
    backoff_factor: float = 2.0,
) -> int:
    """
    Download all paginated pages from a **single** API endpoint.

    Pagination behavior is controlled by:
        options["pagination_key"]

    Nothing loops over multiple endpoints → caller controls that.
    """
    logger.info("Starting download: %s", endpoint)

    if save_fn is None:
        save_fn = default_save_data

    # pagination key from options → fallback to @odata.nextLink
    pagination_key = None
    if options:
        pagination_key = options.get("pagination_key")
    if not pagination_key:
        pagination_key = "@odata.nextLink"

    logger.info("Using pagination key: %s", pagination_key)

    count = 1
    next_url = endpoint
    next_params = params
    total_pages = 0

    # helper for nested pagination keys like "links.next"
    def extract_nested(obj, path: str):
        keys = path.split(".")
        cur = obj
        for k in keys:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(k)
        return cur

    start_total = time.perf_counter()

    while True:
        start_req = time.perf_counter()

        r = request_with_backoff(
            session=session,
            url=next_url,
            params=next_params,
            timeout=timeout,
            max_attempts=max_attempts,
            base_delay=base_delay,
            backoff_factor=backoff_factor,
        )

        logger.info(
            "Page %s downloaded in %.2f sec",
            count,
            time.perf_counter() - start_req,
        )

        # synchronous save → any error is visible and stops the loop
        save_fn(r, download_folder, count)
        total_pages += 1

        # detect pagination link
        try:
            body = r.json()
        except ValueError:
            logger.info("No JSON body → stopping pagination.")
            break

        next_url = extract_nested(body, pagination_key)
        if not next_url:
            logger.info("Pagination finished after %s pages.", total_pages)
            break

        next_params = None
        count += 1

    total_time = time.perf_counter() - start_total
    logger.info("Completed download: %s pages in %.2f sec", total_pages, total_time)

    return total_pages
