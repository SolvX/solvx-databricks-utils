"""
Reusable download logic for APIs on Azure Databricks.

What’s new vs your original:
- Pagination is OPTIONAL (default: none → single request).
- Pagination is pluggable via strategy objects (paginators).
- Built-in paginator modes:
    - none
    - nextlink (OData-style: @odata.nextLink or any JSON path)
    - offset (Matomo-style: limit/offset)

Also included:
- Helpers for reading an endpoint config table:
    (id, endpoint, params, job_settings)
  where job_settings can control things like:
    - pagination_mode
    - destination table name (catalog.schema.table)
    - request method (GET/POST)
    - save mode (volume vs table)
"""

from __future__ import annotations

import socket
import time
from dataclasses import dataclass, replace
from typing import (
    Mapping,
    Optional,
    Callable,
    Literal,
    Any,
    Protocol,
    Dict,
    Iterable,
    Tuple,
)

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
                hostname,
                attempt,
                attempts,
                exc,
                delay,
            )
            time.sleep(delay)
            attempt += 1
            delay *= 2


# --------------------------------------------------------------------
# HTTP request with exponential backoff
# --------------------------------------------------------------------
def request_with_backoff(
    session: requests.Session,
    url: str,
    params: Optional[Mapping[str, str]] = None,
    *,
    method: Literal["GET", "POST"] = "GET",
    json_body: Optional[Any] = None,
    data: Optional[Any] = None,
    headers: Optional[Mapping[str, str]] = None,
    timeout: tuple[float, float] = (30.0, 300.0),
    max_attempts: int = 5,
    base_delay: float = 1.0,
    backoff_factor: float = 2.0,
) -> requests.Response:
    """
    HTTP request with retries + exponential backoff.

    Retries on transient failures:
      - timeouts
      - connection errors (includes DNS hiccups)
      - HTTP 429
      - HTTP 5xx

    Fails fast on:
      - HTTP 4xx (except 429)
      - SSL errors, invalid URL, too many redirects, etc.
    """
    attempt = 1
    delay = base_delay
    hostname = urlparse(url).hostname

    last_exc: Optional[BaseException] = None

    while True:
        try:
            response = session.request(
                method=method,
                url=url,
                params=params,
                json=json_body,
                data=data,
                headers=headers,
                timeout=timeout,
            )

            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError:
                status = response.status_code
                retryable = (status == 429) or (500 <= status <= 599)
                if not retryable:
                    logger.error(
                        "Non-retryable HTTP %s for %s (attempt %s/%s).",
                        status,
                        url,
                        attempt,
                        max_attempts,
                    )
                    raise
                raise

            return response

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_exc = exc
            if hostname:
                try:
                    wait_for_dns(hostname, attempts=max_attempts, base_delay=base_delay)
                except Exception as dns_exc:
                    logger.warning("DNS wait/check failed for %s: %s", hostname, dns_exc)

        except requests.exceptions.HTTPError as exc:
            last_exc = exc

        except requests.exceptions.RequestException as exc:
            logger.error("Non-retryable request error for %s: %s", url, exc)
            raise

        if attempt >= max_attempts:
            logger.error("Request failed after %s attempts: %s", attempt, last_exc)
            raise last_exc  # type: ignore[misc]

        logger.warning(
            "Request failed (attempt %s/%s): %s. Retrying in %.1f sec",
            attempt,
            max_attempts,
            last_exc,
            delay,
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


def default_save_data_to_volume(
    response: requests.Response,
    download_folder: str,
    count: int,
) -> None:
    """
    Save response body as a file in a UC Volume.
    """
    dbutils = _get_dbutils()

    file_path = f"{download_folder}/{str(count).zfill(5)}.json"
    dbutils.fs.put(file_path, response.text, overwrite=True)
    logger.info("Saved page %s → %s", count, file_path)


# --------------------------------------------------------------------
# Pagination strategies
# --------------------------------------------------------------------
@dataclass(frozen=True)
class RequestState:
    url: str
    params: Optional[Dict[str, str]]
    method: Literal["GET", "POST"]
    json_body: Optional[Any]
    data: Optional[Dict[str, Any]]
    headers: Optional[Dict[str, str]]
    page: int = 1


class Paginator(Protocol):
    def next(self, response: requests.Response, state: RequestState) -> Optional[RequestState]:
        """Return next RequestState or None to stop."""
        ...


def _extract_nested(obj: Any, path: str) -> Any:
    """Extract nested dict values using dot-separated keys."""
    keys = path.split(".")
    cur = obj
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


class NoPagination:
    """Always stop after the first request."""
    def next(self, response: requests.Response, state: RequestState) -> Optional[RequestState]:
        return None


class NextLinkPaginator:
    """
    nextLink-style pagination (OData etc.).
    Expects a JSON object containing a full URL at `pagination_key`.
    """
    def __init__(self, pagination_key: str = "@odata.nextLink"):
        self.pagination_key = pagination_key

    def next(self, response: requests.Response, state: RequestState) -> Optional[RequestState]:
        try:
            body = response.json()
        except ValueError:
            logger.info("No JSON body → stopping pagination.")
            return None

        if not isinstance(body, dict):
            logger.info(
                "Expected dict JSON for nextLink pagination, got %s → stopping.",
                type(body).__name__,
            )
            return None

        next_url = _extract_nested(body, self.pagination_key)
        if not next_url:
            return None

        # nextLink usually includes full URL with query string, so params become None
        return replace(state, url=str(next_url), params=None, page=state.page + 1)


class OffsetLimitPaginator:
    """
    Offset/limit pagination (Matomo, many list endpoints).

    Behavior:
    - Response JSON must be a list OR a dict containing a list at items_path.
    - Stop when returned items < limit or empty.
    - Advance offset += limit.

    Supports both GET (params) and POST form (data) pagination fields.
    """
    def __init__(
        self,
        *,
        limit_param: str = "filter_limit",
        offset_param: str = "filter_offset",
        items_path: str = "",  # "" means root is a list
        stop_on_short_page: bool = True,
    ):
        self.limit_param = limit_param
        self.offset_param = offset_param
        self.items_path = items_path
        self.stop_on_short_page = stop_on_short_page

    def _get_payload(self, state: RequestState) -> Dict[str, Any]:
        if state.method == "GET":
            return dict(state.params or {})
        return dict(state.data or {})

    def _set_payload(self, state: RequestState, payload: Dict[str, Any]) -> RequestState:
        # IMPORTANT: normalize everything to strings for query/form payloads
        normalized: Dict[str, Any] = {k: str(v) for k, v in payload.items()}
        if state.method == "GET":
            return replace(state, params=normalized, page=state.page + 1)
        return replace(state, data=normalized, page=state.page + 1)

    def next(self, response: requests.Response, state: RequestState) -> Optional[RequestState]:
        try:
            body = response.json()
        except ValueError:
            logger.info("No JSON body → stopping pagination.")
            return None

        # locate items
        if self.items_path:
            items = _extract_nested(body, self.items_path)
        else:
            items = body

        if not isinstance(items, list):
            logger.info("Expected a list of items, got %s → stopping.", type(items).__name__)
            return None

        payload = self._get_payload(state)

        # Default limit/offset if not present
        try:
            limit = int(str(payload.get(self.limit_param, "100")))
        except ValueError:
            limit = 100

        try:
            offset = int(str(payload.get(self.offset_param, "0")))
        except ValueError:
            offset = 0

        if len(items) == 0:
            return None

        if self.stop_on_short_page and len(items) < limit:
            return None

        payload[self.limit_param] = str(limit)
        payload[self.offset_param] = str(offset + limit)

        # same endpoint URL; only payload changes
        return self._set_payload(state, payload)


def build_paginator(options: Optional[Mapping[str, Any]]) -> Paginator:
    """
    Create a paginator from `options`.

    Supported:
      - pagination_mode: none | nextlink | offset
      - pagination_key: for nextlink (default @odata.nextLink)
      - limit_param, offset_param, items_path, stop_on_short_page: for offset
    """
    if not options:
        return NoPagination()

    mode = str(options.get("pagination_mode", "none")).lower().strip()

    if mode in ("none", "off", "false"):
        return NoPagination()

    if mode in ("nextlink", "next_link", "odata"):
        key = str(options.get("pagination_key", "@odata.nextLink"))
        return NextLinkPaginator(pagination_key=key)

    if mode in ("offset", "offset_limit", "matomo"):
        limit_param = str(options.get("limit_param", "filter_limit"))
        offset_param = str(options.get("offset_param", "filter_offset"))
        items_path = str(options.get("items_path", ""))
        stop_on_short_page = str(options.get("stop_on_short_page", "true")).lower() in ("1", "true", "yes")
        return OffsetLimitPaginator(
            limit_param=limit_param,
            offset_param=offset_param,
            items_path=items_path,
            stop_on_short_page=stop_on_short_page,
        )

    raise ValueError(f"Unknown pagination_mode: {mode}")


# --------------------------------------------------------------------
# Main: Download ONE endpoint into a UC Volume
# --------------------------------------------------------------------
def download_endpoint_to_volume(
    *,
    session: requests.Session,
    endpoint: str,
    params: Optional[Mapping[str, str]] = None,
    options: Optional[Mapping[str, Any]] = None,
    download_folder: str,
    save_fn: Optional[Callable[[requests.Response, str, int], None]] = None,
    timeout: tuple[float, float] = (30.0, 300.0),
    max_attempts: int = 5,
    base_delay: float = 1.0,
    backoff_factor: float = 2.0,
    method: Literal["GET", "POST"] = "GET",
    json_body: Optional[Any] = None,
    data: Optional[Mapping[str, Any]] = None,
    headers: Optional[Mapping[str, str]] = None,
) -> int:
    """
    Download all pages from a single API endpoint.

    Pagination is optional:
      - default mode is "none" (single request).
      - enable via options["pagination_mode"].

    Examples:
      - OData:
          options={"pagination_mode":"nextlink","pagination_key":"@odata.nextLink"}
      - Matomo (offset):
          options={"pagination_mode":"offset","limit_param":"filter_limit","offset_param":"filter_offset"}
    """
    logger.info("Starting download: %s", endpoint)

    if save_fn is None:
        save_fn = default_save_data_to_volume

    paginator = build_paginator(options)

    state = RequestState(
        url=endpoint,
        params=dict(params) if params else None,
        method=method,
        json_body=json_body,
        data=dict(data) if data else None,
        headers=dict(headers) if headers else None,
        page=1,
    )

    total_pages = 0
    start_total = time.perf_counter()

    while True:
        start_req = time.perf_counter()

        r = request_with_backoff(
            session=session,
            url=state.url,
            params=state.params if state.method == "GET" else None,
            method=state.method,
            json_body=state.json_body,
            data=state.data if state.method == "POST" else None,
            headers=state.headers,
            timeout=timeout,
            max_attempts=max_attempts,
            base_delay=base_delay,
            backoff_factor=backoff_factor,
        )

        logger.info("Page %s downloaded in %.2f sec", state.page, time.perf_counter() - start_req)

        save_fn(r, download_folder, state.page)
        total_pages += 1

        next_state = paginator.next(r, state)
        if next_state is None:
            logger.info("Pagination finished after %s page(s).", total_pages)
            break

        state = next_state

    total_time = time.perf_counter() - start_total
    logger.info("Completed download: %s page(s) in %.2f sec", total_pages, total_time)

    return total_pages


# --------------------------------------------------------------------
# Endpoint table integration (Unity Catalog Delta table)
# --------------------------------------------------------------------
def _resolve_spark(spark):
    if spark is not None:
        return spark
    try:
        from pyspark.sql import SparkSession  # type: ignore
        return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    except Exception as exc:
        raise RuntimeError("SparkSession is required but could not be resolved.") from exc


@dataclass(frozen=True)
class EndpointConfig:
    id: int
    endpoint: str
    params: Dict[str, str]
    job_settings: Dict[str, str]


def read_endpoint_configs(
    *,
    table_fqn: str,
    spark=None,
) -> Iterable[EndpointConfig]:
    """
    Read endpoint configurations from a UC Delta table.

    Table schema:
      id BIGINT GENERATED ALWAYS AS IDENTITY,
      endpoint STRING,
      params MAP<STRING, STRING>,
      job_settings MAP<STRING, STRING>

    job_settings is optional and may be NULL.
    """
    spark = _resolve_spark(spark)
    df = spark.table(table_fqn).select("id", "endpoint", "params", "job_settings").orderBy("id")

    for row in df.collect():
        params = dict(row["params"] or {})
        settings = dict(row["job_settings"] or {})
        yield EndpointConfig(
            id=int(row["id"]),
            endpoint=str(row["endpoint"]),
            params={str(k): str(v) for k, v in params.items()},
            job_settings={str(k): str(v) for k, v in settings.items()},
        )


def build_download_invocation_from_config(
    cfg: EndpointConfig,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Translate an EndpointConfig into:
      - download_kwargs for download_endpoint_to_volume()
      - extra metadata (e.g., destination table)

    Conventions in job_settings (examples):
      - request_method: "GET" or "POST" (default: GET)
      - pagination_mode: "none" | "nextlink" | "offset"
      - pagination_key: JSON path (for nextlink)
      - limit_param / offset_param / items_path / stop_on_short_page
      - destination_table: "catalog.schema.table"
      - destination_folder: "dbfs:/Volumes/..." (or caller provides download_folder)

    Notes:
      - We do NOT store secrets (tokens) in the table.
      - Caller should merge tokens into params/data at runtime.
    """
    settings = cfg.job_settings

    method = str(settings.get("request_method", "GET")).upper()
    if method not in ("GET", "POST"):
        raise ValueError(f"Unsupported request_method in job_settings: {method}")

    # options drive paginator creation
    options: Dict[str, Any] = {}
    for key in (
        "pagination_mode",
        "pagination_key",
        "limit_param",
        "offset_param",
        "items_path",
        "stop_on_short_page",
    ):
        if key in settings:
            options[key] = settings[key]

    download_kwargs: Dict[str, Any] = {
        "endpoint": cfg.endpoint,
        "method": method,
        # params are used for GET; for POST you likely want to pass these via `data` instead
        "params": cfg.params if method == "GET" else None,
        "data": cfg.params if method == "POST" else None,
        "options": options or None,
    }

    meta: Dict[str, Any] = {}
    if "destination_table" in settings:
        meta["destination_table"] = settings["destination_table"]
    if "destination_folder" in settings:
        meta["destination_folder"] = settings["destination_folder"]

    return download_kwargs, meta
