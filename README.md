# solvx-databricks-utils

> A Databricks-native, table-driven API ingestion framework that lets teams add and manage API sources by inserting rows — not writing code.

`solvx-databricks-utils` is a **Databricks-first Python package** that makes it easy to build **reliable, transparent, and maintainable API ingestion pipelines** using **Unity Catalog**, **UC Volumes**, and **Delta tables**.

It is designed to take the pain out of API ingestion on Databricks by:

* Making API downloads **simple to set up** and **easy to reason about**
* Storing raw API data in a **clear, auditable folder structure**
* Driving ingestion logic through **configuration tables instead of code**
* Handling long-running downloads with **retries, backoff, and pagination** out of the box

This package is ideal for teams that:

* Run ingestion jobs on Databricks
* Want repeatable and transparent API downloads
* Prefer configuration (tables) over hardcoded logic
* Need ingestion pipelines that can safely run for hours or days
* Want a complete, production-ready package including code, notebooks, and a reusable pipeline that can be deployed in one go with minimal manual setup

This package is **not a generic Python ingestion library** — it assumes:

* You are running on Databricks
* Unity Catalog is enabled

---

## What does this package do?

The package helps you:

1. **Create and manage Unity Catalog objects**

   * Catalogs
   * Schemas
   * Endpoint configuration tables
   * UC Volumes

2. **Download API data into UC Volumes**

   * One folder per endpoint per run
   * Automatic retries, backoff, DNS handling
   * Pagination support (including nested keys)

3. **Ingest raw JSON into Delta tables**

   * Spark-native ingestion
   * Schema evolution handled explicitly
   * Clear separation between download and ingestion

---

At a high level, this package implements a **table-driven ingestion pattern**:

```
Endpoint configuration table
        ↓
Databricks Job (loops over table rows)
        ↓
UC Volume (raw JSON per endpoint per run)
        ↓
Delta tables (processed data)
```

* An **endpoint table** defines which APIs to call, with their parameters and settings
* The pipeline **loops over each row** in this table for every run
* Each endpoint is **downloaded into a dedicated UC Volume folder** (one folder per endpoint per run)
* The downloaded files are then **ingested into Delta tables**, keeping raw data and processed data clearly separated

This makes API ingestion easy to control, transparent to operate, and simple to extend by adding new rows instead of new code.

---

## Setup vs runtime workflow

The ingestion flow is split into **two parts**:

### 1️⃣ One-time setup (or infrequent changes)

You do this once per environment (dev / prod), or when adding new endpoints:

* Create a Unity Catalog catalog & schema (optional)
* Create a UC Volume to store raw API files
* Create an endpoint configuration table
* Insert endpoint definitions (API URLs + params)

### 2️⃣ A reusable ingestion pipeline

A Databricks Job pipeline that you can:

* Run daily
* Run hourly
* Trigger manually
* Schedule however you want

The pipeline:

* Reads endpoint definitions from a table
* Downloads API data into the volume
* Ingests raw files into Delta tables

---

## Installation

This project is primarily intended to be **deployed as a Databricks job pipeline**, not imported as a traditional Python dependency.

### Requirements

* Databricks workspace
* Unity Catalog enabled
* Python 3.9+

### Using the package

Most users do **not** need to install this package manually.

When you deploy the ingestion pipeline (for example using a Databricks Asset Bundle), the package is automatically included as a **Python wheel** and attached to the job. In this setup, all code, notebooks, and pipeline definitions are deployed together and run as a single unit.

### Optional: install as a library

If you want to use parts of the package (for example the helpers) in your own notebooks or jobs, you can install the wheel manually.

Typical workflow:

1. Build the wheel (output is placed in the `dist/` directory)
2. Upload the wheel file to a Databricks-accessible location (for example a UC Volume or DBFS)
3. Install the wheel using a Databricks `%pip install` command or attach it as a library to a cluster or job

This is optional and not required when running the provided ingestion pipeline.

---

## One-time setup

This step prepares Unity Catalog and configuration objects.

### 1. Create the UC Volume

The volume is where raw API responses are stored.

All downloads follow this structure:

```
/Volumes/<catalog>/<schema>/<volume>/<source>/<run_id>/
```

Use the helper provided by the package:

```python
from dbx_utils.ingest import create_volume

create_volume(
    catalog="dev",
    schema="tools",
    volume="api_raw"
)
```

---

### 2. Create the endpoint configuration table

This table is the **control plane** for the entire ingestion pipeline. Each row represents one API endpoint to ingest, along with its parameters and endpoint-specific settings.

In practice, this means:

* **Adding a new endpoint** is usually just inserting a new row (endpoint + params + job_settings)
* **Updating behavior** (pagination, timeouts, etc.) is done by updating table values
* **No code changes are required** to add or modify endpoints

On every run, the pipeline reads this table and automatically picks up all configured endpoints. This makes the pipeline easy to operate, easy to audit, and simple to extend over time.

Each API endpoint is defined as **data**, not code.

The endpoint table schema:

| column       | type                | description                |
| ------------ | ------------------- | -------------------------- |
| id           | BIGINT (identity)   | Internal ID                |
| endpoint     | STRING              | Base API URL               |
| params       | MAP<STRING, STRING> | Query parameters           |
| job_settings | MAP<STRING, STRING> | Endpoint-specific settings |

Create it using:

```python
from dbx_utils.ingest import create_endpoint_table

create_endpoint_table(
    table_name="dev.tools.endpoints"
)
```

You can then insert endpoint rows manually or via a notebook.

Example row:

```json
{
  "endpoint": "https://api.example.com/v1/data",
  "params": {
    "limit": "100"
  },
  "job_settings": {
    "pagination_key": "links.next",
    "timeout": "30,300"
  }
}
```

---

## The ingestion pipeline

The pipeline is intentionally split into **four steps**, each with a single responsibility.

| Step | Module                 | Responsibility                            |
| ---- | ---------------------- | ----------------------------------------- |
| 1    | `step_1_prepare_blob`  | UC setup (catalog, schema, volume, table) |
| 2    | `step_2_get_endpoints` | Select endpoints for this run             |
| 3    | `step_3_download_api`  | Download API data into the volume         |
| 4    | `step_4_blob_to_delta` | Transform and write to Delta              |

This separation makes the pipeline:

* Easier to debug
* Easier to extend
* Safer to rerun

---

## Running the pipeline

The ingestion pipeline runs as a standard **Databricks Job**. How you deploy and run that job is flexible and depends on your team’s workflow.

You can:

* Create and run the job directly from the **Databricks Jobs UI**
* Deploy and run the job using the **Databricks CLI**
* Use a **Databricks Asset Bundle** to version and deploy the job as code (recommended, but optional)

### Using `databricks.yml` (recommended approach)

One common and recommended way to manage the pipeline is via a `databricks.yml` file. This file defines:

* The job
* The task order
* The notebooks or Python modules
* Parameters like catalog, schema, and volume

If you are using Asset Bundles, the job can be deployed and run with:

```bash
databricks bundle deploy
databricks bundle run api_ingestion_pipeline
```

If not, the same job structure can be created manually in the Databricks UI or via the CLI.

---

## Scheduling

Once deployed, the pipeline is scheduled and managed as a **Databricks Job**.

You can schedule the job to run:

* Daily, hourly, or at any custom interval using the **Databricks Job schedule**
* Using cron syntax (supported by Databricks Jobs)
* Manually or via the Databricks Jobs API

Each pipeline run is **idempotent by design**: every execution writes its downloads to a unique `run_id` folder in the UC Volume. Any changes to the endpoint table (such as adding new rows or updating parameters) are automatically picked up on the **next scheduled run**, with no code changes required. This makes reruns safe and auditable, and allows historical runs to be inspected or reprocessed if needed.

No code changes are required to change the schedule or rerun the pipeline.

---

## Design principles

This package intentionally follows these rules:

* **Configuration over code**
* **Databricks-native primitives**
* **Clear step boundaries**
* **Fail fast on bad configuration**
* **Retry only on transient failures**

If you work in Databricks + Unity Catalog, this package is meant to feel *boring and predictable* — in a good way.
