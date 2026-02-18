"""07_taxi_to_gcp_scheduled – Airflow DAGs: NYC taxi CSV → GCS + BigQuery (scheduled).

Three DAGs are defined in this file:

  07_green_taxi_to_gcp_scheduled  – monthly cron (1st of month, 09:00 UTC)
  07_yellow_taxi_to_gcp_scheduled – monthly cron (1st of month, 10:00 UTC)
  07_taxi_to_gcp_backfill         – manual trigger with date-range selection

Each run derives the target year-month from ``logical_date``, so the scheduler
(or a backfill) automatically targets the correct file.

The backfill DAG lets you pick a taxi type and a start/end month from the
Airflow UI.  It uses dynamic task mapping to process every month in the range,
one at a time.

Pipeline steps per month (identical to 06_taxi_to_gcp):
  1. extract              – download and decompress .csv.gz from GitHub
  2. upload_to_gcs        – upload the CSV to a GCS bucket
  3. create_main_table    – CREATE TABLE IF NOT EXISTS (partitioned)
  4. create_external_table – EXTERNAL TABLE pointing at the GCS CSV
  5. create_temp_table    – materialise external table + add MD5 unique_row_id
  6. merge_data           – MERGE INTO main table (insert-only dedup)
  7. cleanup              – remove the local temp CSV

CSV data source: https://github.com/DataTalksClub/nyc-tlc-data/releases
"""

import gzip
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path

import requests
from airflow.decorators import dag, task
from airflow.models.param import Param
from google.cloud import bigquery, storage

logger = logging.getLogger(__name__)

TMP_DIR = Path("/tmp/taxi_data")

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
GCP_DATASET = os.environ["GCP_DATASET"]
GCP_BUCKET_NAME = os.environ["GCP_BUCKET_NAME"]
GCP_LOCATION = os.environ["GCP_LOCATION"]


# ── Schema definitions per taxi type ─────────────────────────────────────────

SCHEMAS = {
    "yellow": {
        "csv_columns": [
            ("VendorID", "STRING"),
            ("tpep_pickup_datetime", "TIMESTAMP"),
            ("tpep_dropoff_datetime", "TIMESTAMP"),
            ("passenger_count", "INTEGER"),
            ("trip_distance", "NUMERIC"),
            ("RatecodeID", "STRING"),
            ("store_and_fwd_flag", "STRING"),
            ("PULocationID", "STRING"),
            ("DOLocationID", "STRING"),
            ("payment_type", "INTEGER"),
            ("fare_amount", "NUMERIC"),
            ("extra", "NUMERIC"),
            ("mta_tax", "NUMERIC"),
            ("tip_amount", "NUMERIC"),
            ("tolls_amount", "NUMERIC"),
            ("improvement_surcharge", "NUMERIC"),
            ("total_amount", "NUMERIC"),
            ("congestion_surcharge", "NUMERIC"),
        ],
        "dedup_keys": [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
        ],
        "partition_col": "tpep_pickup_datetime",
    },
    "green": {
        "csv_columns": [
            ("VendorID", "STRING"),
            ("lpep_pickup_datetime", "TIMESTAMP"),
            ("lpep_dropoff_datetime", "TIMESTAMP"),
            ("store_and_fwd_flag", "STRING"),
            ("RatecodeID", "STRING"),
            ("PULocationID", "STRING"),
            ("DOLocationID", "STRING"),
            ("passenger_count", "INT64"),
            ("trip_distance", "NUMERIC"),
            ("fare_amount", "NUMERIC"),
            ("extra", "NUMERIC"),
            ("mta_tax", "NUMERIC"),
            ("tip_amount", "NUMERIC"),
            ("tolls_amount", "NUMERIC"),
            ("ehail_fee", "NUMERIC"),
            ("improvement_surcharge", "NUMERIC"),
            ("total_amount", "NUMERIC"),
            ("payment_type", "INTEGER"),
            ("trip_type", "STRING"),
            ("congestion_surcharge", "NUMERIC"),
        ],
        "dedup_keys": [
            "VendorID",
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
        ],
        "partition_col": "lpep_pickup_datetime",
    },
}


def _col_defs(columns: list[tuple[str, str]]) -> str:
    """Build 'col1 TYPE,\\n    col2 TYPE, ...' for CREATE TABLE statements."""
    return ",\n    ".join(f"{name} {dtype}" for name, dtype in columns)


def _col_names(columns: list[tuple[str, str]]) -> list[str]:
    """Extract just the column names from (name, type) pairs."""
    return [name for name, _ in columns]


def _md5_expr(keys: list[str]) -> str:
    """Build the MD5(CONCAT(COALESCE(...), ...)) dedup expression."""
    parts = ',\n      '.join(f'COALESCE(CAST({k} AS STRING), "")' for k in keys)
    return f"MD5(CONCAT(\n      {parts}\n    ))"


def _year_month_from_date(d: datetime) -> str:
    return d.strftime("%Y-%m")


def _year_month_parts(year_month: str) -> tuple[str, str]:
    """Split '2019-01' into ('2019', '01')."""
    year, month = year_month.split("-")
    return year, month


# ── Shared pipeline helpers ──────────────────────────────────────────────────
# These functions contain the actual GCS/BQ logic, called by both the
# scheduled DAGs (as @task wrappers) and the backfill DAG (from process_month).


def _extract(taxi_type: str, year_month: str) -> str:
    filename = f"{taxi_type}_tripdata_{year_month}.csv"
    url = (
        f"https://github.com/DataTalksClub/nyc-tlc-data/releases/"
        f"download/{taxi_type}/{filename}.gz"
    )

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = TMP_DIR / filename

    logger.info("Downloading %s", url)
    response = requests.get(url, stream=True, timeout=600)
    response.raise_for_status()

    gz_path = csv_path.with_suffix(".csv.gz")
    with open(gz_path, "wb") as fout:
        shutil.copyfileobj(response.raw, fout)

    logger.info("Decompressing %s", gz_path)
    with gzip.open(gz_path, "rb") as fin, open(csv_path, "wb") as fout:
        shutil.copyfileobj(fin, fout)
    gz_path.unlink()

    size_mb = csv_path.stat().st_size / (1024 * 1024)
    logger.info("Extracted %s (%.1f MB)", csv_path, size_mb)
    return str(csv_path)


def _upload_to_gcs(csv_path: str, taxi_type: str, year_month: str) -> str:
    filename = f"{taxi_type}_tripdata_{year_month}.csv"
    gcs_uri = f"gs://{GCP_BUCKET_NAME}/{filename}"

    client = storage.Client(project=GCP_PROJECT_ID)
    bucket = client.bucket(GCP_BUCKET_NAME)
    blob = bucket.blob(filename)

    logger.info("Uploading %s → %s", csv_path, gcs_uri)
    blob.upload_from_filename(csv_path, timeout=600)
    logger.info("Upload complete: %s", gcs_uri)
    return gcs_uri


def _create_main_table(taxi_type: str) -> None:
    schema = SCHEMAS[taxi_type]
    table_id = f"{GCP_PROJECT_ID}.{GCP_DATASET}.{taxi_type}_tripdata"
    partition_col = schema["partition_col"]

    all_columns = [
        ("unique_row_id", "BYTES"),
        ("filename", "STRING"),
        *schema["csv_columns"],
    ]
    col_defs = _col_defs(all_columns)

    sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_id}` (
        {col_defs}
    )
    PARTITION BY DATE({partition_col});
    """

    client = bigquery.Client(project=GCP_PROJECT_ID, location=GCP_LOCATION)
    logger.info("Creating main table (if not exists): %s", table_id)
    client.query(sql).result()
    logger.info("Main table ready: %s", table_id)


def _create_external_table(
    gcs_uri: str, taxi_type: str, year_month: str
) -> None:
    year, month = _year_month_parts(year_month)
    schema = SCHEMAS[taxi_type]
    table_id = (
        f"{GCP_PROJECT_ID}.{GCP_DATASET}."
        f"{taxi_type}_tripdata_{year}_{month}_ext"
    )
    col_defs = _col_defs(schema["csv_columns"])

    sql = f"""
    CREATE OR REPLACE EXTERNAL TABLE `{table_id}` (
        {col_defs}
    )
    OPTIONS (
        format = 'CSV',
        uris = ['{gcs_uri}'],
        skip_leading_rows = 1,
        ignore_unknown_values = TRUE
    );
    """

    client = bigquery.Client(project=GCP_PROJECT_ID, location=GCP_LOCATION)
    logger.info("Creating external table: %s", table_id)
    client.query(sql).result()
    logger.info("External table ready: %s", table_id)


def _create_temp_table(taxi_type: str, year_month: str) -> None:
    year, month = _year_month_parts(year_month)
    schema = SCHEMAS[taxi_type]
    filename = f"{taxi_type}_tripdata_{year_month}.csv"
    ext_table = (
        f"{GCP_PROJECT_ID}.{GCP_DATASET}."
        f"{taxi_type}_tripdata_{year}_{month}_ext"
    )
    tmp_table = (
        f"{GCP_PROJECT_ID}.{GCP_DATASET}."
        f"{taxi_type}_tripdata_{year}_{month}"
    )
    md5_expr = _md5_expr(schema["dedup_keys"])

    sql = f"""
    CREATE OR REPLACE TABLE `{tmp_table}`
    AS
    SELECT
        {md5_expr} AS unique_row_id,
        "{filename}" AS filename,
        *
    FROM `{ext_table}`;
    """

    client = bigquery.Client(project=GCP_PROJECT_ID, location=GCP_LOCATION)
    logger.info("Creating temp table: %s", tmp_table)
    client.query(sql).result()
    logger.info("Temp table ready: %s", tmp_table)


def _merge_data(taxi_type: str, year_month: str) -> None:
    year, month = _year_month_parts(year_month)
    schema = SCHEMAS[taxi_type]
    main_table = f"{GCP_PROJECT_ID}.{GCP_DATASET}.{taxi_type}_tripdata"
    tmp_table = (
        f"{GCP_PROJECT_ID}.{GCP_DATASET}."
        f"{taxi_type}_tripdata_{year}_{month}"
    )

    all_col_names = ["unique_row_id", "filename"] + _col_names(
        schema["csv_columns"]
    )
    cols = ", ".join(all_col_names)
    s_cols = ", ".join(f"S.{c}" for c in all_col_names)

    sql = f"""
    MERGE INTO `{main_table}` T
    USING `{tmp_table}` S
    ON T.unique_row_id = S.unique_row_id
    WHEN NOT MATCHED THEN
        INSERT ({cols})
        VALUES ({s_cols});
    """

    client = bigquery.Client(project=GCP_PROJECT_ID, location=GCP_LOCATION)
    logger.info("Merging %s → %s", tmp_table, main_table)
    client.query(sql).result()
    logger.info("Merge complete")


def _cleanup(csv_path: str) -> None:
    p = Path(csv_path)
    if p.exists():
        p.unlink()
        logger.info("Removed temp file %s", p)


# ═════════════════════════════════════════════════════════════════════════════
# Part 1 – Scheduled DAGs (factory pattern)
# ═════════════════════════════════════════════════════════════════════════════


def create_scheduled_gcp_dag(taxi_type: str, cron: str) -> None:
    """Create a monthly-scheduled DAG for one taxi type.

    Parameters
    ----------
    taxi_type : "yellow" or "green"
    cron : cron expression (e.g. "0 9 1 * *")
    """

    @dag(
        dag_id=f"07_{taxi_type}_taxi_to_gcp_scheduled",
        description=(
            f"Monthly scheduled load of NYC {taxi_type} taxi trip CSV data "
            f"into GCS + BigQuery with deduplication. "
        ),
        schedule=cron,
        start_date=datetime(2019, 1, 1),
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=True,
        tags=["zoomcamp", "taxi", "gcp", "bigquery", "gcs", taxi_type, "scheduled"],
    )
    def scheduled_taxi_to_gcp():
        """NYC taxi CSV → GCS + BigQuery pipeline (scheduled)."""

        @task
        def extract(**context) -> str:
            year_month = _year_month_from_date(context["logical_date"])
            return _extract(taxi_type, year_month)

        @task
        def upload_to_gcs(csv_path: str, **context) -> str:
            year_month = _year_month_from_date(context["logical_date"])
            return _upload_to_gcs(csv_path, taxi_type, year_month)

        @task
        def create_main_table() -> None:
            _create_main_table(taxi_type)

        @task
        def create_external_table(gcs_uri: str, **context) -> None:
            year_month = _year_month_from_date(context["logical_date"])
            _create_external_table(gcs_uri, taxi_type, year_month)

        @task
        def create_temp_table(**context) -> None:
            year_month = _year_month_from_date(context["logical_date"])
            _create_temp_table(taxi_type, year_month)

        @task
        def merge_data(**context) -> None:
            year_month = _year_month_from_date(context["logical_date"])
            _merge_data(taxi_type, year_month)

        @task
        def cleanup(csv_path: str) -> None:
            _cleanup(csv_path)

        csv_file = extract()
        gcs_uri = upload_to_gcs(csv_file)
        main_ready = create_main_table()
        ext_ready = create_external_table(gcs_uri)
        tmp_ready = create_temp_table()
        merged = merge_data()
        cleaned = cleanup(csv_file)

        gcs_uri >> main_ready >> ext_ready >> tmp_ready >> merged >> cleaned

    scheduled_taxi_to_gcp()


create_scheduled_gcp_dag("green", "0 9 1 * *")
create_scheduled_gcp_dag("yellow", "0 10 1 * *")


# ═════════════════════════════════════════════════════════════════════════════
# Part 2 – Backfill DAG (manual trigger with date-range selection)
# ═════════════════════════════════════════════════════════════════════════════


def _generate_month_list(start_month: str, end_month: str) -> list[str]:
    """Build a list of 'YYYY-MM' strings from start_month to end_month inclusive."""
    start = datetime.strptime(start_month, "%Y-%m")
    end = datetime.strptime(end_month, "%Y-%m")

    months: list[str] = []
    current = start
    while current <= end:
        months.append(current.strftime("%Y-%m"))
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return months


@dag(
    dag_id="07_taxi_to_gcp_backfill",
    description=(
        "On-demand backfill for NYC taxi data into GCS + BigQuery. "
        "Pick a taxi type and a date range from the UI; each month "
        "in the range is processed as a dynamically mapped task."
    ),
    schedule=None,
    start_date=datetime(2019, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["zoomcamp", "taxi", "gcp", "bigquery", "gcs", "backfill"],
    params={
        "taxi": Param(
            default="green",
            enum=["yellow", "green"],
            description="Select taxi type",
        ),
        "start_month": Param(
            default="2019-01",
            type="string",
            description="First month to backfill (YYYY-MM)",
        ),
        "end_month": Param(
            default="2019-12",
            type="string",
            description="Last month to backfill (YYYY-MM)",
        ),
    },
)
def taxi_to_gcp_backfill():
    """Backfill NYC taxi data into GCS + BigQuery for a date range."""

    @task
    def generate_months(**context) -> list[str]:
        p = context["params"]
        return _generate_month_list(p["start_month"], p["end_month"])

    @task(max_active_tis_per_dagrun=1)
    def process_month(year_month: str, **context) -> None:
        """Run the full pipeline for a single month."""
        taxi = context["params"]["taxi"]

        csv_path = _extract(taxi, year_month)
        try:
            gcs_uri = _upload_to_gcs(csv_path, taxi, year_month)
            _create_main_table(taxi)
            _create_external_table(gcs_uri, taxi, year_month)
            _create_temp_table(taxi, year_month)
            _merge_data(taxi, year_month)
        finally:
            _cleanup(csv_path)

    months = generate_months()
    process_month.expand(year_month=months)


taxi_to_gcp_backfill()
