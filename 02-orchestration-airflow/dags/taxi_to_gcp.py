"""06_taxi_to_gcp – Airflow DAG: NYC taxi CSV → GCS + BigQuery.

Downloads NYC taxi trip CSV data, uploads to Google Cloud Storage,
then loads into BigQuery via external tables with deduplication merge.

Pipeline steps (mirroring the Kestra flow):
  1. extract        – download and decompress .csv.gz from GitHub
  2. upload_to_gcs  – upload the CSV to a GCS bucket
  3. create_main_table     – CREATE TABLE IF NOT EXISTS (partitioned)
  4. create_external_table – EXTERNAL TABLE pointing at the GCS CSV
  5. create_temp_table     – materialise external table + add MD5 unique_row_id
  6. merge_data            – MERGE INTO main table (insert-only dedup)
  7. cleanup               – remove the local temp CSV

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

# GCP configuration – injected as env vars from docker-compose.yaml.
GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
GCP_DATASET = os.environ["GCP_DATASET"]
GCP_BUCKET_NAME = os.environ["GCP_BUCKET_NAME"]
GCP_LOCATION = os.environ["GCP_LOCATION"]


# ── Schema definitions per taxi type ─────────────────────────────────────────
# We keep one code path and pick the right schema via the "taxi" parameter.  Each entry has:
#   csv_columns  – (name, BQ_type) tuples as they appear in the source CSV
#   dedup_keys   – columns hashed into MD5 unique_row_id
#   partition_col – TIMESTAMP column used for BigQuery table partitioning

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


# ── DAG definition ───────────────────────────────────────────────────────────


@dag(
    dag_id="06_taxi_to_gcp",
    description=(
        "Downloads NYC taxi trip CSV data, uploads to GCS, "
        "and loads into BigQuery with deduplication. "
    ),
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["zoomcamp", "taxi", "gcp", "bigquery", "gcs"],
    params={
        "taxi": Param(
            default="green",
            enum=["yellow", "green"],
            description="Select taxi type",
        ),
        "year": Param(
            default="2019",
            enum=["2019", "2020"],
            description="Select year",
        ),
        "month": Param(
            default="01",
            enum=[
                "01", "02", "03", "04", "05", "06",
                "07", "08", "09", "10", "11", "12",
            ],
            description="Select month",
        ),
    },
)
def taxi_to_gcp():
    """NYC taxi CSV → GCS + BigQuery pipeline with external-table merge."""

    # ── 1. Extract: download and decompress the CSV ──────────────────────
    # Same as the DuckDB DAG – fetch the .csv.gz from GitHub and gunzip it.
    @task
    def extract(**context) -> str:
        p = context["params"]
        taxi, year, month = p["taxi"], p["year"], p["month"]
        filename = f"{taxi}_tripdata_{year}-{month}.csv"
        url = (
            f"https://github.com/DataTalksClub/nyc-tlc-data/releases/"
            f"download/{taxi}/{filename}.gz"
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

    # ── 2. Upload to GCS ─────────────────────────────────────────────────
    # Uses the google-cloud-storage Python client authenticated via
    # GOOGLE_APPLICATION_CREDENTIALS env var (points to the mounted key file).
    @task
    def upload_to_gcs(csv_path: str, **context) -> str:
        p = context["params"]
        taxi, year, month = p["taxi"], p["year"], p["month"]
        filename = f"{taxi}_tripdata_{year}-{month}.csv"
        gcs_uri = f"gs://{GCP_BUCKET_NAME}/{filename}"

        client = storage.Client(project=GCP_PROJECT_ID)
        bucket = client.bucket(GCP_BUCKET_NAME)
        blob = bucket.blob(filename)

        logger.info("Uploading %s → %s", csv_path, gcs_uri)
        blob.upload_from_filename(csv_path, timeout=600)
        logger.info("Upload complete: %s", gcs_uri)
        return gcs_uri

    # ── 3. Create main table ─────────────────────────────────────────────
    # Creates the main partitioned table IF NOT EXISTS.  Partitioning by
    # pickup datetime lets BigQuery prune partitions on date-range queries.
    @task
    def create_main_table(**context) -> None:
        taxi = context["params"]["taxi"]
        schema = SCHEMAS[taxi]
        table_id = f"{GCP_PROJECT_ID}.{GCP_DATASET}.{taxi}_tripdata"
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

    # ── 4. Create external table ─────────────────────────────────────────
    # An EXTERNAL TABLE doesn't store data – it tells BigQuery "read the CSV
    # directly from this GCS URI".  This avoids a double copy: the data
    # stays in GCS and BigQuery reads it on demand.
    @task
    def create_external_table(gcs_uri: str, **context) -> None:
        p = context["params"]
        taxi, year, month = p["taxi"], p["year"], p["month"]
        schema = SCHEMAS[taxi]
        table_id = (
            f"{GCP_PROJECT_ID}.{GCP_DATASET}."
            f"{taxi}_tripdata_{year}_{month}_ext"
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

    # ── 5. Create temp table ─────────────────────────────────────────────
    # Materialises the external table into a regular BQ table, adding:
    #   - unique_row_id: MD5 hash of key columns (for deduplication)
    #   - filename: source file name (for lineage / debugging)
    # SELECT * pulls all CSV columns from the external table.
    @task
    def create_temp_table(**context) -> None:
        p = context["params"]
        taxi, year, month = p["taxi"], p["year"], p["month"]
        schema = SCHEMAS[taxi]
        filename = f"{taxi}_tripdata_{year}-{month}.csv"
        ext_table = (
            f"{GCP_PROJECT_ID}.{GCP_DATASET}."
            f"{taxi}_tripdata_{year}_{month}_ext"
        )
        tmp_table = (
            f"{GCP_PROJECT_ID}.{GCP_DATASET}."
            f"{taxi}_tripdata_{year}_{month}"
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

    # ── 6. Merge data ────────────────────────────────────────────────────
    # MERGE INTO inserts only rows whose unique_row_id doesn't already exist
    # in the main table.  This makes the pipeline idempotent: running it
    # twice for the same month won't duplicate data.
    @task
    def merge_data(**context) -> None:
        p = context["params"]
        taxi, year, month = p["taxi"], p["year"], p["month"]
        schema = SCHEMAS[taxi]
        main_table = f"{GCP_PROJECT_ID}.{GCP_DATASET}.{taxi}_tripdata"
        tmp_table = (
            f"{GCP_PROJECT_ID}.{GCP_DATASET}."
            f"{taxi}_tripdata_{year}_{month}"
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

    # ── 7. Cleanup ───────────────────────────────────────────────────────
    # Removes the local temp CSV.
    @task
    def cleanup(csv_path: str) -> None:
        p = Path(csv_path)
        if p.exists():
            p.unlink()
            logger.info("Removed temp file %s", p)

    # ── Wire the tasks ───────────────────────────────────────────────────
    # Data dependencies (function arguments) set automatic ordering:
    #   extract → upload_to_gcs  (csv_path)
    #   upload_to_gcs → create_external_table  (gcs_uri)
    #   extract → cleanup  (csv_path)
    #
    # Tasks without data dependencies need explicit ordering via >>:
    csv_file = extract()
    gcs_uri = upload_to_gcs(csv_file)
    main_ready = create_main_table()
    ext_ready = create_external_table(gcs_uri)
    tmp_ready = create_temp_table()
    merged = merge_data()
    cleaned = cleanup(csv_file)

    gcs_uri >> main_ready >> ext_ready >> tmp_ready >> merged >> cleaned


taxi_to_gcp()
