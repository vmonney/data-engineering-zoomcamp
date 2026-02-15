"""04_taxi_to_duckdb – Airflow DAG: NYC taxi CSV data → DuckDB.

Translated from the Kestra '04_postgres_taxi' flow.
Uses the existing DuckDB database at /data/ny_taxi.duckdb instead of PostgreSQL.
Manually triggered with parameter selection for taxi type, year, and month.

CSV data source: https://github.com/DataTalksClub/nyc-tlc-data/releases
"""

import gzip
import logging
import shutil
from datetime import datetime
from pathlib import Path

import duckdb
import requests
from airflow.decorators import dag, task
from airflow.models.param import Param

logger = logging.getLogger(__name__)

DUCKDB_PATH = "/data/ny_taxi.duckdb"
TMP_DIR = Path("/tmp/taxi_data")


# ── Schema definitions per taxi type ─────────────────────────────────────────

YELLOW_COLUMNS = {
    "unique_row_id": "VARCHAR",
    "filename": "VARCHAR",
    "VendorID": "VARCHAR",
    "tpep_pickup_datetime": "TIMESTAMP",
    "tpep_dropoff_datetime": "TIMESTAMP",
    "passenger_count": "INTEGER",
    "trip_distance": "DOUBLE",
    "RatecodeID": "VARCHAR",
    "store_and_fwd_flag": "VARCHAR",
    "PULocationID": "VARCHAR",
    "DOLocationID": "VARCHAR",
    "payment_type": "INTEGER",
    "fare_amount": "DOUBLE",
    "extra": "DOUBLE",
    "mta_tax": "DOUBLE",
    "tip_amount": "DOUBLE",
    "tolls_amount": "DOUBLE",
    "improvement_surcharge": "DOUBLE",
    "total_amount": "DOUBLE",
    "congestion_surcharge": "DOUBLE",
}

YELLOW_CSV_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
    "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra",
    "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
    "total_amount", "congestion_surcharge",
]

YELLOW_DEDUP_COLS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "PULocationID", "DOLocationID", "fare_amount", "trip_distance",
]

GREEN_COLUMNS = {
    "unique_row_id": "VARCHAR",
    "filename": "VARCHAR",
    "VendorID": "VARCHAR",
    "lpep_pickup_datetime": "TIMESTAMP",
    "lpep_dropoff_datetime": "TIMESTAMP",
    "store_and_fwd_flag": "VARCHAR",
    "RatecodeID": "VARCHAR",
    "PULocationID": "VARCHAR",
    "DOLocationID": "VARCHAR",
    "passenger_count": "INTEGER",
    "trip_distance": "DOUBLE",
    "fare_amount": "DOUBLE",
    "extra": "DOUBLE",
    "mta_tax": "DOUBLE",
    "tip_amount": "DOUBLE",
    "tolls_amount": "DOUBLE",
    "ehail_fee": "DOUBLE",
    "improvement_surcharge": "DOUBLE",
    "total_amount": "DOUBLE",
    "payment_type": "INTEGER",
    "trip_type": "INTEGER",
    "congestion_surcharge": "DOUBLE",
}

GREEN_CSV_COLUMNS = [
    "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime",
    "store_and_fwd_flag", "RatecodeID", "PULocationID", "DOLocationID",
    "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "ehail_fee", "improvement_surcharge",
    "total_amount", "payment_type", "trip_type", "congestion_surcharge",
]

GREEN_DEDUP_COLS = [
    "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime",
    "PULocationID", "DOLocationID", "fare_amount", "trip_distance",
]

SCHEMAS = {
    "yellow": {
        "columns": YELLOW_COLUMNS,
        "csv_columns": YELLOW_CSV_COLUMNS,
        "dedup_columns": YELLOW_DEDUP_COLS,
    },
    "green": {
        "columns": GREEN_COLUMNS,
        "csv_columns": GREEN_CSV_COLUMNS,
        "dedup_columns": GREEN_DEDUP_COLS,
    },
}


def _ddl(columns: dict[str, str]) -> str:
    """Build a column-definition string for CREATE TABLE."""
    return ",\n        ".join(f"{col} {dtype}" for col, dtype in columns.items())


# ── DAG definition ───────────────────────────────────────────────────────────


@dag(
    dag_id="04_taxi_to_duckdb",
    description=(
        "Downloads NYC taxi trip CSV data and loads it into DuckDB "
        "with staging-table deduplication. "
        "Translated from Kestra flow '04_postgres_taxi'."
    ),
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["zoomcamp", "taxi", "duckdb"],
    params={
        "taxi": Param(
            default="yellow",
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
def taxi_to_duckdb():
    """NYC taxi CSV → DuckDB pipeline with staging-table merge pattern."""

    # ── 1. Extract: download and decompress the CSV ──────────────────────
    @task
    def extract(**context) -> str:
        """Download and decompress the .csv.gz taxi trip file."""
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

    # ── 2. Create tables: ensure final + staging tables exist ────────────
    @task
    def create_tables(**context) -> None:
        """Create the final and staging tables in DuckDB if they don't exist."""
        taxi = context["params"]["taxi"]
        schema = SCHEMAS[taxi]
        table = f"{taxi}_tripdata"
        staging = f"{taxi}_tripdata_staging"
        ddl = _ddl(schema["columns"])

        conn = duckdb.connect(DUCKDB_PATH)
        try:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (\n        {ddl}\n    );")
            logger.info("Ensured table '%s' exists", table)

            conn.execute(f"CREATE TABLE IF NOT EXISTS {staging} (\n        {ddl}\n    );")
            conn.execute(f"TRUNCATE TABLE {staging};")
            logger.info("Ensured staging table '%s' exists (truncated)", staging)
        finally:
            conn.close()

    # ── 3. Load staging: import CSV rows into the staging table ──────────
    @task
    def load_staging(csv_path: str, **context) -> None:
        """Load the CSV data into the DuckDB staging table."""
        taxi = context["params"]["taxi"]
        schema = SCHEMAS[taxi]
        staging = f"{taxi}_tripdata_staging"
        csv_cols = ", ".join(schema["csv_columns"])

        conn = duckdb.connect(DUCKDB_PATH)
        try:
            conn.execute(
                f"""
                INSERT INTO {staging} ({csv_cols})
                SELECT {csv_cols}
                FROM read_csv('{csv_path}', header=true, auto_detect=true);
                """
            )
            count = conn.execute(f"SELECT COUNT(*) FROM {staging}").fetchone()[0]
            logger.info("Loaded %d rows into staging '%s'", count, staging)
        finally:
            conn.close()

    # ── 4. Add unique ID: compute MD5 hash for deduplication ─────────────
    @task
    def add_unique_id(**context) -> None:
        """Compute MD5 unique_row_id and set filename on every staging row."""
        p = context["params"]
        taxi, year, month = p["taxi"], p["year"], p["month"]
        schema = SCHEMAS[taxi]
        staging = f"{taxi}_tripdata_staging"
        filename = f"{taxi}_tripdata_{year}-{month}.csv"

        concat_expr = " || ".join(
            f"COALESCE(CAST({col} AS VARCHAR), '')"
            for col in schema["dedup_columns"]
        )

        conn = duckdb.connect(DUCKDB_PATH)
        try:
            conn.execute(
                f"""
                UPDATE {staging}
                SET unique_row_id = md5({concat_expr}),
                    filename      = '{filename}';
                """
            )
            logger.info("Set unique_row_id and filename='%s' on staging rows", filename)
        finally:
            conn.close()

    # ── 5. Merge: insert only new rows into the final table ──────────────
    @task
    def merge_data(**context) -> None:
        """Insert new rows from staging into the final table (dedup on unique_row_id)."""
        taxi = context["params"]["taxi"]
        schema = SCHEMAS[taxi]
        table = f"{taxi}_tripdata"
        staging = f"{taxi}_tripdata_staging"
        all_cols = ", ".join(schema["columns"].keys())

        conn = duckdb.connect(DUCKDB_PATH)
        try:
            before = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

            conn.execute(
                f"""
                INSERT INTO {table} ({all_cols})
                SELECT {all_cols}
                FROM {staging} AS s
                WHERE s.unique_row_id NOT IN (
                    SELECT unique_row_id FROM {table}
                );
                """
            )

            after = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logger.info(
                "Merged into '%s': %d before → %d after (%d new rows)",
                table, before, after, after - before,
            )
        finally:
            conn.close()

    # ── 6. Cleanup: remove the temporary CSV file ────────────────────────
    @task
    def cleanup(csv_path: str) -> None:
        """Remove the temporary CSV file."""
        p = Path(csv_path)
        if p.exists():
            p.unlink()
            logger.info("Removed temp file %s", p)

    # ── Wire the tasks together ──────────────────────────────────────────
    csv_file = extract()
    tables_ready = create_tables()
    staging_loaded = load_staging(csv_file)
    ids_set = add_unique_id()
    merged = merge_data()
    cleaned_up = cleanup(csv_file)

    # Explicit ordering for tasks without data dependencies
    tables_ready >> staging_loaded >> ids_set >> merged >> cleaned_up


# Instantiate the DAG
taxi_to_duckdb()
