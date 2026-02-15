"""05_taxi_to_duckdb_scheduled – Airflow DAGs: NYC taxi CSV → DuckDB (scheduled).


Two DAGs are created (one per taxi type) with monthly cron schedules.
The logical_date of each DAG run determines which month's data to process,
removing the need for manual year/month parameter selection.

Set catchup=True so Airflow creates runs for every past interval between
start_date and today (or end_date) – this is the backfill mechanism.

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


# ── DAG factory ──────────────────────────────────────────────────────────────


def create_scheduled_taxi_dag(taxi_type: str, cron: str) -> None:
    """Create a monthly-scheduled DAG for one taxi type.

    Parameters
    ----------
    taxi_type : "yellow" or "green"
    cron : cron expression for the schedule (e.g. "0 10 1 * *")
    """
    schema = SCHEMAS[taxi_type]
    table_name = f"{taxi_type}_tripdata"
    staging_name = f"{taxi_type}_tripdata_staging"

    @dag(
        dag_id=f"05_{taxi_type}_taxi_to_duckdb_scheduled",
        description=(
            f"Monthly scheduled load of NYC {taxi_type} taxi trip CSV data "
            f"into DuckDB with staging-table deduplication. "
            f"Supports backfill via catchup. "
            f"Translated from Kestra flow '05_postgres_taxi_scheduled'."
        ),
        schedule=cron,
        start_date=datetime(2019, 1, 1),
        end_date=datetime(2021, 8, 1),
        catchup=True,
        max_active_runs=1,
        is_paused_upon_creation=True,
        tags=["zoomcamp", "taxi", "duckdb", taxi_type, "scheduled"],
    )
    def scheduled_taxi_to_duckdb():
        """NYC taxi CSV → DuckDB pipeline (scheduled with backfill support)."""

        # ── 1. Extract: download and decompress the CSV ──────────────────
        @task
        def extract(**context) -> str:
            """Download and decompress the .csv.gz taxi trip file.

            The year-month is derived from ``logical_date`` so each scheduled
            (or backfilled) run automatically targets the correct file.
            """
            logical_date = context["logical_date"]
            year_month = logical_date.strftime("%Y-%m")
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

        # ── 2. Create tables ─────────────────────────────────────────────
        @task
        def create_tables() -> None:
            """Create the final and staging tables in DuckDB if needed."""
            ddl = _ddl(schema["columns"])

            conn = duckdb.connect(DUCKDB_PATH)
            try:
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
                    f"        {ddl}\n    );"
                )
                logger.info("Ensured table '%s' exists", table_name)

                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {staging_name} (\n"
                    f"        {ddl}\n    );"
                )
                conn.execute(f"TRUNCATE TABLE {staging_name};")
                logger.info(
                    "Ensured staging table '%s' exists (truncated)", staging_name
                )
            finally:
                conn.close()

        # ── 3. Load staging ──────────────────────────────────────────────
        @task
        def load_staging(csv_path: str) -> None:
            """Load the CSV data into the DuckDB staging table."""
            csv_cols = ", ".join(schema["csv_columns"])

            conn = duckdb.connect(DUCKDB_PATH)
            try:
                conn.execute(
                    f"""
                    INSERT INTO {staging_name} ({csv_cols})
                    SELECT {csv_cols}
                    FROM read_csv('{csv_path}', header=true, auto_detect=true);
                    """
                )
                count = conn.execute(
                    f"SELECT COUNT(*) FROM {staging_name}"
                ).fetchone()[0]
                logger.info("Loaded %d rows into staging '%s'", count, staging_name)
            finally:
                conn.close()

        # ── 4. Add unique ID ─────────────────────────────────────────────
        @task
        def add_unique_id(**context) -> None:
            """Compute MD5 unique_row_id and set filename on staging rows."""
            logical_date = context["logical_date"]
            year_month = logical_date.strftime("%Y-%m")
            filename = f"{taxi_type}_tripdata_{year_month}.csv"

            concat_expr = " || ".join(
                f"COALESCE(CAST({col} AS VARCHAR), '')"
                for col in schema["dedup_columns"]
            )

            conn = duckdb.connect(DUCKDB_PATH)
            try:
                conn.execute(
                    f"""
                    UPDATE {staging_name}
                    SET unique_row_id = md5({concat_expr}),
                        filename      = '{filename}';
                    """
                )
                logger.info(
                    "Set unique_row_id and filename='%s' on staging rows", filename
                )
            finally:
                conn.close()

        # ── 5. Merge ─────────────────────────────────────────────────────
        @task
        def merge_data() -> None:
            """Insert only new rows from staging into the final table."""
            all_cols = ", ".join(schema["columns"].keys())

            conn = duckdb.connect(DUCKDB_PATH)
            try:
                before = conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]

                conn.execute(
                    f"""
                    INSERT INTO {table_name} ({all_cols})
                    SELECT {all_cols}
                    FROM {staging_name} AS s
                    WHERE s.unique_row_id NOT IN (
                        SELECT unique_row_id FROM {table_name}
                    );
                    """
                )

                after = conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                logger.info(
                    "Merged into '%s': %d before → %d after (%d new rows)",
                    table_name,
                    before,
                    after,
                    after - before,
                )
            finally:
                conn.close()

        # ── 6. Cleanup ───────────────────────────────────────────────────
        @task
        def cleanup(csv_path: str) -> None:
            """Remove the temporary CSV file."""
            p = Path(csv_path)
            if p.exists():
                p.unlink()
                logger.info("Removed temp file %s", p)

        # ── Wire the tasks together ──────────────────────────────────────
        csv_file = extract()
        tables_ready = create_tables()
        staging_loaded = load_staging(csv_file)
        ids_set = add_unique_id()
        merged = merge_data()
        cleaned_up = cleanup(csv_file)

    # Explicit ordering for tasks without data dependencies
    tables_ready >> staging_loaded >> ids_set >> merged >> cleaned_up

    scheduled_taxi_to_duckdb()


# ── Create one DAG per taxi type, matching the Kestra trigger schedules ──────
create_scheduled_taxi_dag("green", "0 9 1 * *")    # 1st of month at 09:00 UTC
create_scheduled_taxi_dag("yellow", "0 10 1 * *")  # 1st of month at 10:00 UTC
