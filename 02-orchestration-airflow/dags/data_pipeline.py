"""03_data_pipeline – Airflow equivalent of the Kestra '03_getting_started_data_pipeline' flow.

Kestra original:
  - Downloads product data from https://dummyjson.com/products
  - Filters JSON to keep only selected columns (brand, price)
  - Uses DuckDB to compute average price per brand

Airflow translation:
  - Uses the TaskFlow API with @dag / @task decorators
  - `requests` replaces Kestra's HTTP Download plugin
  - Data is passed between tasks via XCom (return values)
  - `duckdb` (Python library) replaces Kestra's DuckDB plugin
  - DAG params replace Kestra inputs
"""

import json
import logging
import tempfile
from datetime import datetime
from pathlib import Path

import duckdb
import requests
from airflow.decorators import dag, task
from airflow.models.param import Param

logger = logging.getLogger(__name__)


@dag(
    dag_id="03_data_pipeline",
    description=(
        "Downloads product data, filters selected columns, "
        "and computes average price per brand with DuckDB."
    ),
    schedule=None,  # manual trigger only (like the Kestra flow)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["zoomcamp"],
    params={
        "columns_to_keep": Param(
            default=["brand", "price"],
            type="array",
            items={"type": "string"},
            description="List of column names to keep from the product data.",
        ),
    },
)
def data_pipeline():
    # ── Step 1: Extract – download raw data from the API ─────────────────
    @task
    def extract() -> list[dict]:
        """Download product data from dummyjson.com.

        Equivalent to the Kestra `extract` task
        (io.kestra.plugin.core.http.Download).
        """
        url = "https://dummyjson.com/products"
        logger.info("Downloading data from %s", url)

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()
        products = data["products"]
        logger.info("Downloaded %d products", len(products))
        return products

    # ── Step 2: Transform – keep only the requested columns ──────────────
    @task
    def transform(products: list[dict], **context) -> list[dict]:
        """Filter product data to keep only selected columns.

        Equivalent to the Kestra `transform` task
        (io.kestra.plugin.scripts.python.Script).
        """
        columns_to_keep: list[str] = context["params"]["columns_to_keep"]
        logger.info("Keeping columns: %s", columns_to_keep)

        filtered = [
            {col: product.get(col, "N/A") for col in columns_to_keep}
            for product in products
        ]

        logger.info("Filtered %d products down to %d columns", len(filtered), len(columns_to_keep))
        return filtered

    # ── Step 3: Query – aggregate with DuckDB ────────────────────────────
    @task
    def query(products: list[dict]) -> list[dict]:
        """Use DuckDB to compute average price per brand.

        Equivalent to the Kestra `query` task
        (io.kestra.plugin.jdbc.duckdb.Queries).
        """
        # Write filtered data to a temp JSON file for DuckDB to read
        tmp = Path(tempfile.mkdtemp()) / "products.json"
        tmp.write_text(json.dumps(products, indent=4))
        logger.info("Wrote %d products to %s", len(products), tmp)

        sql = f"""
            SELECT brand,
                   round(avg(price), 2) AS avg_price
            FROM read_json_auto('{tmp}')
            GROUP BY brand
            ORDER BY avg_price DESC;
        """
        logger.info("Running DuckDB query:\n%s", sql)

        conn = duckdb.connect()
        result = conn.execute(sql).fetchdf()
        conn.close()

        # Log the result table
        logger.info("Query results:\n%s", result.to_string(index=False))

        # Clean up
        tmp.unlink()

        # Return as list of dicts so it's visible in XCom
        return result.to_dict(orient="records")

    # ── Wire the tasks together (extract → transform → query) ────────────
    raw_products = extract()
    filtered_products = transform(raw_products)
    query(filtered_products)


# Instantiate the DAG
data_pipeline()
