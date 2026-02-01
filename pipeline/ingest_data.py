#!/usr/bin/env python
# coding: utf-8

import click
import duckdb
import polars as pl
from tqdm.auto import tqdm

schema_overrides = {
    "VendorID": pl.Int64,
    "passenger_count": pl.Int64,
    "trip_distance": pl.Float64,
    "RatecodeID": pl.Int64,
    "store_and_fwd_flag": pl.String,
    "PULocationID": pl.Int64,
    "DOLocationID": pl.Int64,
    "payment_type": pl.Int64,
    "fare_amount": pl.Float64,
    "extra": pl.Float64,
    "mta_tax": pl.Float64,
    "tip_amount": pl.Float64,
    "tolls_amount": pl.Float64,
    "improvement_surcharge": pl.Float64,
    "total_amount": pl.Float64,
    "congestion_surcharge": pl.Float64,
    "tpep_pickup_datetime": pl.Datetime("us"),
    "tpep_dropoff_datetime": pl.Datetime("us"),
}


@click.command()
@click.option('--db-path', default='ny_taxi.duckdb', help='Path to DuckDB database file')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')
def run(db_path, year, month, target_table, chunksize):
    """Ingest NYC taxi data into DuckDB database."""
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

    conn = duckdb.connect(db_path)

    reader = pl.read_csv_batched(
        url,
        schema_overrides=schema_overrides,
        batch_size=chunksize,
    )

    def chunk_iter():
        while True:
            batches = reader.next_batches(1)
            if not batches:
                return
            yield batches[0]

    first = True
    for df_chunk in tqdm(chunk_iter(), unit="chunk"):
        conn.register("_chunk", df_chunk)
        if first:
            conn.execute(f"CREATE OR REPLACE TABLE {target_table} AS SELECT * FROM _chunk LIMIT 0")
            first = False
        conn.execute(f"INSERT INTO {target_table} SELECT * FROM _chunk")

    conn.close()


if __name__ == '__main__':
    run()
