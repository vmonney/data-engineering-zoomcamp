#!/usr/bin/env python
# coding: utf-8

import click
import duckdb


@click.command()
@click.option('--db-path', default='/data/ny_taxi.duckdb', help='Path to DuckDB database file')
@click.option('--taxi-url', required=True, help='URL of the taxi trip parquet file')
@click.option('--zones-url', required=True, help='URL of the zones CSV file')
@click.option('--taxi-table', default='green_taxi', help='Target table for taxi data')
@click.option('--zones-table', default='zones', help='Target table for zones data')
def run(db_path, taxi_url, zones_url, taxi_table, zones_table):
    """Ingest NYC taxi data into DuckDB database."""
    conn = duckdb.connect(db_path)

    # Install and load httpfs extension for reading remote files
    conn.install_extension('httpfs')
    conn.load_extension('httpfs')

    print(f"Ingesting taxi data from {taxi_url}...")
    conn.execute(f"""
    CREATE OR REPLACE TABLE {taxi_table} AS
    SELECT * FROM read_parquet('{taxi_url}')
    """)
    count = conn.execute(f"SELECT COUNT(*) FROM {taxi_table}").fetchone()[0]
    print(f"  -> Loaded {count} rows into {taxi_table}")

    print(f"Ingesting zones data from {zones_url}...")
    conn.execute(f"""
    CREATE OR REPLACE TABLE {zones_table} AS
    SELECT * FROM read_csv_auto('{zones_url}')
    """)
    count = conn.execute(f"SELECT COUNT(*) FROM {zones_table}").fetchone()[0]
    print(f"  -> Loaded {count} rows into {zones_table}")

    conn.close()
    print("Done!")

if __name__ == '__main__':
    run()
