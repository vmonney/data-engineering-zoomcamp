import marimo

__generated_with = "0.19.7"
app = marimo.App()


@app.cell
def _():
    import polars as pl
    import duckdb
    conn = duckdb.connect('ny_taxi.duckdb')

    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    return conn, pl, prefix


@app.cell
def _(pl, prefix):
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

    df = pl.read_csv(
        prefix + 'yellow_tripdata_2021-01.csv.gz',
        schema_overrides=schema_overrides,
    )
    return df, schema_overrides


@app.cell
def _(df):
    _pl_to_sql = {
        "Int64": "BIGINT",
        "Float64": "DOUBLE",
        "String": "VARCHAR",
        "Datetime(\"us\")": "TIMESTAMP",
    }
    cols = ", ".join(
        f'"{name}" {_pl_to_sql.get(str(dtype), str(dtype))}'
        for name, dtype in df.schema.items()
    )
    print(f"CREATE TABLE yellow_taxi_data ({cols})")
    return


@app.cell
def _(pl, prefix, schema_overrides):
    reader = pl.read_csv_batched(
        prefix + 'yellow_tripdata_2021-01.csv.gz',
        schema_overrides=schema_overrides,
        batch_size=100000,
    )
    return (reader,)


@app.cell
def _(reader):
    from tqdm.auto import tqdm

    def chunk_iter():
        while True:
            batches = reader.next_batches(1)
            if not batches:
                return
            yield batches[0]

    chunks = chunk_iter()
    first_chunk = next(chunks)
    return chunks, first_chunk, tqdm


@app.cell
def _(chunks, conn, first_chunk, tqdm):

    conn.register("_chunk", first_chunk)
    conn.execute("CREATE OR REPLACE TABLE yellow_taxi_data AS SELECT * FROM _chunk LIMIT 0")
    conn.execute("INSERT INTO yellow_taxi_data SELECT * FROM _chunk")
    print("Table created")
    print("Inserted first chunk:", first_chunk.height)

    for df_chunk in tqdm(chunks, unit="chunk"):
        conn.register("_chunk", df_chunk)
        conn.execute("INSERT INTO yellow_taxi_data SELECT * FROM _chunk")
        print("Inserted chunk:", df_chunk.height)
    return


@app.cell
def _(pl):
    df_zones = pl.read_csv('taxi_zone_lookup.csv')
    return (df_zones,)


@app.cell
def _(df_zones):
    df_zones.head()
    return


@app.cell
def _(conn):
    conn.execute("CREATE OR REPLACE TABLE zones AS SELECT * FROM df_zones")
    return


@app.cell
def _():
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Try querying DuckDB with SQL commands
    """)
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        DESCRIBE yellow_taxi_data;
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        DESCRIBE zones;
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        SELECT 
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            total_amount,
            CONCAT(zpu.Borough, ' / ', zpu.Zone) AS pickup_loc,
            CONCAT(zdo.Borough, ' / ', zdo.Zone) AS dropoff_loc
        FROM
        	yellow_taxi_data	t
        JOIN zones	zpu ON t.PULocationID = zpu.LocationID
        JOIN zones	zdo ON t.DOLocationID = zdo.LocationID
        LIMIT 100;
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        SELECT 
            DATE_TRUNC('DAY', tpep_dropoff_datetime) AS day,
            DOLocationID,
            COUNT(1) as count,
            MAX(total_amount),
            MAX(passenger_count)
        FROM
        	yellow_taxi_data	t
        GROUP BY 1,2
        ORDER BY day ASC, DOLocationID ASC;
        """,
        engine=conn
    )
    return


if __name__ == "__main__":
    app.run()
