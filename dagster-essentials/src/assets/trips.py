import os

import dagster as dg
import requests as rq
from dagster_duckdb import DuckDBResource

from . import constants


@dg.asset(
    group_name="ingestion",
    kinds={"http", "parquet"},
    description="Request taxi trips's parquet file over http",
)
def taxi_trips_file() -> dg.MaterializeResult:
    month_to_fetch = "2023-03"
    raw_trips = rq.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )
    filename = constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)

    with open(filename, "wb") as output_file:
        output_file.write(raw_trips.content)

    return dg.MaterializeResult(
        metadata={"file_size_kb": dg.MetadataValue.int(os.stat(filename).st_size)}
    )


@dg.asset(
    group_name="ingestion",
    kinds={"http", "parquet"},
    description="Request taxi zones's parquet file over http",
)
def taxi_zones_file() -> dg.MaterializeResult:
    raw_zones = rq.get(
        "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
    )
    filename = constants.TAXI_ZONES_FILE_PATH

    with open(filename, "wb") as zones_file:
        zones_file.write(raw_zones.content)

    return dg.MaterializeResult(
        metadata={"file_size_kb": dg.MetadataValue.int(os.stat(filename).st_size)}
    )


@dg.asset(
    deps=[taxi_trips_file],
    group_name="ingestion",
    kinds={"duckdb"},
    description="Save taxi trips data from parquet file to duckdb",
)
def taxi_trips(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE TABLE trips AS (
                SELECT
                    VendorID as vendor_id,
                    PULocationID as pickup_zone_id,
                    DOLocationID as dropoff_zone_id,
                    RatecodeID as rate_code_id,
                    payment_type as payment_type,
                    tpep_dropoff_datetime as dropoff_datetime,
                    tpep_pickup_datetime as pickup_datetime,
                    trip_distance as trip_distance,
                    passenger_count as passenger_count,
                    total_amount as total_amount
                FROM 'data/raw/taxi_trips_2023-03.parquet'
            );
        """)
        preview_df = conn.execute("SELECT * FROM trips LIMIT 10").fetch_df()
        row_count = conn.execute("SELECT COUNT(*) FROM trips").fetchone()
        count = row_count[0] if row_count else 0

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.IntMetadataValue(count),
            "preview": dg.MarkdownMetadataValue(preview_df.to_markdown(index=False)),
        }
    )


@dg.asset(
    deps=[taxi_zones_file],
    group_name="ingestion",
    kinds={"duckdb"},
    description="Save taxi zones data from parquet file to duckdb",
)
def taxi_zones(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(f"""
            CREATE OR REPLACE TABLE zones AS (
                SELECT
                    LocationID as zone_id,
                    zone,
                    borough,
                    the_geom as geometry
                FROM '{constants.TAXI_ZONES_FILE_PATH}'
            );          
        """)
        preview_df = conn.execute("SELECT * FROM zones LIMIT 10").fetch_df()
        row_count = conn.execute("SELECT COUNT(*) FROM zones").fetchone()
        count = row_count[0] if row_count else 0

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.IntMetadataValue(count),
            "preview": dg.MarkdownMetadataValue(preview_df.to_markdown(index=False)),
        }
    )
