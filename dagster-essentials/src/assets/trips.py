import os

import dagster as dg
import requests as rq
from dagster_duckdb import DuckDBResource

from . import constants
from .. import utils
from ..partitions import monthly_partition


@dg.asset(
    group_name="ingestion",
    kinds={"http", "parquet"},
    description="Request taxi trips's parquet file over http",
    partitions_def=monthly_partition,
)
def taxi_trips_file(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    fetch_month = utils.change_date_format(context.partition_key, "%Y-%m-%d", "%Y-%m")
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{fetch_month}.parquet"

    raw_trips = rq.get(url)
    filename = constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(fetch_month)

    with open(filename, "wb") as output_file:
        output_file.write(raw_trips.content)

    return dg.MaterializeResult(
        metadata={
            "fetched_month": fetch_month,
            "file_size_kb": dg.MetadataValue.int(os.stat(filename).st_size),
            "url": url,
        }
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
    partitions_def=monthly_partition,
    automation_condition=dg.AutomationCondition.eager(),
)
def taxi_trips(
    context: dg.AssetExecutionContext, duckdb: DuckDBResource
) -> dg.MaterializeResult:
    trips_month = utils.change_date_format(context.partition_key, "%Y-%m-%d", "%Y-%m")

    with duckdb.get_connection() as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS trips (
                vendor_id INTEGER, pickup_zone_id INTEGER, dropoff_zone_id INTEGER,
                rate_code_id DOUBLE, payment_type INTEGER, dropoff_datetime TIMESTAMP,
                pickup_datetime TIMESTAMP, trip_distance DOUBLE, passenger_count DOUBLE,
                total_amount DOUBLE, partition_date VARCHAR
            );
            
            DELETE FROM trips WHERE partition_date = '{trips_month}';
            
            INSERT INTO trips
            SELECT
                VendorID, PULocationID, DOLocationID, RatecodeID, payment_type, tpep_dropoff_datetime,
                tpep_pickup_datetime, trip_distance, passenger_count, total_amount, '{trips_month}' as partition_date
            FROM '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(trips_month)}';
        """)
        preview_df = conn.execute("SELECT * FROM trips LIMIT 10").fetch_df()
        row_count = conn.execute("SELECT COUNT(*) FROM trips").fetchone()
        count = row_count[0] if row_count else 0

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.IntMetadataValue(count),
            "fetched_month": trips_month,
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
