import base64
from io import BytesIO

import dagster as dg
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from dagster_duckdb import DuckDBResource


@dg.asset(
    deps=["taxi_trips", "taxi_zones"],
    group_name="metrics",
    kinds={"duckdb", "pandas"},
)
def trips_by_zone(duckdb: DuckDBResource) -> dg.Output[gpd.GeoDataFrame]:
    query = """
        SELECT
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips,
        FROM trips
        LEFT JOIN zones ON trips.pickup_zone_id = zones.zone_id
        WHERE borough = 'Manhattan' AND geometry IS NOT NULL
        GROUP BY zone, borough, geometry
    """

    with duckdb.get_connection() as conn:
        df = conn.execute(query).fetch_df()

    df["geometry"] = gpd.GeoSeries.from_wkt(df["geometry"])
    df = gpd.GeoDataFrame(df)

    return dg.Output(
        df,
        metadata={
            "row_count": dg.MetadataValue.int(df.shape[0]),
            "preview": dg.MetadataValue.md(df.head(5).to_markdown()),
        },
    )


@dg.asset(
    deps=["taxi_trips"],
    group_name="metrics",
    kinds={"duckdb", "pandas"},
)
def trips_by_week(duckdb: DuckDBResource) -> dg.Output[pd.DataFrame]:
    query = """
        WITH weekly_trips AS (
            SELECT
                DATE_TRUNC('week', pickup_datetime) + INTERVAL '1 day' AS period,
                COUNT(*) as num_trips,
                SUM(passenger_count) as passenger_count,
                SUM(trip_distance) as trip_distance,
                SUM(total_amount) as total_amount
            FROM trips
            WHERE pickup_datetime BETWEEN '2021-03-01' AND '2023-03-01'
            GROUP BY period
        )
        SELECT 
            *, 
            WEEK(period) as week_number,
            YEAR(period) as year
        FROM weekly_trips
        ORDER BY period, week_number, year
    """

    with duckdb.get_connection() as conn:
        df = conn.execute(query).fetch_df()

    return dg.Output(
        df,
        metadata={
            "row_count": dg.MetadataValue.int(df.shape[0]),
            "preview": dg.MetadataValue.md(df.head(10).to_markdown()),
        },
    )


@dg.asset(
    group_name="metrics",
    kinds={"pandas", "matplotlib"},
)
def manhattan_map(
    context: dg.AssetExecutionContext, trips_by_zone: gpd.GeoDataFrame
) -> dg.MaterializeResult:
    fig, ax = plt.subplots(figsize=(10, 10))
    trips_by_zone.plot(
        column="num_trips", cmap="plasma", legend=True, ax=ax, edgecolor="black"
    )
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim([-74.05, -73.90])  # Adjust longitude range
    ax.set_ylim([40.70, 40.82])  # Adjust latitude range

    # Convert img into a savable format
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    plt.close(fig)
    img_data = base64.b64encode(buffer.getvalue())

    # Prepare markdown of img to show it in Dagster UI
    img_md = f"![img](data:image/png;base64,{img_data.decode()})"

    return dg.MaterializeResult(
        metadata={
            "manhanttan map": dg.MetadataValue.md(img_md),
        }
    )
