from typing import Literal

import dagster as dg
import matplotlib.pyplot as plt
from dagster_duckdb import DuckDBResource

from .. import utils
from . import constants


class AdhocRequestsConfig(dg.Config):
    filename: str
    borough: Literal["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
    start_date: str
    end_date: str


@dg.asset(
    deps=["taxi_zones", "taxi_trips"],
    group_name="adhoc",
    kinds={"duckdb", "pandas"},
    description="The response to an request made in the `requests` directory",
)
def adhoc_request(
    config: AdhocRequestsConfig, duckdb: DuckDBResource
) -> dg.MaterializeResult:
    file_path = constants.REQUEST_DESTINATION_TEMPLATE_FILE_PATH.format(
        config.filename.split(".")[0]
    )
    query = f"""
        SELECT
            date_part('hour', pickup_datetime) AS hour_of_day,
            date_part('dayofweek', pickup_datetime) AS day_of_week_num,
            CASE date_part('dayofweek', pickup_datetime)
                WHEN 0 then 'Sunday'
                WHEN 1 then 'Monday'
                WHEN 2 then 'Tuesday'
                WHEN 3 then 'Wednesday'
                WHEN 4 then 'Thursday'
                WHEN 5 then 'Friday'
                WHEN 6 then 'Saturday'
            END AS day_of_week,
            COUNT(*) AS num_trips
        FROM trips
        LEFT JOIN zones ON trips.pickup_zone_id = zones.zone_id
        WHERE pickup_datetime >= '{config.start_date}'
        AND pickup_datetime < '{config.end_date}'
        AND pickup_zone_id IN (
            SELECT zone_id
            FROM zones
            WHERE borough = '{config.borough}'
        )
        GROUP BY 1, 2
        ORDER BY 1, 2 asc
    """

    with duckdb.get_connection() as conn:
        df = conn.execute(query).fetch_df()

    fig, ax = plt.subplots(figsize=(10, 6))

    # Pivot data for stacked bar chart
    results_pivot = df.pivot(
        index="hour_of_day", columns="day_of_week", values="num_trips"
    )
    results_pivot.plot(kind="bar", stacked=True, ax=ax, colormap="viridis")

    ax.set_title(
        f"Number of trips by hour of day in {config.borough}, from {config.start_date} to {config.end_date}"
    )
    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("Number of Trips")
    ax.legend(title="Day of Week")

    plt.xticks(rotation=45)
    plt.tight_layout()

    fig.savefig(file_path)

    img_md = utils.fig_to_markdown(fig)
    plt.close(fig)

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(df.shape[0]),
            "preview": dg.MetadataValue.md(df.head(10).to_markdown()),
            "adhoc_report": dg.MetadataValue.md(img_md),
        }
    )
