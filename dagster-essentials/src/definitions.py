import dagster as dg

from .assets import metrics, trips
from .resources import duckdb_resource
from .jobs import trip_update_job, weekly_update_job
from .schedules import trip_update_schedule, weekly_update_schedule


trips_assets = dg.load_assets_from_modules([trips])
metrics_assets = dg.load_assets_from_modules([metrics])
all_assets = [*trips_assets, *metrics_assets]

all_jobs = [trip_update_job, weekly_update_job]
all_schedules = [trip_update_schedule, weekly_update_schedule]

defs = dg.Definitions(
    assets=all_assets,
    resources={"duckdb": duckdb_resource},
    jobs=all_jobs,
    schedules=all_schedules,
)
