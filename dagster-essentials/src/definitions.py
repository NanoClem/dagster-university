import dagster as dg

from .assets import metrics, requests, trips
from .jobs import adhoc_request_job, trip_update_job, weekly_update_job
from .resources import duckdb_resource
from .schedules import trip_update_schedule, weekly_update_schedule
from .sensors import adhoc_request_sensor


trips_assets = dg.load_assets_from_modules([trips])
metrics_assets = dg.load_assets_from_modules([metrics])
requests_assets = dg.load_assets_from_modules([requests])
all_assets = [*trips_assets, *metrics_assets, *requests_assets]

all_jobs = [trip_update_job, weekly_update_job, adhoc_request_job]
all_schedules = [trip_update_schedule, weekly_update_schedule]
all_sensors = [adhoc_request_sensor]

defs = dg.Definitions(
    assets=all_assets,
    resources={"duckdb": duckdb_resource},
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors,
)
