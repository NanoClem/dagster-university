import os

import dagster as dg
from dagster_duckdb import DuckDBResource

from .assets import metrics, trips


all_assets = dg.load_assets_from_modules([trips, metrics])

defs = dg.Definitions(
    assets=all_assets,
    resources={"duckdb": DuckDBResource(database=os.getenv("DUCKDB_DATABASE"))},
)
