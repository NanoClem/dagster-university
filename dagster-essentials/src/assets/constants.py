import os

TAXI_ZONES_FILE_PATH = os.path.join("data", "raw", "taxi_zones.csv")
TAXI_TRIPS_TEMPLATE_FILE_PATH = os.path.join("data", "raw", "taxi_trips_{}.parquet")

START_DATE = "2023-01-01"
END_DATE = "2024-04-01"