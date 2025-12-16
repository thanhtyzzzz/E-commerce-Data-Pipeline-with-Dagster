"""
Main Dagster definitions
Combines all assets, resources, schedules, and sensors
"""
from dagster import Definitions
from .assets import all_assets
from .resources import (
    DuckDBResource,
    PublicAPIClient
)
from .schedules import daily_schedule, weekly_full_refresh
from .sensors import csv_upload_sensor
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Define all resources
resources = {
    "duckdb": DuckDBResource(
        database_path=os.getenv("DB_PATH", "data/warehouse.duckdb")
    ),
    "api_client": PublicAPIClient(
        jsonplaceholder_url=os.getenv("JSONPLACEHOLDER_URL", "https://jsonplaceholder.typicode.com"),
        fakestore_url=os.getenv("FAKESTOREAPI_URL", "https://fakestoreapi.com")
    )
}

# Combine everything
defs = Definitions(
    assets=all_assets,
    resources=resources,
    schedules=[daily_schedule, weekly_full_refresh],
    sensors=[csv_upload_sensor]
)