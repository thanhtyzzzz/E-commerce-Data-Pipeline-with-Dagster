"""Production schedules"""
from dagster import (
    ScheduleDefinition,
    DefaultScheduleStatus,
    build_schedule_from_partitioned_job,
    define_asset_job
)
from ..assets.bronze.orders import daily_partitions


# Job for daily incremental load
daily_etl_job = define_asset_job(
    name="daily_etl_job",
    selection=["raw_orders", "clean_orders", "daily_sales_summary"]
)

# Schedule to run every day at 2 AM
daily_schedule = build_schedule_from_partitioned_job(
    daily_etl_job,
    hour_of_day=2,
    minute_of_hour=0,  # ← ĐÂY! SỬA THÀNH minute_of_hour
    default_status=DefaultScheduleStatus.RUNNING
)


# Full refresh job (weekly)
full_refresh_job = define_asset_job(
    name="full_refresh_job",
    selection="*"  # All assets
)

weekly_full_refresh = ScheduleDefinition(
    name="weekly_full_refresh",
    job=full_refresh_job,
    cron_schedule="0 3 * * 0",  # Every Sunday at 3 AM
    default_status=DefaultScheduleStatus.STOPPED
)