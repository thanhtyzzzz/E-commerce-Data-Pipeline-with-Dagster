"""Sensors for monitoring file uploads"""
from dagster import (
    sensor,
    RunRequest,
    SkipReason,
    SensorEvaluationContext,
    define_asset_job
)
import os
from pathlib import Path


# Job to process uploaded files
process_uploads_job = define_asset_job(
    name="process_uploads_job",
    selection=["raw_products", "raw_customers"]
)


@sensor(
    job=process_uploads_job,
    minimum_interval_seconds=60  # Check every minute
)
def csv_upload_sensor(context: SensorEvaluationContext):
    """
    Monitor data/raw/ for new CSV files
    Trigger processing when files are uploaded
    """
    upload_dir = Path("data/raw/uploads")
    upload_dir.mkdir(parents=True, exist_ok=True)
    
    # Get processed files from cursor
    processed_files = set(context.cursor.split(",") if context.cursor else [])
    
    # Find new CSV files
    csv_files = list(upload_dir.glob("*.csv"))
    new_files = [
        f for f in csv_files 
        if f.name not in processed_files
    ]
    
    if not new_files:
        return SkipReason("No new files to process")
    
    # Create run request for each new file
    for file in new_files:
        yield RunRequest(
            run_key=f"upload_{file.name}_{file.stat().st_mtime}",
            run_config={
                "ops": {
                    "process_csv": {
                        "config": {
                            "file_path": str(file)
                        }
                    }
                }
            }
        )
        processed_files.add(file.name)
    
    # Update cursor
    context.update_cursor(",".join(processed_files))