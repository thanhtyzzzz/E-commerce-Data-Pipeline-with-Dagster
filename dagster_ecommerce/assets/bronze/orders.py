"""Bronze layer - Raw orders data"""
from dagster import (
    asset, 
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MetadataValue
)
import pandas as pd
from datetime import datetime
from ...resources.api_client import PublicAPIClient 


daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")


@asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    compute_kind="python"
)
def raw_orders(
    context: AssetExecutionContext,
    api_client: PublicAPIClient
) -> pd.DataFrame:
    """
    Extract raw orders from external API
    Partitioned by order date
    """
    partition_date = context.partition_key
    context.log.info(f"Fetching orders for {partition_date}")
    
    # Fetch from API
    orders_data = api_client.get_orders(
        start_date=partition_date,
        end_date=partition_date
    )
    
    df = pd.DataFrame(orders_data)
    
    # Save to parquet
    output_path = f"data/raw/orders/date={partition_date}/orders.parquet"
    df.to_parquet(output_path, index=False)
    
    # Add metadata
    context.add_output_metadata({
        "num_records": len(df),
        "columns": MetadataValue.md(", ".join(df.columns)),
        "preview": MetadataValue.md(df.head(10).to_markdown()),
        "date_range": f"{df['order_date'].min()} to {df['order_date'].max()}",
        "total_revenue": f"${df['total_amount'].sum():,.2f}"
    })
    
    return df