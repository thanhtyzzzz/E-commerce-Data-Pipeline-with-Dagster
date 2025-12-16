"""Silver layer - Cleaned orders"""
from dagster import (
    asset,
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MetadataValue,
    AssetCheckResult,
    asset_check
)
import pandas as pd
from ...utils.validators import DataValidator


daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")


@asset(
    partitions_def=daily_partitions,
    group_name="silver",
    compute_kind="python"
)
def clean_orders(
    context: AssetExecutionContext,
    raw_orders: pd.DataFrame
) -> pd.DataFrame:
    """
    Clean and validate orders data
    - Remove duplicates
    - Validate amounts
    - Handle nulls
    """
    df = raw_orders.copy()
    
    initial_count = len(df)
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['order_id'])
    
    # Remove invalid amounts
    df = df[df['total_amount'] > 0]
    df = df[df['quantity'] > 0]
    
    # Remove nulls
    df = df.dropna(subset=['order_id', 'customer_id', 'product_id'])
    
    # Add derived columns
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['year'] = df['order_date'].dt.year
    df['month'] = df['order_date'].dt.month
    df['day_of_week'] = df['order_date'].dt.day_name()
    df['unit_price'] = df['total_amount'] / df['quantity']
    
    # Save
    partition_date = context.partition_key
    output_path = f"data/staging/orders/date={partition_date}/orders.parquet"
    df.to_parquet(output_path, index=False)
    
    # Metadata
    context.add_output_metadata({
        "initial_records": initial_count,
        "final_records": len(df),
        "records_dropped": initial_count - len(df),
        "data_quality_score": f"{(len(df)/initial_count)*100:.2f}%",
        "total_revenue": f"${df['total_amount'].sum():,.2f}",
        "avg_order_value": f"${df['total_amount'].mean():.2f}"
    })
    
    return df


@asset_check(asset=clean_orders)
def check_clean_orders_quality(clean_orders: pd.DataFrame) -> AssetCheckResult:
    """Data quality check for clean orders"""
    
    validator = DataValidator()
    
    # Check for nulls
    null_check = validator.check_nulls(
        clean_orders, 
        ['order_id', 'customer_id', 'total_amount']
    )
    
    # Check amount ranges
    amount_check = validator.validate_range(
        clean_orders,
        'total_amount',
        0.01,
        10000
    )
    
    passed = not null_check['has_nulls'] and amount_check['valid']
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "null_checks": str(null_check),
            "amount_validation": str(amount_check)
        }
    )