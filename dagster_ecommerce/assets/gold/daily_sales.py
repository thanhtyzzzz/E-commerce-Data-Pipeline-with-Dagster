"""Gold layer - Daily sales metrics"""
from dagster import (
    asset,
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MetadataValue
)
import pandas as pd


daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")


@asset(
    partitions_def=daily_partitions,
    group_name="gold",
    compute_kind="python"
)
def daily_sales_summary(
    context: AssetExecutionContext,
    clean_orders: pd.DataFrame,
    raw_products: pd.DataFrame
) -> pd.DataFrame:
    """
    Daily sales summary with product details
    """
    # Join with products
    df = clean_orders.merge(
        raw_products[['product_id', 'name', 'category']],
        on='product_id',
        how='left'
    )
    
    # Aggregate daily metrics
    summary = df.groupby(['order_date', 'category']).agg({
        'order_id': 'count',
        'total_amount': ['sum', 'mean'],
        'quantity': 'sum',
        'customer_id': 'nunique'
    }).reset_index()
    
    summary.columns = [
        'date', 'category', 'num_orders', 
        'total_revenue', 'avg_order_value',
        'total_quantity', 'unique_customers'
    ]
    
    # Calculate metrics
    summary['revenue_per_customer'] = (
        summary['total_revenue'] / summary['unique_customers']
    )
    
    # Save
    partition_date = context.partition_key
    output_path = f"data/processed/daily_sales/date={partition_date}/sales.parquet"
    summary.to_parquet(output_path, index=False)
    
    context.add_output_metadata({
        "total_revenue": f"${summary['total_revenue'].sum():,.2f}",
        "total_orders": int(summary['num_orders'].sum()),
        "unique_customers": int(summary['unique_customers'].sum()),
        "top_category": summary.nlargest(1, 'total_revenue')['category'].values[0],
        "preview": MetadataValue.md(summary.to_markdown())
    })
    
    return summary