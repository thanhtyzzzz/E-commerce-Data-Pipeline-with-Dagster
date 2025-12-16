"""Gold layer - Customer analytics"""
from dagster import asset, AssetExecutionContext, MetadataValue
import pandas as pd


@asset(
    group_name="gold",
    compute_kind="python"
)
def customer_lifetime_value(
    context: AssetExecutionContext,
    clean_orders: pd.DataFrame,
    clean_customers: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate customer lifetime value and metrics
    """
    # Aggregate by customer
    customer_metrics = clean_orders.groupby('customer_id').agg({
        'order_id': 'count',
        'total_amount': ['sum', 'mean'],
        'order_date': ['min', 'max']
    }).reset_index()
    
    customer_metrics.columns = [
        'customer_id', 'total_orders', 'lifetime_value',
        'avg_order_value', 'first_order_date', 'last_order_date'
    ]
    
    # Join with customer details
    df = customer_metrics.merge(
        clean_customers[['customer_id', 'full_name', 'email', 
                        'customer_segment', 'customer_age_days']],
        on='customer_id',
        how='left'
    )
    
    # Calculate recency
    df['last_order_date'] = pd.to_datetime(df['last_order_date'])
    df['days_since_last_order'] = (
        pd.Timestamp.now() - df['last_order_date']
    ).dt.days
    
    # RFM Score (simple version)
    df['recency_score'] = pd.qcut(
        df['days_since_last_order'], 
        q=5, 
        labels=[5,4,3,2,1]
    ).astype(int)
    
    df['frequency_score'] = pd.qcut(
        df['total_orders'], 
        q=5, 
        labels=[1,2,3,4,5],
        duplicates='drop'
    ).astype(int)
    
    df['monetary_score'] = pd.qcut(
        df['lifetime_value'], 
        q=5, 
        labels=[1,2,3,4,5],
        duplicates='drop'
    ).astype(int)
    
    df['rfm_score'] = (
        df['recency_score'] + 
        df['frequency_score'] + 
        df['monetary_score']
    )
    
    # Segment customers
    def segment_customer(score):
        if score >= 13:
            return "Champions"
        elif score >= 10:
            return "Loyal"
        elif score >= 7:
            return "Potential"
        else:
            return "At Risk"
    
    df['rfm_segment'] = df['rfm_score'].apply(segment_customer)
    
    # Save
    output_path = "data/processed/customer_metrics/clv.parquet"
    df.to_parquet(output_path, index=False)
    
    # Metadata
    top_customers = df.nlargest(5, 'lifetime_value')[
        ['full_name', 'lifetime_value', 'total_orders', 'rfm_segment']
    ]
    
    context.add_output_metadata({
        "total_customers": len(df),
        "avg_lifetime_value": f"${df['lifetime_value'].mean():.2f}",
        "avg_orders_per_customer": f"{df['total_orders'].mean():.1f}",
        "top_5_customers": MetadataValue.md(top_customers.to_markdown()),
        "segment_distribution": MetadataValue.md(
            df['rfm_segment'].value_counts().to_markdown()
        )
    })
    
    return df