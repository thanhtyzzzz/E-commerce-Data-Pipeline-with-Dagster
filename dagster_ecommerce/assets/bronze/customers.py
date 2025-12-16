"""Bronze layer - Raw customers data"""
from dagster import asset, AssetExecutionContext, MetadataValue
import pandas as pd
from ...resources.api_client import PublicAPIClient


@asset(
    group_name="bronze",
    compute_kind="python"
)
def raw_customers(
    context: AssetExecutionContext,
    api_client: PublicAPIClient
) -> pd.DataFrame:
    """
    Extract customer data from JSONPlaceholder API
    Real users with real data structure
    """
    context.log.info("Fetching customers from JSONPlaceholder API...")
    
    # Get users from public API
    users_data = api_client.get_users()
    df = pd.DataFrame(users_data)
    
    # Flatten address
    if 'address' in df.columns:
        df['city'] = df['address'].apply(lambda x: x.get('city', '') if isinstance(x, dict) else '')
        df['street'] = df['address'].apply(lambda x: x.get('street', '') if isinstance(x, dict) else '')
        df['zipcode'] = df['address'].apply(lambda x: x.get('zipcode', '') if isinstance(x, dict) else '')
    
    # Save
    output_path = "data/raw/customers/customers.parquet"
    df.to_parquet(output_path, index=False)
    
    context.add_output_metadata({
        "num_customers": len(df),
        "segments": MetadataValue.md(
            df['customer_segment'].value_counts().to_markdown()
        ),
        "preview": MetadataValue.md(df.head(10).to_markdown())
    })
    
    return df