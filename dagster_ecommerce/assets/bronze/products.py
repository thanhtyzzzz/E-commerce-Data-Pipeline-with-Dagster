"""Bronze layer - Raw products data"""
from dagster import asset, AssetExecutionContext, MetadataValue
import pandas as pd
from ...resources.api_client import MockAPIClient
from ...resources.api_client import PublicAPIClient 

@asset(
    group_name="bronze",
    compute_kind="python"
)
def raw_products(
    context: AssetExecutionContext,
    api_client: PublicAPIClient
    
) -> pd.DataFrame:
    """Extract product catalog from FakeStore API"""
    
    context.log.info("Fetching products from FakeStore API...")
    products_data = api_client.get_products()
    df = pd.DataFrame(products_data)
    
    # Save
    output_path = "data/raw/products/products.parquet"
    df.to_parquet(output_path, index=False)
    
    context.add_output_metadata({
        "num_products": len(df),
        "categories": MetadataValue.md(
            df['category'].value_counts().to_markdown()
        ),
        "avg_price": f"${df['price'].mean():.2f}",
        "preview": MetadataValue.md(df.head(10).to_markdown())
    })
    
    return df