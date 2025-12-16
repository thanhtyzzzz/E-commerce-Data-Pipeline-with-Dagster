"""Silver layer - Cleaned customers"""
from dagster import asset, AssetExecutionContext, MetadataValue
import pandas as pd
from ...utils.validators import DataValidator


@asset(
    group_name="silver",
    compute_kind="python"
)
def clean_customers(
    context: AssetExecutionContext,
    raw_customers: pd.DataFrame
) -> pd.DataFrame:
    """Clean and enrich customer data"""
    
    df = raw_customers.copy()
    validator = DataValidator()
    
    initial_count = len(df)
    
    # Validate emails
    df['email_valid'] = df['email'].apply(validator.validate_email)
    df = df[df['email_valid'] == True]
    
    # Standardize names
    df['first_name'] = df['first_name'].str.title()
    df['last_name'] = df['last_name'].str.title()
    df['full_name'] = df['first_name'] + ' ' + df['last_name']
    
    # Calculate customer age (days since signup)
    df['signup_date'] = pd.to_datetime(df['signup_date'])
    df['customer_age_days'] = (pd.Timestamp.now() - df['signup_date']).dt.days
    
    # Save
    output_path = "data/staging/customers/customers.parquet"
    df.to_parquet(output_path, index=False)
    
    context.add_output_metadata({
        "initial_records": initial_count,
        "final_records": len(df),
        "invalid_emails_removed": initial_count - len(df),
        "avg_customer_age_days": f"{df['customer_age_days'].mean():.0f}",
        "segments": MetadataValue.md(
            df['customer_segment'].value_counts().to_markdown()
        )
    })
    
    return df