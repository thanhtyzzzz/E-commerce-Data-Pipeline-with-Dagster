"""Test assets"""
import pytest
from dagster import materialize
from dagster_ecommerce.assets.bronze import raw_orders, raw_customers, raw_products
from dagster_ecommerce.assets.silver import clean_orders, clean_customers
from dagster_ecommerce.resources.api_client import PublicAPIClient
import pandas as pd


@pytest.fixture
def public_api_client():
    """Public API client for testing"""
    return PublicAPIClient()


def test_raw_orders_asset(public_api_client):
    """Test raw orders extraction"""
    result = materialize(
        [raw_orders],
        resources={"api_client": public_api_client},
        partition_key="2024-01-01"
    )
    assert result.success
    
    # Check output
    df = result.output_for_node("raw_orders")
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert "order_id" in df.columns
    assert "total_amount" in df.columns


def test_raw_customers_asset(public_api_client):
    """Test customers extraction"""
    result = materialize(
        [raw_customers],
        resources={"api_client": public_api_client}
    )
    assert result.success
    
    df = result.output_for_node("raw_customers")
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert "customer_id" in df.columns


def test_raw_products_asset(public_api_client):
    """Test products extraction"""
    result = materialize(
        [raw_products],
        resources={"api_client": public_api_client}
    )
    assert result.success
    
    df = result.output_for_node("raw_products")
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert "id" in df.columns