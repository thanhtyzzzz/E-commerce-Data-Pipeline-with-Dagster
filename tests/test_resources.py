"""Test resources"""
import pytest
from dagster_ecommerce.resources.api_client import PublicAPIClient
from dagster_ecommerce.utils.validators import DataValidator
import pandas as pd


def test_public_api_client():
    """Test public API client"""
    client = PublicAPIClient()
    
    # Test orders
    orders = client.get_orders("2024-01-01", "2024-01-01")
    assert len(orders) > 0
    assert "order_id" in orders[0]
    
    # Test products (from real FakeStore API)
    products = client.get_products()
    assert len(products) > 0
    assert "id" in products[0]
    assert "title" in products[0]
    
    # Test users (from real JSONPlaceholder API)
    users = client.get_users()
    assert len(users) > 0
    assert "customer_id" in users[0]


def test_data_validator():
    """Test data validation utilities"""
    validator = DataValidator()
    
    # Test null check
    df = pd.DataFrame({
        "a": [1, 2, None],
        "b": [4, 5, 6]
    })
    result = validator.check_nulls(df, ["a", "b"])
    assert result["has_nulls"] == True
    assert result["null_counts"]["a"] == 1
    
    # Test email validation
    assert validator.validate_email("test@example.com") == True
    assert validator.validate_email("invalid-email") == False