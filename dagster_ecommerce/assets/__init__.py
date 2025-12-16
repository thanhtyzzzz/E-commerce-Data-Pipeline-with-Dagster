"""All assets"""
from dagster import load_assets_from_modules
from . import bronze, silver, gold

bronze_assets = load_assets_from_modules([bronze])
silver_assets = load_assets_from_modules([silver])
gold_assets = load_assets_from_modules([gold])

all_assets = [*bronze_assets, *silver_assets, *gold_assets]

__all__ = ["all_assets"]