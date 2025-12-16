"""Dagster E-commerce Pipeline"""
from dagster import Definitions
from .definitions import defs

__version__ = "1.0.0"
__all__ = ["defs"]