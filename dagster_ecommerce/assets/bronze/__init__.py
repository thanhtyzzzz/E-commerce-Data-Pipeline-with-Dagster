"""Bronze layer assets"""
from .orders import raw_orders
from .customers import raw_customers
from .products import raw_products

__all__ = ["raw_orders", "raw_customers", "raw_products"]