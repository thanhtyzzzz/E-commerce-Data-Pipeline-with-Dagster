"""External API client resource - Dùng public APIs"""
from dagster import ConfigurableResource
import requests
from typing import Dict, Any, List
import time
from datetime import datetime, timedelta
import random


class PublicAPIClient(ConfigurableResource):
    """
    Client cho public APIs - KHÔNG CẦN API KEY
    Dùng JSONPlaceholder và FakeStore API
    """
    
    jsonplaceholder_url: str = "https://jsonplaceholder.typicode.com"
    fakestore_url: str = "https://fakestoreapi.com"
    timeout: int = 30
    max_retries: int = 3
    
    def _make_request(self, url: str, params: Dict = None) -> Any:
        """Make API request with retry logic"""
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def get_orders(self, start_date: str, end_date: str) -> List[Dict]:
        """
        Generate realistic orders data
        Combines real products from FakeStore API with synthetic order data
        """
        # Get real products from FakeStore API
        products = self._make_request(f"{self.fakestore_url}/products")
        
        # Generate orders for the date range
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date)
        
        orders = []
        order_id = 1
        
        # Generate 50-100 orders per day
        current_date = start
        while current_date <= end:
            num_orders = random.randint(50, 100)
            
            for _ in range(num_orders):
                product = random.choice(products)
                quantity = random.randint(1, 5)
                
                orders.append({
                    "order_id": order_id,
                    "customer_id": random.randint(1, 100),
                    "product_id": product['id'],
                    "product_name": product['title'],
                    "category": product['category'],
                    "quantity": quantity,
                    "unit_price": round(product['price'], 2),
                    "total_amount": round(product['price'] * quantity, 2),
                    "order_date": current_date.isoformat(),
                    "status": random.choice(["completed", "pending", "shipped"])
                })
                order_id += 1
            
            current_date += timedelta(days=1)
        
        return orders
    
    def get_products(self) -> List[Dict]:
        """
        Fetch real products from FakeStore API
        """
        products = self._make_request(f"{self.fakestore_url}/products")
        
        # Enrich with additional fields
        for product in products:
            product['stock'] = random.randint(10, 500)
            product['supplier'] = random.choice([
                "Global Electronics", "Fashion World", "Book Depot", 
                "Jewelry Co", "Tech Supplies"
            ])
        
        return products
    
    def get_users(self) -> List[Dict]:
        """
        Fetch real users from JSONPlaceholder API
        """
        users = self._make_request(f"{self.jsonplaceholder_url}/users")
        
        # Enrich user data
        segments = ["Premium", "Standard", "Basic"]
        
        for user in users:
            user['customer_id'] = user['id']
            user['customer_segment'] = random.choice(segments)
            user['signup_date'] = (
                datetime.now() - timedelta(days=random.randint(30, 730))
            ).date().isoformat()
            user['total_lifetime_purchases'] = random.randint(1, 50)
        
        return users


class MockAPIClient(ConfigurableResource):
    """
    Fallback mock client nếu không có internet
    """
    
    def get_orders(self, start_date: str, end_date: str) -> List[Dict]:
        """Return mock order data"""
        from faker import Faker
        fake = Faker()
        
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date)
        days = (end - start).days + 1
        
        orders = []
        for i in range(days * 50):  # 50 orders per day
            order_date = start + timedelta(days=i // 50)
            orders.append({
                "order_id": i + 1,
                "customer_id": fake.random_int(1, 100),
                "product_id": fake.random_int(1, 50),
                "product_name": fake.word().title() + " " + fake.word().title(),
                "category": fake.random_element(["Electronics", "Clothing", "Books", "Jewelry"]),
                "quantity": fake.random_int(1, 5),
                "unit_price": round(fake.random.uniform(10, 200), 2),
                "total_amount": round(fake.random.uniform(10, 500), 2),
                "order_date": order_date.isoformat(),
                "status": fake.random_element(["completed", "pending", "shipped"])
            })
        
        return orders
    
    def get_products(self) -> List[Dict]:
        """Return mock product data"""
        from faker import Faker
        fake = Faker()
        
        categories = ["Electronics", "Clothing", "Books", "Jewelry", "Home"]
        
        return [
            {
                "id": i,
                "title": fake.word().title() + " " + fake.word().title(),
                "category": fake.random_element(categories),
                "price": round(fake.random.uniform(5, 200), 2),
                "stock": fake.random_int(0, 500),
                "supplier": fake.company()
            }
            for i in range(1, 51)
        ]
    
    def get_users(self) -> List[Dict]:
        """Return mock user data"""
        from faker import Faker
        fake = Faker()
        
        return [
            {
                "id": i,
                "customer_id": i,
                "name": fake.name(),
                "email": fake.email(),
                "phone": fake.phone_number(),
                "address": {
                    "city": fake.city(),
                    "street": fake.street_address(),
                    "zipcode": fake.zipcode()
                },
                "customer_segment": fake.random_element(["Premium", "Standard", "Basic"]),
                "signup_date": fake.date_between(start_date="-2y", end_date="today").isoformat(),
                "total_lifetime_purchases": fake.random_int(1, 50)
            }
            for i in range(1, 101)
        ]