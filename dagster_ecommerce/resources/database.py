"""Database resources and connections"""
from dagster import ConfigurableResource, InitResourceContext
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import duckdb
from typing import Optional


class PostgresResource(ConfigurableResource):
    """PostgreSQL database connection"""
    
    host: str = "localhost"
    port: int = 5432
    database: str = "ecommerce"
    user: str = "postgres"
    password: str = "postgres"
    
    def get_engine(self) -> Engine:
        """Create SQLAlchemy engine"""
        connection_string = (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )
        return create_engine(connection_string)
    
    def execute_query(self, query: str):
        """Execute SQL query"""
        engine = self.get_engine()
        with engine.connect() as conn:
            return conn.execute(query)


class DuckDBResource(ConfigurableResource):
    """DuckDB local analytics database"""
    
    database_path: str = "data/analytics.duckdb"
    
    def get_connection(self):
        """Get DuckDB connection"""
        return duckdb.connect(self.database_path)
    
    def query(self, sql: str):
        """Execute query and return results"""
        conn = self.get_connection()
        return conn.execute(sql).fetchdf()