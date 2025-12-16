from dataclasses import dataclass
from pathlib import Path
import duckdb

@dataclass
class DuckDBResource:
    """Simple DuckDB resource for local dev."""
    db_path: str

    def connect(self):
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(self.db_path)
