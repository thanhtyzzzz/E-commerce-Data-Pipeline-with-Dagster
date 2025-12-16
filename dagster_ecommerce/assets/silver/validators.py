"""Data validation utilities"""
import pandas as pd
from typing import List, Dict
import re


class DataValidator:
    """Data quality validation"""
    
    @staticmethod
    def check_nulls(df: pd.DataFrame, columns: List[str]) -> Dict:
        """Check for null values in specified columns"""
        null_counts = df[columns].isnull().sum()
        return {
            "has_nulls": null_counts.sum() > 0,
            "null_counts": null_counts.to_dict()
        }
    
    @staticmethod
    def check_duplicates(df: pd.DataFrame, columns: List[str]) -> Dict:
        """Check for duplicates"""
        duplicates = df.duplicated(subset=columns, keep=False)
        return {
            "has_duplicates": duplicates.sum() > 0,
            "duplicate_count": duplicates.sum()
        }
    
    @staticmethod
    def validate_email(email: str) -> bool:
        """Validate email format"""
        pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
        return bool(re.match(pattern, email))
    
    @staticmethod
    def validate_range(df: pd.DataFrame, column: str, min_val: float, max_val: float) -> Dict:
        """Check if values are within range"""
        out_of_range = ((df[column] < min_val) | (df[column] > max_val)).sum()
        return {
            "valid": out_of_range == 0,
            "out_of_range_count": out_of_range
        }