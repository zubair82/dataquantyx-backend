"""
CSV Parser Service

This module provides utilities for parsing and analyzing CSV files,
including column type detection and data validation.
"""

import os
from typing import List, Dict, Any, Tuple
import pandas as pd
import numpy as np


class CSVParser:
    """Parser for CSV files with type detection and validation"""
    def get_status(self) -> str:
        """
        Return a status string for the file (e.g., 'completed', 'empty', 'invalid')
        """
        if self.df is None:
            try:
                self.parse()
            except Exception:
                return "invalid"
        if self.df.empty:
            return "empty"
        return "completed"

    def get_file_size(self) -> int:
        """
        Return the file size in bytes
        """
        return os.path.getsize(self.filepath)
    
    def __init__(self, filepath: str):
        """
        Initialize CSV parser
        
        Args:
            filepath: Path to the CSV file
            
        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file is not a valid CSV
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")
        
        if not filepath.endswith(".csv"):
            raise ValueError("File must be a CSV file")
        
        self.filepath = filepath
        self.df = None
        self.metadata = {}
    
    def parse(self) -> pd.DataFrame:
        """
        Parse CSV file and return DataFrame
        Also cleans non-numeric values in numeric columns
        
        Returns:
            pd.DataFrame: Parsed and cleaned data
            
        Raises:
            Exception: If parsing fails
        """
        try:
            self.df = pd.read_csv(self.filepath)
            # Clean non-numeric values in numeric columns
            self._clean_non_numeric_values()
            return self.df
        except Exception as e:
            raise Exception(f"Failed to parse CSV: {str(e)}")
    
    def _clean_non_numeric_values(self) -> None:
        """
        Clean non-numeric values in columns that should be numeric.
        Converts invalid values to NaN and attempts type conversion.
        Threshold: 75% of values must be numeric-convertible to treat column as numeric
        """
        if self.df is None:
            return
        
        for col in self.df.columns:
            # Try to convert to numeric
            try:
                # Attempt conversion with 'coerce' to convert invalid values to NaN
                converted = pd.to_numeric(self.df[col], errors='coerce')
                
                # Check if conversion was successful (at least 75% of values converted)
                non_null_count = self.df[col].notna().sum()
                converted_count = converted.notna().sum()
                
                if non_null_count > 0 and converted_count >= non_null_count * 0.75:
                    # If at least 50% of values converted successfully, use the numeric version
                    self.df[col] = converted
            except Exception:
                # If conversion fails, leave the column as is
                pass
    
    def get_columns(self) -> List[str]:
        """
        Get list of column names
        
        Returns:
            List of column names
        """
        if self.df is None:
            self.parse()
        
        return self.df.columns.tolist()
    
    def get_column_types(self) -> Dict[str, str]:
        """
        Detect and return column types
        
        Returns:
            Dict mapping column name to type (numeric, string, datetime, boolean)
        """
        if self.df is None:
            self.parse()
        
        column_types = {}
        
        for col in self.df.columns:
            dtype = self.df[col].dtype
            
            # Determine column type
            if pd.api.types.is_numeric_dtype(dtype):
                column_types[col] = "numeric"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                column_types[col] = "datetime"
            elif pd.api.types.is_bool_dtype(dtype):
                column_types[col] = "boolean"
            else:
                # Try to infer datetime if not already detected
                try:
                    pd.to_datetime(self.df[col], errors='coerce')
                    non_null_count = self.df[col].notna().sum()
                    converted_count = pd.to_datetime(
                        self.df[col], errors='coerce'
                    ).notna().sum()
                    
                    if converted_count > non_null_count * 0.8:  # 80% conversion rate
                        column_types[col] = "datetime"
                    else:
                        column_types[col] = "string"
                except:
                    column_types[col] = "string"
        
        return column_types
    
    def get_numeric_columns(self) -> List[str]:
        """
        Get list of numeric column names
        
        Returns:
            List of numeric column names
        """
        column_types = self.get_column_types()
        return [col for col, dtype in column_types.items() if dtype == "numeric"]
    
    def get_cleaning_report(self) -> Dict[str, Dict[str, Any]]:
        """
        Get report of cleaned values
        
        Returns:
            Dict with cleaning information per column
        """
        if self.df is None:
            self.parse()
        
        cleaning_report = {}
        
        for col in self.df.columns:
            # Try to convert to numeric to see what would be cleaned
            converted = pd.to_numeric(self.df[col], errors='coerce')
            
            # Count values that were converted to NaN
            original_non_null = self.df[col].notna().sum()
            converted_non_null = converted.notna().sum()
            cleaned_count = original_non_null - converted_non_null
            
            # Only report if there's actual cleaning and column is/should-be numeric
            if cleaned_count > 0:
                converted_count = converted.notna().sum()
                if original_non_null > 0 and converted_count >= original_non_null * 0.75:
                    non_numeric_values = self.df.loc[converted.isna() & self.df[col].notna(), col].unique()
                    cleaning_report[col] = {
                        "cleaned_count": int(cleaned_count),
                        "original_count": int(original_non_null),
                        "non_numeric_values": [str(v) for v in non_numeric_values[:10]]  # Show first 10
                    }
        
        return cleaning_report
    
    def get_row_count(self) -> int:
        """
        Get number of rows
        
        Returns:
            Number of rows
        """
        if self.df is None:
            self.parse()
        
        return len(self.df)
    
    def get_summary_statistics(self) -> Dict[str, Dict[str, Any]]:
        """
        Calculate summary statistics for numeric columns
        
        Returns:
            Dict with statistics for each numeric column
        """
        if self.df is None:
            self.parse()
        
        stats = {}
        numeric_cols = self.get_numeric_columns()
        
        for col in numeric_cols:
            stats[col] = {
                "min": float(self.df[col].min()),
                "max": float(self.df[col].max()),
                "mean": float(self.df[col].mean()),
                "median": float(self.df[col].median()),
                "std": float(self.df[col].std()),
                "count": int(self.df[col].count()),
                "missing": int(self.df[col].isna().sum())
            }
        
        return stats
    
    def get_missing_values(self) -> Dict[str, int]:
        """
        Get count of missing values per column
        
        Returns:
            Dict mapping column name to missing value count
        """
        if self.df is None:
            self.parse()
        
        return self.df.isna().sum().to_dict()
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get complete metadata about the CSV file
        
        Returns:
            Dictionary containing comprehensive file metadata
        """
        if self.df is None:
            self.parse()
        
        column_types = self.get_column_types()
        numeric_cols = self.get_numeric_columns()
        missing_vals = self.get_missing_values()
        cleaning_report = self.get_cleaning_report()
        
        return {
            "filename": os.path.basename(self.filepath),
            "filepath": self.filepath,
            "row_count": self.get_row_count(),
            "columns": self.get_columns(),
            "column_types": column_types,
            "numeric_columns": numeric_cols,
            "missing_values": missing_vals,
            "statistics": self.get_summary_statistics(),
            "file_size_bytes": os.path.getsize(self.filepath),
            "cleaning_report": cleaning_report
        }
    
    def validate(self) -> Tuple[bool, List[str]]:
        """
        Validate CSV file
        
        Returns:
            Tuple of (is_valid: bool, errors: List[str])
        """
        errors = []
        
        try:
            if self.df is None:
                self.parse()
            
            # Check for empty dataframe
            if len(self.df) == 0:
                errors.append("CSV file is empty")
            
            # Check for columns
            if len(self.df.columns) == 0:
                errors.append("CSV has no columns")
            
            # Check for all null columns
            for col in self.df.columns:
                if self.df[col].isna().all():
                    errors.append(f"Column '{col}' contains all null values")
            
            return len(errors) == 0, errors
        
        except Exception as e:
            return False, [f"Validation failed: {str(e)}"]
    
    def get_sample(self, n: int = 5) -> Dict[str, Any]:
        """
        Get sample of first n rows
        
        Args:
            n: Number of rows to return
            
        Returns:
            Dict with sample data
        """
        if self.df is None:
            self.parse()
        
        return {
            "row_count": self.df.head(n).to_dict(orient='records'),
            "count": len(self.df.head(n))
        }


# Convenience functions

def parse_csv(filepath: str) -> pd.DataFrame:
    """
    Parse a CSV file and return DataFrame
    
    Args:
        filepath: Path to CSV file
        
    Returns:
        Parsed DataFrame
    """
    parser = CSVParser(filepath)
    return parser.parse()


def get_csv_metadata(filepath: str) -> Dict[str, Any]:
    """
    Get metadata for a CSV file
    
    Args:
        filepath: Path to CSV file
        
    Returns:
        Dictionary with file metadata
    """
    parser = CSVParser(filepath)
    return parser.get_metadata()


def validate_csv(filepath: str) -> Tuple[bool, List[str]]:
    """
    Validate a CSV file
    
    Args:
        filepath: Path to CSV file
        
    Returns:
        Tuple of (is_valid, errors)
    """
    parser = CSVParser(filepath)
    return parser.validate()
