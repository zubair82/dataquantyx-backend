"""
Data Analysis Service

This module provides functionality for analyzing CSV data including
statistics computation and insight generation.
"""

from typing import Dict, Any, List
import pandas as pd
import numpy as np


class DataAnalyzer:
    """Analyzer for computing statistics and generating insights"""
    
    def __init__(self, filepath: str):
        """
        Initialize analyzer with CSV file
        
        Args:
            filepath: Path to CSV file
        """
        self.filepath = filepath
        self.df = None
        self.numeric_columns = []
    
    def load_data(self) -> pd.DataFrame:
        """Load CSV data and clean non-numeric values in numeric columns"""
        self.df = pd.read_csv(self.filepath)
        self._clean_non_numeric_values()
        self.numeric_columns = self.df.select_dtypes(include=['number']).columns.tolist()
        return self.df
    
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
    
    def get_summary_statistics(self) -> Dict[str, Dict[str, Any]]:
        """
        Compute summary statistics for numeric columns
        Automatically handles NaN values by excluding them from calculations
        
        Returns:
            Dict mapping column names to statistics
        """
        if self.df is None:
            self.load_data()
        
        summary = {}
        
        for col in self.numeric_columns:
            # Get non-null values
            data = self.df[col].dropna()
            
            # Only add to summary if there are values after dropping NaN
            if len(data) > 0:
                summary[col] = {
                    "min": float(data.min()),
                    "max": float(data.max()),
                    "mean": float(data.mean()),
                    "median": float(data.median()),
                    "std": float(data.std()),
                    "count": int(data.count()),
                    "missing": int(self.df[col].isna().sum())
                }
        
        return summary
    
    def detect_outliers(self, column: str, method: str = "iqr") -> List[int]:
        """
        Detect outliers in a numeric column
        
        Args:
            column: Column name
            method: Detection method ('iqr' or 'zscore')
            
        Returns:
            List of outlier indices
        """
        if column not in self.numeric_columns:
            return []
        
        data = self.df[column].dropna()
        
        if method == "iqr":
            Q1 = data.quantile(0.25)
            Q3 = data.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            outliers = data[(data < lower_bound) | (data > upper_bound)].index.tolist()
        
        elif method == "zscore":
            z_scores = np.abs((data - data.mean()) / data.std())
            outliers = data[z_scores > 3].index.tolist()
        
        else:
            outliers = []
        
        return outliers
    
    def detect_spikes(self, column: str, threshold: float = 2.0) -> List[int]:
        """
        Detect sudden spikes in a numeric column
        
        Args:
            column: Column name
            threshold: Standard deviation threshold for spike detection
            
        Returns:
            List of spike indices
        """
        if column not in self.numeric_columns:
            return []
        
        data = self.df[column].dropna()
        
        if len(data) < 2:
            return []
        
        # Calculate differences between consecutive values
        diff = data.diff().abs()
        
        # Find spikes based on threshold
        mean_diff = diff.mean()
        std_diff = diff.std()
        
        if std_diff == 0:
            return []
        
        spikes = diff[diff > (mean_diff + threshold * std_diff)].index.tolist()
        return spikes
    
    def detect_trends(self, column: str) -> Dict[str, Any]:
        """
        Detect trends in a numeric column
        
        Args:
            column: Column name
            
        Returns:
            Dict with trend information
        """
        if column not in self.numeric_columns:
            return {}
        
        data = self.df[column].dropna()
        
        if len(data) < 2:
            return {"trend": "insufficient_data"}
        
        # Simple trend detection using first and last values
        first_half = data.iloc[:len(data)//2].mean()
        second_half = data.iloc[len(data)//2:].mean()
        
        if second_half > first_half * 1.1:
            trend = "increasing"
        elif second_half < first_half * 0.9:
            trend = "decreasing"
        else:
            trend = "stable"
        
        # Calculate correlation with index (time)
        correlation = data.corr(pd.Series(range(len(data))))
        
        return {
            "trend": trend,
            "first_half_mean": float(first_half),
            "second_half_mean": float(second_half),
            "correlation_with_time": float(correlation)
        }
    
    def generate_insights(self) -> List[str]:
        """
        Generate insights from data analysis
        Handles NaN values by excluding them from calculations
        
        Returns:
            List of insight strings
        """
        if self.df is None:
            self.load_data()
        
        insights = []
        
        # Check for missing values
        missing_cols = [col for col in self.numeric_columns 
                       if self.df[col].isna().sum() > 0]
        if missing_cols:
            for col in missing_cols:
                missing_count = self.df[col].isna().sum()
                missing_pct = (missing_count / len(self.df)) * 100
                insights.append(f"⚠️  {col}: {missing_count} missing values ({missing_pct:.1f}% of data) - excluded from analysis")
        
        # Check for outliers
        for col in self.numeric_columns:
            outliers = self.detect_outliers(col)
            if len(outliers) > 0:
                non_null_count = self.df[col].notna().sum()
                outlier_pct = len(outliers) / non_null_count * 100 if non_null_count > 0 else 0
                insights.append(f"📊 {col}: {len(outliers)} outliers detected ({outlier_pct:.1f}% of non-null data)")
        
        # Check for spikes
        for col in self.numeric_columns:
            spikes = self.detect_spikes(col)
            if len(spikes) > 0:
                insights.append(f"⚡ {col}: {len(spikes)} spike(s) detected")
        
        # Check for trends
        for col in self.numeric_columns:
            trend_info = self.detect_trends(col)
            if trend_info.get("trend") and trend_info["trend"] != "stable":
                insights.append(f"📈 {col}: {trend_info['trend'].capitalize()} trend detected")
        
        # Check for high variance
        for col in self.numeric_columns:
            data = self.df[col].dropna()
            if len(data) > 0:
                std = data.std()
                mean = data.mean()
                if mean != 0:
                    cv = std / mean  # Coefficient of variation
                    if cv > 1:
                        insights.append(f"🔄 {col}: High variability (CV: {cv:.2f})")
        
        # Basic statistics insights
        if len(self.numeric_columns) > 0:
            insights.append(f"📋 Dataset contains {len(self.df)} rows and {len(self.numeric_columns)} numeric columns")
        
        return insights if insights else ["✅ No significant patterns detected"]
    
    def get_correlation_matrix(self) -> Dict[str, Dict[str, float]]:
        """
        Calculate correlation matrix between numeric columns
        
        Returns:
            Dict with correlation values
        """
        if self.df is None:
            self.load_data()
        
        if len(self.numeric_columns) < 2:
            return {}
        
        corr_matrix = self.df[self.numeric_columns].corr()
        
        # Convert to dict for JSON serialization
        return corr_matrix.to_dict()
    
    def get_column_distribution(self, column: str) -> Dict[str, Any]:
        """
        Get distribution info for a column
        Automatically handles NaN values by excluding them
        
        Args:
            column: Column name
            
        Returns:
            Distribution statistics
        """
        if column not in self.numeric_columns:
            return {}
        
        data = self.df[column].dropna()
        
        if len(data) == 0:
            return {}
        
        return {
            "min": float(data.min()),
            "q1": float(data.quantile(0.25)),
            "median": float(data.median()),
            "q3": float(data.quantile(0.75)),
            "max": float(data.max()),
            "mean": float(data.mean()),
            "std": float(data.std()),
            "skewness": float(data.skew()),
            "kurtosis": float(data.kurtosis()),
            "non_null_count": int(len(data)),
            "null_count": int(self.df[column].isna().sum())
        }


def analyze_file(filepath: str) -> Dict[str, Any]:
    """
    Analyze a CSV file and return comprehensive analysis
    
    Args:
        filepath: Path to CSV file
        
    Returns:
        Analysis results dictionary
    """
    analyzer = DataAnalyzer(filepath)
    analyzer.load_data()
    
    return {
        "summary": analyzer.get_summary_statistics(),
        "insights": analyzer.generate_insights(),
        "correlations": analyzer.get_correlation_matrix()
    }
