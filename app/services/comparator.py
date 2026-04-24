"""
Data Comparison Service

This module provides functionality for comparing two simulation datasets,
computing differences, and generating comparative visualizations.
"""

import os
from typing import Dict, Any, List, Tuple
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
matplotlib = __import__('matplotlib')
matplotlib.use('Agg')  # Use non-interactive backend

from app.utils.paths import PLOTS_DIR, public_asset_url


class DataComparator:
    """Comparator for analyzing differences between two datasets"""
    
    def __init__(self, filepath1: str, filepath2: str, output_dir: str = str(PLOTS_DIR)):
        """
        Initialize comparator with two CSV files
        
        Args:
            filepath1: Path to first CSV file
            filepath2: Path to second CSV file
            output_dir: Directory to save comparison plots
        """
        self.filepath1 = filepath1
        self.filepath2 = filepath2
        self.output_dir = output_dir
        self.df1 = None
        self.df2 = None
        self.common_columns = []
        self.common_numeric_columns = []
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    
    def _clean_non_numeric_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean non-numeric values in columns
        Threshold: 75% of values must be numeric-convertible
        
        Args:
            df: DataFrame to clean
            
        Returns:
            Cleaned DataFrame
        """
        for col in df.columns:
            try:
                converted = pd.to_numeric(df[col], errors='coerce')
                non_null_count = df[col].notna().sum()
                converted_count = converted.notna().sum()
                
                if non_null_count > 0 and converted_count >= non_null_count * 0.75:
                    df[col] = converted
            except Exception:
                pass
        
        return df
    
    def load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load both CSV files and clean data
        
        Returns:
            Tuple of (df1, df2)
        """
        self.df1 = pd.read_csv(self.filepath1)
        self.df2 = pd.read_csv(self.filepath2)
        
        # Clean both dataframes
        self.df1 = self._clean_non_numeric_values(self.df1)
        self.df2 = self._clean_non_numeric_values(self.df2)
        
        return self.df1, self.df2
    
    def equalize_lengths(self) -> None:
        """Trim both dataframes to the same length"""
        if self.df1 is None or self.df2 is None:
            self.load_data()
        
        min_len = min(len(self.df1), len(self.df2))
        self.df1 = self.df1.iloc[:min_len]
        self.df2 = self.df2.iloc[:min_len]
    
    def _get_common_time_column(self) -> str:
        """
        Find common time or timestamp column in both datasets
        Checks for 'time' first, then 'timestamp', case-insensitive
        
        Returns:
            Column name if found, None otherwise
        """
        if self.df1 is None or self.df2 is None:
            return None
        
        cols1 = [col.lower() for col in self.df1.columns]
        cols2 = [col.lower() for col in self.df2.columns]
        
        # Check for 'time' column
        if 'time' in cols1 and 'time' in cols2:
            return 'time'
        
        # Check for 'timestamp' column
        if 'timestamp' in cols1 and 'timestamp' in cols2:
            return 'timestamp'
        
        return None
    
    def find_common_columns(self) -> List[str]:
        """
        Find common numeric columns between both datasets
        
        Returns:
            List of common numeric column names
        """
        if self.df1 is None or self.df2 is None:
            self.load_data()
        
        # Get numeric columns from both
        numeric_cols1 = self.df1.select_dtypes(include=['number']).columns.tolist()
        numeric_cols2 = self.df2.select_dtypes(include=['number']).columns.tolist()
        
        # Find intersection
        self.common_numeric_columns = list(set(numeric_cols1) & set(numeric_cols2))
        
        return self.common_numeric_columns
    
    def compute_column_stats(self, df: pd.DataFrame, column: str) -> Dict[str, Any]:
        """
        Compute statistics for a column
        
        Args:
            df: DataFrame
            column: Column name
            
        Returns:
            Dictionary with statistics
        """
        data = df[column].dropna()
        
        if len(data) == 0:
            return {}
        
        return {
            "mean": float(data.mean()),
            "std": float(data.std()),
            "var": float(data.var()),
            "min": float(data.min()),
            "max": float(data.max()),
            "median": float(data.median())
        }
    
    def compute_differences(self) -> Dict[str, Dict[str, float]]:
        """
        Compute statistical differences between datasets for common columns
        
        Returns:
            Dict mapping column names to difference metrics
        """
        if self.df1 is None or self.df2 is None:
            self.load_data()
        
        self.equalize_lengths()
        self.find_common_columns()
        
        differences = {}
        
        for col in self.common_numeric_columns:
            stats1 = self.compute_column_stats(self.df1, col)
            stats2 = self.compute_column_stats(self.df2, col)
            
            if not stats1 or not stats2:
                continue
            
            differences[col] = {
                "mean_diff": round(stats2["mean"] - stats1["mean"], 4),
                "std_diff": round(stats2["std"] - stats1["std"], 4),
                "var_diff": round(stats2["var"] - stats1["var"], 4),
                "max_diff": round(stats2["max"] - stats1["max"], 4),
                "min_diff": round(stats2["min"] - stats1["min"], 4),
                "dataset1_mean": round(stats1["mean"], 4),
                "dataset2_mean": round(stats2["mean"], 4),
                "dataset1_std": round(stats1["std"], 4),
                "dataset2_std": round(stats2["std"], 4)
            }
        
        return differences
    
    def plot_comparison(self, column: str, file_id_1: str, file_id_2: str) -> str:
        """
        Create overlay comparison plot for a column
        Uses time/timestamp column as x-axis if available, otherwise uses index
        
        Args:
            column: Column name
            file_id_1: File ID for dataset 1
            file_id_2: File ID for dataset 2
            
        Returns:
            Path to saved plot
        """
        if self.df1 is None or self.df2 is None:
            self.load_data()
        
        self.equalize_lengths()
        
        if column not in self.df1.columns or column not in self.df2.columns:
            return None
        
        # Get data and drop NaN
        data1 = self.df1[column].dropna()
        data2 = self.df2[column].dropna()
        
        if len(data1) == 0 or len(data2) == 0:
            return None
        
        # Check for time/timestamp column
        time_column = self._get_common_time_column()
        xlabel = 'Index'
        x_data1 = data1.index.values
        x_data2 = data2.index.values
        
        if time_column:
            # Use the actual column name from df1 (with correct case)
            time_col_actual = [col for col in self.df1.columns if col.lower() == time_column.lower()][0]
            x_data1 = self.df1.iloc[data1.index][time_col_actual].values
            x_data2 = self.df2.iloc[data2.index][time_col_actual].values
            xlabel = time_column.capitalize()
        
        plt.figure(figsize=(12, 6))
        plt.plot(x_data1, data1.values, linewidth=1.5, label=f'Dataset 1', color='#2E86AB', alpha=0.7)
        plt.plot(x_data2, data2.values, linewidth=1.5, label=f'Dataset 2', color='#A23B72', alpha=0.7)
        plt.title(f'{column} Comparison', fontsize=14, fontweight='bold')
        plt.xlabel(xlabel, fontsize=12)
        plt.ylabel(column, fontsize=12)
        plt.legend(fontsize=11)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        # Save plot
        plot_filename = f"compare_{file_id_1}_{file_id_2}_{column.replace(' ', '_')}.png"
        plot_path = os.path.join(self.output_dir, plot_filename)
        plt.savefig(plot_path, dpi=100, bbox_inches='tight')
        plt.close()
        
        return public_asset_url("plots", plot_filename)
    
    def generate_all_comparison_plots(self, file_id_1: str, file_id_2: str) -> Dict[str, str]:
        """
        Generate comparison plots for all common numeric columns
        
        Args:
            file_id_1: File ID for dataset 1
            file_id_2: File ID for dataset 2
            
        Returns:
            Dict mapping column names to plot paths
        """
        if self.df1 is None or self.df2 is None:
            self.load_data()
        
        self.find_common_columns()
        
        plots = {}
        
        for col in self.common_numeric_columns:
            plot_path = self.plot_comparison(col, file_id_1, file_id_2)
            if plot_path:
                plots[col] = plot_path
        
        return plots
    
    def generate_insights(self) -> List[str]:
        """
        Generate comparison insights
        
        Returns:
            List of insight strings
        """
        if self.df1 is None or self.df2 is None:
            self.load_data()
        
        self.equalize_lengths()
        self.find_common_columns()
        
        differences = self.compute_differences()
        insights = []
        
        # Compare row counts
        if len(self.df1) != len(self.df2):
            insights.append(f"ℹ️  Datasets have different original row counts ({len(self.df1)} vs {len(self.df2)}) - trimmed to match")
        
        # Compare common columns
        if len(self.common_numeric_columns) == 0:
            insights.append("⚠️  No common numeric columns found for comparison")
            return insights
        
        insights.append(f"📊 Comparing {len(self.common_numeric_columns)} common numeric column(s)")
        
        # Generate insights for each column
        for col, diffs in differences.items():
            mean_diff = diffs["mean_diff"]
            std_diff = diffs["std_diff"]
            
            # Interpret mean difference
            if abs(mean_diff) > abs(diffs["dataset1_mean"]) * 0.2:  # 20% difference
                if mean_diff > 0:
                    insights.append(f"📈 {col}: Dataset 2 has significantly higher average ({diffs['dataset2_mean']} vs {diffs['dataset1_mean']})")
                else:
                    insights.append(f"📉 {col}: Dataset 2 has significantly lower average ({diffs['dataset2_mean']} vs {diffs['dataset1_mean']})")
            
            # Interpret variance difference
            if std_diff > abs(diffs["dataset1_std"]) * 0.2:  # 20% more variance
                insights.append(f"🔄 {col}: Dataset 2 is more variable (std: {diffs['dataset2_std']} vs {diffs['dataset1_std']})")
            elif std_diff < -abs(diffs["dataset1_std"]) * 0.2:  # 20% less variance
                insights.append(f"📌 {col}: Dataset 2 is more stable/consistent (std: {diffs['dataset2_std']} vs {diffs['dataset1_std']})")
        
        if not insights:
            insights.append("✅ Datasets are very similar in statistical characteristics")
        
        return insights


def compare_files(filepath1: str, filepath2: str, file_id_1: str, file_id_2: str) -> Dict[str, Any]:
    """
    Compare two CSV files and return comprehensive comparison
    
    Args:
        filepath1: Path to first CSV file
        filepath2: Path to second CSV file
        file_id_1: File ID for dataset 1
        file_id_2: File ID for dataset 2
        
    Returns:
        Comparison results dictionary
    """
    comparator = DataComparator(filepath1, filepath2)
    comparator.load_data()
    comparator.equalize_lengths()
    comparator.find_common_columns()
    
    return {
        "common_numeric_columns": comparator.common_numeric_columns,
        "differences": comparator.compute_differences(),
        "plots": comparator.generate_all_comparison_plots(file_id_1, file_id_2),
        "insights": comparator.generate_insights()
    }
