"""
Data Visualization Service

This module provides functionality for generating plots and visualizations
from CSV data.
"""

import os
from typing import List, Dict, Any
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend

from app.utils.paths import PLOTS_DIR, public_asset_url


class DataPlotter:
    """Plotter for generating data visualizations"""
    
    def __init__(self, filepath: str, output_dir: str = str(PLOTS_DIR)):
        """
        Initialize plotter
        
        Args:
            filepath: Path to CSV file
            output_dir: Directory to save plots
        """
        self.filepath = filepath
        self.output_dir = output_dir
        self.df = None
        self.numeric_columns = []
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    
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
                    # If at least 75% of values converted successfully, use the numeric version
                    self.df[col] = converted
            except Exception:
                # If conversion fails, leave the column as is
                pass
    
    def get_time_column(self) -> str:
        """
        Get the time/timestamp column name if it exists
        
        Returns:
            Column name ('time', 'timestamp', or None)
        """
        if self.df is None:
            self.load_data()
        
        # Check for 'time' column (case-insensitive)
        for col in self.df.columns:
            if col.lower() == 'time':
                return col
        
        # Check for 'timestamp' column (case-insensitive)
        for col in self.df.columns:
            if col.lower() == 'timestamp':
                return col
        
        return None
    
    def plot_numeric_column(self, column: str, file_id: str) -> str:
        """
        Create a line plot for a numeric column
        Plots against time/timestamp column if available, otherwise uses index
        Automatically handles NaN values by excluding them
        
        Args:
            column: Column name
            file_id: File identifier for naming
            
        Returns:
            Path to saved plot
        """
        if self.df is None:
            self.load_data()
        
        if column not in self.numeric_columns:
            return None
        
        # Get data and drop NaN values
        data = self.df[column].dropna()
        
        if len(data) == 0:
            return None
        
        # Get time/timestamp column if available
        time_col = self.get_time_column()
        
        plt.figure(figsize=(12, 5))
        
        if time_col:
            # Use time/timestamp column as x-axis
            x_data = self.df.loc[data.index, time_col]
            plt.plot(x_data, data.values, linewidth=1.5, color='#2E86AB')
            plt.title(f'{column} vs {time_col}', fontsize=14, fontweight='bold')
            plt.xlabel(time_col, fontsize=12)
        else:
            # Use index as x-axis
            plt.plot(data, linewidth=1.5, color='#2E86AB')
            plt.title(f'{column} Over Time', fontsize=14, fontweight='bold')
            plt.xlabel('Index', fontsize=12)
        
        plt.ylabel(column, fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        # Save plot
        plot_filename = f"{file_id}_{column.replace(' ', '_')}.png"
        plot_path = os.path.join(self.output_dir, plot_filename)
        plt.savefig(plot_path, dpi=100, bbox_inches='tight')
        plt.close()
        
        return public_asset_url("plots", plot_filename)
    
    def plot_histogram(self, column: str, file_id: str) -> str:
        """
        Create a histogram for a numeric column
        Automatically handles NaN values by excluding them
        
        Args:
            column: Column name
            file_id: File identifier for naming
            
        Returns:
            Path to saved plot
        """
        if self.df is None:
            self.load_data()
        
        if column not in self.numeric_columns:
            return None
        
        # Get data and drop NaN values
        data = self.df[column].dropna()
        
        if len(data) == 0:
            return None
        
        plt.figure(figsize=(10, 5))
        plt.hist(data, bins=30, color='#A23B72', alpha=0.7, edgecolor='black')
        plt.title(f'{column} Distribution', fontsize=14, fontweight='bold')
        plt.xlabel(column, fontsize=12)
        plt.ylabel('Frequency', fontsize=12)
        plt.grid(True, alpha=0.3, axis='y')
        plt.tight_layout()
        
        # Save plot
        plot_filename = f"{file_id}_{column.replace(' ', '_')}_hist.png"
        plot_path = os.path.join(self.output_dir, plot_filename)
        plt.savefig(plot_path, dpi=100, bbox_inches='tight')
        plt.close()
        
        return public_asset_url("plots", plot_filename)
    
    def plot_box_plot(self, columns: List[str], file_id: str) -> str:
        """
        Create a box plot for multiple numeric columns
        Automatically handles NaN values by excluding them
        
        Args:
            columns: List of column names
            file_id: File identifier for naming
            
        Returns:
            Path to saved plot
        """
        if self.df is None:
            self.load_data()
        
        valid_cols = [col for col in columns if col in self.numeric_columns]
        
        if not valid_cols:
            return None
        
        plt.figure(figsize=(12, 6))
        # boxplot automatically ignores NaN values
        self.df[valid_cols].boxplot(ax=plt.gca())
        plt.title('Box Plot - Numeric Columns', fontsize=14, fontweight='bold')
        plt.ylabel('Value', fontsize=12)
        plt.grid(True, alpha=0.3, axis='y')
        plt.tight_layout()
        
        # Save plot
        plot_filename = f"{file_id}_boxplot.png"
        plot_path = os.path.join(self.output_dir, plot_filename)
        plt.savefig(plot_path, dpi=100, bbox_inches='tight')
        plt.close()
        
        return public_asset_url("plots", plot_filename)
    
    def plot_correlation_heatmap(self, file_id: str) -> str:
        """
        Create a correlation heatmap for numeric columns
        
        Args:
            file_id: File identifier for naming
            
        Returns:
            Path to saved plot
        """
        if self.df is None:
            self.load_data()
        
        if len(self.numeric_columns) < 2:
            return None
        
        try:
            import seaborn as sns
            
            plt.figure(figsize=(10, 8))
            corr_matrix = self.df[self.numeric_columns].corr()
            sns.heatmap(corr_matrix, annot=True, fmt='.2f', cmap='coolwarm', 
                       center=0, square=True, cbar_kws={"shrink": 0.8})
            plt.title('Correlation Matrix Heatmap', fontsize=14, fontweight='bold')
            plt.tight_layout()
            
            # Save plot
            plot_filename = f"{file_id}_heatmap.png"
            plot_path = os.path.join(self.output_dir, plot_filename)
            plt.savefig(plot_path, dpi=100, bbox_inches='tight')
            plt.close()
            
            return public_asset_url("plots", plot_filename)
        except ImportError:
            # Fallback if seaborn not available
            return None
    
    def plot_scatter_matrix(self, file_id: str) -> str:
        """
        Create a scatter plot matrix for numeric columns
        
        Args:
            file_id: File identifier for naming
            
        Returns:
            Path to saved plot
        """
        if self.df is None:
            self.load_data()
        
        if len(self.numeric_columns) < 2:
            return None
        
        try:
            from pandas.plotting import scatter_matrix
            
            axes = scatter_matrix(self.df[self.numeric_columns], figsize=(14, 14), 
                                 alpha=0.7, diagonal='hist')
            
            # Save plot
            plot_filename = f"{file_id}_scatter_matrix.png"
            plot_path = os.path.join(self.output_dir, plot_filename)
            plt.savefig(plot_path, dpi=100, bbox_inches='tight')
            plt.close()
            
            return public_asset_url("plots", plot_filename)
        except Exception:
            return None
    
    def generate_all_plots(self, file_id: str) -> Dict[str, List[str]]:
        """
        Generate all available plots for the data
        
        Args:
            file_id: File identifier
            
        Returns:
            Dict with plot categories and paths
        """
        if self.df is None:
            self.load_data()
        
        plots = {
            "line_plots": [],
            "histograms": [],
            "box_plots": [],
            "heatmap": None
        }
        
        # Generate line plots for each numeric column
        for col in self.numeric_columns:
            plot_path = self.plot_numeric_column(col, file_id)
            if plot_path:
                plots["line_plots"].append(plot_path)
        
        # Generate histograms
        for col in self.numeric_columns:
            plot_path = self.plot_histogram(col, file_id)
            if plot_path:
                plots["histograms"].append(plot_path)
        
        # Generate box plot
        box_plot = self.plot_box_plot(self.numeric_columns, file_id)
        if box_plot:
            plots["box_plots"].append(box_plot)
        
        # Generate heatmap
        heatmap = self.plot_correlation_heatmap(file_id)
        if heatmap:
            plots["heatmap"] = heatmap
        
        return plots


def generate_plots(filepath: str, file_id: str) -> Dict[str, List[str]]:
    """
    Generate plots for a CSV file
    
    Args:
        filepath: Path to CSV file
        file_id: File identifier
        
    Returns:
        Dict with generated plot paths
    """
    plotter = DataPlotter(filepath)
    return plotter.generate_all_plots(file_id)
