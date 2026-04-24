"""
Report Generation Service

This module provides functionality for generating comprehensive HTML reports
that consolidate file metadata, statistics, insights, visualizations, and
optional comparison data.
"""

from fastapi import Depends
import os
from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd
from app.utils.database import get_db
from app.services.analyzer import DataAnalyzer
from app.services.plotting import DataPlotter
from app.services.comparator import DataComparator
from app.utils.file_service import FileService
from app.utils.paths import PLOTS_DIR, REPORTS_DIR, public_asset_url, resolve_storage_path
from sqlalchemy.orm import Session


class ReportGenerator:
    """Generator for comprehensive HTML reports"""
    
    def __init__(self, output_dir: str = str(REPORTS_DIR)):
        """
        Initialize report generator
        
        Args:
            output_dir: Directory to save reports
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def _get_css_stylesheet(self) -> str:
        """
        Get CSS stylesheet for the report
        
        Returns:
            CSS string
        """
        return """
        <style>
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                line-height: 1.6;
                color: #333;
                background-color: #f5f5f5;
            }
            
            .container {
                max-width: 1000px;
                margin: 0 auto;
                padding: 40px 20px;
                background-color: white;
                min-height: 100vh;
            }
            
            header {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 30px 20px;
                margin: -40px -20px 40px -20px;
                border-radius: 0 0 10px 10px;
            }
            
            h1 {
                font-size: 2.5em;
                margin-bottom: 10px;
            }
            
            .report-meta {
                font-size: 0.9em;
                opacity: 0.9;
                margin-top: 10px;
            }
            
            h2 {
                color: #667eea;
                margin-top: 40px;
                margin-bottom: 20px;
                padding-bottom: 10px;
                border-bottom: 3px solid #667eea;
                font-size: 1.8em;
            }
            
            h3 {
                color: #764ba2;
                margin-top: 20px;
                margin-bottom: 10px;
                font-size: 1.2em;
            }
            
            .overview-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }
            
            .overview-card {
                background: #f9f9f9;
                border-left: 4px solid #667eea;
                padding: 15px;
                border-radius: 5px;
            }
            
            .overview-card strong {
                color: #667eea;
                display: block;
                margin-bottom: 5px;
            }
            
            .statistics-table {
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 20px;
                background-color: #f9f9f9;
            }
            
            .statistics-table th {
                background-color: #667eea;
                color: white;
                padding: 12px;
                text-align: left;
                font-weight: 600;
            }
            
            .statistics-table td {
                padding: 12px;
                border-bottom: 1px solid #ddd;
            }
            
            .statistics-table tr:hover {
                background-color: #f0f0f0;
            }
            
            .insights-list {
                list-style: none;
                margin: 20px 0;
            }
            
            .insights-list li {
                padding: 12px 15px;
                margin-bottom: 10px;
                background-color: #f0f8ff;
                border-left: 4px solid #764ba2;
                border-radius: 4px;
            }
            
            .insights-list li:before {
                content: "✓ ";
                color: #764ba2;
                font-weight: bold;
                margin-right: 10px;
            }
            
            .plots-gallery {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
                gap: 30px;
                margin: 30px 0;
            }
            
            .plot-container {
                text-align: center;
                padding: 15px;
                background-color: #f9f9f9;
                border-radius: 8px;
            }
            
            .plot-container img {
                max-width: 100%;
                height: auto;
                border-radius: 5px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            }
            
            .plot-title {
                margin-top: 10px;
                color: #764ba2;
                font-weight: 600;
            }
            
            .comparison-section {
                background-color: #faf9f7;
                padding: 20px;
                border-radius: 8px;
                margin: 20px 0;
            }
            
            .comparison-table {
                width: 100%;
                border-collapse: collapse;
                margin-top: 15px;
            }
            
            .comparison-table th {
                background-color: #764ba2;
                color: white;
                padding: 12px;
                text-align: left;
                font-weight: 600;
            }
            
            .comparison-table td {
                padding: 12px;
                border-bottom: 1px solid #ddd;
            }
            
            .comparison-table tr:hover {
                background-color: #f0f0f0;
            }
            
            .no-data {
                color: #999;
                font-style: italic;
                padding: 20px;
                text-align: center;
                background-color: #f9f9f9;
                border-radius: 5px;
            }
            
            footer {
                margin-top: 60px;
                padding-top: 20px;
                border-top: 1px solid #ddd;
                text-align: center;
                color: #999;
                font-size: 0.9em;
            }
        </style>
        """
    

    def generate_single_report(
        self,
        file_id: str,
        filename: str,
        filepath: str
    ) -> Dict[str, str]:
        """
        Generate HTML report for a single file (no comparison section)
        """
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Simulation Report: {filename}</title>
    {self._get_css_stylesheet()}
</head>
<body>
    <div class="container">
        <header>
            <h1>📊 DataQuantyx Analysis Report</h1>
            <div class="report-meta">
                Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            </div>
        </header>
"""
        html += self._generate_file_overview(file_id, filename, filepath)
        html += self._generate_summary_statistics(filepath)
        html += self._generate_insights(filepath)
        html += self._generate_plots_section(file_id)
        html += """
        <footer>
            <p>This report was automatically generated by DataQuantyx</p>
        </footer>
    </div>
</body>
</html>
        """
        report_filename = f"{file_id}.html"
        report_path = os.path.join(self.output_dir, report_filename)
        with open(report_path, 'w') as f:
            f.write(html)
        return {"report_url": public_asset_url("reports", report_filename)}

    def generate_comparison_report(
        self,
        file_id_1: str,
        filename_1: str,
        filepath_1: str,
        file_id_2: str,
        db_session
    ) -> Dict[str, str]:
        """
        Generate HTML report for comparison only (no single file plots/insights)
        """
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Comparison Report: {filename_1} vs {file_id_2}</title>
    {self._get_css_stylesheet()}
</head>
<body>
    <div class="container">
        <header>
            <h1>⚖️ DataQuantyx Comparison Report</h1>
            <div class="report-meta">
                Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            </div>
        </header>
"""
        html += self._generate_file_overview(file_id_1, filename_1, filepath_1)
        html += self._generate_comparison_section(file_id_1, filepath_1, file_id_2, db_session)
        html += """
        <footer>
            <p>This report was automatically generated by DataQuantyx</p>
        </footer>
    </div>
</body>
</html>
        """
        report_filename = f"compare_{file_id_1}_{file_id_2}.html"
        report_path = os.path.join(self.output_dir, report_filename)
        with open(report_path, 'w') as f:
            f.write(html)
        return {"report_url": public_asset_url("reports", report_filename)}
    
    def _generate_file_overview(self, file_id: str, filename: str, filepath: str) -> str:
        """Generate file overview section"""
        try:
            df = pd.read_csv(filepath)
            columns = ", ".join(df.columns.tolist())
            row_count = len(df)
            
            return f"""
        <h2>📋 File Overview</h2>
        <div class="overview-grid">
            <div class="overview-card">
                <strong>Filename</strong>
                <span>{filename}</span>
            </div>
            <div class="overview-card">
                <strong>Rows</strong>
                <span>{row_count:,}</span>
            </div>
            <div class="overview-card">
                <strong>Columns</strong>
                <span>{len(df.columns)}</span>
            </div>
        </div>
        <div style="background-color: #f9f9f9; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
            <strong>Column Names:</strong>
            <p style="margin-top: 10px; font-family: monospace; color: #666;">{columns}</p>
        </div>
            """
        except Exception as e:
            return f'<div class="no-data">Error reading file: {str(e)}</div>'
    
    def _generate_summary_statistics(self, filepath: str) -> str:
        """Generate summary statistics section"""
        try:
            analyzer = DataAnalyzer(filepath)
            analyzer.load_data()
            stats = analyzer.get_summary_statistics()
            
            if not stats:
                return '<h2>📊 Summary Statistics</h2><div class="no-data">No numeric columns found</div>'
            
            html = '<h2>📊 Summary Statistics</h2>'
            html += '<table class="statistics-table">'
            html += '<thead><tr><th>Column</th><th>Min</th><th>Max</th><th>Mean</th><th>Median</th><th>Std Dev</th></tr></thead>'
            html += '<tbody>'
            
            for col, stats_dict in stats.items():
                html += f"""
                <tr>
                    <td><strong>{col}</strong></td>
                    <td>{stats_dict.get('min', 'N/A'):.4f}</td>
                    <td>{stats_dict.get('max', 'N/A'):.4f}</td>
                    <td>{stats_dict.get('mean', 'N/A'):.4f}</td>
                    <td>{stats_dict.get('median', 'N/A'):.4f}</td>
                    <td>{stats_dict.get('std', 'N/A'):.4f}</td>
                </tr>
                """
            
            html += '</tbody></table>'
            return html
        except Exception as e:
            return f'<h2>📊 Summary Statistics</h2><div class="no-data">Error generating statistics: {str(e)}</div>'
    
    def _generate_insights(self, filepath: str) -> str:
        """Generate insights section"""
        try:
            analyzer = DataAnalyzer(filepath)
            analyzer.load_data()
            insights = analyzer.generate_insights()
            
            if not insights:
                insights = ["No significant insights detected"]
            
            html = '<h2>💡 Insights</h2>'
            html += '<ul class="insights-list">'
            
            for insight in insights:
                html += f'<li>{insight}</li>'
            
            html += '</ul>'
            return html
        except Exception as e:
            return f'<h2>💡 Insights</h2><div class="no-data">Error generating insights: {str(e)}</div>'
    
    def _generate_plots_section(self, file_id: str) -> str:
        """Generate plots section"""
        try:
            plots_dir = str(PLOTS_DIR)
            if not os.path.exists(plots_dir):
                return '<h2>📈 Plots</h2><div class="no-data">No plots generated yet</div>'
            
            # Find all plots for this file ID
            plot_files = [f for f in os.listdir(plots_dir) if f.startswith(file_id)]
            
            if not plot_files:
                return '<h2>📈 Plots</h2><div class="no-data">No plots found for this file</div>'
            
            html = '<h2>📈 Plots</h2>'
            html += '<div class="plots-gallery">'
            
            for plot_file in sorted(plot_files):
                # Extract column name from filename
                # Format: {file_id}_{column}.png
                parts = plot_file.replace('.png', '').split('_', 1)
                if len(parts) > 1:
                    col_name = parts[1].replace('_', ' ').title()
                else:
                    col_name = plot_file
                
                plot_path = public_asset_url("plots", plot_file)
                html += f"""
                <div class="plot-container">
                    <img src="{plot_path}" alt="{col_name}">
                    <div class="plot-title">{col_name}</div>
                </div>
                """
            
            html += '</div>'
            return html
        except Exception as e:
            return f'<h2>📈 Plots</h2><div class="no-data">Error loading plots: {str(e)}</div>'
    
    def _generate_comparison_section(
        self,
        file_id_1: str,
        filepath_1: str,
        file_id_2: str,
        db: Session = Depends(get_db)
    ) -> str:
        """Generate comparison section"""
        try:
            # Get file path for second file
            # file_service = FileService(db_session)
            file2_record = FileService.get_file_by_id(db, file_id_2)
            file1_record = FileService.get_file_by_id(db, file_id_1)
            
            resolved_file_path_2 = resolve_storage_path(file2_record.file_path) if file2_record else None
            if not file2_record or not os.path.exists(resolved_file_path_2):
                return '<h2>⚖️ Comparison</h2><div class="no-data">Comparison file not found</div>'
            
            filepath_2 = str(resolved_file_path_2)
            
            # Run comparison
            comparator = DataComparator(filepath_1, filepath_2)
            comparator.load_data()
            comparator.equalize_lengths()
            comparator.find_common_columns()
            
            differences = comparator.compute_differences()
            insights = comparator.generate_insights()
            comparison_plots = comparator.generate_all_comparison_plots(file_id_1, file_id_2)
            
            html = '<h2>⚖️ Comparison</h2>'
            html += f'<div class="comparison-section">'
            html += f'<p><strong>Comparing:</strong> {file1_record.filename} vs {file2_record.filename}</p>'
            
            if differences:
                html += '<table class="comparison-table">'
                html += '<thead><tr><th>Column</th><th>Mean Diff</th><th>Std Diff</th><th>Var Diff</th><th>Max Diff</th><th>Min Diff</th></tr></thead>'
                html += '<tbody>'
                
                for col, diffs in differences.items():
                    html += f"""
                    <tr>
                        <td><strong>{col}</strong></td>
                        <td>{diffs.get('mean_diff', 'N/A')}</td>
                        <td>{diffs.get('std_diff', 'N/A')}</td>
                        <td>{diffs.get('var_diff', 'N/A')}</td>
                        <td>{diffs.get('max_diff', 'N/A')}</td>
                        <td>{diffs.get('min_diff', 'N/A')}</td>
                    </tr>
                    """
                
                html += '</tbody></table>'
            
            if insights:
                html += '<h3>Comparison Insights</h3>'
                html += '<ul class="insights-list">'
                for insight in insights:
                    html += f'<li>{insight}</li>'
                html += '</ul>'
            
            # Add comparison plots
            html += '<h3>Comparison Plots</h3>'
            if comparison_plots:
                html += '<div class="plots-gallery">'
                for col_name, plot_path in sorted(comparison_plots.items()):
                    html += f"""
                    <div class="plot-container">
                        <img src="{plot_path}" alt="{col_name} Comparison">
                        <div class="plot-title">{col_name} Comparison</div>
                    </div>
                    """
                html += '</div>'
            else:
                html += '<div class="no-data">No comparison plots could be generated for the selected files</div>'
            
            html += '</div>'
            return html
        except Exception as e:
            return f'<h2>⚖️ Comparison</h2><div class="no-data">Error generating comparison: {str(e)}</div>'
