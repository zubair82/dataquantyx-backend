"""
Analysis API Endpoints

This module provides API endpoints for data analysis including
statistics computation, insights generation, and visualization.
"""

from typing import List, Dict, Any

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api.dependencies import get_current_user
from app.utils.database import get_db
from app.utils.database import User
from app.utils.file_service import FileService
from app.utils.paths import resolve_storage_path
from app.services.analyzer import DataAnalyzer
from app.services.plotting import generate_plots

router = APIRouter()


def get_owned_file_or_404(db: Session, file_id: str, user_id: str):
    file_record = FileService.get_user_file_by_id(db, file_id, user_id)
    if not file_record:
        raise HTTPException(status_code=404, detail="File not found")
    if not resolve_storage_path(file_record.file_path).exists():
        raise HTTPException(status_code=404, detail="Uploaded file is missing on disk. Please re-upload it.")
    return file_record


# Response Models
class StatisticsSummary(BaseModel):
    min: float
    max: float
    mean: float
    median: float
    std: float
    count: int
    missing: int


class AnalysisResponse(BaseModel):
    file_id: str
    summary: Dict[str, StatisticsSummary]
    insights: List[str]
    plots: Dict[str, Any]
    
    class Config:
        from_attributes = True


class CorrelationResponse(BaseModel):
    file_id: str
    filename: str
    correlations: Dict[str, Dict[str, float]]
    
    class Config:
        from_attributes = True


class DistributionResponse(BaseModel):
    column: str
    min: float
    q1: float
    median: float
    q3: float
    max: float
    mean: float
    std: float
    skewness: float
    kurtosis: float


class OutliersResponse(BaseModel):
    column: str
    count: int
    percentage: float
    indices: List[int]


@router.post("/analyze/{file_id}", response_model=AnalysisResponse)
async def analyze_file(
    file_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Analyze an uploaded CSV file
    
    - **file_id**: UUID of the uploaded file
    - **Returns**: Summary statistics, insights, and visualizations
    """
    try:
        # Get file from database
        file_record = get_owned_file_or_404(db, file_id, current_user.user_id)
        
        if not file_record.is_valid == "1":
            raise HTTPException(status_code=400, detail="File is not valid for analysis")
        
        # Analyze the file
        resolved_file_path = str(resolve_storage_path(file_record.file_path))
        analyzer = DataAnalyzer(resolved_file_path)
        analyzer.load_data()
        
        # Get statistics
        summary = analyzer.get_summary_statistics()
        
        # Generate insights
        insights = analyzer.generate_insights()
        
        # Generate plots
        plots = generate_plots(resolved_file_path, file_id)
        
        return AnalysisResponse(
            file_id=file_id,
            summary=summary,
            insights=insights,
            plots=plots
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error analyzing file: {str(e)}")


@router.get("/analyze/{file_id}/insights")
async def get_insights(
    file_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get insights for an uploaded file
    
    - **file_id**: UUID of the uploaded file
    - **Returns**: List of generated insights
    """
    try:
        file_record = get_owned_file_or_404(db, file_id, current_user.user_id)
        
        analyzer = DataAnalyzer(str(resolve_storage_path(file_record.file_path)))
        analyzer.load_data()
        
        insights = analyzer.generate_insights()
        
        return {
            "file_id": file_id,
            "filename": file_record.filename,
            "insights": insights
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating insights: {str(e)}")


@router.get("/analyze/{file_id}/correlations", response_model=CorrelationResponse)
async def get_correlations(
    file_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get correlation matrix for numeric columns
    
    - **file_id**: UUID of the uploaded file
    - **Returns**: Correlation matrix
    """
    try:
        file_record = get_owned_file_or_404(db, file_id, current_user.user_id)
        
        analyzer = DataAnalyzer(str(resolve_storage_path(file_record.file_path)))
        analyzer.load_data()
        
        correlations = analyzer.get_correlation_matrix()
        
        return CorrelationResponse(
            file_id=file_id,
            filename=file_record.filename,
            correlations=correlations
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error computing correlations: {str(e)}")


@router.get("/analyze/{file_id}/distribution/{column}")
async def get_distribution(
    file_id: str,
    column: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get distribution statistics for a specific column
    
    - **file_id**: UUID of the uploaded file
    - **column**: Column name
    - **Returns**: Distribution statistics
    """
    try:
        file_record = get_owned_file_or_404(db, file_id, current_user.user_id)
        
        # Verify column exists
        if column not in file_record.columns:
            raise HTTPException(status_code=400, detail=f"Column '{column}' not found in file")
        
        analyzer = DataAnalyzer(str(resolve_storage_path(file_record.file_path)))
        analyzer.load_data()
        
        distribution = analyzer.get_column_distribution(column)
        
        if not distribution:
            raise HTTPException(status_code=400, detail=f"Column '{column}' is not numeric")
        
        return {
            "file_id": file_id,
            "column": column,
            **distribution
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error computing distribution: {str(e)}")


@router.get("/analyze/{file_id}/outliers/{column}")
async def detect_outliers(
    file_id: str, 
    column: str, 
    method: str = "iqr",
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Detect outliers in a specific column
    
    - **file_id**: UUID of the uploaded file
    - **column**: Column name
    - **method**: Detection method ('iqr' or 'zscore')
    - **Returns**: List of outlier indices and statistics
    """
    try:
        file_record = get_owned_file_or_404(db, file_id, current_user.user_id)
        
        if column not in file_record.columns:
            raise HTTPException(status_code=400, detail=f"Column '{column}' not found")
        
        if column not in file_record.numeric_columns:
            raise HTTPException(status_code=400, detail=f"Column '{column}' is not numeric")
        
        analyzer = DataAnalyzer(str(resolve_storage_path(file_record.file_path)))
        analyzer.load_data()
        
        outlier_indices = analyzer.detect_outliers(column, method=method)
        outlier_count = len(outlier_indices)
        outlier_percentage = (outlier_count / len(analyzer.df)) * 100 if len(analyzer.df) > 0 else 0
        
        return {
            "file_id": file_id,
            "column": column,
            "method": method,
            "count": outlier_count,
            "percentage": round(outlier_percentage, 2),
            "indices": outlier_indices[:100]  # Return first 100 for safety
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error detecting outliers: {str(e)}")


@router.get("/analyze/{file_id}/spikes/{column}")
async def detect_spikes(
    file_id: str, 
    column: str, 
    threshold: float = 2.0,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Detect spikes in a specific column
    
    - **file_id**: UUID of the uploaded file
    - **column**: Column name
    - **threshold**: Standard deviation threshold for spike detection
    - **Returns**: List of spike indices and statistics
    """
    try:
        file_record = get_owned_file_or_404(db, file_id, current_user.user_id)
        
        if column not in file_record.columns:
            raise HTTPException(status_code=400, detail=f"Column '{column}' not found")
        
        if column not in file_record.numeric_columns:
            raise HTTPException(status_code=400, detail=f"Column '{column}' is not numeric")
        
        analyzer = DataAnalyzer(str(resolve_storage_path(file_record.file_path)))
        analyzer.load_data()
        
        spike_indices = analyzer.detect_spikes(column, threshold=threshold)
        
        return {
            "file_id": file_id,
            "column": column,
            "threshold": threshold,
            "count": len(spike_indices),
            "indices": spike_indices[:100]  # Return first 100 for safety
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error detecting spikes: {str(e)}")


@router.get("/analyze/{file_id}/trends/{column}")
async def detect_trends(
    file_id: str,
    column: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Detect trends in a specific column
    
    - **file_id**: UUID of the uploaded file
    - **column**: Column name
    - **Returns**: Trend information
    """
    try:
        file_record = get_owned_file_or_404(db, file_id, current_user.user_id)
        
        if column not in file_record.columns:
            raise HTTPException(status_code=400, detail=f"Column '{column}' not found")
        
        if column not in file_record.numeric_columns:
            raise HTTPException(status_code=400, detail=f"Column '{column}' is not numeric")
        
        analyzer = DataAnalyzer(str(resolve_storage_path(file_record.file_path)))
        analyzer.load_data()
        
        trend_info = analyzer.detect_trends(column)
        
        return {
            "file_id": file_id,
            "column": column,
            **trend_info
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error detecting trends: {str(e)}")
