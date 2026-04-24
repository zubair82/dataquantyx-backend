"""
File Comparison API Endpoints

This module provides API endpoints for comparing two uploaded datasets.
"""

from typing import Dict, Any

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api.dependencies import get_current_user
from app.utils.database import get_db
from app.utils.database import User
from app.utils.file_service import FileService
from app.utils.paths import resolve_storage_path
from app.services.comparator import compare_files

router = APIRouter()


# Request Models
class CompareRequest(BaseModel):
    file_id_1: str
    file_id_2: str


# Response Models
class ColumnDifferences(BaseModel):
    mean_diff: float
    std_diff: float
    var_diff: float
    max_diff: float
    min_diff: float
    dataset1_mean: float
    dataset2_mean: float
    dataset1_std: float
    dataset2_std: float


class ComparisonResponse(BaseModel):
    file_id_1: str
    file_id_2: str
    filename_1: str
    filename_2: str
    common_numeric_columns: list
    differences: Dict[str, ColumnDifferences]
    plots: Dict[str, str]
    insights: list
    
    class Config:
        from_attributes = True


@router.post("/compare")
async def compare_datasets(
    request: CompareRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Compare two uploaded datasets
    
    Request:
    - **file_id_1**: UUID of first dataset
    - **file_id_2**: UUID of second dataset
    
    Returns:
    - Differences in statistics (mean, std, variance, etc.)
    - Comparison plots (overlay graphs)
    - Generated insights
    """
    try:
        # Validate file_id_1
        file1_record = FileService.get_user_file_by_id(db, request.file_id_1, current_user.user_id)
        if not file1_record:
            raise HTTPException(status_code=404, detail=f"File 1 not found: {request.file_id_1}")
        if not resolve_storage_path(file1_record.file_path).exists():
            raise HTTPException(status_code=404, detail="File 1 is missing on disk. Please re-upload it.")
        
        if not file1_record.is_valid == "1":
            raise HTTPException(status_code=400, detail="File 1 is not valid for comparison")
        
        # Validate file_id_2
        file2_record = FileService.get_user_file_by_id(db, request.file_id_2, current_user.user_id)
        if not file2_record:
            raise HTTPException(status_code=404, detail=f"File 2 not found: {request.file_id_2}")
        if not resolve_storage_path(file2_record.file_path).exists():
            raise HTTPException(status_code=404, detail="File 2 is missing on disk. Please re-upload it.")
        
        if not file2_record.is_valid == "1":
            raise HTTPException(status_code=400, detail="File 2 is not valid for comparison")
        
        # Perform comparison
        comparison_results = compare_files(
            filepath1=str(resolve_storage_path(file1_record.file_path)),
            filepath2=str(resolve_storage_path(file2_record.file_path)),
            file_id_1=request.file_id_1,
            file_id_2=request.file_id_2
        )
        
        return {
            "file_id_1": request.file_id_1,
            "file_id_2": request.file_id_2,
            "filename_1": file1_record.filename,
            "filename_2": file2_record.filename,
            "common_numeric_columns": comparison_results["common_numeric_columns"],
            "differences": comparison_results["differences"],
            "plots": comparison_results["plots"],
            "insights": comparison_results["insights"]
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error comparing files: {str(e)}")


@router.get("/compare/{file_id_1}/{file_id_2}")
async def get_comparison(
    file_id_1: str,
    file_id_2: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Compare two datasets (GET endpoint alternative)
    
    - **file_id_1**: UUID of first dataset
    - **file_id_2**: UUID of second dataset
    
    Returns comparison results
    """
    return await compare_datasets(
        CompareRequest(file_id_1=file_id_1, file_id_2=file_id_2),
        db,
        current_user,
    )


@router.post("/compare/insights-only")
async def get_comparison_insights_only(
    request: CompareRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get only insights from comparison (no plots)
    
    Request:
    - **file_id_1**: UUID of first dataset
    - **file_id_2**: UUID of second dataset
    
    Returns:
    - Insights only
    """
    try:
        file1_record = FileService.get_user_file_by_id(db, request.file_id_1, current_user.user_id)
        if not file1_record:
            raise HTTPException(status_code=404, detail=f"File 1 not found")
        if not resolve_storage_path(file1_record.file_path).exists():
            raise HTTPException(status_code=404, detail="File 1 is missing on disk. Please re-upload it.")
        
        file2_record = FileService.get_user_file_by_id(db, request.file_id_2, current_user.user_id)
        if not file2_record:
            raise HTTPException(status_code=404, detail=f"File 2 not found")
        if not resolve_storage_path(file2_record.file_path).exists():
            raise HTTPException(status_code=404, detail="File 2 is missing on disk. Please re-upload it.")
        
        comparison_results = compare_files(
            filepath1=str(resolve_storage_path(file1_record.file_path)),
            filepath2=str(resolve_storage_path(file2_record.file_path)),
            file_id_1=request.file_id_1,
            file_id_2=request.file_id_2
        )
        
        return {
            "file_id_1": request.file_id_1,
            "file_id_2": request.file_id_2,
            "filename_1": file1_record.filename,
            "filename_2": file2_record.filename,
            "common_numeric_columns": comparison_results["common_numeric_columns"],
            "insights": comparison_results["insights"]
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating insights: {str(e)}")
