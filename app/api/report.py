"""
Report API

This module exposes endpoints for generating simulation reports.
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import Optional
import os

from app.api.dependencies import get_current_user
from app.services.report_generator import ReportGenerator
from app.utils.file_service import FileService
from app.utils.database import User, get_db
from app.utils.paths import resolve_storage_path
from sqlalchemy.orm import Session
from app.services.plotting import generate_plots

router = APIRouter(prefix="/report", tags=["Report"])



# Single file report endpoint
@router.post("/{file_id}")
def generate_single_report(
    file_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Generate report for a single file (no comparison)
    """
    try:
        file_record = FileService.get_user_file_by_id(db, file_id, current_user.user_id)
        if not file_record:
            raise HTTPException(status_code=404, detail="File not found")
        resolved_file_path = resolve_storage_path(file_record.file_path)
        if not os.path.exists(resolved_file_path):
            raise HTTPException(status_code=404, detail="File path does not exist")
        
        # Generate plots
        plots = generate_plots(resolved_file_path, file_id)
        
        report_generator = ReportGenerator()
        result = report_generator.generate_single_report(
            file_id=file_id,
            filename=file_record.filename,
            filepath=str(resolved_file_path)
        )
        return {
            "status": "success",
            "report_url": result["report_url"]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate report: {str(e)}"
        )

# Comparison report endpoint
@router.post("/compare/{file_id_1}/{file_id_2}")
def generate_comparison_report(
    file_id_1: str,
    file_id_2: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Generate comparison report for two files (comparison only)
    """
    try:
        file_record_1 = FileService.get_user_file_by_id(db, file_id_1, current_user.user_id)
        file_record_2 = FileService.get_user_file_by_id(db, file_id_2, current_user.user_id)
        if not file_record_1 or not file_record_2:
            raise HTTPException(status_code=404, detail="One or both files not found")
        resolved_file_path_1 = resolve_storage_path(file_record_1.file_path)
        resolved_file_path_2 = resolve_storage_path(file_record_2.file_path)
        if not os.path.exists(resolved_file_path_1) or not os.path.exists(resolved_file_path_2):
            raise HTTPException(status_code=404, detail="One or both file paths do not exist")
        report_generator = ReportGenerator()
        result = report_generator.generate_comparison_report(
            file_id_1=file_id_1,
            filename_1=file_record_1.filename,
            filepath_1=str(resolved_file_path_1),
            file_id_2=file_id_2,
            db_session=db
        )
        return {
            "status": "success",
            "report_url": result["report_url"]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate comparison report: {str(e)}"
        )
