import os
import uuid
from datetime import datetime
from typing import List, Dict, Any

from fastapi import APIRouter, File, UploadFile, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api.dependencies import get_current_user
from app.services.parser import CSVParser
from app.utils.database import User
from app.utils.database import get_db
from app.utils.file_service import FileService
from app.utils.paths import DATA_DIR, resolve_storage_path

router = APIRouter()

# Constants
UPLOAD_DIR = DATA_DIR
ALLOWED_EXTENSIONS = {"csv"}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
MIN_FILE_SIZE = 1  # At least 1 byte


# Response Models
class UploadResponse(BaseModel):
    file_id: str
    filename: str
    columns: List[str]
    numeric_columns: List[str]
    row_count: int
    upload_time: str


class FileDetailResponse(BaseModel):
    file_id: str
    filename: str
    columns: List[str]
    column_types: Dict[str, str]
    numeric_columns: List[str]
    row_count: int
    file_size_bytes: int
    missing_values: Dict[str, int]
    upload_time: str
    is_valid: bool
    
    class Config:
        from_attributes = True


class ErrorResponse(BaseModel):
    error: str
    detail: str = None
    file_name: str = None


def file_is_available(file_record) -> bool:
    return resolve_storage_path(file_record.file_path).exists()


def validate_file_extension(filename: str) -> None:
    """Validate file has CSV extension"""
    if not filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=400,
            detail=ErrorResponse(
                error="Invalid file format",
                detail="Only CSV files are allowed. Please upload a .csv file."
            ).dict()
        )


def validate_file_not_empty(file_path: str, filename: str) -> None:
    """Validate file is not empty"""
    file_size = os.path.getsize(file_path)
    
    if file_size < MIN_FILE_SIZE:
        if os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(
            status_code=400,
            detail=ErrorResponse(
                error="Empty file",
                detail="The uploaded file is empty. Please upload a file with content.",
                file_name=filename
            ).dict()
        )


def validate_csv_format(file_path: str, filename: str) -> None:
    """Validate CSV format and structure"""
    try:
        import pandas as pd
        
        # Try to read CSV
        df = pd.read_csv(file_path)
        
        # Check if dataframe is empty
        if df.empty:
            if os.path.exists(file_path):
                os.remove(file_path)
            raise HTTPException(
                status_code=400,
                detail=ErrorResponse(
                    error="Empty CSV",
                    detail="The CSV file contains no data rows. Please upload a CSV with at least one data row.",
                    file_name=filename
                ).dict()
            )
        
        # Check if has columns
        if len(df.columns) == 0:
            if os.path.exists(file_path):
                os.remove(file_path)
            raise HTTPException(
                status_code=400,
                detail=ErrorResponse(
                    error="Invalid CSV structure",
                    detail="The CSV file has no columns/headers. Please upload a valid CSV with column headers.",
                    file_name=filename
                ).dict()
            )
    
    except pd.errors.ParserError as e:
        if os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(
            status_code=400,
            detail=ErrorResponse(
                error="Corrupt CSV format",
                detail=f"The CSV file is corrupted or malformed: {str(e)}",
                file_name=filename
            ).dict()
        )
    except pd.errors.EmptyDataError:
        if os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(
            status_code=400,
            detail=ErrorResponse(
                error="Empty CSV",
                detail="The CSV file is empty or contains no readable data.",
                file_name=filename
            ).dict()
        )
    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(
            status_code=400,
            detail=ErrorResponse(
                error="Invalid CSV format",
                detail=f"Failed to parse CSV file: {str(e)}",
                file_name=filename
            ).dict()
        )


@router.post("/upload", response_model=UploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> UploadResponse:
    """
    Upload a CSV file and store metadata in database
    
    - **file**: CSV file to upload
    - **Returns**: File metadata including file_id, columns, numeric columns, row count
    
    Validations:
    - File must be CSV format
    - File must not be empty
    - File must be valid CSV structure
    - File size must not exceed 50MB
    """
    
    file_path = None
    
    try:
        # Validate file extension first
        validate_file_extension(file.filename)
        
        # Generate unique file_id
        file_id = str(uuid.uuid4())
        
        # Create upload directory if it doesn't exist
        os.makedirs(UPLOAD_DIR, exist_ok=True)
        
        # Save file with unique name
        file_path = os.path.join(str(UPLOAD_DIR), f"{file_id}_{file.filename}")
        
        # Write file to disk and check size
        file_size = 0
        with open(file_path, "wb") as buffer:
            while True:
                chunk = await file.read(1024 * 1024)  # Read 1MB at a time
                if not chunk:
                    break
                file_size += len(chunk)
                
                if file_size > MAX_FILE_SIZE:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    raise HTTPException(
                        status_code=413,
                        detail=ErrorResponse(
                            error="File too large",
                            detail=f"File size exceeds {MAX_FILE_SIZE / 1024 / 1024:.0f}MB limit",
                            file_name=file.filename
                        ).dict()
                    )
                buffer.write(chunk)
        
        # Validate file is not empty
        validate_file_not_empty(file_path, file.filename)
        
        # Validate CSV format
        validate_csv_format(file_path, file.filename)
        
        # Parse and validate CSV using CSVParser
        parser = CSVParser(file_path)
        
        # Validate CSV (comprehensive check)
        is_valid, errors = parser.validate()
        if not is_valid:
            if os.path.exists(file_path):
                os.remove(file_path)
            raise HTTPException(
                status_code=400,
                detail=ErrorResponse(
                    error="Invalid CSV content",
                    detail=f"CSV validation failed: {', '.join(errors)}",
                    file_name=file.filename
                ).dict()
            )
        
        # Store file metadata in database
        file_record = FileService.create_file_record(
            db=db,
            file_id=file_id,
            filename=file.filename,
            file_path=file_path,
            csv_parser=parser,
            user_id=current_user.user_id,
        )
        
        # Extract metadata for response
        columns = parser.get_columns()
        numeric_columns = parser.get_numeric_columns()
        row_count = parser.get_row_count()
        upload_time = datetime.now().isoformat()
        
        return UploadResponse(
            file_id=file_id,
            filename=file.filename,
            columns=columns,
            numeric_columns=numeric_columns,
            row_count=row_count,
            upload_time=upload_time
        )
    
    except HTTPException:
        raise
    except Exception as e:
        # Clean up on error
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(
            status_code=500,
            detail=ErrorResponse(
                error="Internal server error",
                detail=f"Failed to process file: {str(e)}",
                file_name=file.filename
            ).dict()
        )


@router.get("/files/{file_id}", response_model=FileDetailResponse)
async def get_file_metadata(
    file_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> FileDetailResponse:
    """
    Retrieve detailed metadata for an uploaded file from database
    
    - **file_id**: UUID of the uploaded file
    - **Returns**: Complete file metadata including columns, types, and statistics
    """
    try:
        # Fetch from database
        file_record = FileService.get_user_file_by_id(db, file_id, current_user.user_id)
        
        if not file_record:
            raise HTTPException(status_code=404, detail="File not found")
        if not file_is_available(file_record):
            raise HTTPException(status_code=404, detail="Uploaded file is missing on disk. Please re-upload it.")
        
        return FileDetailResponse(
            file_id=file_record.file_id,
            filename=file_record.filename,
            columns=file_record.columns,
            column_types=file_record.column_types,
            numeric_columns=file_record.numeric_columns,
            row_count=file_record.row_count,
            file_size_bytes=file_record.file_size_bytes,
            missing_values=file_record.missing_values,
            upload_time=file_record.upload_time.isoformat(),
            is_valid=file_record.is_valid == "1"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving file: {str(e)}")


@router.get("/files")
async def list_all_files(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    List all uploaded files with basic metadata
    
    - **Returns**: List of all files with their metadata
    """
    try:
        files = FileService.get_all_files(db) if current_user.role == "admin" else FileService.get_user_files(db, current_user.user_id)
        files = [file for file in files if file_is_available(file)]
        
        return {
            "total": len(files),
            "files": [
                {
                    "file_id": f.file_id,
                    "filename": f.filename,
                    "row_count": f.row_count,
                    "columns": len(f.columns),
                    "upload_time": f.upload_time.isoformat(),
                    "is_valid": f.is_valid == "1"
                }
                for f in files
            ]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing files: {str(e)}")


@router.delete("/files/{file_id}")
async def delete_file(
    file_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Delete an uploaded file and remove it from database
    
    - **file_id**: UUID of the file to delete
    - **Returns**: Deletion status
    """
    try:
        # Get file record to find file path
        file_record = FileService.get_user_file_by_id(db, file_id, current_user.user_id)
        
        if not file_record:
            raise HTTPException(status_code=404, detail="File not found")
        if not file_is_available(file_record):
            raise HTTPException(status_code=404, detail="Uploaded file is missing on disk. Please re-upload it.")
        
        # Delete physical file
        resolved_file_path = resolve_storage_path(file_record.file_path)
        if os.path.exists(resolved_file_path):
            os.remove(resolved_file_path)
        
        # Delete database record
        FileService.delete_file(db, file_id)
        
        return {
            "message": "File deleted successfully",
            "file_id": file_id
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting file: {str(e)}")


@router.get("/files/{file_id}/cleaning-report")
async def get_cleaning_report(
    file_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get cleaning report for an uploaded file
    Shows what non-numeric values were cleaned/converted to NaN
    
    - **file_id**: UUID of the uploaded file
    - **Returns**: Cleaning report with details of cleaned values
    """
    try:
        file_record = FileService.get_user_file_by_id(db, file_id, current_user.user_id)
        
        if not file_record:
            raise HTTPException(status_code=404, detail="File not found")
        if not file_is_available(file_record):
            raise HTTPException(status_code=404, detail="Uploaded file is missing on disk. Please re-upload it.")
        
        cleaning_report = file_record.cleaning_report or {}
        
        if not cleaning_report:
            return {
                "file_id": file_id,
                "filename": file_record.filename,
                "message": "No non-numeric values were cleaned",
                "cleaning_report": {}
            }
        
        return {
            "file_id": file_id,
            "filename": file_record.filename,
            "summary": {
                "total_columns_cleaned": len(cleaning_report),
                "total_values_cleaned": sum(r.get("cleaned_count", 0) for r in cleaning_report.values())
            },
            "cleaning_report": cleaning_report
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving cleaning report: {str(e)}")
