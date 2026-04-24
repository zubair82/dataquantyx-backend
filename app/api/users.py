from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional
from pydantic import BaseModel

from app.api.dependencies import get_current_admin, get_current_user, require_self_or_admin
from app.services.user_service import UserService, UserDataAccessService
from app.utils.database import Admin, User, get_db

router = APIRouter(prefix="/api/users", tags=["users"])

# ==================== Pydantic Models ====================

class UserResponse(BaseModel):
    user_id: str
    user_name: str
    email: str
    first_name: Optional[str]
    last_name: Optional[str]
    organization: Optional[str]
    is_active: bool
    
    class Config:
        from_attributes = True

class UserUpdateRequest(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    organization: Optional[str] = None
    email: Optional[str] = None

class FileResponse(BaseModel):
    file_id: str
    filename: str
    row_count: int
    
    class Config:
        from_attributes = True

# ==================== User Endpoints ====================

@router.get("/user/{user_id}", response_model=UserResponse)
def get_user(
    user_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get user by ID"""
    require_self_or_admin(user_id, current_user)
    user = UserService.get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return user

@router.get("", response_model=List[UserResponse])
def list_all_users(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_admin: Admin = Depends(get_current_admin),
):
    """List all users for an authenticated admin."""
    users = UserService.list_users(db, skip, limit)
    return users

@router.get("/username/{user_name}", response_model=UserResponse)
def get_user_by_username(
    user_name: str,
    db: Session = Depends(get_db),
    current_admin: Admin = Depends(get_current_admin),
):
    """Get user by username for an authenticated admin."""
    user = UserService.get_user_by_username(db, user_name)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return user

@router.put("/user/{user_id}", response_model=UserResponse)
def update_user(
    user_id: str,
    user_data: UserUpdateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Update user details"""
    require_self_or_admin(user_id, current_user)
    try:
        user = UserService.update_user(db, user_id, **user_data.dict(exclude_unset=True))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return user

@router.post("/user/{user_id}/deactivate", response_model=UserResponse)
def deactivate_user(
    user_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Deactivate user"""
    require_self_or_admin(user_id, current_user)
    user = UserService.deactivate_user(db, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return user

@router.post("/user/{user_id}/activate", response_model=UserResponse)
def activate_user(
    user_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Activate user"""
    require_self_or_admin(user_id, current_user)
    user = UserService.activate_user(db, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return user

@router.get("/list", response_model=List[UserResponse])
def list_users(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_admin: Admin = Depends(get_current_admin),
):
    """List all users for an authenticated admin."""
    users = UserService.list_users(db, skip, limit)
    return users

# ==================== User Files Endpoints (Data Isolation) ====================

@router.get("/user/{user_id}/files")
def get_user_files(
    user_id: str,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get all files for a user (USER-ISOLATED)"""
    require_self_or_admin(user_id, current_user)
    user = UserService.get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    
    files = UserDataAccessService.get_user_files(db, user_id, skip, limit)
    formatted_files = [
        {
            "file_id": file.file_id,
            "user_id": file.user_id,
            "filename": file.filename,
            "row_count": file.row_count,
            "upload_time": file.upload_time.isoformat() if hasattr(file.upload_time, 'isoformat') else str(file.upload_time),
            "is_valid": file.is_valid == "1" if hasattr(file, 'is_valid') else True,
        }
        for file in files
    ]
    return {
        "user_id": user_id,
        "total": len(files),
        "files": formatted_files
    }

@router.get("/user/{user_id}/analysis-history")
def get_user_analysis_history(
    user_id: str,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get user's analysis history"""
    require_self_or_admin(user_id, current_user)
    user = UserService.get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    
    history = UserDataAccessService.get_user_analysis_history(db, user_id, skip, limit)
    formatted_history = [
        {
            "history_id": h.history_id,
            "user_id": h.user_id,
            "file_id": h.file_id,
            "analysis_type": h.analysis_type,
            "result_path": h.result_path,
            "status": h.status,
            "created_at": h.created_at.isoformat() if hasattr(h.created_at, 'isoformat') else str(h.created_at)
        }
        for h in history
    ]
    return {
        "user_id": user_id,
        "total": len(history),
        "history": formatted_history
    }
