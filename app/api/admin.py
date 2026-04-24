import os
from typing import List, Optional

from google.auth.transport import requests as google_requests
from google.oauth2 import id_token

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session

from app.api.dependencies import get_current_admin, security
from app.services.admin_service import (
    authenticate_admin,
    create_admin_session_token,
    delete_admin_session_token,
)
from app.services.user_service import UserDataAccessService, UserService
from app.utils.file_service import FileService
from app.utils.database import Admin, get_db
from app.utils.paths import resolve_storage_path

router = APIRouter(prefix="/api/admin", tags=["Admin"])


class AdminLoginRequest(BaseModel):
    credential: str


class AdminResponse(BaseModel):
    admin_id: str
    email: str
    full_name: str
    is_active: bool

    class Config:
        from_attributes = True


class AdminLoginResponse(BaseModel):
    message: str
    access_token: str
    token_type: str = "bearer"
    admin: AdminResponse


class MessageResponse(BaseModel):
    message: str


class UserSummaryResponse(BaseModel):
    user_id: str
    user_name: str
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    organization: Optional[str] = None
    is_active: bool

    class Config:
        from_attributes = True


class AdminStatsResponse(BaseModel):
    total_users: int
    active_users: int
    inactive_users: int
    total_files: int


class AdminFileResponse(BaseModel):
    file_id: str
    user_id: str
    filename: str
    row_count: int
    upload_time: str
    is_valid: bool


class UserFilesResponse(BaseModel):
    user_id: str
    total: int
    files: list


class UserHistoryResponse(BaseModel):
    user_id: str
    total: int
    history: list


@router.post("/login", response_model=AdminLoginResponse)
def admin_login(data: AdminLoginRequest, db: Session = Depends(get_db)):
    try:
        client_id = os.getenv("GOOGLE_CLIENT_ID", "YOUR_GOOGLE_CLIENT_ID")
        idinfo = id_token.verify_oauth2_token(data.credential, google_requests.Request(), client_id)

        email = idinfo.get("email")

        if not email:
            raise ValueError("Token missing email")

        admin = authenticate_admin(db, email)
        if not admin:
            raise ValueError("This Google account does not have Admin privileges")
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(exc),
        )

    session_token = create_admin_session_token(db, admin)
    return {
        "message": "Admin logged in successfully",
        "access_token": session_token,
        "token_type": "bearer",
        "admin": admin,
    }


@router.get("/me", response_model=AdminResponse)
def get_logged_in_admin(current_admin: Admin = Depends(get_current_admin)):
    return current_admin


@router.get("/stats", response_model=AdminStatsResponse)
def get_admin_stats(
    db: Session = Depends(get_db),
    current_admin: Admin = Depends(get_current_admin),
):
    users = UserService.list_users(db, skip=0, limit=100000)
    files = FileService.get_all_files(db)
    total_users = len(users)
    active_users = len([user for user in users if user.is_active])
    return {
        "total_users": total_users,
        "active_users": active_users,
        "inactive_users": total_users - active_users,
        "total_files": len(files),
    }


@router.get("/users", response_model=List[UserSummaryResponse])
def list_all_users_for_admin(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_admin: Admin = Depends(get_current_admin),
):
    return UserService.list_users(db, skip=skip, limit=limit)


@router.get("/users/{user_id}", response_model=UserSummaryResponse)
def get_user_for_admin(
    user_id: str,
    db: Session = Depends(get_db),
    current_admin: Admin = Depends(get_current_admin),
):
    user = UserService.get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return user


@router.post("/users/{user_id}/activate", response_model=UserSummaryResponse)
def activate_user_for_admin(
    user_id: str,
    db: Session = Depends(get_db),
    current_admin: Admin = Depends(get_current_admin),
):
    user = UserService.activate_user(db, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return user


@router.post("/users/{user_id}/deactivate", response_model=UserSummaryResponse)
def deactivate_user_for_admin(
    user_id: str,
    db: Session = Depends(get_db),
    current_admin: Admin = Depends(get_current_admin),
):
    user = UserService.deactivate_user(db, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return user


@router.get("/users/{user_id}/files", response_model=UserFilesResponse)
def list_user_files_for_admin(
    user_id: str,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_admin: Admin = Depends(get_current_admin),
):
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
        "files": formatted_files,
    }


@router.get("/users/{user_id}/history", response_model=UserHistoryResponse)
def get_user_history_for_admin(
    user_id: str,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_admin: Admin = Depends(get_current_admin),
):
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
        "history": formatted_history,
    }


@router.get("/files", response_model=List[AdminFileResponse])
def list_all_files_for_admin(
    db: Session = Depends(get_db),
    current_admin: Admin = Depends(get_current_admin),
):
    files = FileService.get_all_files(db)
    files = [file for file in files if resolve_storage_path(file.file_path).exists()]
    return [
        {
            "file_id": file.file_id,
            "user_id": file.user_id,
            "filename": file.filename,
            "row_count": file.row_count,
            "upload_time": file.upload_time.isoformat(),
            "is_valid": file.is_valid == "1",
        }
        for file in files
    ]


@router.delete("/files/{file_id}", response_model=MessageResponse)
def delete_file_for_admin(
    file_id: str,
    db: Session = Depends(get_db),
    current_admin: Admin = Depends(get_current_admin),
):
    file_record = FileService.get_file_by_id(db, file_id)
    if not file_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")

    resolved_file_path = resolve_storage_path(file_record.file_path)
    if os.path.exists(resolved_file_path):
        os.remove(resolved_file_path)

    FileService.delete_file(db, file_id)
    return {"message": f"File {file_record.filename} deleted successfully"}


@router.post("/logout", response_model=MessageResponse)
def admin_logout(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    current_admin: Admin = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    delete_admin_session_token(db, credentials.credentials)
    return {"message": f"Logged out successfully for {current_admin.email}"}
