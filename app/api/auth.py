import os
from typing import Optional

from google.auth.transport import requests as google_requests
from google.oauth2 import id_token

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session

from app.api.dependencies import get_current_user, security
from app.services.auth_service import create_session_token, delete_session_token, get_or_create_google_user
from app.utils.database import User, get_db

router = APIRouter(prefix="/api/auth", tags=["Auth"])


class GoogleAuthRequest(BaseModel):
    credential: str


class AuthUserResponse(BaseModel):
    user_id: str
    user_name: str
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    organization: Optional[str] = None
    role: str
    is_active: bool

    class Config:
        from_attributes = True


class AuthResponse(BaseModel):
    message: str
    access_token: str
    token_type: str = "bearer"
    auth_provider: str = "google"
    is_new_user: bool
    user: AuthUserResponse


class MessageResponse(BaseModel):
    message: str


@router.post("/google/login", response_model=AuthResponse)
def google_login(data: GoogleAuthRequest, db: Session = Depends(get_db)):
    try:
        client_id = os.getenv("GOOGLE_CLIENT_ID", "YOUR_GOOGLE_CLIENT_ID")
        idinfo = id_token.verify_oauth2_token(data.credential, google_requests.Request(), client_id)

        email = idinfo.get("email")
        name = idinfo.get("name")

        if not email:
            raise ValueError("Token missing email")

        user, is_new_user = get_or_create_google_user(db, email, name)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    session_token = create_session_token(db, user, provider="google")
    return {
        "message": "User registered and logged in" if is_new_user else "User logged in successfully",
        "access_token": session_token,
        "token_type": "bearer",
        "auth_provider": "google",
        "is_new_user": is_new_user,
        "user": user,
    }


@router.get("/me", response_model=AuthUserResponse)
def get_logged_in_user(current_user: User = Depends(get_current_user)):
    return current_user


@router.post("/logout", response_model=MessageResponse)
def logout(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    delete_session_token(db, credentials.credentials)
    return {"message": f"Logged out successfully for {current_user.email}"}
