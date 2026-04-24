from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

from app.services.admin_service import get_admin_by_session_token
from app.services.auth_service import get_user_by_session_token
from app.utils.database import Admin, User, get_db

security = HTTPBearer()


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
) -> User:
    user = get_user_by_session_token(db, credentials.credentials)
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired session",
        )

    return user


def get_current_admin(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
) -> Admin:
    admin = get_admin_by_session_token(db, credentials.credentials)
    if not admin or not admin.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired admin session",
        )

    return admin


def require_self_or_admin(requested_user_id: str, current_user: User) -> None:
    if current_user.role == "admin":
        return

    if current_user.user_id != requested_user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You are not allowed to access another user's data",
        )


def require_admin(current_user: User) -> None:
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
