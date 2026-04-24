import hashlib
import os
import secrets
import uuid
from datetime import datetime, timedelta
from typing import Optional, Tuple

from sqlalchemy.orm import Session

from app.utils.database import AuthSession, User

SESSION_TTL_DAYS = int(os.getenv("SESSION_TTL_DAYS", "7"))
GOOGLE_PROVIDER = "google"


def _normalize_email(email: str) -> str:
    return email.strip().lower()


def _hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _split_name(name: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not name:
        return None, None

    cleaned = " ".join(name.strip().split())
    if not cleaned:
        return None, None

    parts = cleaned.split(" ", 1)
    first_name = parts[0]
    last_name = parts[1] if len(parts) > 1 else None
    return first_name, last_name


def _build_unique_username(db: Session, email: str, name: Optional[str]) -> str:
    base_value = (name or email.split("@", 1)[0]).strip().lower().replace(" ", "_")
    candidate = "".join(ch for ch in base_value if ch.isalnum() or ch == "_").strip("_")
    if not candidate:
        candidate = f"user_{uuid.uuid4().hex[:8]}"

    username = candidate
    suffix = 1
    while db.query(User).filter(User.user_name == username).first():
        username = f"{candidate}_{suffix}"
        suffix += 1

    return username


def delete_expired_sessions(db: Session) -> None:
    db.query(AuthSession).filter(AuthSession.expires_at < datetime.utcnow()).delete()
    db.commit()


def get_or_create_google_user(db: Session, email: str, name: Optional[str] = None) -> Tuple[User, bool]:
    normalized_email = _normalize_email(email)
    user = db.query(User).filter(User.email == normalized_email).first()
    if user:
        if not user.is_active:
            raise ValueError("User account is deactivated")
        return user, False

    first_name, last_name = _split_name(name)
    user = User(
        user_id=str(uuid.uuid4()),
        user_name=_build_unique_username(db, normalized_email, name),
        email=normalized_email,
        first_name=first_name,
        last_name=last_name,
        role="user",
        is_active=True,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user, True


def create_session_token(db: Session, user: User, provider: str = GOOGLE_PROVIDER) -> str:
    delete_expired_sessions(db)

    session_token = secrets.token_urlsafe(32)
    db.add(
        AuthSession(
            session_id=str(uuid.uuid4()),
            session_token_hash=_hash_session_token(session_token),
            user_id=user.user_id,
            provider=provider,
            expires_at=datetime.utcnow() + timedelta(days=SESSION_TTL_DAYS),
        )
    )
    db.commit()
    return session_token


def get_user_by_session_token(db: Session, session_token: str) -> Optional[User]:
    delete_expired_sessions(db)

    token_hash = _hash_session_token(session_token)
    session = db.query(AuthSession).filter(AuthSession.session_token_hash == token_hash).first()
    if not session:
        return None

    return db.query(User).filter(User.user_id == session.user_id).first()


def delete_session_token(db: Session, session_token: str) -> bool:
    token_hash = _hash_session_token(session_token)
    session = db.query(AuthSession).filter(AuthSession.session_token_hash == token_hash).first()
    if not session:
        return False

    db.delete(session)
    db.commit()
    return True
