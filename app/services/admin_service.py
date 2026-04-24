import base64
import hashlib
import hmac
import os
import secrets
import uuid
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy.orm import Session

from app.utils.database import Admin, AdminSession

SESSION_TTL_DAYS = int(os.getenv("SESSION_TTL_DAYS", "7"))
PBKDF2_ITERATIONS = int(os.getenv("PASSWORD_HASH_ITERATIONS", "390000"))
DEFAULT_ADMIN_EMAIL = os.getenv("DEFAULT_ADMIN_EMAIL", "admin.dataqtx@gmail.com").strip().lower()
DEFAULT_ADMIN_PASSWORD = os.getenv("DEFAULT_ADMIN_PASSWORD", "Admin@123")
DEFAULT_ADMIN_NAME = os.getenv("DEFAULT_ADMIN_NAME", "DataQuantyx Admin")


def _normalize_email(email: str) -> str:
    return email.strip().lower()


def _hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    derived_key = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PBKDF2_ITERATIONS,
    )
    salt_b64 = base64.b64encode(salt).decode("utf-8")
    key_b64 = base64.b64encode(derived_key).decode("utf-8")
    return f"pbkdf2_sha256${PBKDF2_ITERATIONS}${salt_b64}${key_b64}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        algorithm, iteration_text, salt_b64, key_b64 = password_hash.split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False

        iterations = int(iteration_text)
        salt = base64.b64decode(salt_b64.encode("utf-8"))
        expected_key = base64.b64decode(key_b64.encode("utf-8"))
    except (TypeError, ValueError):
        return False

    actual_key = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        iterations,
    )
    return hmac.compare_digest(actual_key, expected_key)


def delete_expired_admin_sessions(db: Session) -> None:
    db.query(AdminSession).filter(AdminSession.expires_at < datetime.utcnow()).delete()
    db.commit()


def ensure_default_admin_exists(db: Session) -> Admin:
    admin = db.query(Admin).filter(Admin.email == DEFAULT_ADMIN_EMAIL).first()
    if admin:
        return admin

    admin = Admin(
        admin_id=str(uuid.uuid4()),
        email=DEFAULT_ADMIN_EMAIL,
        full_name=DEFAULT_ADMIN_NAME,
        password_hash=hash_password(DEFAULT_ADMIN_PASSWORD),
        is_active=True,
    )
    db.add(admin)
    db.commit()
    db.refresh(admin)
    return admin


def authenticate_admin(db: Session, email: str) -> Optional[Admin]:
    admin = db.query(Admin).filter(Admin.email == _normalize_email(email)).first()
    if not admin or not admin.is_active:
        return None

    return admin


def create_admin_session_token(db: Session, admin: Admin) -> str:
    delete_expired_admin_sessions(db)

    session_token = secrets.token_urlsafe(32)
    db.add(
        AdminSession(
            session_id=str(uuid.uuid4()),
            session_token_hash=_hash_session_token(session_token),
            admin_id=admin.admin_id,
            expires_at=datetime.utcnow() + timedelta(days=SESSION_TTL_DAYS),
        )
    )
    db.commit()
    return session_token


def get_admin_by_session_token(db: Session, session_token: str) -> Optional[Admin]:
    delete_expired_admin_sessions(db)

    token_hash = _hash_session_token(session_token)
    session = db.query(AdminSession).filter(AdminSession.session_token_hash == token_hash).first()
    if not session:
        return None

    return db.query(Admin).filter(Admin.admin_id == session.admin_id).first()


def delete_admin_session_token(db: Session, session_token: str) -> bool:
    token_hash = _hash_session_token(session_token)
    session = db.query(AdminSession).filter(AdminSession.session_token_hash == token_hash).first()
    if not session:
        return False

    db.delete(session)
    db.commit()
    return True
