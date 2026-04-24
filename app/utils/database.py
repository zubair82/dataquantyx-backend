"""
Database Configuration and Models

This module sets up SQLite database connection and defines ORM models
for storing file metadata and user information.
"""

from sqlalchemy import create_engine, Column, String, Integer, DateTime, JSON, Text, Boolean, ForeignKey, inspect, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime

from app.utils.paths import DB_PATH

# Database configuration
DATABASE_URL = f"sqlite:///{DB_PATH}"

# Create database engine
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    echo=False  # Set to True for SQL debug logging
)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for ORM models
Base = declarative_base()


# ==================== USER MODELS ====================

class User(Base):
    """User model for user management"""
    __tablename__ = "users"
    
    user_id = Column(String(50), primary_key=True, index=True)
    user_name = Column(String(100), unique=True, index=True, nullable=False)
    email = Column(String(120), unique=True, index=True, nullable=False)
    first_name = Column(String(50), nullable=True)
    last_name = Column(String(50), nullable=True)
    organization = Column(String(120), nullable=True)
    password_hash = Column(String(255), nullable=True)
    role = Column(String(50), default="user", nullable=False)
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    files = relationship("File", back_populates="user", cascade="all, delete-orphan")
    analysis_history = relationship("AnalysisHistory", back_populates="user", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<User(user_id={self.user_id}, user_name={self.user_name})>"


class Admin(Base):
    """Admin model for privileged platform access."""
    __tablename__ = "admins"

    admin_id = Column(String(50), primary_key=True, index=True)
    email = Column(String(120), unique=True, index=True, nullable=False)
    full_name = Column(String(120), nullable=False)
    password_hash = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True, index=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<Admin(admin_id={self.admin_id}, email={self.email})>"


class UserProfile(Base):
    """Extended user profile information"""
    __tablename__ = "user_profiles"
    
    profile_id = Column(String(50), primary_key=True, index=True)
    user_id = Column(String(50), index=True, nullable=False)
    phone = Column(String(20), nullable=True)
    department = Column(String(100), nullable=True)
    role = Column(String(50), nullable=True)
    preferences = Column(String(500), nullable=True)
    last_login = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<UserProfile(profile_id={self.profile_id}, user_id={self.user_id})>"


# ==================== FILE MODELS ====================

class File(Base):
    """File metadata model with user ownership"""
    __tablename__ = "files"
    
    file_id = Column(String(36), primary_key=True, index=True)
    user_id = Column(String(50), ForeignKey("users.user_id"), nullable=False, index=True)
    filename = Column(String(255), nullable=False)
    file_path = Column(String(500), nullable=False)
    upload_time = Column(DateTime, default=datetime.utcnow, index=True)
    status = Column(String(50), default="completed")
    
    # CSV metadata
    columns = Column(JSON, nullable=False)  # List of column names
    column_types = Column(JSON, nullable=False)  # Dict of column types
    numeric_columns = Column(JSON, nullable=False)  # List of numeric columns
    row_count = Column(Integer, nullable=False)
    file_size_bytes = Column(Integer, nullable=False)
    
    # Statistics and validation
    missing_values = Column(JSON, nullable=False)  # Dict of missing value counts
    statistics = Column(JSON, nullable=True)  # Dict of statistics for numeric columns
    cleaning_report = Column(JSON, nullable=True)  # Dict of cleaned values per column
    validation_errors = Column(Text, nullable=True)  # Any validation errors
    is_valid = Column(String(1), default="1", nullable=False)  # 1=valid, 0=invalid
    
    # Relationships
    user = relationship("User", back_populates="files")
    analysis_history = relationship("AnalysisHistory", back_populates="file", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<File(file_id={self.file_id}, filename={self.filename}, user_id={self.user_id})>"


class AnalysisHistory(Base):
    """Track analysis history per user and file"""
    __tablename__ = "analysis_history"
    
    history_id = Column(String(50), primary_key=True, index=True)
    user_id = Column(String(50), ForeignKey("users.user_id"), nullable=False, index=True)
    file_id = Column(String(36), ForeignKey("files.file_id"), nullable=False, index=True)
    analysis_type = Column(String(50), nullable=False)
    result_path = Column(String(500), nullable=True)
    status = Column(String(50), default="completed")
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Relationships
    user = relationship("User", back_populates="analysis_history")
    file = relationship("File", back_populates="analysis_history")
    
    def __repr__(self):
        return f"<AnalysisHistory(history_id={self.history_id}, analysis_type={self.analysis_type})>"


class RevokedToken(Base):
    """Tracks access tokens that have been explicitly logged out."""
    __tablename__ = "revoked_tokens"

    token_hash = Column(String(64), primary_key=True, index=True)
    user_id = Column(String(50), ForeignKey("users.user_id"), nullable=False, index=True)
    revoked_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    expires_at = Column(DateTime, nullable=False, index=True)

    def __repr__(self):
        return f"<RevokedToken(user_id={self.user_id}, token_hash={self.token_hash[:8]}...)>"


class AuthSession(Base):
    """Stores active server-side login sessions."""
    __tablename__ = "auth_sessions"

    session_id = Column(String(50), primary_key=True, index=True)
    session_token_hash = Column(String(64), unique=True, nullable=False, index=True)
    user_id = Column(String(50), ForeignKey("users.user_id"), nullable=False, index=True)
    provider = Column(String(50), default="google_mock", nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    expires_at = Column(DateTime, nullable=False, index=True)

    def __repr__(self):
        return f"<AuthSession(session_id={self.session_id}, user_id={self.user_id})>"


class AdminSession(Base):
    """Stores active admin login sessions."""
    __tablename__ = "admin_sessions"

    session_id = Column(String(50), primary_key=True, index=True)
    session_token_hash = Column(String(64), unique=True, nullable=False, index=True)
    admin_id = Column(String(50), ForeignKey("admins.admin_id"), nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    expires_at = Column(DateTime, nullable=False, index=True)

    def __repr__(self):
        return f"<AdminSession(session_id={self.session_id}, admin_id={self.admin_id})>"


# ==================== DATABASE FUNCTIONS ====================

def init_db():
    """
    Create all tables in the database
    """
    Base.metadata.create_all(bind=engine)
    ensure_schema_updates()
    print("✅ Database initialized successfully")
    print("   Tables created:")
    print("   - users")
    print("   - admins")
    print("   - user_profiles")
    print("   - files")
    print("   - analysis_history")
    print("   - revoked_tokens")
    print("   - auth_sessions")
    print("   - admin_sessions")


def ensure_schema_updates():
    """Apply lightweight schema updates for existing SQLite databases."""
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())

    if "users" not in existing_tables:
        return

    user_columns = {column["name"] for column in inspector.get_columns("users")}

    with engine.begin() as connection:
        if "password_hash" not in user_columns:
            connection.execute(text("ALTER TABLE users ADD COLUMN password_hash VARCHAR(255)"))
            print("   - added users.password_hash")


def get_db():
    """
    Get database session
    
    Yields:
        SQLAlchemy session
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def close_db():
    """
    Close database connection
    """
    engine.dispose()
