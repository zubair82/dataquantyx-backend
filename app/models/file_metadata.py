from sqlalchemy import Column, String, DateTime, Integer, ForeignKey, JSON, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class User(Base):
    """User model for user management"""
    __tablename__ = "users"
    
    user_id = Column(String(50), primary_key=True, index=True)
    user_name = Column(String(100), unique=True, index=True, nullable=False)
    email = Column(String(120), unique=True, index=True, nullable=False)
    first_name = Column(String(50), nullable=True)
    last_name = Column(String(50), nullable=True)
    organization = Column(String(120), nullable=True)
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    files = relationship("File", back_populates="user", cascade="all, delete-orphan")
    analysis_history = relationship("AnalysisHistory", back_populates="user", cascade="all, delete-orphan")

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

class File(Base):
    """File metadata model with user ownership"""
    __tablename__ = "files"
    
    file_id = Column(String(50), primary_key=True, index=True)
    user_id = Column(String(50), ForeignKey("users.user_id"), nullable=False, index=True)
    filename = Column(String(255), nullable=False)
    file_path = Column(String(500), nullable=False)
    file_size_bytes = Column(Integer, nullable=True)
    columns = Column(JSON, nullable=True)
    numeric_columns = Column(JSON, nullable=True)
    row_count = Column(Integer, nullable=True)
    status = Column(String(50), default="completed")
    upload_time = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Relationships
    user = relationship("User", back_populates="files")
    analysis_history = relationship("AnalysisHistory", back_populates="file", cascade="all, delete-orphan")

class AnalysisHistory(Base):
    """Track analysis history per user and file"""
    __tablename__ = "analysis_history"
    
    history_id = Column(String(50), primary_key=True, index=True)
    user_id = Column(String(50), ForeignKey("users.user_id"), nullable=False, index=True)
    file_id = Column(String(50), ForeignKey("files.file_id"), nullable=False, index=True)
    analysis_type = Column(String(50), nullable=False)
    result_path = Column(String(500), nullable=True)
    status = Column(String(50), default="completed")
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Relationships
    user = relationship("User", back_populates="analysis_history")
    file = relationship("File", back_populates="analysis_history")