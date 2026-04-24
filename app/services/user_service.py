from typing import Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import and_
from datetime import datetime

from app.utils.database import User, File, AnalysisHistory


class UserService:
    """Service for managing user operations"""

    @staticmethod
    def get_user_by_id(db: Session, user_id: str) -> Optional[User]:
        """Get user by user_id"""
        return db.query(User).filter(User.user_id == user_id).first()
    
    @staticmethod
    def get_user_by_username(db: Session, user_name: str) -> Optional[User]:
        """Get user by username"""
        return db.query(User).filter(User.user_name == user_name).first()
    
    @staticmethod
    def get_user_by_email(db: Session, email: str) -> Optional[User]:
        """Get user by email"""
        return db.query(User).filter(User.email == email.strip().lower()).first()

    @staticmethod
    def list_users(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        active_only: bool = False,
    ) -> List[User]:
        """List users with optional active-only filtering."""
        query = db.query(User)
        if active_only:
            query = query.filter(User.is_active == True)
        return query.order_by(User.created_at.desc()).offset(skip).limit(limit).all()

    @staticmethod
    def list_active_users(db: Session, skip: int = 0, limit: int = 100) -> List[User]:
        """List all active users"""
        return UserService.list_users(db, skip=skip, limit=limit, active_only=True)
    
    @staticmethod
    def update_user(
        db: Session,
        user_id: str,
        **kwargs
    ) -> Optional[User]:
        """Update user details"""
        user = db.query(User).filter(User.user_id == user_id).first()
        
        if not user:
            return None

        email = kwargs.get("email")
        if email is not None:
            normalized_email = email.strip().lower()
            existing_user = db.query(User).filter(
                and_(User.email == normalized_email, User.user_id != user_id)
            ).first()
            if existing_user:
                raise ValueError("Email already registered")
            kwargs["email"] = normalized_email
        
        allowed_fields = ['first_name', 'last_name', 'organization', 'email']
        for field, value in kwargs.items():
            if field in allowed_fields and value is not None:
                setattr(user, field, value)
        
        user.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(user)
        return user
    
    @staticmethod
    def deactivate_user(db: Session, user_id: str) -> Optional[User]:
        """Deactivate a user (soft delete)"""
        user = db.query(User).filter(User.user_id == user_id).first()
        
        if not user:
            return None
        
        user.is_active = False
        user.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(user)
        return user
    
    @staticmethod
    def activate_user(db: Session, user_id: str) -> Optional[User]:
        """Activate a deactivated user"""
        user = db.query(User).filter(User.user_id == user_id).first()
        
        if not user:
            return None
        
        user.is_active = True
        user.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(user)
        return user


class UserDataAccessService:
    """Service to enforce user-based data isolation and access control"""
    
    @staticmethod
    def verify_file_ownership(db: Session, file_id: str, user_id: str) -> bool:
        """Verify that a file belongs to the user"""
        file = db.query(File).filter(
            and_(File.file_id == file_id, File.user_id == user_id)
        ).first()
        return file is not None
    
    @staticmethod
    def get_user_files(db: Session, user_id: str, skip: int = 0, limit: int = 100) -> List[File]:
        """Get all files for a specific user"""
        return db.query(File).filter(File.user_id == user_id).offset(skip).limit(limit).all()
    
    @staticmethod
    def get_user_file_by_id(db: Session, user_id: str, file_id: str) -> Optional[File]:
        """Get a specific file for a user"""
        return db.query(File).filter(
            and_(File.user_id == user_id, File.file_id == file_id)
        ).first()
    
    @staticmethod
    def get_user_analysis_history(
        db: Session,
        user_id: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[AnalysisHistory]:
        """Get analysis history for a user"""
        return db.query(AnalysisHistory).filter(
            AnalysisHistory.user_id == user_id
        ).order_by(AnalysisHistory.created_at.desc()).offset(skip).limit(limit).all()
    
    @staticmethod
    def get_file_analysis_history(
        db: Session,
        user_id: str,
        file_id: str
    ) -> List[AnalysisHistory]:
        """Get analysis history for a specific file (user must own the file)"""
        return db.query(AnalysisHistory).filter(
            and_(
                AnalysisHistory.user_id == user_id,
                AnalysisHistory.file_id == file_id
            )
        ).order_by(AnalysisHistory.created_at.desc()).all()
    
    @staticmethod
    def delete_user_file(db: Session, user_id: str, file_id: str) -> bool:
        """Delete a file (only if user owns it)"""
        file = db.query(File).filter(
            and_(File.file_id == file_id, File.user_id == user_id)
        ).first()
        
        if not file:
            return False
        
        db.delete(file)
        db.commit()
        return True
