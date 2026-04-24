"""
File Service

This module handles file-related database operations.
"""

from sqlalchemy.orm import Session
from datetime import datetime
import json

from app.utils.database import File

class FileService:
    """Service for managing file metadata in database"""
    
    @staticmethod
    def create_file_record(db: Session, file_id: str, filename: str, file_path: str, csv_parser, user_id: str = None) -> File:
        """
        Create a new file record in database
        
        Args:
            db: Database session
            file_id: Unique file identifier
            filename: Original filename
            file_path: Path to stored file
            csv_parser: CSVParser instance with parsed data
            user_id: User ID (optional, for user-based isolation)
        
        Returns:
            FileMetadata object
        """
        try:
            # Get data from parser
            columns = csv_parser.get_columns()
            numeric_columns = csv_parser.get_numeric_columns()
            row_count = csv_parser.get_row_count()
            column_types = csv_parser.get_column_types()
            missing_values = csv_parser.get_missing_values()
            file_size = csv_parser.get_file_size()
            status = csv_parser.get_status()
            
            # Create file record
            file_record = File(
                file_id=file_id,
                user_id=user_id,
                filename=filename,
                file_path=file_path,
                columns=columns,
                column_types=column_types,
                numeric_columns=numeric_columns,
                row_count=row_count,
                status=status,
                file_size_bytes=file_size,
                missing_values=missing_values,
                is_valid="1"
            )
            
            db.add(file_record)
            db.commit()
            db.refresh(file_record)
            
            return file_record
        
        except Exception as e:
            db.rollback()
            raise Exception(f"Error creating file record: {str(e)}")
    
    @staticmethod
    def get_file_by_id(db: Session, file_id: str) -> File:
        """Get file record by file_id"""
        return db.query(File).filter(File.file_id == file_id).first()

    @staticmethod
    def get_user_file_by_id(db: Session, file_id: str, user_id: str) -> File:
        """Get a file record owned by a specific user."""
        return db.query(File).filter(
            File.file_id == file_id,
            File.user_id == user_id,
        ).first()
    
    @staticmethod
    def get_all_files(db: Session) -> list:
        """Get all file records"""
        return db.query(File).all()
    
    @staticmethod
    def get_user_files(db: Session, user_id: str) -> list:
        """Get all files for a specific user"""
        return db.query(File).filter(File.user_id == user_id).all()
    
    @staticmethod
    def update_file(db: Session, file_id: str, **kwargs) -> File:
        """Update file record"""
        file_record = db.query(File).filter(File.file_id == file_id).first()
        
        if not file_record:
            return None
        
        for key, value in kwargs.items():
            if hasattr(file_record, key):
                setattr(file_record, key, value)
        
        db.commit()
        db.refresh(file_record)
        return file_record
    
    @staticmethod
    def delete_file(db: Session, file_id: str) -> bool:
        """Delete file record"""
        file_record = db.query(File).filter(File.file_id == file_id).first()
        
        if not file_record:
            return False
        
        db.delete(file_record)
        db.commit()
        return True
