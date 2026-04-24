from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from app.utils.database import Base

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    username = Column(String, unique=True, index=True)
    password_hash = Column(String)
    profile = relationship("UserProfile", back_populates="owner")

class UserProfile(Base):
    __tablename__ = 'user_profiles'
    id = Column(Integer, primary_key=True, index=True)
    full_name = Column(String, index=True)
    bio = Column(String)
    user_id = Column(Integer, ForeignKey('users.id'))
    owner = relationship("User", back_populates="profile")
