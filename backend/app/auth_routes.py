from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from datetime import timedelta
from typing import List, Optional
from .database import get_db
from .models import User
from .auth import (
    get_current_user, 
    hash_password,
    authenticate_user,
    create_access_token
)
from .rbac import PermissionChecker
from .audit import log_audit
from pydantic import BaseModel, Field
from enum import Enum
from fastapi import Request
from .auth import limiter

router = APIRouter(prefix="/auth", tags=["auth"])

class UserRole(str, Enum):
    admin = "admin"
    uploader = "uploader"
    viewer = "viewer"

class UserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=50, pattern=r'^[a-zA-Z0-9_.-]+$')
    password: str
    role: UserRole = UserRole.viewer

import secrets
from datetime import datetime

class UserOut(BaseModel):
    id: int
    username: str
    role: str
    is_active: bool
    api_key: Optional[str] = None
    api_key_last_used: Optional[datetime] = None
    api_rate_limit: int = 60
    api_total_limit: Optional[int] = None
    api_usage_count: int = 0
    
    class Config:
        from_attributes = True

class Token(BaseModel):
    access_token: str
    token_type: str
    user: UserOut

@router.post("/login", response_model=Token)
@limiter.limit("5/minute")
async def login(request: Request, form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = authenticate_user(db, form_data.username, form_data.password)
    if not user:
        log_audit(db, None, "LOGIN_FAIL", "users", 0, new_data={"username": form_data.username})
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    log_audit(db, user, "LOGIN_SUCCESS", "users", user.id)
    access_token = create_access_token(data={"sub": user.username})
    return {
        "access_token": access_token, 
        "token_type": "bearer",
        "user": user
    }

@router.get("/me", response_model=UserOut)
async def read_users_me(current_user: User = Depends(get_current_user)):
    return current_user

# Admin-only routes
@router.post("/users", response_model=UserOut, dependencies=[Depends(PermissionChecker("manage_users"))])
async def create_user(
    user_in: UserCreate, 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # Check if user already exists
    existing = db.query(User).filter(User.username == user_in.username).first()
    if existing:
        raise HTTPException(status_code=400, detail="Username already registered")
    
    new_user = User(
        username=user_in.username,
        hashed_password=hash_password(user_in.password),
        role=user_in.role
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    
    log_audit(db, current_user, "CREATE", "users", new_user.id, new_data={"username": new_user.username, "role": new_user.role})
    
    return new_user

@router.get("/users", response_model=List[UserOut], dependencies=[Depends(PermissionChecker("manage_users"))])
async def list_users(
    db: Session = Depends(get_db)
):
    return db.query(User).all()


class UpdateUser(BaseModel):
    password: Optional[str] = None
    role: Optional[str] = None
    api_rate_limit: Optional[int] = None
    api_total_limit: Optional[int] = None

@router.put("/users/{user_id}", response_model=UserOut, dependencies=[Depends(PermissionChecker("manage_users"))])
async def update_user(
    user_id: int,
    user_update: UpdateUser,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    old_data = {
        "role": user.role,
        "api_rate_limit": user.api_rate_limit,
        "api_total_limit": user.api_total_limit
    }
    new_data = {}
    
    if user_update.role:
        user.role = user_update.role
        new_data["role"] = user_update.role
        
    if user_update.password:
        user.hashed_password = hash_password(user_update.password)
        new_data["password"] = "[HIDDEN]"

    if user_update.api_rate_limit is not None:
        user.api_rate_limit = user_update.api_rate_limit
        new_data["api_rate_limit"] = user_update.api_rate_limit

    if user_update.api_total_limit is not None:
        user.api_total_limit = user_update.api_total_limit
        new_data["api_total_limit"] = user_update.api_total_limit
        
    db.commit()
    db.refresh(user)
    
    log_audit(db, current_user, "UPDATE", "users", user_id, old_data=old_data, new_data=new_data)
    
    return user

@router.post("/users/{user_id}/generate-api-key", response_model=UserOut, dependencies=[Depends(PermissionChecker("manage_users"))])
async def generate_user_api_key(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    import hashlib
    new_key = secrets.token_hex(32) # 64 chars
    key_hash = hashlib.sha256(new_key.encode()).hexdigest()
    user.api_key = key_hash
    db.commit()
    db.refresh(user)
    
    log_audit(db, current_user, "GENERATE_KEY", "users", user_id, new_data={"username": user.username})
    
    # Temporarily attach the raw key to the response object for ONE-TIME viewing
    user.api_key = new_key
    return user

@router.delete("/users/{user_id}", dependencies=[Depends(PermissionChecker("manage_users"))])
async def delete_user(
    user_id: int, 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user.username == current_user.username:
        raise HTTPException(status_code=400, detail="Cannot delete yourself")
    
    username = user.username
    db.delete(user)
    db.commit()
    
    log_audit(db, current_user, "DELETE", "users", user_id, old_data={"username": username})
    
    return {"detail": "User deleted"}
