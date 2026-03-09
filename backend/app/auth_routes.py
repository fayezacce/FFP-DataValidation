from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from datetime import timedelta
from typing import List
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
from pydantic import BaseModel

router = APIRouter(prefix="/auth", tags=["auth"])

class UserCreate(BaseModel):
    username: str
    password: str
    role: str = "viewer"

class UserOut(BaseModel):
    id: int
    username: str
    role: str
    is_active: bool
    
    class Config:
        from_attributes = True

class Token(BaseModel):
    access_token: str
    token_type: str
    user: UserOut

@router.post("/login", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = authenticate_user(db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
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
