from fastapi import HTTPException, Depends, status
from sqlalchemy.orm import Session
from .database import get_db
from .auth import get_current_user
from .models import User, RolePermission

class PermissionChecker:
    def __init__(self, required_permission: str):
        self.required_permission = required_permission

    def __call__(self, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
        # Admin bypass
        if current_user.role == "admin":
            return current_user
            
        # Check specific role permission
        has_perm = db.query(RolePermission).filter(
            RolePermission.role == current_user.role,
            RolePermission.permission_name == self.required_permission
        ).first()
        
        if not has_perm:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Not enough permissions. Required: {self.required_permission}"
            )
        return current_user

# Usage in routes:
# @app.post("/upload", dependencies=[Depends(PermissionChecker("upload_data"))])
