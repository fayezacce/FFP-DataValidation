from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import desc
from typing import List, Optional
import math

from .database import get_db
from .models import User, AuditLog, ApiUsageLog
from .auth import get_current_user
from .rbac import PermissionChecker

router = APIRouter(tags=["audit"])

@router.get("/logs", dependencies=[Depends(PermissionChecker("view_admin"))])
def get_audit_logs(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=1000),
    action: Optional[str] = None,
    target_table: Optional[str] = None,
    username: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get paginated audit logs (Admin only).
    """
    # Double check role just in case permission checker isn't strict enough on "view_admin"
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Only administrators can view audit logs.")

    query = db.query(AuditLog)

    if action:
        query = query.filter(AuditLog.action == action)
    if target_table:
        query = query.filter(AuditLog.target_table == target_table)
    if username:
        query = query.filter(AuditLog.username.ilike(f"%{username}%"))

    total = query.count()
    total_pages = math.ceil(total / limit)
    offset = (page - 1) * limit

    logs = query.order_by(desc(AuditLog.created_at)).offset(offset).limit(limit).all()

    return {
        "items": [
            {
                "id": log.id,
                "user_id": log.user_id,
                "username": log.username,
                "action": log.action,
                "target_table": log.target_table,
                "target_id": log.target_id,
                "details": log.details,
                "created_at": log.created_at.isoformat() + "Z"
            }
            for log in logs
        ],
        "total": total,
        "page": page,
        "limit": limit,
        "total_pages": total_pages
    }

@router.get("/api-usage", dependencies=[Depends(PermissionChecker("view_admin"))])
def get_api_usage_logs(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=1000),
    db: Session = Depends(get_db)
):
    """
    Get paginated API usage logs.
    """
    query = db.query(ApiUsageLog)
    total = query.count()
    offset = (page - 1) * limit
    logs = query.order_by(desc(ApiUsageLog.created_at)).offset(offset).limit(limit).all()

    return {
        "items": [
            {
                "id": log.id,
                "username": log.username,
                "method": log.method,
                "path": log.path,
                "status_code": log.status_code,
                "latency_ms": log.latency_ms,
                "created_at": log.created_at.isoformat() + "Z"
            }
            for log in logs
        ],
        "total": total,
        "page": page
    }
