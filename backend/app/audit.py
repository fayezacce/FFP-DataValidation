from sqlalchemy.orm import Session
from .models import AuditLog, ApiUsageLog, User
from datetime import datetime
import json
from typing import Any, Optional

def log_audit(db: Session, user: User, action: str, target_table: str, target_id: Any, old_data: Optional[dict] = None, new_data: Optional[dict] = None):
    """Log an administrative action (CREATE, UPDATE, DELETE)."""
    details = {}
    if old_data: details["old"] = old_data
    if new_data: details["new"] = new_data
    
    log = AuditLog(
        user_id=user.id,
        username=user.username,
        action=action,
        target_table=target_table,
        target_id=str(target_id),
        details=details
    )
    db.add(log)
    db.commit()

def log_api_usage(db: Session, user_id: Optional[int], username: Optional[str], method: str, path: str, status_code: int, ip_address: str, latency_ms: float):
    """Log an API interaction."""
    log = ApiUsageLog(
        user_id=user_id,
        username=username,
        method=method,
        path=path,
        status_code=status_code,
        ip_address=ip_address,
        latency_ms=latency_ms
    )
    db.add(log)
    db.commit()
