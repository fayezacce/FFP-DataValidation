"""
FFP Data Validator — Task Routes
Handles background task polling, listing, and cleanup.
"""
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
import os
import glob
import urllib.parse
import logging

from .database import get_db
from .models import User, BackgroundTask
from .auth import get_current_user
from .rbac import PermissionChecker

logger = logging.getLogger("ffp")
router = APIRouter(tags=["tasks"])


def _task_to_dict(t):
    return {
        "id": t.id,
        "task_name": t.task_name,
        "user_id": t.user_id,
        "status": t.status,
        "progress": t.progress,
        "message": t.message,
        "result_url": t.result_url,
        "error_details": t.error_details,
        "created_at": t.created_at.isoformat() if t.created_at else None,
        "updated_at": t.updated_at.isoformat() if t.updated_at else None,
    }


@router.get("/my-tasks", dependencies=[Depends(get_current_user)])
def get_my_tasks(db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    tasks = db.query(BackgroundTask).filter(
        BackgroundTask.user_id == current_user.id,
    ).order_by(BackgroundTask.created_at.desc()).limit(20).all()
    return [_task_to_dict(t) for t in tasks]


@router.get("/{task_id}", dependencies=[Depends(get_current_user)])
def get_task_status(task_id: str, db: Session = Depends(get_db)):
    task = db.query(BackgroundTask).filter(BackgroundTask.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return _task_to_dict(task)


@router.delete("/cleanup", dependencies=[Depends(PermissionChecker("view_admin"))])
def cleanup_tasks(db: Session = Depends(get_db)):
    tasks = db.query(BackgroundTask).filter(
        BackgroundTask.status.in_(["completed", "error"]),
    ).all()
    count = len(tasks)

    for t in tasks:
        if t.result_url:
            filename = urllib.parse.unquote(t.result_url.split("/")[-1])
            filepath = os.path.join("downloads", filename)
            if os.path.exists(filepath):
                try:
                    os.remove(filepath)
                except Exception:
                    pass

    db.query(BackgroundTask).filter(
        BackgroundTask.status.in_(["completed", "error"]),
    ).delete(synchronize_session=False)
    db.commit()

    for f in glob.glob("downloads/all_live_*.zip"):
        try:
            os.remove(f)
        except Exception:
            pass

    return {"message": f"Cleaned up {count} tasks and associated files"}
