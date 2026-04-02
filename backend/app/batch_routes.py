"""
FFP Data Validator — Batch Routes
Handles upload batch history, deletion, and batch file listings.
"""
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from datetime import datetime, timezone
import os
import urllib.parse
import logging

from .database import get_db
from .models import User, SummaryStats, ValidRecord, InvalidRecord, UploadBatch, Upazila
from .auth import get_current_user
from .rbac import PermissionChecker
from .audit import log_audit

logger = logging.getLogger("ffp")
router = APIRouter(tags=["batches"])


@router.get("/statistics/history", dependencies=[Depends(PermissionChecker("view_stats"))])
async def get_upazila_history(
    district: str,
    upazila: str,
    db: Session = Depends(get_db),
):
    """Return upload history for a specific upazila, validated against official names."""
    official = db.query(Upazila).filter(
        (Upazila.name.ilike(upazila)) & (Upazila.district_name.ilike(district))
    ).first()

    target_upazila = official.name if official else upazila
    target_district = official.district_name if official else district

    batches = db.query(UploadBatch).filter(
        UploadBatch.district == target_district,
        UploadBatch.upazila == target_upazila,
    ).order_by(UploadBatch.created_at.desc()).all()
    return batches


@router.delete("/batches/{batch_id}", dependencies=[Depends(PermissionChecker("manage_geo"))])
async def delete_batch(
    batch_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Delete an upload batch and its associated valid records. Updates summary stats."""
    batch = db.query(UploadBatch).filter(UploadBatch.id == batch_id).first()
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")

    if batch.status == "deleted":
        return {"message": "Batch already deleted"}

    db.query(ValidRecord).filter(ValidRecord.batch_id == batch_id).delete()
    db.query(InvalidRecord).filter(InvalidRecord.batch_id == batch_id).delete()

    batch.status = "deleted"
    db.commit()

    summary = db.query(SummaryStats).filter(
        SummaryStats.district == batch.district,
        SummaryStats.upazila == batch.upazila,
    ).first()
    if summary:
        live_valid = db.query(ValidRecord).filter(ValidRecord.district == batch.district, ValidRecord.upazila == batch.upazila).count()
        live_invalid = db.query(InvalidRecord).filter(InvalidRecord.district == batch.district, InvalidRecord.upazila == batch.upazila).count()
        summary.valid = live_valid
        summary.invalid = live_invalid
        summary.total = live_valid + live_invalid
        summary.version += 1
        db.commit()

    log_audit(db, current_user, "DELETE", "upload_batch", batch_id, old_data={"filename": batch.filename})
    return {"message": "Batch deleted successfully", "deleted_valid": batch.new_records}


@router.get("/upazila/batch-files", dependencies=[Depends(PermissionChecker("view_stats"))])
async def upazila_batch_files(
    division: str,
    district: str,
    upazila: str,
    db: Session = Depends(get_db),
):
    """Return all upload batches for this upazila with their archived file URLs."""
    batches = db.query(UploadBatch).filter(
        UploadBatch.division == division,
        UploadBatch.district == district,
        UploadBatch.upazila == upazila,
        UploadBatch.status == "completed",
    ).order_by(UploadBatch.created_at.desc()).all()

    result = []
    for b in batches:
        safe = f"{b.district}_{b.upazila}".replace(" ", "_").replace("/", "_")
        entry = {
            "batch_id": b.id,
            "filename": b.original_name or b.filename,
            "uploaded_at": b.created_at.isoformat() + "Z",
            "uploaded_by": b.username,
            "total_rows": b.total_rows,
            "valid_count": b.valid_count,
            "invalid_count": b.invalid_count,
            "new_records": b.new_records,
            "updated_records": b.updated_records,
            "valid_url": f"/api/downloads/{urllib.parse.quote(safe + '_valid.xlsx')}",
            "invalid_url": f"/api/downloads/{urllib.parse.quote(safe + '_invalid.xlsx')}",
            "pdf_url": f"/api/downloads/{urllib.parse.quote(safe + '_validation_Report.pdf')}",
            "pdf_invalid_url": f"/api/downloads/{urllib.parse.quote(safe + '_invalid_Report.pdf')}",
        }
        for key in ("valid_url", "invalid_url", "pdf_url", "pdf_invalid_url"):
            local = os.path.join("downloads", urllib.parse.unquote(os.path.basename(entry[key])))
            entry[key] = entry[key] if os.path.exists(local) else None
        result.append(entry)

    return {"batches": result, "total": len(result)}
