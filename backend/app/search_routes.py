"""
FFP Data Validator — Search Routes
Handles NID/Name/DOB search, NID lookup, and record deletion.
"""
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
import logging

from .database import get_db
from .models import User, SummaryStats, ValidRecord
from .auth import get_current_user
from .rbac import PermissionChecker
from .audit import log_audit

logger = logging.getLogger("ffp")
router = APIRouter(tags=["search"])


@router.get("/search", dependencies=[Depends(PermissionChecker("view_stats"))])
async def search_records(
    query: str,
    type: str = "nid",
    page: int = 1,
    limit: int = 50,
    regex: bool = False,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Search for valid records by NID, DOB, or Name with pagination and regex support."""
    query = query.strip()
    if not query:
        return {"results": [], "total": 0, "page": page, "limit": limit}

    limit = min(limit, 200)
    offset = (page - 1) * limit

    base_query = db.query(ValidRecord)

    if type == "dob":
        data_query = base_query.filter(ValidRecord.dob == query)
    elif type == "name":
        data_query = base_query.filter(ValidRecord.name.ilike(f"%{query}%"))
    else:
        if regex:
            import re as re_mod
            try:
                re_mod.compile(query)
            except re_mod.error:
                raise HTTPException(status_code=400, detail="Invalid regex pattern")
            data_query = base_query.filter(ValidRecord.nid.op("~*")(query))
        else:
            data_query = base_query.filter(ValidRecord.nid.contains(query))

    total = data_query.count()
    results = data_query.offset(offset).limit(limit).all()

    return {"results": results, "total": total, "page": page, "limit": limit}


@router.get("/nid/{nid}", dependencies=[Depends(PermissionChecker("view_stats"))])
async def check_nid(
    nid: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Check if an NID exists in the database. Returns the full record if found."""
    record = db.query(ValidRecord).filter(ValidRecord.nid == nid.strip()).first()
    if not record:
        raise HTTPException(status_code=404, detail=f"NID {nid} not found in database")
    return {
        "found": True,
        "nid": record.nid,
        "dob": record.dob,
        "name": record.name,
        "division": record.division,
        "district": record.district,
        "upazila": record.upazila,
        "source_file": record.source_file,
        "data": record.data,
        "created_at": record.created_at.isoformat() + "Z" if record.created_at else None,
        "updated_at": record.updated_at.isoformat() + "Z" if record.updated_at else None,
    }


@router.delete("/record/{record_id}", dependencies=[Depends(PermissionChecker("manage_users"))])
async def delete_record(
    record_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Delete a single ValidRecord by ID. Decrements the related SummaryStats."""
    record = db.query(ValidRecord).filter(ValidRecord.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")

    summary = db.query(SummaryStats).filter(
        SummaryStats.district == record.district,
        SummaryStats.upazila == record.upazila,
    ).first()

    if summary:
        summary.valid = max(0, summary.valid - 1)
        summary.total = max(0, summary.total - 1)

    db.delete(record)
    db.commit()

    log_audit(db, current_user, "DELETE", "valid_records", record_id, old_data={"nid": record.nid})
    return {"deleted": True, "id": record_id, "nid": record.nid}
