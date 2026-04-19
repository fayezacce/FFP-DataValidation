"""
FFP Data Validator — Statistics Routes
Handles statistics dashboard, deletion, updates, audit/API logs.
"""
from fastapi import APIRouter, HTTPException, Depends
from fastapi.requests import Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import datetime, timezone
import hashlib
import urllib.parse
import logging

from .database import get_db
from .models import (
    User, SummaryStats, ValidRecord, InvalidRecord, UploadBatch,
    Division, District, Upazila,
)
from .auth import get_current_user
from .rbac import PermissionChecker
from .audit import log_audit
from .bd_geo import get_division_for_district

logger = logging.getLogger("ffp")
router = APIRouter(tags=["statistics"])


@router.get("", dependencies=[Depends(PermissionChecker("view_stats"))])
async def get_statistics(
    request: Request,
    has_invalid: bool = False,
    division: str = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return accumulated stats derived from official Upazila table + SummaryStats."""
    query = db.query(Upazila, SummaryStats).outerjoin(
        SummaryStats,
        (Upazila.name == SummaryStats.upazila) & (Upazila.district_name == SummaryStats.district),
    ).filter(Upazila.is_active == True)

    # Multi-tenancy access control
    if current_user.role != "admin":
        if getattr(current_user, "division_access", None):
            query = query.filter(Upazila.division_name == current_user.division_access)
        if getattr(current_user, "district_access", None):
            query = query.filter(Upazila.district_name == current_user.district_access)
        if getattr(current_user, "upazila_access", None):
            query = query.filter(Upazila.name == current_user.upazila_access)

    if division:
        query = query.filter(Upazila.division_name == division)
    if has_invalid:
        query = query.filter(SummaryStats.invalid > 0)

    entries_query = query.order_by(Upazila.division_name, Upazila.district_name, Upazila.name).all()

    entries = []
    now = datetime.utcnow()
    for u, s in entries_query:
        entry_data = {
            "id": u.id,
            "division": u.division_name,
            "district": u.district_name,
            "upazila": u.name,
            "total": s.total if s else 0,
            "valid": s.valid if s else 0,
            "invalid": s.invalid if s else 0,
            "quota": u.quota or 0,
            "filename": s.filename if s else "",
            "version": s.version if s else 0,
            "created_at": (s.created_at if s else now),
            "updated_at": (s.updated_at if s else now),
            "pdf_url": f"/api/export/live?division={urllib.parse.quote(u.division_name)}&district={urllib.parse.quote(u.district_name)}&upazila={urllib.parse.quote(u.name)}&fmt=pdf",
            "pdf_invalid_url": f"/api/export/live-invalid?division={urllib.parse.quote(u.division_name)}&district={urllib.parse.quote(u.district_name)}&upazila={urllib.parse.quote(u.name)}&fmt=pdf",
            "excel_url": f"/api/export/live?division={urllib.parse.quote(u.division_name)}&district={urllib.parse.quote(u.district_name)}&upazila={urllib.parse.quote(u.name)}&fmt=xlsx",
            "excel_valid_url": f"/api/export/live?division={urllib.parse.quote(u.division_name)}&district={urllib.parse.quote(u.district_name)}&upazila={urllib.parse.quote(u.name)}&fmt=xlsx",
            "excel_invalid_url": f"/api/export/live-invalid?division={urllib.parse.quote(u.division_name)}&district={urllib.parse.quote(u.district_name)}&upazila={urllib.parse.quote(u.name)}&fmt=xlsx",
        }
        entries.append(entry_data)

    latest_ts = max((e["updated_at"] for e in entries), default=now)
    etag_raw = f"{len(entries)}:{latest_ts.isoformat()}"
    etag = '"' + hashlib.sha256(etag_raw.encode()).hexdigest()[:32] + '"'

    if request.headers.get("If-None-Match") == etag:
        return Response(status_code=304, headers={"ETag": etag})

    div_master_query = db.query(Division.name, func.count(District.id)).join(District, Division.name == District.division_name).group_by(Division.name)
    dist_master_query = db.query(District.name, func.count(Upazila.id)).join(Upazila, District.name == Upazila.district_name).group_by(District.name)
    
    grand_query = db.query(
        func.coalesce(func.sum(SummaryStats.total), 0).label("total"),
        func.coalesce(func.sum(SummaryStats.valid), 0).label("valid"),
        func.coalesce(func.sum(SummaryStats.invalid), 0).label("invalid")
    )

    if current_user.role != "admin":
        if getattr(current_user, "division_access", None):
            grand_query = grand_query.filter(SummaryStats.division == current_user.division_access)
            div_master_query = div_master_query.filter(Division.name == current_user.division_access)
            dist_master_query = dist_master_query.filter(District.division_name == current_user.division_access)
        if getattr(current_user, "district_access", None):
            grand_query = grand_query.filter(SummaryStats.district == current_user.district_access)
            dist_master_query = dist_master_query.filter(District.name == current_user.district_access)
        if getattr(current_user, "upazila_access", None):
            grand_query = grand_query.filter(SummaryStats.upazila == current_user.upazila_access)
            
    if division:
        grand_query = grand_query.filter(SummaryStats.division == division)

    div_counts = {row[0]: row[1] for row in div_master_query.all()}
    dist_counts = {row[0]: row[1] for row in dist_master_query.all()}

    grand = grand_query.first()
    grand_total = {"total": grand.total, "valid": grand.valid, "invalid": grand.invalid}

    data = {
        "entries": [
            {
                **e,
                "created_at": e["created_at"].isoformat() + "Z",
                "updated_at": e["updated_at"].isoformat() + "Z",
            }
            for e in entries
        ],
        "grand_total": grand_total,
        "master_counts": {"divisions": div_counts, "districts": dist_counts},
        "last_modified": latest_ts.isoformat() + "Z",
    }
    return JSONResponse(content=data, headers={"ETag": etag, "Cache-Control": "private, no-store"})


@router.delete("/{division}/{district}/{upazila}", dependencies=[Depends(PermissionChecker("manage_geo"))])
async def delete_upazila_data(
    division: str,
    district: str,
    upazila: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Admin-only: Completely wipe all data for a specific upazila."""
    db.query(ValidRecord).filter(ValidRecord.division == division, ValidRecord.district == district, ValidRecord.upazila == upazila).delete(synchronize_session=False)
    db.query(SummaryStats).filter(SummaryStats.division == division, SummaryStats.district == district, SummaryStats.upazila == upazila).delete(synchronize_session=False)
    db.query(InvalidRecord).filter(InvalidRecord.division == division, InvalidRecord.district == district, InvalidRecord.upazila == upazila).delete(synchronize_session=False)
    db.query(UploadBatch).filter(UploadBatch.division == division, UploadBatch.district == district, UploadBatch.upazila == upazila).delete(synchronize_session=False)
    db.commit()

    log_audit(
        db, 
        current_user, 
        "DELETE", 
        "upazila_full_wipe", 
        f"{division}/{district}/{upazila}", 
        old_data={
            "details": f"Fully wiped all records, stats, and batches for {upazila}",
            "location": f"{division} > {district} > {upazila}",
            "type": "full_wipe"
        }
    )
    return {"status": "success", "message": f"All data for {upazila} has been deleted."}


class ManualStatsUpdate(BaseModel):
    old_district: str
    old_upazila: str
    new_district: str
    new_upazila: str
    total: int
    valid: int
    invalid: int


@router.put("/update", dependencies=[Depends(PermissionChecker("manage_geo"))])
async def update_statistics(
    update: ManualStatsUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Manually update the statistics and location for a specific entry."""
    summary = db.query(SummaryStats).filter(SummaryStats.district == update.old_district, SummaryStats.upazila == update.old_upazila).first()
    if not summary:
        raise HTTPException(status_code=404, detail="Statistic entry not found")

    summary.district = update.new_district
    summary.upazila = update.new_upazila
    summary.division = get_division_for_district(update.new_district)
    summary.total = update.total
    summary.valid = update.valid
    summary.invalid = update.invalid
    summary.version += 1
    db.commit()
    db.refresh(summary)

    log_audit(db, current_user, "UPDATE", "summary_stats", summary.id, new_data={"action": "summary_update"})
    return summary


@router.post("/refresh-all", dependencies=[Depends(PermissionChecker("manage_geo"))])
async def refresh_all_stats(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Recalculate ALL SummaryStats from truth tables (valid_records + invalid_records)
    using a single set-based SQL query. Fixes any stale/ghost counts.

    This is the definitive fix for statistics desynchronization.
    """
    from sqlalchemy import text

    try:
        # Step 1: Recalculate valid/invalid counts from live records for existing SummaryStats entries
        recalculate_sql = text("""
            WITH counts AS (
                SELECT 
                    district, upazila,
                    SUM(valid_cnt) as live_valid,
                    SUM(invalid_cnt) as live_invalid
                FROM (
                    SELECT district, upazila, COUNT(*) as valid_cnt, 0 as invalid_cnt 
                    FROM valid_records GROUP BY district, upazila
                    UNION ALL
                    SELECT district, upazila, 0 as valid_cnt, COUNT(*) as invalid_cnt 
                    FROM invalid_records GROUP BY district, upazila
                ) combined
                GROUP BY district, upazila
            )
            UPDATE summary_stats s
            SET valid = COALESCE(c.live_valid, 0),
                invalid = COALESCE(c.live_invalid, 0),
                total = COALESCE(c.live_valid, 0) + COALESCE(c.live_invalid, 0),
                updated_at = NOW()
            FROM counts c
            WHERE LOWER(TRIM(s.district)) = LOWER(TRIM(c.district))
              AND LOWER(TRIM(s.upazila))  = LOWER(TRIM(c.upazila))
        """)
        result = db.execute(recalculate_sql)
        updated = result.rowcount

        # Step 2: Zero out SummaryStats entries that have NO matching records at all
        # (ghost entries where all records were deleted but SummaryStats wasn't cleaned)
        zero_ghosts_sql = text("""
            UPDATE summary_stats s
            SET valid = 0,
                invalid = 0,
                total = 0,
                updated_at = NOW()
            WHERE NOT EXISTS (
                SELECT 1 FROM valid_records v
                WHERE LOWER(TRIM(v.district)) = LOWER(TRIM(s.district))
                  AND LOWER(TRIM(v.upazila))  = LOWER(TRIM(s.upazila))
            )
            AND NOT EXISTS (
                SELECT 1 FROM invalid_records i
                WHERE LOWER(TRIM(i.district)) = LOWER(TRIM(s.district))
                  AND LOWER(TRIM(i.upazila))  = LOWER(TRIM(s.upazila))
            )
            AND (s.valid > 0 OR s.invalid > 0)
        """)
        ghost_result = db.execute(zero_ghosts_sql)
        zeroed = ghost_result.rowcount

        db.commit()

        log_audit(db, current_user, "MAINTENANCE", "summary_stats", 0, new_data={
            "action": "refresh_all_stats",
            "updated": updated,
            "ghost_entries_zeroed": zeroed
        })

        logger.info("refresh-all-stats: %d entries updated, %d ghost entries zeroed", updated, zeroed)
        return {
            "success": True,
            "message": f"Refreshed {updated} stats entries from truth tables. Zeroed {zeroed} ghost entries.",
            "updated": updated,
            "ghost_entries_zeroed": zeroed,
        }
    except Exception as e:
        db.rollback()
        logger.error("refresh-all-stats failed: %s", str(e))
        return {"success": False, "error": str(e)}
