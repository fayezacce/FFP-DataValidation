"""
FFP Data Validator — Geo & Misc Routes
Handles geo hierarchy, location guessing, and password change.
"""
from fastapi import APIRouter, HTTPException, Depends
from fastapi.requests import Request
from pydantic import BaseModel
from sqlalchemy.orm import Session
import logging

from .database import get_db
from .models import User
from .auth import get_current_user, verify_password, get_password_hash
from .rbac import PermissionChecker
from .audit import log_audit

logger = logging.getLogger("ffp")
router = APIRouter(tags=["geo"])


@router.get("/info", dependencies=[Depends(PermissionChecker("view_geo"))])
async def get_geo_info(db: Session = Depends(get_db)):
    """Return the hierarchy of divisions, districts, and upazilas."""
    from .bd_geo import _division_lookup, _district_lookup, get_dynamic_upazilas

    divisions = sorted(list(set(_division_lookup.values())))

    districts = {}
    for norm_name, record in _district_lookup.items():
        div_name = record["division_name"]
        dist_name = record["name"]
        if div_name not in districts:
            districts[div_name] = []
        if dist_name not in districts[div_name]:
            districts[div_name].append(dist_name)

    for div in districts:
        districts[div].sort()

    upazilas = get_dynamic_upazilas(db)

    return {"divisions": divisions, "districts": districts, "upazilas": upazilas}


@router.get("/guess")
async def guess_location(filename: str):
    """Guess the location from the filename."""
    from .bd_geo import fuzzy_match_location
    return fuzzy_match_location(filename)


class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str


@router.post("/change-password")
async def change_password(
    req: ChangePasswordRequest,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if not verify_password(req.old_password, current_user.hashed_password):
        raise HTTPException(status_code=400, detail="Incorrect old password")

    current_user.hashed_password = get_password_hash(req.new_password)
    db.commit()

    request.app.state.security_lockout = False
    log_audit(db, current_user, "UPDATE", "users", current_user.id, new_data={"action": "password_change"})
    return {"message": "Password updated successfully"}
