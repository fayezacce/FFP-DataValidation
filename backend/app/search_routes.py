"""
FFP Data Validator — Search Routes
Handles NID/Name/DOB search, NID lookup, and record deletion.
"""
from fastapi import APIRouter, HTTPException, Depends, Body
from sqlalchemy.orm import Session
from sqlalchemy import func
from pydantic import BaseModel, Field
from typing import Optional
import logging
from datetime import datetime

from .database import get_db
from .models import User, SummaryStats, ValidRecord, Division, District, Upazila, Dealer
from .auth import get_current_user
from .rbac import PermissionChecker
from .audit import log_audit
from .validator import validate_nid, clean_dob

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


# ── Manual Entry Schema ─────────────────────────────────────────────────────

class BeneficiaryCreate(BaseModel):
    name: str = Field(..., description="Beneficiary Name (Bangla)")
    name_en: str = Field(..., description="Beneficiary Name (English)")
    nid: str = Field(..., description="National ID Number")
    dob: str = Field(..., description="Date of Birth (YYYY-MM-DD)")
    father_husband_name: Optional[str] = None
    mobile: Optional[str] = None
    card_no: Optional[str] = None
    division: str = Field(...)
    district: str = Field(...)
    upazila: str = Field(...)
    union_name: Optional[str] = None
    ward: Optional[str] = None
    address: Optional[str] = None
    gender: Optional[str] = None
    occupation: Optional[str] = None
    religion: Optional[str] = None
    spouse_name: Optional[str] = None
    spouse_nid: Optional[str] = None
    spouse_dob: Optional[str] = None
    # ── Dealer Info ──
    dealer_id: Optional[int] = Field(None, description="Optional ID if selecting existing dealer")
    dealer_name: Optional[str] = Field(None, description="Name for new dealer registration")
    dealer_mobile: Optional[str] = Field(None, description="Mobile for new dealer registration")
    dealer_nid: Optional[str] = Field(None, description="NID for new dealer registration")

@router.post("/beneficiary", dependencies=[Depends(PermissionChecker("add_beneficiary"))])
async def add_beneficiary(
    data: BeneficiaryCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Manually add a single beneficiary with full validation and geo-access check."""
    
    # 1. Geo Access Authorization
    if current_user.role != "admin":
        if current_user.division_access and data.division != current_user.division_access:
            raise HTTPException(status_code=403, detail=f"Permission denied for division: {data.division}")
        if current_user.district_access and data.district != current_user.district_access:
            raise HTTPException(status_code=403, detail=f"Permission denied for district: {data.district}")
        if current_user.upazila_access and data.upazila != current_user.upazila_access:
            raise HTTPException(status_code=403, detail=f"Permission denied for upazila: {data.upazila}")

    # 2. Validation (Reuse existing logic)
    cleaned_dob, dob_year = clean_dob(data.dob)
    if not cleaned_dob:
        raise HTTPException(status_code=400, detail="Invalid Date of Birth format. Use YYYY-MM-DD.")
        
    final_nid, status, message = validate_nid(data.nid, dob_year)
    if status == "error":
        raise HTTPException(status_code=400, detail=f"NID Validation Failed: {message}")

    # 2.1 Multi-field Hardening
    if data.mobile and not data.mobile.isdigit():
        raise HTTPException(status_code=400, detail="Mobile number must contain digits only")
    if data.mobile and len(data.mobile) != 11:
         raise HTTPException(status_code=400, detail="Mobile number must be exactly 11 digits")
    
    if data.gender and data.gender not in ['Male', 'Female', 'Other', 'পুরুষ', 'মহিলা', 'অন্যান্য']:
        raise HTTPException(status_code=400, detail="Invalid Gender selection")

    # 3. Duplicate Check
    existing = db.query(ValidRecord).filter(ValidRecord.nid == final_nid).first()
    if existing:
        raise HTTPException(status_code=409, detail=f"Beneficiary with NID {final_nid} already exists ({existing.district} -> {existing.upazila})")

    # 4. Resolve Geo IDs
    upz = db.query(Upazila).filter(
        func.lower(Upazila.district_name) == data.district.lower().strip(),
        func.lower(Upazila.name) == data.upazila.lower().strip()
    ).first()
    
    if not upz:
        raise HTTPException(status_code=404, detail=f"Upazila '{data.upazila}' not found in district '{data.district}'")

    div = db.query(Division).filter(func.lower(Division.name) == data.division.lower().strip()).first()

    # 4.1 Dealer Resolution (Mandatory)
    final_dealer_id = None
    
    # Priority A: Existing Dealer ID
    if data.dealer_id:
        dealer = db.query(Dealer).filter(Dealer.id == data.dealer_id).first()
        if not dealer:
            raise HTTPException(status_code=404, detail="Selected dealer not found")
        # Safety check: Dealer must be in the same upazila
        if dealer.upazila_id != upz.id:
            raise HTTPException(status_code=400, detail="The selected dealer does not belong to this Upazila")
        final_dealer_id = dealer.id
    
    # Priority B: Manual Entry (Resolve or Create)
    elif data.dealer_nid and data.dealer_name:
        # Check if dealer already exists in this Upazila by NID
        existing_dealer = db.query(Dealer).filter(
            Dealer.nid == data.dealer_nid.strip(),
            Dealer.upazila_id == upz.id
        ).first()
        
        if existing_dealer:
            final_dealer_id = existing_dealer.id
        else:
            # Create a permanent record in the central 'Dealer Registry' for this Upazila
            new_dealer = Dealer(
                nid=data.dealer_nid.strip(),
                name=data.dealer_name.strip(),
                mobile=data.dealer_mobile.strip() if data.dealer_mobile else None,
                division=data.division,
                district=data.district,
                upazila=data.upazila,
                division_id=div.id if div else None,
                district_id=dist.id if dist else None,
                upazila_id=upz.id,
                is_active=True
            )
            db.add(new_dealer)
            db.flush() # Get the new ID
            final_dealer_id = new_dealer.id
            logger.info(f"New Dealer registered: {data.dealer_name} ({data.dealer_nid}) in {data.upazila}")
    
    # C. Failure Case
    if not final_dealer_id:
        raise HTTPException(
            status_code=400, 
            detail="Dealer information is mandatory. Select an existing dealer or provide new dealer details (Name, NID, Mobile)."
        )

    # 5. Create Record
    full_data = data.dict()
    new_record = ValidRecord(
        nid=final_nid,
        dob=cleaned_dob,
        name=data.name,      # Primary identifier
        name_bn=data.name,   # Aligned for new architecture
        name_en=data.name_en, 
        division=data.division,
        district=data.district,
        upazila=data.upazila,
        division_id=div.id if div else None,
        district_id=dist.id if dist else None,
        upazila_id=upz.id,
        card_no=data.card_no,
        mobile=data.mobile,
        union_name=data.union_name,
        ward=data.ward,
        address=data.address,
        father_husband_name=data.father_husband_name,
        occupation=data.occupation,
        gender=data.gender,
        religion=data.religion,
        spouse_name=data.spouse_name,
        spouse_nid=data.spouse_nid,
        spouse_dob=data.spouse_dob,
        dealer_id=final_dealer_id,
        data=full_data, # Store the full input as original data
        source_file="Manual Entry",
        batch_id=0,     # Manual entry batch
        upload_batch=0
    )

    db.add(new_record)
    
    # 6. Update SummaryStats
    stats = db.query(SummaryStats).filter(
        SummaryStats.district == data.district,
        SummaryStats.upazila == data.upazila
    ).first()
    
    if not stats:
        stats = SummaryStats(
            division=data.division, district=data.district, upazila=data.upazila,
            division_id=div.id if div else None, district_id=dist.id if dist else None, upazila_id=upz.id,
            total=1, valid=1
        )
        db.add(stats)
    else:
        stats.total += 1
        stats.valid += 1

    db.commit()
    db.refresh(new_record)

    log_audit(db, current_user, "CREATE", "valid_records", new_record.id, details={"nid": final_nid})

    return {
        "success": True,
        "message": "Beneficiary added successfully",
        "id": new_record.id,
        "nid": final_nid
    }
